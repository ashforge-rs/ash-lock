//! The fluent [`LockBuilder`] returned by [`LockClient::lock`](crate::LockClient::lock).

use std::sync::Arc;
use std::time::Duration;

use tokio::time::{Instant as TokioInstant, sleep};
use tracing::{Instrument, info_span};

use crate::backend::LockBackend;
use crate::error::{Error, Result};
use crate::guard::LockGuard;
use crate::watchdog::{self, LeaseState};
use crate::{ClientInner, telemetry};

/// Fluent configuration for a single acquire attempt.
///
/// Created by [`LockClient::lock`](crate::LockClient::lock). Configure it, then
/// call [`acquire`](LockBuilder::acquire).
pub struct LockBuilder<'a> {
    pub(crate) inner: &'a ClientInner,
    pub(crate) resource: String,
    pub(crate) ttl: Duration,
    pub(crate) fair: bool,
    /// How long to keep retrying a contended lock before giving up. Derived
    /// from the TTL; kept here so it is overridable in future.
    pub(crate) wait_budget: Duration,
}

impl<'a> LockBuilder<'a> {
    /// Override the lease TTL for this lock.
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Enable FIFO-fair queueing.
    ///
    /// MOCK: the in-memory / consensus(mock) backends do not yet implement a
    /// server-side wait queue, so `fair` currently only influences the client
    /// retry pacing (fixed interval instead of jittered backoff) to approximate
    /// arrival-order fairness. Recorded on the builder for the real backend.
    pub fn with_fairness(mut self, fair: bool) -> Self {
        self.fair = fair;
        self
    }

    /// Attempt to acquire the lock, retrying on contention until the wait
    /// budget is exhausted.
    pub async fn acquire(self) -> Result<LockGuard> {
        let backend = Arc::clone(&self.inner.backend);
        let node_id = self.inner.node_id;
        let span = info_span!(
            "ash_lock.acquire",
            resource_name = %self.resource,
            node_id = node_id,
            fair = self.fair,
        );
        self.acquire_inner(backend, node_id).instrument(span).await
    }

    async fn acquire_inner(self, backend: Arc<dyn LockBackend>, node_id: u64) -> Result<LockGuard> {
        if self.ttl.is_zero() {
            return Err(Error::Config("lock ttl must be non-zero".into()));
        }
        let deadline = TokioInstant::now() + self.wait_budget;
        let mut attempt: u32 = 0;

        loop {
            match backend.try_acquire(&self.resource, self.ttl).await {
                Ok((token, _lease_deadline)) => {
                    telemetry::inc_acquisitions();
                    let stamp = self.inner.clock.stamp()?;

                    let state = Arc::new(LeaseState::new(
                        self.resource.clone(),
                        token,
                        node_id,
                        self.ttl,
                    ));
                    let handle = watchdog::spawn(
                        Arc::clone(&backend),
                        Arc::clone(&state),
                        self.inner.watchdog_interval,
                    );
                    return Ok(LockGuard::new(backend, state, handle, stamp));
                }
                Err(Error::Contended { .. }) => {
                    // First contention on this call counts as a contention event.
                    if attempt == 0 {
                        telemetry::inc_contention();
                    }
                    attempt += 1;

                    if TokioInstant::now() >= deadline {
                        return Err(Error::Timeout {
                            resource: self.resource.clone(),
                            waited: self.wait_budget,
                        });
                    }
                    sleep(self.retry_delay(attempt)).await;
                }
                // Any non-contention error is terminal.
                Err(e) => return Err(e),
            }
        }
    }

    /// Retry pacing. Fair mode uses a steady interval to preserve arrival order;
    /// unfair mode uses capped exponential backoff with simple deterministic
    /// jitter (derived from node_id + attempt, so tests stay reproducible).
    fn retry_delay(&self, attempt: u32) -> Duration {
        let base = self.inner.watchdog_interval.min(Duration::from_millis(50));
        if self.fair {
            return base;
        }
        let backoff = base.saturating_mul(1u32 << attempt.min(5));
        // Deterministic pseudo-jitter in [0, base): avoids thundering herd
        // without pulling in an RNG dependency for the mock.
        let jitter_ns = self
            .inner
            .node_id
            .wrapping_mul(2654435761)
            .wrapping_add(attempt as u64)
            % (base.as_nanos() as u64).max(1);
        backoff.saturating_add(Duration::from_nanos(jitter_ns))
    }
}
