//! The RAII [`LockGuard`] returned by a successful acquire.

use std::sync::Arc;
use std::time::Duration;

use tracing::{Instrument, info_span, warn};

use crate::backend::LockBackend;
use crate::clock::Stamp;
use crate::error::{Error, Result};
use crate::watchdog::{LeaseState, WatchdogHandle};

/// Validate that a write carrying `token` is still allowed against a resource
/// whose last-seen fencing token is `current_token`.
///
/// This is the canonical guard external systems should place in front of any
/// side-effecting write protected by an `ash-lock` lock. A write is only safe
/// if its token is **at least** the highest token the resource has observed;
/// any smaller token belongs to a lease that has since been superseded (e.g. a
/// paused process whose lock expired and was re-acquired elsewhere).
///
/// ```
/// use ash_lock::validate_write;
/// assert!(validate_write(7, 7));   // current holder
/// assert!(validate_write(8, 7));   // newer holder writing first
/// assert!(!validate_write(6, 7));  // stale holder — reject!
/// ```
#[must_use]
pub fn validate_write(token: u64, current_token: u64) -> bool {
    token >= current_token
}

/// A held distributed lock.
///
/// The lock is released when the guard is dropped (background, best-effort) or
/// via [`LockGuard::unlock`] (awaited). While held, a supervised watchdog renews
/// the lease automatically; if renewal fails the guard becomes **poisoned** and
/// [`LockGuard::fencing_token`] / [`LockGuard::extend`] start returning errors.
#[must_use = "If dropped, the lock will be released immediately"]
pub struct LockGuard {
    backend: Arc<dyn LockBackend>,
    state: Arc<LeaseState>,
    watchdog: WatchdogHandle,
    /// The causal stamp at acquisition time (from `ash-time` when enabled).
    acquired_at: Stamp,
    /// Set to false once `unlock()` has consumed the guard, so `Drop` skips the
    /// background release.
    active: bool,
}

impl LockGuard {
    /// Construct a guard and start its watchdog. Internal: called by the builder.
    pub(crate) fn new(
        backend: Arc<dyn LockBackend>,
        state: Arc<LeaseState>,
        watchdog: WatchdogHandle,
        acquired_at: Stamp,
    ) -> Self {
        Self {
            backend,
            state,
            watchdog,
            acquired_at,
            active: true,
        }
    }

    /// The strictly-monotonic fencing token for this lock.
    ///
    /// # Errors
    /// Returns [`Error::LeaseExpired`] if the guard has been poisoned by a
    /// failed renewal — the token must not be used to authorise writes.
    pub fn fencing_token(&self) -> Result<u64> {
        if self.state.is_poisoned() {
            return Err(Error::LeaseExpired {
                resource: self.state.resource.clone(),
            });
        }
        Ok(self.state.token)
    }

    /// The resource key this guard locks.
    pub fn resource(&self) -> &str {
        &self.state.resource
    }

    /// The causal stamp recorded when the lock was acquired.
    ///
    /// Backed by the `ash-time` Hybrid Logical Clock when that feature is
    /// enabled; otherwise a local monotonic counter. Useful for ordering lock
    /// events in logs/audit trails across nodes.
    pub fn acquired_at(&self) -> Stamp {
        self.acquired_at
    }

    /// Manually extend the lease by `new_ttl` from now. On success the
    /// watchdog adopts `new_ttl` for all subsequent renewals, so the extension
    /// is not undone at the next renewal tick.
    ///
    /// # Errors
    /// Fails if the guard is poisoned, or if the backend rejects the renewal
    /// (stale token / expired lease).
    pub async fn extend(&self, new_ttl: Duration) -> Result<()> {
        if self.state.is_poisoned() {
            return Err(Error::LeaseExpired {
                resource: self.state.resource.clone(),
            });
        }
        let span = info_span!(
            "ash_lock.extend",
            resource_name = %self.state.resource,
            fencing_token = self.state.token,
            node_id = self.state.node_id,
        );
        async {
            self.backend
                .try_renew(&self.state.resource, self.state.token, new_ttl)
                .await?;
            self.state.set_ttl(new_ttl);
            Ok(())
        }
        .instrument(span)
        .await
    }

    /// Successful watchdog renewals so far. Test observability helper.
    #[cfg(test)]
    pub(crate) fn state_renewals(&self) -> u64 {
        self.state
            .renewals
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Release the lock, awaiting confirmation.
    pub async fn unlock(mut self) -> Result<()> {
        self.active = false;
        self.state.mark_released();
        self.watchdog.abort();
        let span = info_span!(
            "ash_lock.release",
            resource_name = %self.state.resource,
            fencing_token = self.state.token,
            node_id = self.state.node_id,
        );
        async {
            self.backend
                .release(&self.state.resource, self.state.token)
                .await
        }
        .instrument(span)
        .await
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if !self.active {
            return; // already released via unlock()
        }
        // Signal the watchdog to stop and fire a best-effort background release.
        self.state.mark_released();
        self.watchdog.abort();

        let backend = Arc::clone(&self.backend);
        let resource = self.state.resource.clone();
        let token = self.state.token;
        let node_id = self.state.node_id;

        // The spec's Drop "triggers a background release, supervised by Flare".
        // We spawn the release so Drop stays non-blocking. If no runtime is
        // active (e.g. guard dropped after the runtime shut down), fall back to
        // logging — the lease will lapse via TTL regardless.
        let span = info_span!(
            "ash_lock.release.drop",
            resource_name = %resource,
            fencing_token = token,
            node_id = node_id,
        );
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(
                    async move {
                        let _ = backend.release(&resource, token).await;
                    }
                    .instrument(span),
                );
            }
            Err(_) => {
                warn!(
                    parent: &span,
                    "no tokio runtime on drop; relying on TTL {:?} to reclaim lock",
                    self.state.ttl()
                );
            }
        }
    }
}
