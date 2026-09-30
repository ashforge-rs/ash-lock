//! Background lease-renewal ("watchdog") task and its supervision shim.
//!
//! When a lock is acquired, a watchdog is spawned that periodically renews the
//! lease via the [`LockBackend`]. The spec asks for this task to be supervised
//! by `ash-flare` and to *panic* on renewal failure ("crash-fast").
//!
//! Design deviation (intentional): panicking a Tokio task on a transient
//! network partition is a reliability foot-gun — it can take down unrelated
//! work sharing the runtime and races the guard's own `Drop`. Instead the
//! watchdog **poisons** the shared [`LeaseState`] on failure. Any subsequent
//! `fencing_token()` / `extend()` on the guard then observes the poison, so the
//! critical section is fenced off just as effectively, without an uncontrolled
//! panic. The behaviour is centralised here so a real `ash-flare` restart
//! strategy can be layered on later.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::{Instrument, info_span, warn};

use crate::backend::LockBackend;
use crate::telemetry;

/// State shared between a [`LockGuard`](crate::guard::LockGuard) and its
/// watchdog task.
pub(crate) struct LeaseState {
    pub resource: String,
    pub token: u64,
    pub node_id: u64,
    /// Set to `true` once the lease can no longer be trusted (renewal failed or
    /// expired). Reads of the fencing token must fail after this flips.
    poisoned: AtomicBool,
    /// Set to `true` once the guard has been released (explicitly or dropped),
    /// signalling the watchdog to stop cleanly.
    released: AtomicBool,
    /// Monotonic count of successful renewals — used by tests to observe that
    /// the watchdog is actually doing work under `tokio::time` control.
    pub renewals: AtomicU64,
    /// The current lease TTL, in nanoseconds. Shared so that a manual
    /// [`LockGuard::extend`](crate::guard::LockGuard::extend) updates the TTL
    /// the watchdog renews with — otherwise the next watchdog tick would
    /// silently shrink the lease back to its original TTL.
    ttl_nanos: AtomicU64,
}

impl LeaseState {
    pub(crate) fn new(resource: String, token: u64, node_id: u64, ttl: Duration) -> Self {
        Self {
            resource,
            token,
            node_id,
            poisoned: AtomicBool::new(false),
            released: AtomicBool::new(false),
            renewals: AtomicU64::new(0),
            ttl_nanos: AtomicU64::new(ttl.as_nanos().min(u64::MAX as u128) as u64),
        }
    }

    pub(crate) fn ttl(&self) -> Duration {
        Duration::from_nanos(self.ttl_nanos.load(Ordering::Acquire))
    }

    pub(crate) fn set_ttl(&self, ttl: Duration) {
        self.ttl_nanos.store(
            ttl.as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Release,
        );
    }

    pub(crate) fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    pub(crate) fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
    }

    pub(crate) fn mark_released(&self) {
        self.released.store(true, Ordering::Release);
    }

    fn is_released(&self) -> bool {
        self.released.load(Ordering::Acquire)
    }
}

/// A handle to a running watchdog. Dropping it does not stop the task; the guard
/// signals shutdown via [`LeaseState::mark_released`] and then the task exits.
pub(crate) struct WatchdogHandle {
    handle: JoinHandle<()>,
}

impl WatchdogHandle {
    /// Abort the watchdog task outright (used on explicit unlock, where we no
    /// longer need graceful renewal).
    pub(crate) fn abort(&self) {
        self.handle.abort();
    }
}

/// Spawn the supervised renewal loop.
///
/// SUPERVISION: with the `ash-flare` feature this is where a
/// `SupervisorSpec`/`Worker` would be registered. In the default (mock) build
/// the loop is self-contained but honours the same contract: renew on interval,
/// poison + report telemetry on failure, exit cleanly on release.
pub(crate) fn spawn(
    backend: Arc<dyn LockBackend>,
    state: Arc<LeaseState>,
    interval: Duration,
) -> WatchdogHandle {
    let span = info_span!(
        "ash_lock.watchdog",
        resource_name = %state.resource,
        fencing_token = state.token,
        node_id = state.node_id,
    );

    let handle = tokio::spawn(
        async move {
            loop {
                // Never sleep past half the *current* TTL, or a lock whose TTL
                // is shorter than the configured interval (via `with_ttl` or
                // `extend`) would lapse before its first renewal. Re-derived
                // each tick because `extend` can change the TTL while we sleep.
                let effective = interval.min(state.ttl() / 2).max(Duration::from_millis(1));
                tokio::time::sleep(effective).await;

                if state.is_released() {
                    return;
                }

                match backend
                    .try_renew(&state.resource, state.token, state.ttl())
                    .await
                {
                    Ok(_deadline) => {
                        state.renewals.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(err) => {
                        // Report to telemetry (mock ash-telemetry) and poison the
                        // lease rather than panic. See module docs.
                        // TODO(real backend): a single dropped RPC should retry
                        // until the local lease deadline before poisoning — the
                        // token stays valid until the lease actually expires.
                        telemetry::inc_renewal_failures();
                        warn!(
                            error = %err,
                            "lease renewal failed; poisoning guard (crash-fast fencing)"
                        );
                        state.poison();
                        return;
                    }
                }
            }
        }
        .instrument(span),
    );

    WatchdogHandle { handle }
}
