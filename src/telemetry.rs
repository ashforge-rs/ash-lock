//! Metrics for `ash-lock`.
//!
//! MOCK: the spec calls for exporting these counters through an `ash-telemetry`
//! crate, which does not yet exist. This module provides the same
//! `AtomicU64`-backed counters with a stable API so that, once `ash-telemetry`
//! lands, the export sink can be swapped in without touching call sites.
//!
//! The counter names mirror the spec:
//! - `ash_lock_acquisitions_total`
//! - `ash_lock_contention_events_total`
//! - `ash_lock_renewal_failures_total`

use std::sync::atomic::{AtomicU64, Ordering};

/// Number of successful lock acquisitions.
pub static ACQUISITIONS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Number of times an acquire had to block/retry due to contention.
pub static CONTENTION_EVENTS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Number of watchdog renewal failures.
pub static RENEWAL_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);

#[inline]
pub(crate) fn inc_acquisitions() {
    ACQUISITIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub(crate) fn inc_contention() {
    CONTENTION_EVENTS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub(crate) fn inc_renewal_failures() {
    RENEWAL_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// A point-in-time snapshot of every exported counter. Useful in tests and for
/// the eventual `ash-telemetry` scrape endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Snapshot {
    /// `ash_lock_acquisitions_total`
    pub acquisitions_total: u64,
    /// `ash_lock_contention_events_total`
    pub contention_events_total: u64,
    /// `ash_lock_renewal_failures_total`
    pub renewal_failures_total: u64,
}

/// Read all counters atomically enough for observability purposes.
pub fn snapshot() -> Snapshot {
    Snapshot {
        acquisitions_total: ACQUISITIONS_TOTAL.load(Ordering::Relaxed),
        contention_events_total: CONTENTION_EVENTS_TOTAL.load(Ordering::Relaxed),
        renewal_failures_total: RENEWAL_FAILURES_TOTAL.load(Ordering::Relaxed),
    }
}
