//! Causality clock used to stamp lock operations.
//!
//! ADOPTS `ash-time`: when the `ash-time` feature is enabled, every acquire /
//! renew / release is stamped with a Hybrid Logical Clock timestamp from
//! `ash_time::HlcClock`. HLC timestamps are totally ordered and causally
//! consistent, which makes them a natural fit for a coordination service: the
//! stamp on a [`LockGuard`](crate::guard::LockGuard) lets callers reason about
//! the causal order of lock events across nodes (e.g. for audit logs or
//! debugging split-brain), independently of the fencing token.
//!
//! Without the feature, a monotonic in-process counter provides the same
//! *interface* (`Stamp` is still totally ordered locally) so downstream code
//! never needs `cfg` guards. The counter's `physical` field is left at 0 to
//! make it obvious the value is not wall-clock-derived.

#[cfg(not(feature = "ash-time"))]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "ash-time")]
use crate::error::Error;
use crate::error::Result;

/// A causally-ordered stamp for a lock operation.
///
/// This mirrors `ash_time::HlcTimestamp` (a `(physical, logical)` pair) so the
/// public shape is identical whether or not the `ash-time` feature is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Stamp {
    /// Wall-clock component (nanoseconds since the Unix epoch) when backed by
    /// `ash-time`; `0` in the fallback counter mode.
    pub physical: u64,
    /// Logical tie-breaker / fallback monotonic counter.
    pub logical: u64,
}

#[cfg(feature = "ash-time")]
impl From<ash_time::HlcTimestamp> for Stamp {
    fn from(ts: ash_time::HlcTimestamp) -> Self {
        Stamp {
            physical: ts.physical,
            logical: ts.logical as u64,
        }
    }
}

/// A source of causally-ordered [`Stamp`]s, shared across a client's operations.
pub struct Clock {
    #[cfg(feature = "ash-time")]
    hlc: ash_time::HlcClock,
    #[cfg(not(feature = "ash-time"))]
    counter: AtomicU64,
}

impl Clock {
    /// Create a fresh clock.
    pub fn new() -> Self {
        #[cfg(feature = "ash-time")]
        {
            Self {
                hlc: ash_time::HlcClock::new(),
            }
        }
        #[cfg(not(feature = "ash-time"))]
        {
            Self {
                counter: AtomicU64::new(0),
            }
        }
    }

    /// Produce a stamp for a new local event (a lock operation).
    ///
    /// The result is strictly greater than every stamp previously produced by
    /// this clock, in both backends.
    pub fn stamp(&self) -> Result<Stamp> {
        #[cfg(feature = "ash-time")]
        {
            self.hlc
                .now()
                .map(Stamp::from)
                .map_err(|e| Error::cluster(format!("hlc clock error: {e}")))
        }
        #[cfg(not(feature = "ash-time"))]
        {
            let logical = self.counter.fetch_add(1, Ordering::Relaxed) + 1;
            Ok(Stamp {
                physical: 0,
                logical,
            })
        }
    }
}

impl Default for Clock {
    fn default() -> Self {
        Self::new()
    }
}
