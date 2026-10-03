//! The [`LockBackend`] abstraction and its implementations.
//!
//! A backend owns the authoritative lock state for a set of resource keys. The
//! client / guard / watchdog layers are backend-agnostic and drive everything
//! through this trait.
//!
//! Implementations provided:
//! - [`InMemoryBackend`] — a fully working single-process backend (the
//!   **default**), and the target of the deterministic simulation test.
//! - `etcd::EtcdBackend` — real distributed locking over an etcd v3 cluster
//!   (leases + transactions, revision-based fencing tokens). Requires the
//!   `etcd` feature.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use dashmap::DashMap;

use crate::error::{Error, Result};

#[cfg(feature = "etcd")]
pub mod etcd;

/// Authoritative storage and arbitration for distributed locks.
///
/// The returned [`Instant`] on `try_acquire` / `try_renew` is the **local**
/// lease deadline (when this node should consider the lease expired absent a
/// renewal). It is intentionally not serialized across the wire — remote
/// backends translate a TTL into a local deadline on receipt.
#[async_trait]
pub trait LockBackend: Send + Sync {
    /// Attempt to acquire `key` for `ttl`. On success returns the fencing token
    /// (strictly monotonic per key) and the local lease deadline.
    async fn try_acquire(&self, key: &str, ttl: Duration) -> Result<(u64, Instant)>;

    /// Renew a lease previously acquired with `token`, extending it by `ttl`.
    /// Returns the new local lease deadline.
    async fn try_renew(&self, key: &str, token: u64, ttl: Duration) -> Result<Instant>;

    /// Release a lease. A no-op if `token` is already stale (someone else owns
    /// the key), so releasing an expired guard is harmless.
    async fn release(&self, key: &str, token: u64) -> Result<()>;

    /// Human-readable backend name, used in tracing spans and errors.
    fn name(&self) -> &'static str {
        "unknown"
    }
}

// ---------------------------------------------------------------------------
// InMemoryBackend
// ---------------------------------------------------------------------------

struct Entry {
    /// Currently valid fencing token for this key (monotonic).
    token: u64,
    /// Whether the key is presently held.
    held: bool,
    /// Local deadline after which the lease is considered expired.
    deadline: Instant,
}

/// A single-process, in-memory [`LockBackend`].
///
/// Fully functional: fencing tokens are strictly monotonic per key, and an
/// expired lease is reclaimable by the next acquirer. This is the backend the
/// deterministic simulation test exercises.
#[derive(Default)]
pub struct InMemoryBackend {
    entries: DashMap<String, Entry>,
}

impl InMemoryBackend {
    /// Create an empty backend.
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    /// Number of tracked keys (held or historically seen). Test helper.
    pub fn tracked_keys(&self) -> usize {
        self.entries.len()
    }
}

#[async_trait]
impl LockBackend for InMemoryBackend {
    async fn try_acquire(&self, key: &str, ttl: Duration) -> Result<(u64, Instant)> {
        let now = Instant::now();
        let deadline = now + ttl;

        // DashMap entry API gives us per-key atomicity for the read-modify-write.
        let mut e = self.entries.entry(key.to_string()).or_insert(Entry {
            token: 0,
            held: false,
            deadline: now,
        });

        let expired = now >= e.deadline;
        if e.held && !expired {
            return Err(Error::Contended {
                resource: key.to_string(),
            });
        }

        // Free or expired: hand out a fresh, strictly larger token.
        e.token = e
            .token
            .checked_add(1)
            .expect("fencing token overflow (u64)");
        e.held = true;
        e.deadline = deadline;
        Ok((e.token, deadline))
    }

    async fn try_renew(&self, key: &str, token: u64, ttl: Duration) -> Result<Instant> {
        let now = Instant::now();
        let mut e = self.entries.get_mut(key).ok_or_else(|| Error::StaleToken {
            resource: key.to_string(),
            token,
            current: 0,
        })?;

        if !e.held || now >= e.deadline {
            return Err(Error::LeaseExpired {
                resource: key.to_string(),
            });
        }
        if e.token != token {
            return Err(Error::StaleToken {
                resource: key.to_string(),
                token,
                current: e.token,
            });
        }

        let deadline = now + ttl;
        e.deadline = deadline;
        Ok(deadline)
    }

    async fn release(&self, key: &str, token: u64) -> Result<()> {
        if let Some(mut e) = self.entries.get_mut(key) {
            // Only the current holder may release; a stale release is a no-op so
            // that dropping an already-expired guard is safe.
            if e.held && e.token == token {
                e.held = false;
            }
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "in-memory"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn acquire_is_exclusive_and_monotonic() {
        let b = InMemoryBackend::new();
        let (t1, _) = b.try_acquire("k", Duration::from_secs(10)).await.unwrap();
        assert_eq!(t1, 1);
        // Second acquire while held must fail.
        assert!(matches!(
            b.try_acquire("k", Duration::from_secs(10)).await,
            Err(Error::Contended { .. })
        ));
        b.release("k", t1).await.unwrap();
        let (t2, _) = b.try_acquire("k", Duration::from_secs(10)).await.unwrap();
        assert!(t2 > t1, "tokens must strictly increase across acquisitions");
    }

    #[tokio::test]
    async fn stale_renew_is_rejected() {
        let b = InMemoryBackend::new();
        let (t, _) = b.try_acquire("k", Duration::from_secs(10)).await.unwrap();
        assert!(
            b.try_renew("k", t + 999, Duration::from_secs(10))
                .await
                .is_err()
        );
        assert!(b.try_renew("k", t, Duration::from_secs(10)).await.is_ok());
    }
}
