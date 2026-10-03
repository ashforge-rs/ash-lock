//! etcd-backed [`LockBackend`], enabled with the `etcd` feature.
//!
//! This is a *real* distributed backend (no mock): lock state lives in an etcd
//! cluster and survives client restarts.
//!
//! ## Mapping onto etcd primitives
//!
//! - **Acquire** — grant a lease for the TTL, then a transaction that puts the
//!   lock key *only if* the key does not exist (`create_revision == 0`), bound
//!   to that lease. If the txn fails the lock is held by someone else.
//! - **Fencing token** — the cluster revision at which our put committed
//!   (`mod_revision` of the key). etcd revisions are strictly monotonic across
//!   the whole cluster and durable across restarts, which makes them stronger
//!   than a per-key counter: any later acquisition of any key observes a larger
//!   revision.
//! - **Renew** — a single `lease_keep_alive` round-trip. etcd renews a lease to
//!   its *originally granted* TTL; when a renewal asks for a different TTL
//!   (via [`LockGuard::extend`](crate::guard::LockGuard::extend)), the lock key
//!   is atomically re-bound to a freshly granted lease instead.
//! - **Release** — revoke the lease, which deletes the attached lock key.
//!
//! etcd lease TTLs have **whole-second granularity**; sub-second TTLs are
//! rounded up, and the server may raise very small TTLs to its own minimum.
//! The deadlines returned to the watchdog use the server's granted TTL, so
//! renewal pacing follows what the cluster actually promised.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use dashmap::DashMap;
use etcd_client::{Client, Compare, CompareOp, ConnectOptions, PutOptions, Txn, TxnOp};

use super::LockBackend;
use crate::error::{Error, Result};

/// Namespace prefix for all lock keys this backend writes into etcd.
const KEY_PREFIX: &str = "ash-lock/";

/// One live acquisition owned by this backend instance.
struct Lease {
    /// The etcd lease id the lock key is bound to.
    lease_id: i64,
    /// The TTL (whole seconds) the server granted for that lease.
    granted_ttl: i64,
}

/// A [`LockBackend`] over an etcd v3 cluster.
///
/// Cheap to share behind an `Arc`; every operation clones the underlying
/// channel-based client.
pub struct EtcdBackend {
    client: Client,
    /// Our own acquisitions: `(key, fencing token) -> lease`. Only entries in
    /// this map are ever renewed or revoked, so a stale caller can never touch
    /// somebody else's lease.
    leases: DashMap<(String, u64), Lease>,
}

impl EtcdBackend {
    /// Connect to the etcd cluster at `endpoints` (e.g. `["localhost:2379"]`).
    pub async fn connect(
        endpoints: impl IntoIterator<Item = impl AsRef<str>>,
        options: Option<ConnectOptions>,
    ) -> Result<Self> {
        let endpoints: Vec<String> = endpoints
            .into_iter()
            .map(|e| e.as_ref().to_string())
            .collect();
        if endpoints.is_empty() {
            return Err(Error::Config("etcd endpoints must be non-empty".into()));
        }
        let client = Client::connect(endpoints, options)
            .await
            .map_err(|e| Error::cluster(format!("etcd connect: {e}")))?;
        Ok(Self {
            client,
            leases: DashMap::new(),
        })
    }

    /// Build a backend from an already-connected [`etcd_client::Client`].
    pub fn from_client(client: Client) -> Self {
        Self {
            client,
            leases: DashMap::new(),
        }
    }

    fn lock_key(key: &str) -> String {
        format!("{KEY_PREFIX}{key}")
    }

    /// etcd lease TTLs are whole seconds; round up so a lease never expires
    /// before the caller's requested TTL.
    fn ttl_secs(ttl: Duration) -> i64 {
        let mut secs = ttl.as_secs();
        if ttl.subsec_nanos() > 0 {
            secs += 1;
        }
        secs.max(1).min(i64::MAX as u64) as i64
    }

    /// Grant a fresh lease, returning `(lease_id, granted_ttl_secs)`.
    async fn grant(&self, ttl: Duration) -> Result<(i64, i64)> {
        let mut client = self.client.clone();
        let resp = client
            .lease_grant(Self::ttl_secs(ttl), None)
            .await
            .map_err(|e| Error::cluster(format!("etcd lease_grant: {e}")))?;
        Ok((resp.id(), resp.ttl()))
    }

    /// Best-effort revoke; failure only means the lease will lapse by TTL.
    async fn revoke(&self, lease_id: i64) {
        let mut client = self.client.clone();
        let _ = client.lease_revoke(lease_id).await;
    }
}

#[async_trait]
impl LockBackend for EtcdBackend {
    async fn try_acquire(&self, key: &str, ttl: Duration) -> Result<(u64, Instant)> {
        let now = Instant::now();
        let (lease_id, granted_ttl) = self.grant(ttl).await?;
        let lock_key = Self::lock_key(key);

        // Put the lock key only if nobody holds it (key absent).
        let txn = Txn::new()
            .when([Compare::create_revision(
                lock_key.clone(),
                CompareOp::Equal,
                0,
            )])
            .and_then([TxnOp::put(
                lock_key,
                lease_id.to_string(),
                Some(PutOptions::new().with_lease(lease_id)),
            )]);
        let mut client = self.client.clone();
        let resp = client
            .txn(txn)
            .await
            .map_err(|e| Error::cluster(format!("etcd txn(acquire): {e}")))?;

        if !resp.succeeded() {
            self.revoke(lease_id).await;
            return Err(Error::Contended {
                resource: key.to_string(),
            });
        }

        // The commit revision is our fencing token: strictly monotonic across
        // the cluster and durable across restarts.
        let token = resp
            .header()
            .map(|h| h.revision())
            .filter(|r| *r > 0)
            .ok_or_else(|| Error::cluster("etcd txn response missing header revision"))?
            as u64;

        self.leases.insert(
            (key.to_string(), token),
            Lease {
                lease_id,
                granted_ttl,
            },
        );
        Ok((token, now + Duration::from_secs(granted_ttl as u64)))
    }

    async fn try_renew(&self, key: &str, token: u64, ttl: Duration) -> Result<Instant> {
        let now = Instant::now();
        let map_key = (key.to_string(), token);
        let (lease_id, granted_ttl) = {
            let lease = self.leases.get(&map_key).ok_or_else(|| Error::StaleToken {
                resource: key.to_string(),
                token,
                current: 0,
            })?;
            (lease.lease_id, lease.granted_ttl)
        };

        // Common path: same TTL — a plain keep-alive renews the lease to its
        // originally granted TTL.
        if Self::ttl_secs(ttl) == granted_ttl {
            let mut client = self.client.clone();
            let (mut keeper, mut stream) = client
                .lease_keep_alive(lease_id)
                .await
                .map_err(|e| Error::cluster(format!("etcd keep_alive: {e}")))?;
            keeper
                .keep_alive()
                .await
                .map_err(|e| Error::cluster(format!("etcd keep_alive send: {e}")))?;
            let resp = stream
                .message()
                .await
                .map_err(|e| Error::cluster(format!("etcd keep_alive recv: {e}")))?;

            return match resp {
                // ttl > 0: the server renewed the lease.
                Some(r) if r.ttl() > 0 => Ok(now + Duration::from_secs(r.ttl() as u64)),
                // ttl == 0 (or stream closed): the lease already expired.
                _ => {
                    self.leases.remove(&map_key);
                    Err(Error::LeaseExpired {
                        resource: key.to_string(),
                    })
                }
            };
        }

        // TTL changed (guard.extend with a new TTL): etcd cannot change a live
        // lease's TTL, so atomically re-bind the lock key to a fresh lease —
        // but only while the key is still bound to our old lease.
        let (new_lease_id, new_granted_ttl) = self.grant(ttl).await?;
        let lock_key = Self::lock_key(key);
        let txn = Txn::new()
            .when([Compare::lease(lock_key.clone(), CompareOp::Equal, lease_id)])
            .and_then([TxnOp::put(
                lock_key,
                new_lease_id.to_string(),
                Some(PutOptions::new().with_lease(new_lease_id)),
            )]);
        let mut client = self.client.clone();
        let resp = client
            .txn(txn)
            .await
            .map_err(|e| Error::cluster(format!("etcd txn(rebind): {e}")))?;

        if !resp.succeeded() {
            // Our lease no longer backs the key: it expired (and possibly got
            // re-acquired by someone else).
            self.revoke(new_lease_id).await;
            self.leases.remove(&map_key);
            return Err(Error::LeaseExpired {
                resource: key.to_string(),
            });
        }

        self.revoke(lease_id).await;
        self.leases.insert(
            map_key,
            Lease {
                lease_id: new_lease_id,
                granted_ttl: new_granted_ttl,
            },
        );
        Ok(now + Duration::from_secs(new_granted_ttl as u64))
    }

    async fn release(&self, key: &str, token: u64) -> Result<()> {
        // Only leases we granted ourselves are in the map, so this can never
        // revoke another holder's lease; releasing an unknown/stale token is a
        // no-op, matching the trait contract.
        if let Some((_, lease)) = self.leases.remove(&(key.to_string(), token)) {
            // Revoking the lease deletes the attached lock key. If the lease
            // already expired this fails harmlessly.
            self.revoke(lease.lease_id).await;
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "etcd"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn endpoints() -> Vec<String> {
        std::env::var("ETCD_ENDPOINTS")
            .unwrap_or_else(|_| "localhost:2379".to_string())
            .split(',')
            .map(str::to_string)
            .collect()
    }

    /// Requires a running etcd, e.g.:
    /// `docker run --rm -p 2379:2379 quay.io/coreos/etcd:v3.6.0 etcd --advertise-client-urls http://0.0.0.0:2379 --listen-client-urls http://0.0.0.0:2379`
    #[tokio::test]
    #[ignore = "requires a running etcd (set ETCD_ENDPOINTS, default localhost:2379)"]
    async fn etcd_exclusive_acquire_release_and_monotonic_tokens() {
        let backend = Arc::new(EtcdBackend::connect(endpoints(), None).await.unwrap());
        let key = format!("it/{}", std::process::id());
        let ttl = Duration::from_secs(5);

        let (t1, _) = backend.try_acquire(&key, ttl).await.unwrap();
        assert!(matches!(
            backend.try_acquire(&key, ttl).await,
            Err(Error::Contended { .. })
        ));

        // Renew (same TTL) and re-bind (different TTL) both succeed while held.
        backend.try_renew(&key, t1, ttl).await.unwrap();
        backend
            .try_renew(&key, t1, Duration::from_secs(30))
            .await
            .unwrap();

        backend.release(&key, t1).await.unwrap();
        let (t2, _) = backend.try_acquire(&key, ttl).await.unwrap();
        assert!(t2 > t1, "etcd revisions must be strictly monotonic");

        // A stale token cannot renew.
        assert!(backend.try_renew(&key, t1, ttl).await.is_err());
        backend.release(&key, t2).await.unwrap();
    }
}
