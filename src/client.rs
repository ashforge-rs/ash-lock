//! The [`LockClient`] and its [`LockConfig`].

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tracing::info;

use crate::backend::{InMemoryBackend, LockBackend};
use crate::builder::LockBuilder;
use crate::clock::Clock;
use crate::error::{Error, Result};

/// Configuration for connecting a [`LockClient`] to a lock cluster.
#[derive(Clone, Debug)]
pub struct LockConfig {
    /// Seed addresses of the lock cluster.
    ///
    /// MOCK: the spec resolves these through `ash-discovery` via `ash-rpc`.
    /// That crate does not exist yet, so the seeds are recorded but otherwise
    /// unused (the default backend runs in-process; an empty list is fine).
    /// Kept so the real transport can be dropped in unchanged.
    pub cluster_seed: Vec<SocketAddr>,
    /// Default lease TTL for locks that do not override it.
    pub default_ttl: Duration,
    /// How often the watchdog renews a lease. Conventionally ~½ the TTL.
    pub watchdog_interval: Duration,
}

impl Default for LockConfig {
    fn default() -> Self {
        Self {
            cluster_seed: vec![],
            default_ttl: Duration::from_secs(30),
            watchdog_interval: Duration::from_secs(15),
        }
    }
}

/// Shared, cheaply-cloneable client state (held behind an `Arc` in the client
/// and borrowed by [`LockBuilder`]).
pub struct ClientInner {
    pub(crate) backend: Arc<dyn LockBackend>,
    pub(crate) clock: Clock,
    pub(crate) node_id: u64,
    pub(crate) default_ttl: Duration,
    pub(crate) watchdog_interval: Duration,
}

/// A handle to the distributed lock service.
///
/// Cloning is cheap (`Arc` bump) and all clones share the same backend, clock,
/// and node identity.
#[derive(Clone)]
pub struct LockClient {
    inner: Arc<ClientInner>,
}

/// Process-wide node id source. In a real deployment this would come from
/// `ash-discovery`; here we assign a distinct id per constructed client.
static NEXT_NODE_ID: AtomicU64 = AtomicU64::new(1);

impl LockClient {
    /// Connect using the default [`InMemoryBackend`] — single-process locking
    /// only. For real distributed locking, connect an `EtcdBackend` (behind
    /// the `etcd` feature) and pass it to [`connect_with`](Self::connect_with).
    pub async fn connect(config: LockConfig) -> Result<Self> {
        Self::connect_with(config, Arc::new(InMemoryBackend::new())).await
    }

    /// Connect using an explicit [`LockBackend`] (e.g. `InMemoryBackend` for
    /// tests, or `EtcdBackend` for a real cluster). Bypasses discovery
    /// entirely.
    pub async fn connect_with(config: LockConfig, backend: Arc<dyn LockBackend>) -> Result<Self> {
        if config.default_ttl.is_zero() {
            return Err(Error::Config("default_ttl must be non-zero".into()));
        }
        if config.watchdog_interval.is_zero() {
            return Err(Error::Config("watchdog_interval must be non-zero".into()));
        }
        if config.watchdog_interval >= config.default_ttl {
            return Err(Error::Config(
                "watchdog_interval must be shorter than default_ttl".into(),
            ));
        }

        // MOCK discovery: with the real ash-discovery this is where we'd resolve
        // `cluster_seed` to a live membership view. For now we just note it.
        let node_id = NEXT_NODE_ID.fetch_add(1, Ordering::Relaxed);
        info!(
            node_id,
            backend = backend.name(),
            seeds = config.cluster_seed.len(),
            "ash-lock client connected (discovery mocked)"
        );

        Ok(Self {
            inner: Arc::new(ClientInner {
                backend,
                clock: Clock::new(),
                node_id,
                default_ttl: config.default_ttl,
                watchdog_interval: config.watchdog_interval,
            }),
        })
    }

    /// This client's node id.
    pub fn node_id(&self) -> u64 {
        self.inner.node_id
    }

    /// Begin acquiring `resource`. Returns a [`LockBuilder`] for fluent
    /// configuration; nothing is acquired until
    /// [`acquire`](LockBuilder::acquire) is awaited.
    ///
    /// Note: this is `async` to match the spec signature, though it currently
    /// does no awaiting itself.
    pub async fn lock(&self, resource: &str) -> LockBuilder<'_> {
        let ttl = self.inner.default_ttl;
        LockBuilder {
            inner: &self.inner,
            resource: resource.to_string(),
            ttl,
            fair: false,
            // Default wait budget: one full TTL of retries before timing out.
            wait_budget: ttl,
        }
    }
}
