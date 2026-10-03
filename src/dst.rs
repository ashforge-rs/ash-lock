//! Deterministic Simulation Test (DST).
//!
//! Spawns 10 concurrent tasks contending for the same lock under
//! `tokio::time` control (no real sleeping). Verifies:
//!   1. mutual exclusion — never two holders of the same key at once;
//!   2. fencing tokens are strictly monotonic across the whole run;
//!   3. a simulated "network partition" (a backend that drops renewals) poisons
//!      the guard rather than silently keeping a zombie lock.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tokio::time::Instant;

use crate::backend::{InMemoryBackend, LockBackend};
use crate::client::{LockClient, LockConfig};
use crate::error::{Error, Result};

/// A backend wrapper that can be flipped into "partitioned" mode, where all
/// renewals fail — simulating a node that can no longer reach the cluster.
struct PartitionableBackend {
    inner: InMemoryBackend,
    partitioned: AtomicBool,
}

impl PartitionableBackend {
    fn new() -> Self {
        Self {
            inner: InMemoryBackend::new(),
            partitioned: AtomicBool::new(false),
        }
    }
    fn set_partitioned(&self, v: bool) {
        self.partitioned.store(v, Ordering::SeqCst);
    }
}

#[async_trait]
impl LockBackend for PartitionableBackend {
    async fn try_acquire(&self, key: &str, ttl: Duration) -> Result<(u64, std::time::Instant)> {
        self.inner.try_acquire(key, ttl).await
    }
    async fn try_renew(&self, key: &str, token: u64, ttl: Duration) -> Result<std::time::Instant> {
        if self.partitioned.load(Ordering::SeqCst) {
            return Err(Error::cluster("simulated network partition: renew dropped"));
        }
        self.inner.try_renew(key, token, ttl).await
    }
    async fn release(&self, key: &str, token: u64) -> Result<()> {
        self.inner.release(key, token).await
    }
    fn name(&self) -> &'static str {
        "partitionable(dst)"
    }
}

fn test_config() -> LockConfig {
    LockConfig {
        cluster_seed: vec![],
        default_ttl: Duration::from_secs(10),
        watchdog_interval: Duration::from_secs(4),
    }
}

#[tokio::test(start_paused = true)]
async fn dst_ten_tasks_mutual_exclusion_and_monotonic_tokens() {
    // Shared "witness" of how many tasks believe they hold the lock right now,
    // and the highest fencing token observed. `live_holders` must never exceed 1.
    let live_holders = Arc::new(AtomicU64::new(0));
    let max_token = Arc::new(AtomicU64::new(0));
    let violations = Arc::new(AtomicU64::new(0));
    let token_regressions = Arc::new(AtomicU64::new(0));

    let backend = Arc::new(InMemoryBackend::new());

    let mut tasks = Vec::new();
    for _ in 0..10 {
        let backend = Arc::clone(&backend) as Arc<dyn LockBackend>;
        let live_holders = Arc::clone(&live_holders);
        let max_token = Arc::clone(&max_token);
        let violations = Arc::clone(&violations);
        let token_regressions = Arc::clone(&token_regressions);

        tasks.push(tokio::spawn(async move {
            let client = LockClient::connect_with(test_config(), backend)
                .await
                .expect("connect");

            // Each task tries a few times to grab the lock, holds it briefly,
            // then releases — maximising contention.
            for _ in 0..3 {
                let guard = client
                    .lock("shared/resource")
                    .await
                    .with_ttl(Duration::from_secs(8))
                    .acquire()
                    .await
                    .expect("acquire within budget");

                // Critical section entered.
                let now_live = live_holders.fetch_add(1, Ordering::SeqCst) + 1;
                if now_live > 1 {
                    violations.fetch_add(1, Ordering::SeqCst);
                }

                let token = guard.fencing_token().expect("token valid");
                // Token must never regress relative to the global max.
                let prev = max_token.fetch_max(token, Ordering::SeqCst);
                if token < prev {
                    token_regressions.fetch_add(1, Ordering::SeqCst);
                }

                // Hold for a simulated moment.
                tokio::time::sleep(Duration::from_millis(1)).await;

                live_holders.fetch_sub(1, Ordering::SeqCst);
                guard.unlock().await.expect("unlock");
            }
        }));
    }

    // Drive simulated time forward so retries and sleeps make progress. With
    // start_paused, the runtime auto-advances to the next timer when idle, so
    // simply awaiting the joins is sufficient — but we bound it defensively.
    let deadline = Instant::now() + Duration::from_secs(120);
    for t in tasks {
        t.await.expect("task panicked");
    }
    assert!(Instant::now() <= deadline, "DST took too long in sim time");

    assert_eq!(
        violations.load(Ordering::SeqCst),
        0,
        "mutual exclusion violated: two tasks held the lock simultaneously"
    );
    assert_eq!(
        token_regressions.load(Ordering::SeqCst),
        0,
        "fencing token regressed"
    );
    // 10 tasks * 3 acquisitions = 30 successful acquisitions.
    assert!(
        max_token.load(Ordering::SeqCst) >= 30,
        "expected >=30 acquisitions"
    );
}

#[tokio::test]
async fn acquired_at_stamp_is_monotonic() {
    // Each acquisition of the *same* key should carry a strictly greater causal
    // stamp than the previous one. With the `ash-time` feature this exercises
    // the real Hybrid Logical Clock; without it, the fallback counter.
    let backend = Arc::new(InMemoryBackend::new()) as Arc<dyn LockBackend>;
    let client = LockClient::connect_with(test_config(), backend)
        .await
        .unwrap();

    let mut prev = None;
    for _ in 0..5 {
        let g = client.lock("stamped/key").await.acquire().await.unwrap();
        let s = g.acquired_at();
        if let Some(p) = prev {
            assert!(
                s > p,
                "acquired_at stamp must be strictly monotonic: {s:?} !> {p:?}"
            );
        }
        prev = Some(s);
        g.unlock().await.unwrap();
    }

    // Under the ash-time feature the physical component is real wall-clock ns.
    #[cfg(feature = "ash-time")]
    assert!(
        prev.unwrap().physical > 0,
        "ash-time stamp should carry a wall-clock component"
    );
}

/// A backend wrapper that records the TTL of the most recent renew, so tests
/// can observe what the watchdog actually asks for.
struct RenewRecordingBackend {
    inner: InMemoryBackend,
    last_renew_ttl_ms: AtomicU64,
}

impl RenewRecordingBackend {
    fn new() -> Self {
        Self {
            inner: InMemoryBackend::new(),
            last_renew_ttl_ms: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl LockBackend for RenewRecordingBackend {
    async fn try_acquire(&self, key: &str, ttl: Duration) -> Result<(u64, std::time::Instant)> {
        self.inner.try_acquire(key, ttl).await
    }
    async fn try_renew(&self, key: &str, token: u64, ttl: Duration) -> Result<std::time::Instant> {
        self.last_renew_ttl_ms
            .store(ttl.as_millis() as u64, Ordering::SeqCst);
        self.inner.try_renew(key, token, ttl).await
    }
    async fn release(&self, key: &str, token: u64) -> Result<()> {
        self.inner.release(key, token).await
    }
    fn name(&self) -> &'static str {
        "renew-recording(dst)"
    }
}

#[tokio::test(start_paused = true)]
async fn extend_updates_watchdog_renewal_ttl() {
    let backend = Arc::new(RenewRecordingBackend::new());
    let client =
        LockClient::connect_with(test_config(), Arc::clone(&backend) as Arc<dyn LockBackend>)
            .await
            .unwrap();

    let guard = client
        .lock("extended/key")
        .await
        .with_ttl(Duration::from_secs(8))
        .acquire()
        .await
        .unwrap();
    tokio::task::yield_now().await;

    guard.extend(Duration::from_secs(600)).await.unwrap();

    // Advance past a watchdog interval so a renewal fires. It must renew with
    // the extended TTL, not shrink the lease back to the original 8s.
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    assert!(guard.state_renewals() >= 1, "watchdog should have renewed");
    assert_eq!(
        backend.last_renew_ttl_ms.load(Ordering::SeqCst),
        600_000,
        "watchdog must renew with the extended TTL"
    );
    guard.unlock().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn short_ttl_lock_is_renewed_before_it_lapses() {
    // TTL (2s) shorter than the configured watchdog interval (4s): the watchdog
    // must tighten its tick to ttl/2 or the lease lapses before first renewal.
    let backend = Arc::new(InMemoryBackend::new()) as Arc<dyn LockBackend>;
    let client = LockClient::connect_with(test_config(), backend)
        .await
        .unwrap();

    let guard = client
        .lock("short/key")
        .await
        .with_ttl(Duration::from_secs(2))
        .acquire()
        .await
        .unwrap();
    tokio::task::yield_now().await;

    // Well past the original 2s TTL; without adaptive pacing the first renewal
    // (at 4s) would find the lease already expired and poison the guard.
    for _ in 0..10 {
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }

    assert!(
        guard.fencing_token().is_ok(),
        "short-TTL lease must be kept alive by the watchdog"
    );
    assert!(guard.state_renewals() >= 4, "expected multiple renewals");
    guard.unlock().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn dst_partition_poisons_guard_no_zombie_lock() {
    let backend = Arc::new(PartitionableBackend::new());
    let client =
        LockClient::connect_with(test_config(), Arc::clone(&backend) as Arc<dyn LockBackend>)
            .await
            .unwrap();

    let guard = client
        .lock("partitioned/key")
        .await
        .with_ttl(Duration::from_secs(8))
        .acquire()
        .await
        .unwrap();

    assert!(
        guard.fencing_token().is_ok(),
        "healthy guard has a valid token"
    );

    // Let the watchdog task be polled so it registers its interval timer.
    tokio::task::yield_now().await;

    // Sever the network: renewals now fail.
    backend.set_partitioned(true);

    // Advance past a watchdog interval (4s) so a renewal is attempted, fails,
    // and poisons the guard. Yield between/after advances so the watchdog task
    // actually runs to completion under paused time.
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    // The guard must now be poisoned: its token can no longer authorise writes.
    assert!(
        matches!(guard.fencing_token(), Err(Error::LeaseExpired { .. })),
        "after a partition, the guard must be poisoned (crash-fast fencing)"
    );
    assert!(
        matches!(
            guard.extend(Duration::from_secs(8)).await,
            Err(Error::LeaseExpired { .. })
        ),
        "extend on a poisoned guard must fail"
    );
}
