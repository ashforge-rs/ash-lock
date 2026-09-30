# ash-lock

Distributed coordination / locking for the `ash-*` ecosystem.

> **Status.** The default build is **single-process** (`InMemoryBackend`). Real
> distributed locking is provided by the **`etcd` feature** (`EtcdBackend` over
> an etcd v3 cluster). A few collaborators from the original spec are still
> stubbed — see [Mock surface](#mock-surface).

## What works today

- Full public API: `LockClient`, `LockConfig`, `LockBuilder`, `LockGuard`.
- Real distributed locking over **etcd** (`--features etcd`): leases +
  transactions, fencing tokens from cluster revisions (monotonic across the
  whole cluster and durable across restarts).
- Strictly-monotonic **fencing tokens**, per resource key.
- RAII release on `Drop` + explicit `unlock().await`.
- A supervised **watchdog** that auto-renews the lease and **poisons** the guard
  on renewal failure (see [Watchdog behaviour](#watchdog-behaviour)).
- Pluggable `LockBackend` trait with a fully-working `InMemoryBackend`.
- Causal stamping of every acquisition via **`ash-time`** (Hybrid Logical Clock),
  behind the `ash-time` feature.
- Mock metrics matching the spec's `ash-telemetry` counter names.
- A deterministic simulation test (DST): 10 tasks contend for one lock under
  `tokio::time` control; mutual exclusion and token monotonicity are asserted,
  plus a simulated network partition.

## Quick start

```rust
use ash_lock::{LockClient, LockConfig, validate_write};
use std::time::Duration;

#[tokio::main]
async fn main() -> ash_lock::Result<()> {
    let client = LockClient::connect(LockConfig {
        default_ttl: Duration::from_secs(30),
        watchdog_interval: Duration::from_secs(10), // ~1/3 of TTL
        ..Default::default()
    }).await?;

    // Fluent acquire.
    let guard = client
        .lock("orders/42").await
        .with_ttl(Duration::from_secs(20))
        .with_fairness(true)
        .acquire().await?;

    // Attach this token to EVERY write against the protected resource.
    let token = guard.fencing_token()?;
    do_write(token).await;

    guard.unlock().await?; // or just drop it
    Ok(())
}

async fn do_write(_token: u64) {}
```

## ⚠️ Fencing tokens — do not skip

A distributed lock **cannot** stop a stalled holder (GC pause, paused VM,
swapped-out process) from waking up *after* its lease has expired and been
re-granted to someone else, and then writing stale data. The lock alone is not
mutual exclusion against the *resource*.

The only robust defence is a **fencing token**: a strictly monotonic number
handed out on every acquire. Attach it to every write, and have the resource
reject any write whose token is below the highest it has seen.

```rust
use ash_lock::validate_write;

assert!(validate_write(7, 7));    // current holder — ok
assert!(validate_write(8, 7));    // newer holder wrote first — ok, bump watermark
assert!(!validate_write(6, 7));   // stale holder — REJECT
```

If a guard's watchdog fails to renew, the guard is **poisoned** and
`fencing_token()` returns `Err(Error::LeaseExpired)` — so a poisoned holder can
no longer produce a token to authorise writes.

## Watchdog behaviour

The original spec asked the watchdog to **panic** the local task on renewal
failure ("crash-fast"). This crate **intentionally deviates**: panicking a Tokio
task on a transient partition can take down unrelated work on the same runtime
and races the guard's own `Drop`. Instead, a renewal failure **poisons** the
shared lease state. Any later `fencing_token()` / `extend()` then fails, which
fences off the critical section just as effectively without an uncontrolled
crash. The poisoning is centralised in `watchdog.rs` so a real `ash-flare`
restart strategy can be layered on later.

## Backends

| Backend | Status |
| --- | --- |
| `InMemoryBackend` | ✅ **default**; fully working (single process) |
| `EtcdBackend` | ✅ real distributed locking over etcd v3 (`--features etcd`) |

Using etcd:

```rust,ignore
use ash_lock::{EtcdBackend, LockClient, LockConfig};
use std::sync::Arc;

let backend = Arc::new(EtcdBackend::connect(["localhost:2379"], None).await?);
let client = LockClient::connect_with(LockConfig::default(), backend).await?;
```

How it maps onto etcd: acquire grants a lease for the TTL and puts the lock key
(bound to the lease) in a `create_revision == 0` transaction; the commit
revision becomes the fencing token; the watchdog renews via lease keep-alives;
release revokes the lease, deleting the key. `extend` with a different TTL
atomically re-binds the key to a freshly granted lease. Note that etcd lease
TTLs have whole-second granularity.

Or implement your own by providing `LockBackend`:

```rust
use ash_lock::{LockBackend, InMemoryBackend, LockClient, LockConfig};
use std::sync::Arc;

# async fn ex() -> ash_lock::Result<()> {
let backend = Arc::new(InMemoryBackend::new());
let client = LockClient::connect_with(LockConfig::default(), backend).await?;
# Ok(()) }
```

## Features

| Feature | Effect |
| --- | --- |
| *(default)* | Single-process build; no network dependencies in the graph |
| `etcd` | Real distributed locking via `EtcdBackend` (requires `protoc` to build) |
| `ash-time` | Adopt the real Hybrid Logical Clock for causal acquisition stamps |
| `ash-flare` | Reserved for wiring the real supervision tree into the watchdog |
| `ash-rpc` | Reserved for the (still mock) remote lock transport |

## Mock surface

Still stubbed:

- **`ash-telemetry`** — export the counters in `telemetry.rs` to the real sink.
- **`ash-discovery`** — resolve `LockConfig::cluster_seed` to a live membership
  view in `LockClient::connect`.
- **Server-side fair queue** — `with_fairness(true)` currently only affects
  client retry pacing; a real FIFO wait queue belongs in the backend.

## Testing

```bash
cargo test                      # single-process build
cargo test --features ash-time  # with the real HLC
cargo test --features etcd      # unit tests (etcd integration test is #[ignore]d)

# Integration test against a live etcd (default endpoint localhost:2379):
cargo test --features etcd -- --ignored
```

## License

Apache-2.0
