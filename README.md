# ash-lock

[![crates.io](https://img.shields.io/crates/v/ash-lock.svg)](https://crates.io/crates/ash-lock)
[![docs.rs](https://docs.rs/ash-lock/badge.svg)](https://docs.rs/ash-lock)
[![CI](https://github.com/ashforge-rs/ash-lock/actions/workflows/ci.yml/badge.svg)](https://github.com/ashforge-rs/ash-lock/actions/workflows/ci.yml)

Distributed locking with fencing tokens for async Rust. It uses an in-process
backend by default, and etcd for locking across processes.

> **Disclaimer:** this repository contains AI-generated code.

## Features

- `LockClient`, a fluent `LockBuilder`, and `LockGuard`, which releases on
  `Drop` or with an explicit `unlock().await`.
- **Fencing tokens:** a strictly increasing token per resource key, handed out
  on every acquire.
- **Watchdog:** renews the lease in the background and poisons the guard if
  renewal fails.
- **Backends:** `InMemoryBackend` (default, single process) and `EtcdBackend`
  (the `etcd` feature). Implement `LockBackend` for anything else.
- **Causal stamps:** acquisitions can be stamped with a Hybrid Logical Clock
  from [`ash-time`](https://docs.rs/ash-time) (the `ash-time` feature).

## Installation

```sh
cargo add ash-lock
# for locking across processes over etcd
cargo add ash-lock --features etcd
```

## Quick start

```rust
use ash_lock::{LockClient, LockConfig};
use std::time::Duration;

#[tokio::main]
async fn main() -> ash_lock::Result<()> {
    let client = LockClient::connect(LockConfig {
        default_ttl: Duration::from_secs(30),
        watchdog_interval: Duration::from_secs(10), // about a third of the TTL
        ..Default::default()
    })
    .await?;

    let guard = client
        .lock("orders/42")
        .await
        .with_ttl(Duration::from_secs(20))
        .acquire()
        .await?;

    // Attach this token to every write against the protected resource.
    let token = guard.fencing_token()?;
    do_write(token).await;

    guard.unlock().await?; // or drop the guard
    Ok(())
}

async fn do_write(_token: u64) {}
```

## Fencing tokens

A distributed lock can't stop a stalled holder (a GC pause, a paused VM) from
waking up after its lease has expired and been granted to someone else, and
then writing stale data. The lock alone doesn't give mutual exclusion on the
resource.

The defence is to attach the fencing token to every write and have the
resource reject any write whose token is lower than the highest it has seen:

```rust
use ash_lock::validate_write;

assert!(validate_write(7, 7));  // current holder: accept
assert!(validate_write(8, 7));  // newer holder: accept and raise the watermark
assert!(!validate_write(6, 7)); // stale holder: reject
```

If the watchdog fails to renew a lease, the guard is poisoned and
`fencing_token()` returns `Err(Error::LeaseExpired)`, so a holder that has lost
its lease can't authorise further writes. A renewal failure poisons the guard
instead of panicking, so a network partition can't take down unrelated tasks
on the same runtime.

## Using etcd

```rust,ignore
use ash_lock::{EtcdBackend, LockClient, LockConfig};
use std::sync::Arc;

let backend = Arc::new(EtcdBackend::connect(["localhost:2379"], None).await?);
let client = LockClient::connect_with(LockConfig::default(), backend).await?;
```

An acquire grants an etcd lease for the TTL and creates the lock key, bound to
that lease, in a transaction that only succeeds if the key doesn't exist. The
commit revision becomes the fencing token, so tokens increase across the whole
cluster and survive restarts. Renewal uses lease keep-alives, and release
revokes the lease. etcd lease TTLs have whole-second granularity. Building with
`etcd` requires `protoc`.

## Cargo features

| Feature | Default | Enables |
|---|---|---|
| `etcd` | no | `EtcdBackend`, distributed locking over etcd v3 |
| `ash-time` | no | Hybrid Logical Clock stamps on acquisitions |

## Current limitations

- `with_fairness(true)` only affects client-side retry pacing. There is no
  server-side FIFO wait queue.
- `LockConfig::cluster_seed` is recorded but not used for discovery.
- Lock metrics are kept in the in-crate `telemetry` module and aren't exported
  anywhere yet.

## Testing

```sh
cargo test                      # in-process backend
cargo test --features ash-time  # with the Hybrid Logical Clock
cargo test --features etcd      # etcd unit tests

# integration test against a live etcd at localhost:2379
cargo test --features etcd -- --ignored
```

## Minimum supported Rust version

Rust 1.88 or later.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). To report a security issue, see
[SECURITY.md](SECURITY.md).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
