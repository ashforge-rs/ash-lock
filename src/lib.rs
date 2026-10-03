//! # ash-lock — distributed coordination for the `ash-*` ecosystem
//!
//! **Status:** the default build is single-process ([`InMemoryBackend`]). Real
//! distributed locking is provided by the **`etcd` feature** (`EtcdBackend`:
//! etcd leases + transactions, with cluster-wide monotonic revision-based
//! fencing tokens). Some collaborators named in the original spec are still
//! stubbed:
//!
//! | Spec dependency | Status in this crate |
//! |-----------------|----------------------|
//! | lock backend | [`InMemoryBackend`] (default) or `EtcdBackend` (`etcd` feature) |
//! | `ash-telemetry` | replaced by the in-crate [`telemetry`] counter module |
//! | `ash-discovery` | seeds are recorded but unused; [`LockClient::connect`] runs in-process |
//! | `ash-time` (HLC) | **adopted** behind the `ash-time` feature: stamps every acquire — see [`clock`] |
//! | `ash-flare` | supervision contract honoured by the internal watchdog; no `ash-flare` dependency yet |
//! | `ash-rpc` | remote backend transport not implemented |
//!
//! ## Fencing tokens — READ THIS
//!
//! A distributed lock **cannot** prevent a stalled lock holder (GC pause, paused
//! VM) from waking up and writing after its lease has already expired and been
//! re-granted. The only robust defence is a *fencing token*: a strictly
//! monotonic number handed out on every acquire, which you attach to every write
//! to the protected resource. The resource rejects any write whose token is
//! lower than the highest it has seen. Use [`validate_write`] at the resource.
//!
//! ```
//! # async fn ex() -> ash_lock::Result<()> {
//! use ash_lock::{LockClient, LockConfig, validate_write};
//! use std::time::Duration;
//!
//! let client = LockClient::connect(LockConfig {
//!     default_ttl: Duration::from_secs(30),
//!     watchdog_interval: Duration::from_secs(10),
//!     ..Default::default()
//! }).await?;
//!
//! let guard = client.lock("orders/42").await.with_ttl(Duration::from_secs(20)).acquire().await?;
//! let token = guard.fencing_token()?;      // attach `token` to every write
//! assert!(validate_write(token, token));   // resource-side check
//! // ... do work ...
//! guard.unlock().await?;                    // or just drop `guard`
//! # Ok(()) }
//! ```

#![forbid(unsafe_code)]

pub mod backend;
mod builder;
mod client;
pub mod clock;
mod error;
mod guard;
pub mod telemetry;
mod watchdog;

#[cfg(feature = "etcd")]
pub use backend::etcd::EtcdBackend;
pub use backend::{InMemoryBackend, LockBackend};
pub use builder::LockBuilder;
pub use client::{LockClient, LockConfig};
pub use clock::Stamp;
// Re-exported so `LockBuilder`'s borrow type is nameable, and for advanced use.
pub(crate) use client::ClientInner;
pub use error::{Error, Result};
pub use guard::{LockGuard, validate_write};

#[cfg(test)]
mod dst;
