//! Error types for `ash-lock`.

use std::time::Duration;
use thiserror::Error;

/// The result type used throughout the public API.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur while acquiring, renewing, or releasing a lock.
#[derive(Debug, Error)]
pub enum Error {
    /// The lock is currently held by another owner and could not be acquired
    /// within the allotted time / attempts.
    #[error("resource `{resource}` is already locked")]
    Contended {
        /// The contended resource key.
        resource: String,
    },

    /// The acquire attempt exceeded the configured wait budget.
    #[error("timed out after {waited:?} waiting for `{resource}`")]
    Timeout {
        /// The contended resource key.
        resource: String,
        /// How long we waited before giving up.
        waited: Duration,
    },

    /// A renew or release referenced a fencing token the backend no longer
    /// recognises — typically because the lease already expired and was
    /// re-acquired by someone else.
    #[error("fencing token {token} for `{resource}` is stale (current: {current})")]
    StaleToken {
        /// The contended resource key.
        resource: String,
        /// The token the caller presented.
        token: u64,
        /// The token the backend currently considers valid.
        current: u64,
    },

    /// The lock's lease expired locally before it could be renewed — the guard
    /// is now poisoned and its fencing token must not be trusted.
    #[error("lease for `{resource}` expired (guard poisoned)")]
    LeaseExpired {
        /// The resource whose lease lapsed.
        resource: String,
    },

    /// A cluster / transport level failure (network partition, no reachable
    /// nodes, RPC error, …).
    #[error("cluster error: {0}")]
    Cluster(String),

    /// Configuration was rejected (e.g. empty seed list, zero TTL).
    #[error("invalid configuration: {0}")]
    Config(String),
}

impl Error {
    /// Convenience constructor for cluster/transport failures.
    // Only referenced from tests and the `ash-time` / `etcd` feature paths.
    #[cfg_attr(
        not(any(test, feature = "ash-time", feature = "etcd")),
        allow(dead_code)
    )]
    pub(crate) fn cluster(msg: impl Into<String>) -> Self {
        Error::Cluster(msg.into())
    }
}
