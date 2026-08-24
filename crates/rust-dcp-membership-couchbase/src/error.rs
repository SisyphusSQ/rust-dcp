use std::time::Duration;

use thiserror::Error;

/// Couchbase membership configuration, fencing, storage, or task failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CouchbaseMembershipError {
    /// Invalid local or persisted membership configuration.
    #[error("invalid Couchbase membership configuration: {0}")]
    Configuration(String),
    /// A live process already owns the configured member ID.
    #[error("Couchbase membership member {member_id:?} is already live")]
    DuplicateMember {
        /// Conflicting logical member ID.
        member_id: String,
    },
    /// This process incarnation was replaced or removed from the registry.
    #[error("Couchbase membership member {member_id:?} was fenced by another incarnation")]
    Fenced {
        /// Fenced logical member ID.
        member_id: String,
    },
    /// The registry document is malformed or violates its schema invariants.
    #[error("invalid Couchbase membership registry: {0}")]
    Registry(String),
    /// JSON encoding or decoding failed.
    #[error("Couchbase membership JSON error: {0}")]
    Json(#[from] serde_json::Error),
    /// Underlying rust-dcp connection or protocol operation failed.
    #[error("Couchbase membership KV error: {0}")]
    Dcp(#[from] rust_dcp_core::DcpError),
    /// A bounded membership operation exceeded its configured deadline.
    #[error("Couchbase membership operation timed out after {0:?}")]
    Timeout(Duration),
    /// A custom or built-in registry store failed.
    #[error("Couchbase membership store error: {0}")]
    Store(String),
    /// Coordination could not be renewed before the stale-member deadline.
    #[error(
        "Couchbase membership lease expired after {idle_for:?} without a successful renewal: {last_error}"
    )]
    LeaseExpired {
        /// Time elapsed since the last successful registry heartbeat.
        idle_for: Duration,
        /// Most recent transient store or KV failure.
        last_error: String,
    },
    /// The membership runtime was cancelled.
    #[error("Couchbase membership runtime was cancelled")]
    Cancelled,
    /// The membership background task failed to join.
    #[error("Couchbase membership task failed: {0}")]
    Task(String),
}

/// Couchbase membership result type.
pub type Result<T> = std::result::Result<T, CouchbaseMembershipError>;
