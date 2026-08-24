use std::time::Duration;

use thiserror::Error;

/// Kubernetes membership configuration, watcher, fencing, or task failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum KubernetesMembershipError {
    /// Invalid static or dynamic membership configuration.
    #[error("invalid Kubernetes membership configuration: {0}")]
    Configuration(String),
    /// Watch events violated the kube watcher state-machine contract.
    #[error("invalid Kubernetes watch sequence: {0}")]
    WatchSequence(String),
    /// The local pod name now belongs to another Kubernetes UID or disappeared.
    #[error(
        "Kubernetes membership pod {pod_name:?} was fenced (expected UID {expected_uid:?}, observed {observed_uid:?})"
    )]
    Fenced {
        /// Configured local pod name.
        pod_name: String,
        /// UID of this exact process pod.
        expected_uid: String,
        /// Current UID for the same name, or `None` after removal.
        observed_uid: Option<String>,
    },
    /// The Kubernetes API client could not be initialized or used.
    #[error("Kubernetes client error: {0}")]
    Kubernetes(String),
    /// A recoverable kube watcher error was emitted by the source.
    #[error("Kubernetes pod watch error: {0}")]
    Watch(String),
    /// The pod watch ended instead of remaining recoverable.
    #[error("Kubernetes pod watch ended")]
    WatchEnded,
    /// No initial assignment became available within the configured deadline.
    #[error("Kubernetes membership startup timed out after {0:?}")]
    StartupTimeout(Duration),
    /// The local assignment generation cannot advance safely.
    #[error("Kubernetes membership generation overflow")]
    GenerationOverflow,
    /// vBucket assignment could not be derived from the effective pod set.
    #[error("Kubernetes membership assignment error: {0}")]
    Assignment(String),
    /// The membership runtime was cancelled.
    #[error("Kubernetes membership runtime was cancelled")]
    Cancelled,
    /// The background Tokio task failed to join.
    #[error("Kubernetes membership task failed: {0}")]
    Task(String),
}

/// Kubernetes membership result type.
pub type Result<T> = std::result::Result<T, KubernetesMembershipError>;
