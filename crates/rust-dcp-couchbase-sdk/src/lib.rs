//! Adapters that reuse the official Couchbase Rust SDK for ordinary metadata KV operations.
//!
//! This crate deliberately does not replace rust-dcp's Tokio DCP transport. It accepts an
//! already configured official SDK collection and adapts its supported KV/sub-document API to
//! rust-dcp's metadata contracts.
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use couchbase::{
//!     authenticator::PasswordAuthenticator,
//!     cluster::Cluster,
//!     options::cluster_options::ClusterOptions,
//! };
//! use rust_dcp_core::CouchbaseCheckpointStore;
//! use rust_dcp_couchbase_sdk::CouchbaseSdkCheckpointCollection;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let cluster = Cluster::connect(
//!     "couchbase://127.0.0.1",
//!     ClusterOptions::new(PasswordAuthenticator::new("user", "password").into()),
//! )
//! .await?;
//! let collection = cluster.bucket("travel").default_collection();
//! let adapter = Arc::new(CouchbaseSdkCheckpointCollection::new(collection));
//! let store = CouchbaseCheckpointStore::new(adapter, "consumer-group")?;
//! # let _ = store;
//! # Ok(())
//! # }
//! ```
//!
//! The same official SDK collection can back Couchbase membership by wrapping it with
//! [`CouchbaseSdkMembershipStore`] and passing the result to
//! `rust_dcp_membership_couchbase::CouchbaseMembership::with_store`.

#![forbid(unsafe_code)]

mod checkpoint;
mod membership;

pub use checkpoint::CouchbaseSdkCheckpointCollection;
pub use membership::CouchbaseSdkMembershipStore;
