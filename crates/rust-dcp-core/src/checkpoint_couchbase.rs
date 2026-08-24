use std::{num::NonZeroUsize, sync::Arc};

use bytes::Bytes;
use rust_dcp_protocol::{
    Frame, HelloFeature, Magic, Opcode, ProtocolError, Status, delete_document,
    parse_subdoc_get_xattr, parse_subdoc_mutation, subdoc_get_xattr, subdoc_upsert_xattr,
};
use tokio::sync::Semaphore;

use crate::{
    CheckpointStoreFuture, CouchbaseCheckpointCollection, CouchbaseCheckpointStore, DcpConfig,
    DcpError, Result, SeedAddress, bootstrap_kv_connection, couchbase_vbucket_for_key,
    discover_topology, resolve_collection_id,
};

const CLIENT_NAME: &str = "rust-dcp-checkpoint-couchbase";
const DEFAULT_SCOPE: &str = "_default";
const DEFAULT_COLLECTION: &str = "_default";
const MAX_DOCUMENT_KEY_LEN: usize = 250;
const DEFAULT_ROUTING_ATTEMPTS: usize = 3;
const DEFAULT_MAX_CONCURRENCY: usize = 16;

/// Scope and collection containing Couchbase checkpoint metadata documents.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CouchbaseCheckpointCollectionSpec {
    scope: String,
    collection: String,
}

impl Default for CouchbaseCheckpointCollectionSpec {
    fn default() -> Self {
        Self {
            scope: DEFAULT_SCOPE.into(),
            collection: DEFAULT_COLLECTION.into(),
        }
    }
}

impl CouchbaseCheckpointCollectionSpec {
    /// Creates a named checkpoint collection.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for a name that cannot be represented by
    /// Couchbase's collection-ID lookup command.
    pub fn new(scope: impl Into<String>, collection: impl Into<String>) -> Result<Self> {
        let scope = scope.into();
        let collection = collection.into();
        validate_collection_name("scope", &scope)?;
        validate_collection_name("collection", &collection)?;
        Ok(Self { scope, collection })
    }

    /// Checkpoint scope name.
    #[must_use]
    pub fn scope(&self) -> &str {
        &self.scope
    }

    /// Checkpoint collection name.
    #[must_use]
    pub fn collection(&self) -> &str {
        &self.collection
    }

    fn is_default(&self) -> bool {
        self.scope == DEFAULT_SCOPE && self.collection == DEFAULT_COLLECTION
    }
}

/// Tokio KV/XATTR implementation of [`CouchbaseCheckpointCollection`].
///
/// Each operation discovers the current active owner for the key's vBucket.
/// Stale routing and collection IDs are retried only after a fresh bootstrap,
/// within a caller-configurable bound. A shared Tokio semaphore prevents one
/// checkpoint batch from creating an unbounded number of KV connections.
#[derive(Clone)]
pub struct CouchbaseKvCheckpointCollection {
    config: DcpConfig,
    collection: CouchbaseCheckpointCollectionSpec,
    routing_attempts: NonZeroUsize,
    max_concurrency: NonZeroUsize,
    operation_gate: Arc<Semaphore>,
}

impl std::fmt::Debug for CouchbaseKvCheckpointCollection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CouchbaseKvCheckpointCollection")
            .field("config", &self.config)
            .field("collection", &self.collection)
            .field("routing_attempts", &self.routing_attempts)
            .field("max_concurrency", &self.max_concurrency)
            .finish_non_exhaustive()
    }
}

impl CouchbaseKvCheckpointCollection {
    /// Creates a network-backed adapter for the default collection.
    ///
    /// # Errors
    ///
    /// Returns a configuration error before any network operation.
    pub fn new(config: DcpConfig) -> Result<Self> {
        Self::in_collection(config, CouchbaseCheckpointCollectionSpec::default())
    }

    /// Creates a network-backed adapter for a named collection.
    ///
    /// # Errors
    ///
    /// Returns a configuration error before any network operation.
    pub fn in_collection(
        config: DcpConfig,
        collection: CouchbaseCheckpointCollectionSpec,
    ) -> Result<Self> {
        config.validate()?;
        let routing_attempts =
            NonZeroUsize::new(DEFAULT_ROUTING_ATTEMPTS).unwrap_or(NonZeroUsize::MIN);
        let max_concurrency =
            NonZeroUsize::new(DEFAULT_MAX_CONCURRENCY).unwrap_or(NonZeroUsize::MIN);
        Ok(Self {
            config,
            collection,
            routing_attempts,
            max_concurrency,
            operation_gate: Arc::new(Semaphore::new(max_concurrency.get())),
        })
    }

    /// Sets the bounded number of fresh topology/collection lookups used after
    /// routing failures.
    #[must_use]
    pub const fn routing_attempts(mut self, attempts: NonZeroUsize) -> Self {
        self.routing_attempts = attempts;
        self
    }

    /// Sets the maximum number of concurrent network operations shared by all
    /// clones of the returned adapter.
    #[must_use]
    pub fn max_concurrency(mut self, limit: NonZeroUsize) -> Self {
        self.max_concurrency = limit;
        self.operation_gate = Arc::new(Semaphore::new(limit.get()));
        self
    }

    async fn execute(
        &self,
        key: &str,
        operation: CheckpointKvOperation,
    ) -> Result<CheckpointKvResult> {
        validate_document_key(key)?;
        for _ in 0..self.routing_attempts.get() {
            let mut seed_session = bootstrap_kv_connection(&self.config, CLIENT_NAME).await?;
            let collection_id = self
                .resolve_collection_id(
                    seed_session
                        .capabilities()
                        .supports(HelloFeature::Collections),
                    seed_session.connection_mut(),
                )
                .await?;
            let topology = discover_topology(
                seed_session.connection_mut(),
                &self.config.bucket,
                self.config.tls.enabled,
                &self.config.network,
            )
            .await?;
            let vbucket = couchbase_vbucket_for_key(key.as_bytes(), topology.num_vbuckets())?;
            let endpoint = topology.active_endpoint(vbucket)?.address().to_owned();

            let mut node_config = self.config.clone();
            node_config.seeds = vec![endpoint.parse::<SeedAddress>()?];
            let mut node_session = bootstrap_kv_connection(&node_config, CLIENT_NAME).await?;
            if !node_session.capabilities().supports(HelloFeature::Xattr) {
                return Err(DcpError::Unsupported(
                    "Couchbase checkpoint persistence requires XATTR support".into(),
                ));
            }
            if collection_id.is_some()
                && !node_session
                    .capabilities()
                    .supports(HelloFeature::Collections)
            {
                return Err(DcpError::Unsupported(format!(
                    "checkpoint collection {}.{} requires Couchbase Collections on the active KV node",
                    self.collection.scope, self.collection.collection
                )));
            }

            let request = operation.request(key, collection_id, vbucket)?;
            let expected_opcode = request.opcode;
            let response = node_session.connection_mut().request(request).await?;
            validate_response_envelope(&response, expected_opcode)?;
            if matches!(
                response.status,
                Status::NOT_MY_VBUCKET | Status::COLLECTION_UNKNOWN
            ) {
                continue;
            }
            return operation.parse_response(&response);
        }
        Err(DcpError::CheckpointStore(format!(
            "checkpoint KV owner or collection changed during all {} routing attempts",
            self.routing_attempts
        )))
    }

    async fn resolve_collection_id(
        &self,
        collections_supported: bool,
        connection: &mut crate::KvConnection,
    ) -> Result<Option<u32>> {
        if !collections_supported {
            if self.collection.is_default() {
                return Ok(None);
            }
            return Err(DcpError::Unsupported(format!(
                "checkpoint collection {}.{} requires Couchbase Collections",
                self.collection.scope, self.collection.collection
            )));
        }
        if self.collection.is_default() {
            return Ok(Some(0));
        }
        Ok(Some(
            resolve_collection_id(
                connection,
                self.collection.scope(),
                self.collection.collection(),
            )
            .await?
            .collection_id,
        ))
    }

    async fn acquire_operation_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit> {
        Arc::clone(&self.operation_gate)
            .acquire_owned()
            .await
            .map_err(|error| {
                DcpError::CheckpointStore(format!(
                    "checkpoint KV concurrency gate closed unexpectedly: {error}"
                ))
            })
    }
}

impl CouchbaseCheckpointCollection for CouchbaseKvCheckpointCollection {
    fn get_xattr<'a>(
        &'a self,
        key: &'a str,
        xattr: &'a str,
    ) -> CheckpointStoreFuture<'a, Option<Bytes>> {
        Box::pin(async move {
            let _permit = self.acquire_operation_permit().await?;
            match self
                .execute(
                    key,
                    CheckpointKvOperation::GetXattr {
                        xattr: xattr.to_owned(),
                    },
                )
                .await?
            {
                CheckpointKvResult::Xattr(value) => Ok(value),
                CheckpointKvResult::Mutation => Err(unexpected_result("XATTR lookup")),
            }
        })
    }

    fn upsert_xattr<'a>(
        &'a self,
        key: &'a str,
        xattr: &'a str,
        value: Bytes,
    ) -> CheckpointStoreFuture<'a, ()> {
        Box::pin(async move {
            let _permit = self.acquire_operation_permit().await?;
            match self
                .execute(
                    key,
                    CheckpointKvOperation::UpsertXattr {
                        xattr: xattr.to_owned(),
                        value,
                    },
                )
                .await?
            {
                CheckpointKvResult::Mutation => Ok(()),
                CheckpointKvResult::Xattr(_) => Err(unexpected_result("XATTR upsert")),
            }
        })
    }

    fn remove_document<'a>(&'a self, key: &'a str) -> CheckpointStoreFuture<'a, ()> {
        Box::pin(async move {
            let _permit = self.acquire_operation_permit().await?;
            match self
                .execute(key, CheckpointKvOperation::RemoveDocument)
                .await?
            {
                CheckpointKvResult::Mutation => Ok(()),
                CheckpointKvResult::Xattr(_) => Err(unexpected_result("document removal")),
            }
        })
    }
}

impl CouchbaseCheckpointStore {
    /// Creates a go-dcp-compatible checkpoint store backed by the default
    /// Couchbase collection and the built-in Tokio KV adapter.
    ///
    /// # Errors
    ///
    /// Returns a configuration error before any network operation.
    pub fn from_config(config: DcpConfig, group: impl Into<String>) -> Result<Self> {
        let collection = Arc::new(CouchbaseKvCheckpointCollection::new(config)?);
        Self::new(collection, group)
    }

    /// Creates a go-dcp-compatible checkpoint store backed by a named
    /// Couchbase collection and the built-in Tokio KV adapter.
    ///
    /// # Errors
    ///
    /// Returns a configuration error before any network operation.
    pub fn from_config_in_collection(
        config: DcpConfig,
        collection: CouchbaseCheckpointCollectionSpec,
        group: impl Into<String>,
    ) -> Result<Self> {
        let collection = Arc::new(CouchbaseKvCheckpointCollection::in_collection(
            config, collection,
        )?);
        Self::new(collection, group)
    }
}

#[derive(Clone, Debug)]
enum CheckpointKvOperation {
    GetXattr { xattr: String },
    UpsertXattr { xattr: String, value: Bytes },
    RemoveDocument,
}

impl CheckpointKvOperation {
    fn request(&self, key: &str, collection_id: Option<u32>, vbucket: u16) -> Result<Frame> {
        match self {
            Self::GetXattr { xattr } => {
                Ok(subdoc_get_xattr(key, xattr, collection_id, vbucket, 0)?)
            }
            Self::UpsertXattr { xattr, value } => Ok(subdoc_upsert_xattr(
                key,
                xattr,
                value.clone(),
                collection_id,
                vbucket,
                0,
            )?),
            Self::RemoveDocument => Ok(delete_document(key, collection_id, vbucket, 0, 0)),
        }
    }

    fn parse_response(&self, response: &Frame) -> Result<CheckpointKvResult> {
        match self {
            Self::GetXattr { .. } => {
                Ok(CheckpointKvResult::Xattr(parse_subdoc_get_xattr(response)?))
            }
            Self::UpsertXattr { .. } => {
                parse_subdoc_mutation(response)?;
                Ok(CheckpointKvResult::Mutation)
            }
            Self::RemoveDocument => match response.status {
                Status::SUCCESS | Status::KEY_NOT_FOUND => Ok(CheckpointKvResult::Mutation),
                _ => Err(server_status(response, "checkpoint document removal")),
            },
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
enum CheckpointKvResult {
    Xattr(Option<Bytes>),
    Mutation,
}

fn validate_response_envelope(response: &Frame, expected_opcode: Opcode) -> Result<()> {
    if !matches!(response.magic, Magic::Response | Magic::AltResponse) {
        return Err(DcpError::Protocol(ProtocolError::MalformedFrame(format!(
            "checkpoint KV response used request magic 0x{:02x}",
            response.magic.as_u8()
        ))));
    }
    if response.opcode != expected_opcode {
        return Err(DcpError::Protocol(ProtocolError::MalformedFrame(format!(
            "checkpoint KV response opcode 0x{:02x} does not match request 0x{:02x}",
            response.opcode.as_u8(),
            expected_opcode.as_u8()
        ))));
    }
    Ok(())
}

fn server_status(response: &Frame, context: &str) -> DcpError {
    DcpError::ServerStatus {
        status: response.status.as_u16(),
        opcode: response.opcode.as_u8(),
        message: format!("{context}: {}", String::from_utf8_lossy(&response.value)),
    }
}

fn unexpected_result(operation: &str) -> DcpError {
    DcpError::CheckpointStore(format!(
        "checkpoint KV {operation} returned an incompatible result"
    ))
}

fn validate_document_key(key: &str) -> Result<()> {
    if key.is_empty() || key.len() > MAX_DOCUMENT_KEY_LEN {
        return Err(DcpError::InvalidConfiguration(format!(
            "checkpoint document key must contain 1..={MAX_DOCUMENT_KEY_LEN} bytes"
        )));
    }
    Ok(())
}

fn validate_collection_name(kind: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 251
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'%'))
    {
        return Err(DcpError::InvalidConfiguration(format!(
            "checkpoint {kind} contains a character unsupported by Couchbase Collections"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use bytes::Bytes;
    use rust_dcp_protocol::{Frame, Opcode, Status};

    use super::*;
    use crate::{Credentials, DcpConfig};

    fn config() -> DcpConfig {
        DcpConfig::builder(Credentials::new("alice", "secret"), "travel")
            .seed("cb.example.test")
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn checkpoint_collection_spec_and_runtime_bounds_are_validated() {
        let default = CouchbaseCheckpointCollectionSpec::default();
        assert_eq!(default.scope(), "_default");
        assert_eq!(default.collection(), "_default");
        assert!(CouchbaseCheckpointCollectionSpec::new("inventory", "metadata").is_ok());
        assert!(CouchbaseCheckpointCollectionSpec::new("bad.scope", "metadata").is_err());

        let collection = CouchbaseKvCheckpointCollection::new(config())
            .unwrap()
            .routing_attempts(NonZeroUsize::new(4).unwrap())
            .max_concurrency(NonZeroUsize::new(8).unwrap());
        assert_eq!(collection.routing_attempts.get(), 4);
        assert_eq!(collection.max_concurrency, NonZeroUsize::new(8).unwrap());
    }

    #[test]
    fn checkpoint_kv_operations_build_collection_aware_requests() {
        let get = CheckpointKvOperation::GetXattr {
            xattr: "cbgo".into(),
        }
        .request("checkpoint", Some(0xcafe), 12)
        .unwrap();
        assert_eq!(get.opcode, Opcode::SUBDOC_MULTI_LOOKUP);
        assert_eq!(get.collection_id, Some(0xcafe));
        assert_eq!(get.vbucket, 12);

        let upsert = CheckpointKvOperation::UpsertXattr {
            xattr: "cbgo".into(),
            value: Bytes::from_static(br#"{"seqno":42}"#),
        }
        .request("checkpoint", Some(0xcafe), 12)
        .unwrap();
        assert_eq!(upsert.opcode, Opcode::SUBDOC_MULTI_MUTATION);
        assert_eq!(&upsert.extras[..], &[0x01]);

        let remove = CheckpointKvOperation::RemoveDocument
            .request("checkpoint", Some(0xcafe), 12)
            .unwrap();
        assert_eq!(remove.opcode, Opcode::DELETE);
    }

    #[test]
    fn checkpoint_kv_results_preserve_absence_and_idempotent_delete() {
        let get = CheckpointKvOperation::GetXattr {
            xattr: "cbgo".into(),
        };
        let missing = Frame::response(Opcode::SUBDOC_MULTI_LOOKUP, Status::KEY_NOT_FOUND);
        assert!(matches!(
            get.parse_response(&missing).unwrap(),
            CheckpointKvResult::Xattr(None)
        ));

        let remove = CheckpointKvOperation::RemoveDocument;
        assert!(matches!(
            remove
                .parse_response(&Frame::response(Opcode::DELETE, Status::KEY_NOT_FOUND))
                .unwrap(),
            CheckpointKvResult::Mutation
        ));
        assert!(
            remove
                .parse_response(&Frame::response(Opcode::DELETE, Status::INVALID_ARGUMENTS))
                .is_err()
        );
    }

    #[test]
    fn checkpoint_kv_response_envelope_rejects_wrong_direction_and_opcode() {
        assert!(
            validate_response_envelope(&Frame::request(Opcode::DELETE), Opcode::DELETE).is_err()
        );
        assert!(
            validate_response_envelope(
                &Frame::response(Opcode::GET, Status::SUCCESS),
                Opcode::DELETE,
            )
            .is_err()
        );
    }
}
