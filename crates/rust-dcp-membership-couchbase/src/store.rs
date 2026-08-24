use std::{future::Future, num::NonZeroUsize, pin::Pin};

use bytes::Bytes;
use rust_dcp_core::{
    DcpConfig, DcpError, SeedAddress, bootstrap_kv_connection, couchbase_vbucket_for_key,
    discover_topology, resolve_collection_id,
};
use rust_dcp_protocol::{
    DocumentStoreMode, DocumentStoreRequest, Frame, HelloFeature, Magic, Opcode, ProtocolError,
    Status, get_document, store_document,
};

use crate::{CouchbaseMembershipError, Result};

const CLIENT_NAME: &str = "rust-dcp-membership-couchbase";
const DEFAULT_SCOPE: &str = "_default";
const DEFAULT_COLLECTION: &str = "_default";
const MAX_DOCUMENT_KEY_LEN: usize = 250;
const JSON_DATATYPE: u8 = 0x01;

/// Boxed asynchronous operation returned by a membership registry store.
pub type MembershipStoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Registry bytes and the Couchbase compare-and-swap token that fenced them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredRegistryDocument {
    /// Versioned JSON registry payload.
    pub value: Bytes,
    /// CAS token required to replace this exact revision.
    pub cas: u64,
}

/// Result of a conditional registry write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreWriteResult {
    /// The conditional write was accepted.
    Stored,
    /// The document was concurrently created, replaced, or removed.
    Conflict,
}

/// Async CAS document contract used by the Tokio membership runtime.
///
/// Implementations must never translate a CAS mismatch into success. The
/// runtime retries [`StoreWriteResult::Conflict`] from a newly loaded revision.
pub trait MembershipStore: Send + Sync {
    /// Loads the current registry revision.
    fn load<'a>(
        &'a self,
        key: &'a str,
    ) -> MembershipStoreFuture<'a, Option<StoredRegistryDocument>>;

    /// Creates a registry only if it does not exist.
    fn create<'a>(
        &'a self,
        key: &'a str,
        value: Bytes,
    ) -> MembershipStoreFuture<'a, StoreWriteResult>;

    /// Replaces the exact registry revision identified by `cas`.
    fn replace<'a>(
        &'a self,
        key: &'a str,
        value: Bytes,
        cas: u64,
    ) -> MembershipStoreFuture<'a, StoreWriteResult>;
}

/// Scope and collection containing the shared membership registry document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CouchbaseRegistryCollection {
    scope: String,
    collection: String,
}

impl Default for CouchbaseRegistryCollection {
    fn default() -> Self {
        Self {
            scope: DEFAULT_SCOPE.into(),
            collection: DEFAULT_COLLECTION.into(),
        }
    }
}

impl CouchbaseRegistryCollection {
    /// Creates a named registry collection.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for a name that cannot be sent through
    /// Couchbase's collection-ID lookup command.
    pub fn new(scope: impl Into<String>, collection: impl Into<String>) -> Result<Self> {
        let scope = scope.into();
        let collection = collection.into();
        validate_collection_name("scope", &scope)?;
        validate_collection_name("collection", &collection)?;
        Ok(Self { scope, collection })
    }

    /// Registry scope name.
    #[must_use]
    pub fn scope(&self) -> &str {
        &self.scope
    }

    /// Registry collection name.
    #[must_use]
    pub fn collection(&self) -> &str {
        &self.collection
    }

    fn is_default(&self) -> bool {
        self.scope == DEFAULT_SCOPE && self.collection == DEFAULT_COLLECTION
    }
}

/// Couchbase KV implementation of the membership CAS document contract.
///
/// Each operation bootstraps through the configured seed set, discovers the
/// current active vBucket owner, and opens a normal Tokio KV connection to that
/// node. `NOT_MY_VBUCKET` is retried only after fresh topology discovery.
#[derive(Clone, Debug)]
pub struct CouchbaseKvMembershipStore {
    config: DcpConfig,
    collection: CouchbaseRegistryCollection,
    routing_attempts: NonZeroUsize,
}

impl CouchbaseKvMembershipStore {
    /// Creates a store in the default collection.
    ///
    /// # Errors
    ///
    /// Returns a rust-dcp configuration error before any network operation.
    pub fn new(config: DcpConfig) -> Result<Self> {
        Self::in_collection(config, CouchbaseRegistryCollection::default())
    }

    /// Creates a store in the requested collection.
    ///
    /// # Errors
    ///
    /// Returns a rust-dcp configuration error before any network operation.
    pub fn in_collection(
        config: DcpConfig,
        collection: CouchbaseRegistryCollection,
    ) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            collection,
            routing_attempts: NonZeroUsize::new(3).unwrap_or(NonZeroUsize::MIN),
        })
    }

    /// Sets the bounded number of topology rediscovery attempts used after
    /// `NOT_MY_VBUCKET`.
    #[must_use]
    pub const fn routing_attempts(mut self, attempts: NonZeroUsize) -> Self {
        self.routing_attempts = attempts;
        self
    }

    async fn execute(&self, key: &str, operation: KvOperation) -> Result<KvResult> {
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
            if collection_id.is_some()
                && !node_session
                    .capabilities()
                    .supports(HelloFeature::Collections)
            {
                return Err(DcpError::Unsupported(
                    "registry collection requires Couchbase Collections on the active KV node"
                        .into(),
                )
                .into());
            }
            let datatype = if node_session.capabilities().supports(HelloFeature::Json) {
                JSON_DATATYPE
            } else {
                0
            };
            let request = operation.request(key, collection_id, vbucket, datatype);
            let expected_opcode = request.opcode;
            let response = node_session.connection_mut().request(request).await?;
            validate_response_envelope(&response, expected_opcode)?;
            if response.status == Status::NOT_MY_VBUCKET {
                continue;
            }
            return operation.parse_response(response);
        }
        Err(CouchbaseMembershipError::Store(format!(
            "active vBucket owner changed during all {} routing attempts",
            self.routing_attempts
        )))
    }

    async fn resolve_collection_id(
        &self,
        collections_supported: bool,
        connection: &mut rust_dcp_core::KvConnection,
    ) -> Result<Option<u32>> {
        if !collections_supported {
            if self.collection.is_default() {
                return Ok(None);
            }
            return Err(DcpError::Unsupported(format!(
                "registry collection {}.{} requires Couchbase Collections",
                self.collection.scope, self.collection.collection
            ))
            .into());
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
}

impl MembershipStore for CouchbaseKvMembershipStore {
    fn load<'a>(
        &'a self,
        key: &'a str,
    ) -> MembershipStoreFuture<'a, Option<StoredRegistryDocument>> {
        Box::pin(async move {
            match self.execute(key, KvOperation::Load).await? {
                KvResult::Loaded(document) => Ok(document),
                KvResult::Written(_) => Err(CouchbaseMembershipError::Store(
                    "KV load returned a write result".into(),
                )),
            }
        })
    }

    fn create<'a>(
        &'a self,
        key: &'a str,
        value: Bytes,
    ) -> MembershipStoreFuture<'a, StoreWriteResult> {
        Box::pin(async move {
            match self.execute(key, KvOperation::Create(value)).await? {
                KvResult::Written(result) => Ok(result),
                KvResult::Loaded(_) => Err(CouchbaseMembershipError::Store(
                    "KV create returned a load result".into(),
                )),
            }
        })
    }

    fn replace<'a>(
        &'a self,
        key: &'a str,
        value: Bytes,
        cas: u64,
    ) -> MembershipStoreFuture<'a, StoreWriteResult> {
        Box::pin(async move {
            match self
                .execute(key, KvOperation::Replace { value, cas })
                .await?
            {
                KvResult::Written(result) => Ok(result),
                KvResult::Loaded(_) => Err(CouchbaseMembershipError::Store(
                    "KV replace returned a load result".into(),
                )),
            }
        })
    }
}

#[derive(Clone, Debug)]
enum KvOperation {
    Load,
    Create(Bytes),
    Replace { value: Bytes, cas: u64 },
}

impl KvOperation {
    fn request(&self, key: &str, collection_id: Option<u32>, vbucket: u16, datatype: u8) -> Frame {
        match self {
            Self::Load => get_document(key, collection_id, vbucket, 0),
            Self::Create(value) => store_document(DocumentStoreRequest {
                mode: DocumentStoreMode::Add,
                key: Bytes::copy_from_slice(key.as_bytes()),
                value: value.clone(),
                collection_id,
                vbucket,
                flags: 0,
                expiry: 0,
                datatype,
                cas: 0,
                opaque: 0,
            }),
            Self::Replace { value, cas } => store_document(DocumentStoreRequest {
                mode: DocumentStoreMode::Replace,
                key: Bytes::copy_from_slice(key.as_bytes()),
                value: value.clone(),
                collection_id,
                vbucket,
                flags: 0,
                expiry: 0,
                datatype,
                cas: *cas,
                opaque: 0,
            }),
        }
    }

    fn parse_response(&self, response: Frame) -> Result<KvResult> {
        match self {
            Self::Load => match response.status {
                Status::SUCCESS => Ok(KvResult::Loaded(Some(StoredRegistryDocument {
                    value: response.value,
                    cas: response.cas,
                }))),
                Status::KEY_NOT_FOUND => Ok(KvResult::Loaded(None)),
                _ => Err(server_status(&response, "membership registry load").into()),
            },
            Self::Create(_) => match response.status {
                Status::SUCCESS => Ok(KvResult::Written(StoreWriteResult::Stored)),
                Status::KEY_EXISTS => Ok(KvResult::Written(StoreWriteResult::Conflict)),
                _ => Err(server_status(&response, "membership registry create").into()),
            },
            Self::Replace { .. } => match response.status {
                Status::SUCCESS => Ok(KvResult::Written(StoreWriteResult::Stored)),
                Status::KEY_EXISTS | Status::KEY_NOT_FOUND => {
                    Ok(KvResult::Written(StoreWriteResult::Conflict))
                }
                _ => Err(server_status(&response, "membership registry CAS replace").into()),
            },
        }
    }
}

enum KvResult {
    Loaded(Option<StoredRegistryDocument>),
    Written(StoreWriteResult),
}

fn validate_response_envelope(response: &Frame, expected_opcode: Opcode) -> Result<()> {
    if !matches!(response.magic, Magic::Response | Magic::AltResponse) {
        return Err(DcpError::Protocol(ProtocolError::MalformedFrame(format!(
            "membership KV response used request magic 0x{:02x}",
            response.magic.as_u8()
        )))
        .into());
    }
    if response.opcode != expected_opcode {
        return Err(DcpError::Protocol(ProtocolError::MalformedFrame(format!(
            "membership KV response opcode 0x{:02x} does not match request 0x{:02x}",
            response.opcode.as_u8(),
            expected_opcode.as_u8()
        )))
        .into());
    }
    Ok(())
}

fn server_status(response: &Frame, context: &str) -> DcpError {
    DcpError::ServerStatus {
        status: response.status.0,
        opcode: response.opcode.as_u8(),
        message: format!("{context}: {}", String::from_utf8_lossy(&response.value)),
    }
}

fn validate_document_key(key: &str) -> Result<()> {
    if key.is_empty()
        || key.len() > MAX_DOCUMENT_KEY_LEN
        || !key.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err(CouchbaseMembershipError::Configuration(format!(
            "registry document key must contain 1..={MAX_DOCUMENT_KEY_LEN} visible ASCII bytes"
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
        return Err(CouchbaseMembershipError::Configuration(format!(
            "registry {kind} contains a character unsupported by Couchbase Collections"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kv_results_preserve_cas_and_classify_conditional_conflicts() {
        let mut loaded = Frame::response(Opcode::GET, Status::SUCCESS);
        loaded.value = Bytes::from_static(br#"{"generation":1}"#);
        loaded.cas = 42;
        match KvOperation::Load.parse_response(loaded).unwrap() {
            KvResult::Loaded(Some(document)) => {
                assert_eq!(document.cas, 42);
                assert_eq!(&document.value[..], br#"{"generation":1}"#);
            }
            _ => panic!("expected a loaded registry document"),
        }

        let create_conflict = Frame::response(Opcode::ADD, Status::KEY_EXISTS);
        assert!(matches!(
            KvOperation::Create(Bytes::new())
                .parse_response(create_conflict)
                .unwrap(),
            KvResult::Written(StoreWriteResult::Conflict)
        ));
        let replace_conflict = Frame::response(Opcode::REPLACE, Status::KEY_NOT_FOUND);
        assert!(matches!(
            KvOperation::Replace {
                value: Bytes::new(),
                cas: 7
            }
            .parse_response(replace_conflict)
            .unwrap(),
            KvResult::Written(StoreWriteResult::Conflict)
        ));
    }

    #[test]
    fn kv_response_envelope_rejects_wrong_direction_and_opcode() {
        assert!(validate_response_envelope(&Frame::request(Opcode::GET), Opcode::GET).is_err());
        assert!(
            validate_response_envelope(&Frame::response(Opcode::SET, Status::SUCCESS), Opcode::GET)
                .is_err()
        );
    }

    #[test]
    fn registry_collection_and_document_key_validation_is_bounded() {
        assert_eq!(
            CouchbaseRegistryCollection::default().scope(),
            DEFAULT_SCOPE
        );
        assert!(CouchbaseRegistryCollection::new("inventory", "airline").is_ok());
        assert!(CouchbaseRegistryCollection::new("bad.scope", "airline").is_err());
        assert!(validate_document_key("").is_err());
        assert!(validate_document_key(&"x".repeat(251)).is_err());
    }
}
