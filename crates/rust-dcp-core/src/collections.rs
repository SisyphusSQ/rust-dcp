use std::{
    collections::{BTreeMap, BTreeSet},
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Context, Poll},
};

use futures_util::Stream;
use rust_dcp_protocol::{
    CollectionId, HelloFeature, StreamFilter, get_collection_id, get_collection_manifest,
    parse_collection_id, parse_collection_manifest,
};
use serde::{Deserialize, Serialize};

use crate::{
    BootstrapCapabilities, CollectionFilter, DcpControlFeature, DcpError, DcpEvent, DcpStreamItem,
    KvConnection, Result, fetch_active_high_seqnos,
};

/// One collection in a Couchbase collection manifest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManifestCollection {
    /// Numeric collection identifier decoded from hexadecimal JSON.
    pub uid: u32,
    /// Case-sensitive collection name.
    pub name: String,
    /// Collection maximum TTL in seconds; `0` inherits the bucket value.
    pub max_ttl: i32,
    /// Whether change history is enabled when reported by the server.
    pub history: Option<bool>,
}

/// One scope and its collections in a Couchbase manifest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManifestScope {
    /// Numeric scope identifier decoded from hexadecimal JSON.
    pub uid: u32,
    /// Case-sensitive scope name.
    pub name: String,
    /// Collections currently present in the scope.
    pub collections: Vec<ManifestCollection>,
}

/// Parsed bucket-level collection manifest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CollectionManifest {
    /// Monotonic manifest identifier decoded from hexadecimal JSON.
    pub uid: u64,
    /// Scopes currently present in the bucket.
    pub scopes: Vec<ManifestScope>,
}

/// Server-side stream filter resolved from case-sensitive collection names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedCollectionFilter {
    manifest_uid: u64,
    scope_id: u32,
    collection_ids: Vec<u32>,
    collection_names: std::collections::BTreeMap<u32, String>,
    stream_filter: StreamFilter,
}

/// Effective collection selection for either a collections-aware or legacy
/// server.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionSelection {
    resolved: Option<ResolvedCollectionFilter>,
    legacy_collection_name: Option<String>,
}

impl From<ResolvedCollectionFilter> for CollectionSelection {
    fn from(resolved: ResolvedCollectionFilter) -> Self {
        Self {
            resolved: Some(resolved),
            legacy_collection_name: None,
        }
    }
}

impl CollectionSelection {
    /// Optional wire-filter template. Legacy `_default._default` streams have
    /// none. [`crate::VBucketStreamRequest::resolve`] adds the manifest UID
    /// observed at each vBucket checkpoint.
    #[must_use]
    pub fn stream_filter(&self) -> Option<&StreamFilter> {
        self.resolved
            .as_ref()
            .map(ResolvedCollectionFilter::stream_filter)
    }

    /// Resolves an event collection ID, or the absent legacy prefix, to a
    /// configured collection name.
    #[must_use]
    pub fn collection_name(&self, collection_id: Option<u32>) -> Option<&str> {
        match (collection_id, &self.resolved) {
            (Some(collection_id), Some(resolved)) => resolved.collection_name(collection_id),
            (None, None) => self.legacy_collection_name.as_deref(),
            _ => None,
        }
    }

    /// Consumes the selection and returns its optional wire-filter template.
    /// The per-vBucket checkpoint manifest UID is added while resolving the
    /// stream request.
    #[must_use]
    pub fn into_stream_filter(self) -> Option<StreamFilter> {
        self.resolved
            .map(ResolvedCollectionFilter::into_stream_filter)
    }

    fn high_seqno_collection_ids(&self) -> Option<&[u32]> {
        self.resolved.as_ref().and_then(|resolved| {
            (!resolved.collection_ids.is_empty()).then_some(resolved.collection_ids.as_slice())
        })
    }
}

/// Thread-safe collection-name registry shared by stream adapters and
/// observers.
#[derive(Clone, Debug)]
pub struct CollectionRegistry {
    state: Arc<RwLock<RegistryState>>,
}

/// Stream adapter that resolves collection names and applies system events
/// before yielding each DCP item.
pub struct CollectionStream<S> {
    inner: S,
    registry: CollectionRegistry,
}

impl<S> std::fmt::Debug for CollectionStream<S>
where
    S: std::fmt::Debug,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CollectionStream")
            .field("inner", &self.inner)
            .field("registry", &self.registry)
            .finish()
    }
}

impl<S> CollectionStream<S> {
    /// Registry used to decorate this stream.
    #[must_use]
    pub const fn registry(&self) -> &CollectionRegistry {
        &self.registry
    }

    /// Releases the underlying stream and registry.
    #[must_use]
    pub fn into_parts(self) -> (S, CollectionRegistry) {
        (self.inner, self.registry)
    }
}

impl<S> Stream for CollectionStream<S>
where
    S: Stream<Item = Result<DcpStreamItem>> + Unpin,
{
    type Item = Result<DcpStreamItem>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(context) {
            Poll::Ready(Some(Ok(DcpStreamItem::Event(event)))) => Poll::Ready(Some(
                self.registry.decorate(event).map(DcpStreamItem::Event),
            )),
            Poll::Ready(other) => Poll::Ready(other),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Observable collection-registry freshness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CollectionRegistryStatus {
    /// Highest manifest UID applied or deliberately ignored as understood.
    pub manifest_uid: Option<u64>,
    /// Whether a newer unknown event made the name mapping incomplete.
    pub stale: bool,
}

impl CollectionRegistry {
    /// Creates a registry from one resolved stream selection.
    #[must_use]
    pub fn new(selection: CollectionSelection) -> Self {
        let observed_manifest_uid = selection
            .resolved
            .as_ref()
            .map(ResolvedCollectionFilter::manifest_uid);
        Self {
            state: Arc::new(RwLock::new(RegistryState {
                selection,
                observed_manifest_uid,
                stale: false,
            })),
        }
    }

    /// Wraps a DCP item stream with collection-name resolution.
    #[must_use]
    pub fn wrap<S>(&self, stream: S) -> CollectionStream<S> {
        CollectionStream {
            inner: stream,
            registry: self.clone(),
        }
    }

    /// Returns the current manifest generation and freshness flag.
    ///
    /// # Errors
    ///
    /// Returns a collection-state error when the registry lock was poisoned.
    pub fn status(&self) -> Result<CollectionRegistryStatus> {
        let state = self
            .state
            .read()
            .map_err(|_| DcpError::Collection("collection registry state was poisoned".into()))?;
        Ok(CollectionRegistryStatus {
            manifest_uid: state.observed_manifest_uid,
            stale: state.stale,
        })
    }

    /// Adds a resolved collection name to one document event.
    ///
    /// # Errors
    ///
    /// Returns a collection-state error when the registry lock was poisoned.
    pub fn decorate(&self, mut event: DcpEvent) -> Result<DcpEvent> {
        if let DcpEvent::SystemEvent(system_event) = &event {
            self.state
                .write()
                .map_err(|_| DcpError::Collection("collection registry state was poisoned".into()))?
                .apply_system_event(system_event)?;
            return Ok(event);
        }
        let state = self
            .state
            .read()
            .map_err(|_| DcpError::Collection("collection registry state was poisoned".into()))?;
        match &mut event {
            DcpEvent::Mutation(mutation) => {
                mutation.collection_name = Some(required_collection_name(
                    &state.selection,
                    mutation.collection_id,
                )?);
            }
            DcpEvent::Deletion(deletion) => {
                deletion.collection_name = Some(required_collection_name(
                    &state.selection,
                    deletion.collection_id,
                )?);
            }
            DcpEvent::Expiration(expiration) => {
                expiration.collection_name = Some(required_collection_name(
                    &state.selection,
                    expiration.collection_id,
                )?);
            }
            _ => {}
        }
        Ok(event)
    }
}

#[derive(Debug)]
struct RegistryState {
    selection: CollectionSelection,
    observed_manifest_uid: Option<u64>,
    stale: bool,
}

impl RegistryState {
    fn apply_system_event(&mut self, event: &crate::SystemEvent) -> Result<()> {
        let observed = self.observed_manifest_uid.ok_or_else(|| {
            DcpError::Collection("received a collection system event on a legacy stream".into())
        })?;
        if event.manifest_uid <= observed {
            return Ok(());
        }
        let resolved = self
            .selection
            .resolved
            .as_mut()
            .expect("manifest UID exists only for a resolved selection");
        match &event.kind {
            crate::SystemEventKind::CollectionCreated {
                scope_id,
                collection_id,
                ..
            } if *scope_id == resolved.scope_id && resolved.collection_ids.is_empty() => {
                let name = parse_system_event_name(&event.key)?;
                if let Some(existing) = resolved.collection_names.get(collection_id)
                    && existing != &name
                {
                    return Err(DcpError::Collection(format!(
                        "collection ID 0x{collection_id:x} changed name from {existing:?} to {name:?}"
                    )));
                }
                if let Some((&existing_id, _)) =
                    resolved
                        .collection_names
                        .iter()
                        .find(|(existing_id, existing_name)| {
                            **existing_id != *collection_id && *existing_name == &name
                        })
                {
                    return Err(DcpError::Collection(format!(
                        "collection name {name:?} is already mapped to ID 0x{existing_id:x}"
                    )));
                }
                resolved.collection_names.insert(*collection_id, name);
            }
            crate::SystemEventKind::CollectionDropped {
                scope_id,
                collection_id,
            } if *scope_id == resolved.scope_id => {
                let was_selected = resolved.collection_ids.is_empty()
                    || resolved.collection_ids.contains(collection_id);
                if resolved.collection_names.remove(collection_id).is_none() && was_selected {
                    return Err(DcpError::Collection(format!(
                        "dropped collection ID 0x{collection_id:x} was absent from the registry"
                    )));
                }
            }
            crate::SystemEventKind::ScopeDropped { scope_id } if *scope_id == resolved.scope_id => {
                resolved.collection_names.clear();
            }
            crate::SystemEventKind::Unknown { .. } => {
                self.stale = true;
            }
            _ => {}
        }
        self.observed_manifest_uid = Some(event.manifest_uid);
        Ok(())
    }
}

fn required_collection_name(
    selection: &CollectionSelection,
    collection_id: Option<u32>,
) -> Result<String> {
    selection
        .collection_name(collection_id)
        .map(str::to_owned)
        .ok_or_else(|| {
            DcpError::Collection(format!(
                "event collection ID {collection_id:?} is outside the resolved stream selection"
            ))
        })
}

fn parse_system_event_name(key: &[u8]) -> Result<String> {
    let name = std::str::from_utf8(key).map_err(|error| {
        DcpError::Collection(format!("system-event name is not UTF-8: {error}"))
    })?;
    if name.is_empty()
        || name.len() > 251
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'%'))
    {
        return Err(DcpError::Collection(format!(
            "system-event collection name {name:?} violates Couchbase naming rules"
        )));
    }
    Ok(name.to_owned())
}

impl ResolvedCollectionFilter {
    /// Manifest generation used for the complete resolution.
    #[must_use]
    pub const fn manifest_uid(&self) -> u64 {
        self.manifest_uid
    }

    /// Resolved scope identifier.
    #[must_use]
    pub const fn scope_id(&self) -> u32 {
        self.scope_id
    }

    /// Collection identifiers in configured order.
    #[must_use]
    pub fn collection_ids(&self) -> &[u32] {
        &self.collection_ids
    }

    /// Resolves a selected collection ID back to its configured name.
    #[must_use]
    pub fn collection_name(&self, collection_id: u32) -> Option<&str> {
        self.collection_names
            .get(&collection_id)
            .map(String::as_str)
    }

    /// Wire-filter template supplied while resolving each vBucket request.
    /// Its manifest UID is deliberately empty until the checkpoint is known.
    #[must_use]
    pub const fn stream_filter(&self) -> &StreamFilter {
        &self.stream_filter
    }

    /// Consumes the resolved names and returns the wire-filter template.
    #[must_use]
    pub fn into_stream_filter(self) -> StreamFilter {
        self.stream_filter
    }
}

impl CollectionManifest {
    /// Parses the JSON returned by `COLLECTIONS_GET_MANIFEST`.
    ///
    /// # Errors
    ///
    /// Returns a protocol error for malformed JSON or hexadecimal IDs.
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let manifest = serde_json::from_slice::<WireManifest>(bytes)
            .map_err(|error| DcpError::Protocol(error.into()))?;
        let manifest = Self {
            uid: parse_hex_u64("manifest", &manifest.uid)?,
            scopes: manifest
                .scopes
                .into_iter()
                .map(ManifestScope::try_from)
                .collect::<Result<_>>()?,
        };
        manifest.validate_identity()?;
        Ok(manifest)
    }

    /// Resolves a configured scope and collection list against this single
    /// manifest generation.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when the scope or any collection is not
    /// present in the manifest.
    pub fn resolve(
        &self,
        configured: &CollectionFilter,
        stream_id: Option<u16>,
    ) -> Result<ResolvedCollectionFilter> {
        configured.validate()?;
        let scope = self
            .scopes
            .iter()
            .find(|scope| scope.name == configured.scope)
            .ok_or_else(|| {
                DcpError::InvalidConfiguration(format!(
                    "scope {:?} is not present in collection manifest 0x{:x}",
                    configured.scope, self.uid
                ))
            })?;
        let mut collection_ids = Vec::with_capacity(configured.collections.len());
        let mut collection_names = std::collections::BTreeMap::new();
        if configured.collections.is_empty() {
            collection_names.extend(
                scope
                    .collections
                    .iter()
                    .map(|collection| (collection.uid, collection.name.clone())),
            );
        } else {
            for name in &configured.collections {
                let collection = scope
                    .collections
                    .iter()
                    .find(|collection| collection.name == *name)
                    .ok_or_else(|| {
                        DcpError::InvalidConfiguration(format!(
                            "collection {:?}.{:?} is not present in manifest 0x{:x}",
                            configured.scope, name, self.uid
                        ))
                    })?;
                collection_ids.push(collection.uid);
                collection_names.insert(collection.uid, collection.name.clone());
            }
        }
        let stream_filter = StreamFilter {
            scope_id: configured.collections.is_empty().then_some(scope.uid),
            collection_ids: collection_ids.clone(),
            manifest_uid: None,
            stream_id,
        };
        Ok(ResolvedCollectionFilter {
            manifest_uid: self.uid,
            scope_id: scope.uid,
            collection_ids,
            collection_names,
            stream_filter,
        })
    }

    fn validate_identity(&self) -> Result<()> {
        let mut scope_ids = BTreeSet::new();
        let mut scope_names = BTreeSet::new();
        let mut collection_ids = BTreeSet::new();
        for scope in &self.scopes {
            if !scope_ids.insert(scope.uid) {
                return Err(manifest_error(&format!(
                    "duplicate scope UID 0x{:x}",
                    scope.uid
                )));
            }
            if !scope_names.insert(scope.name.as_str()) {
                return Err(manifest_error(&format!(
                    "duplicate scope name {:?}",
                    scope.name
                )));
            }
            let mut collection_names = BTreeSet::new();
            for collection in &scope.collections {
                if !collection_ids.insert(collection.uid) {
                    return Err(manifest_error(&format!(
                        "duplicate collection UID 0x{:x}",
                        collection.uid
                    )));
                }
                if !collection_names.insert(collection.name.as_str()) {
                    return Err(manifest_error(&format!(
                        "duplicate collection name {:?} in scope {:?}",
                        collection.name, scope.name
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Fetches and parses the current collection manifest over a Tokio KV
/// connection.
///
/// # Errors
///
/// Returns transport, timeout, server-status, JSON, or manifest validation
/// errors.
pub async fn fetch_collection_manifest(
    connection: &mut KvConnection,
) -> Result<CollectionManifest> {
    let response = connection.request(get_collection_manifest(0)).await?;
    CollectionManifest::parse(parse_collection_manifest(&response)?)
}

/// Resolves one scope and collection name over a Tokio KV connection.
///
/// # Errors
///
/// Returns name-validation, transport, timeout, server-status, or malformed
/// response errors.
pub async fn resolve_collection_id(
    connection: &mut KvConnection,
    scope: &str,
    collection: &str,
) -> Result<CollectionId> {
    let request = get_collection_id(scope, collection, 0)?;
    let response = connection.request(request).await?;
    Ok(parse_collection_id(&response)?)
}

/// Resolves configured names into one coherent stream selection.
///
/// Collections-aware servers are resolved from one manifest snapshot. A
/// legacy server is accepted only for the default collection, whose absent
/// collection-ID prefix is represented without a wire filter.
///
/// # Errors
///
/// Returns configuration, unsupported-capability, transport, server-status,
/// or manifest errors.
pub async fn resolve_collection_selection(
    connection: &mut KvConnection,
    capabilities: &BootstrapCapabilities,
    configured: &CollectionFilter,
    stream_id: Option<u16>,
) -> Result<CollectionSelection> {
    configured.validate()?;
    if stream_id.is_some() && !capabilities.supports_control(DcpControlFeature::StreamId) {
        return Err(DcpError::Unsupported(
            "stream ID requested but enable_stream_id was not accepted".into(),
        ));
    }
    if capabilities.supports(HelloFeature::Collections) {
        let manifest = fetch_collection_manifest(connection).await?;
        return Ok(CollectionSelection {
            resolved: Some(manifest.resolve(configured, stream_id)?),
            legacy_collection_name: None,
        });
    }

    let default_collection = configured.scope == "_default"
        && (configured.collections.is_empty() || configured.collections.as_slice() == ["_default"]);
    if !default_collection {
        return Err(DcpError::Unsupported(format!(
            "server did not negotiate collections required by scope {:?} and collections {:?}",
            configured.scope, configured.collections
        )));
    }
    Ok(CollectionSelection {
        resolved: None,
        legacy_collection_name: Some("_default".into()),
    })
}

/// Fetches finite-stream high sequence numbers for an effective selection.
///
/// Explicit multi-collection filters query each collection and retain the
/// maximum per vBucket. Scope and legacy streams use the unfiltered vBucket
/// high sequence number so system events and filtered progress can reach a
/// deterministic end.
///
/// # Errors
///
/// Returns transport, timeout, server-status, malformed-response, or
/// duplicate-vBucket errors.
pub async fn fetch_selection_high_seqnos(
    connection: &mut KvConnection,
    selection: &CollectionSelection,
) -> Result<BTreeMap<u16, u64>> {
    let Some(collection_ids) = selection.high_seqno_collection_ids() else {
        return fetch_active_high_seqnos(connection, None).await;
    };
    let mut merged = BTreeMap::<u16, u64>::new();
    for &collection_id in collection_ids {
        for (vbucket, seqno) in fetch_active_high_seqnos(connection, Some(collection_id)).await? {
            merged
                .entry(vbucket)
                .and_modify(|current| *current = (*current).max(seqno))
                .or_insert(seqno);
        }
    }
    Ok(merged)
}

#[derive(Deserialize)]
struct WireManifest {
    uid: String,
    scopes: Vec<WireScope>,
}

#[derive(Deserialize)]
struct WireScope {
    uid: String,
    name: String,
    collections: Vec<WireCollection>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireCollection {
    uid: String,
    name: String,
    #[serde(default, rename = "maxTTL")]
    max_ttl: i32,
    #[serde(default)]
    history: Option<bool>,
}

impl TryFrom<WireScope> for ManifestScope {
    type Error = DcpError;

    fn try_from(scope: WireScope) -> Result<Self> {
        Ok(Self {
            uid: parse_hex_u32("scope", &scope.uid)?,
            name: scope.name,
            collections: scope
                .collections
                .into_iter()
                .map(ManifestCollection::try_from)
                .collect::<Result<_>>()?,
        })
    }
}

impl TryFrom<WireCollection> for ManifestCollection {
    type Error = DcpError;

    fn try_from(collection: WireCollection) -> Result<Self> {
        Ok(Self {
            uid: parse_hex_u32("collection", &collection.uid)?,
            name: collection.name,
            max_ttl: collection.max_ttl,
            history: collection.history,
        })
    }
}

fn parse_hex_u64(kind: &str, value: &str) -> Result<u64> {
    u64::from_str_radix(value, 16).map_err(|error| {
        DcpError::Protocol(rust_dcp_protocol::ProtocolError::MalformedFrame(format!(
            "invalid {kind} UID {value:?}: {error}"
        )))
    })
}

fn parse_hex_u32(kind: &str, value: &str) -> Result<u32> {
    u32::from_str_radix(value, 16).map_err(|error| {
        DcpError::Protocol(rust_dcp_protocol::ProtocolError::MalformedFrame(format!(
            "invalid {kind} UID {value:?}: {error}"
        )))
    })
}

fn manifest_error(message: &str) -> DcpError {
    DcpError::Protocol(rust_dcp_protocol::ProtocolError::MalformedFrame(format!(
        "invalid collection manifest: {message}"
    )))
}
