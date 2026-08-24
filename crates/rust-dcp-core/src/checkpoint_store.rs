use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    future::Future,
    io::Write,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
};

use bytes::Bytes;
use futures_util::{StreamExt, stream::FuturesUnordered};
use serde::{Deserialize, Serialize};
use tokio::{sync::Mutex, task};

use crate::{DcpCheckpoint, DcpError, Result};

const GO_DCP_KEY_PREFIX: &str = "_connector:cbgo:";
const GO_DCP_XATTR: &str = "cbgo";

/// Boxed asynchronous operation returned by checkpoint persistence adapters.
pub type CheckpointStoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Object-safe asynchronous persistence contract for durable checkpoints.
///
/// Implementations may partially persist a batch before returning an error.
/// The coordinator conservatively keeps the whole batch dirty in that case;
/// partial writes are safe because every supplied position was already
/// acknowledged as processed.
pub trait CheckpointStore: Send + Sync {
    /// Loads the subset of requested vBuckets that currently exists.
    fn load<'a>(
        &'a self,
        bucket_uuid: &'a str,
        vbuckets: &'a [u16],
    ) -> CheckpointStoreFuture<'a, BTreeMap<u16, DcpCheckpoint>>;

    /// Persists one batch of processed checkpoints.
    fn save<'a>(&'a self, checkpoints: &'a [DcpCheckpoint]) -> CheckpointStoreFuture<'a, ()>;

    /// Removes checkpoints for the supplied vBuckets.
    fn clear<'a>(
        &'a self,
        bucket_uuid: &'a str,
        vbuckets: &'a [u16],
    ) -> CheckpointStoreFuture<'a, ()>;
}

/// Checkpoint store that deliberately never persists state.
///
/// Loads always return an empty map so the configured start-position fallback
/// applies. Saves and clears succeed without side effects, matching go-dcp's
/// noop metadata mode.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopCheckpointStore;

impl NoopCheckpointStore {
    /// Creates a no-op checkpoint store.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl CheckpointStore for NoopCheckpointStore {
    fn load<'a>(
        &'a self,
        _bucket_uuid: &'a str,
        _vbuckets: &'a [u16],
    ) -> CheckpointStoreFuture<'a, BTreeMap<u16, DcpCheckpoint>> {
        Box::pin(async { Ok(BTreeMap::new()) })
    }

    fn save<'a>(&'a self, _checkpoints: &'a [DcpCheckpoint]) -> CheckpointStoreFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn clear<'a>(
        &'a self,
        _bucket_uuid: &'a str,
        _vbuckets: &'a [u16],
    ) -> CheckpointStoreFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

/// Read-only checkpoint adapter over another asynchronous store.
///
/// Loads are delegated to the wrapped store. Saves and clears succeed without
/// touching it, allowing repeatable replay/debug sessions from a durable
/// checkpoint without mutating that checkpoint.
#[derive(Clone)]
pub struct ReadOnlyCheckpointStore {
    inner: Arc<dyn CheckpointStore>,
}

impl std::fmt::Debug for ReadOnlyCheckpointStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReadOnlyCheckpointStore")
            .finish_non_exhaustive()
    }
}

impl ReadOnlyCheckpointStore {
    /// Wraps a checkpoint store and suppresses all mutations.
    #[must_use]
    pub fn new(inner: Arc<dyn CheckpointStore>) -> Self {
        Self { inner }
    }
}

impl CheckpointStore for ReadOnlyCheckpointStore {
    fn load<'a>(
        &'a self,
        bucket_uuid: &'a str,
        vbuckets: &'a [u16],
    ) -> CheckpointStoreFuture<'a, BTreeMap<u16, DcpCheckpoint>> {
        self.inner.load(bucket_uuid, vbuckets)
    }

    fn save<'a>(&'a self, _checkpoints: &'a [DcpCheckpoint]) -> CheckpointStoreFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn clear<'a>(
        &'a self,
        _bucket_uuid: &'a str,
        _vbuckets: &'a [u16],
    ) -> CheckpointStoreFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

/// Atomic, go-dcp-compatible JSON checkpoint file.
pub struct FileCheckpointStore {
    path: PathBuf,
    gate: Arc<Mutex<()>>,
}

impl std::fmt::Debug for FileCheckpointStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FileCheckpointStore")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl FileCheckpointStore {
    /// Creates a store for one JSON file.
    ///
    /// # Errors
    ///
    /// Returns an invalid-configuration error when `path` has no file name.
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if path.file_name().is_none() {
            return Err(DcpError::InvalidConfiguration(
                "checkpoint file path must name a file".into(),
            ));
        }
        Ok(Self {
            path,
            gate: Arc::new(Mutex::new(())),
        })
    }

    /// Backing JSON path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl CheckpointStore for FileCheckpointStore {
    fn load<'a>(
        &'a self,
        bucket_uuid: &'a str,
        vbuckets: &'a [u16],
    ) -> CheckpointStoreFuture<'a, BTreeMap<u16, DcpCheckpoint>> {
        Box::pin(async move {
            validate_vbuckets(vbuckets)?;
            let guard = Arc::clone(&self.gate).lock_owned().await;
            let path = self.path.clone();
            let documents = task::spawn_blocking(move || {
                let _guard = guard;
                read_file_documents(&path)
            })
            .await
            .map_err(|error| checkpoint_task_error(&error))??
            .unwrap_or_default();
            let requested = vbuckets.iter().copied().collect::<BTreeSet<_>>();
            documents
                .into_iter()
                .filter(|(vbucket, _)| requested.contains(vbucket))
                .map(|(vbucket, document)| {
                    document
                        .into_checkpoint(vbucket, bucket_uuid)
                        .map(|checkpoint| (vbucket, checkpoint))
                })
                .collect()
        })
    }

    fn save<'a>(&'a self, checkpoints: &'a [DcpCheckpoint]) -> CheckpointStoreFuture<'a, ()> {
        Box::pin(async move {
            let updates = serialize_checkpoint_batch(checkpoints)?;
            if updates.is_empty() {
                return Ok(());
            }
            let guard = Arc::clone(&self.gate).lock_owned().await;
            let path = self.path.clone();
            task::spawn_blocking(move || {
                let _guard = guard;
                let mut documents = read_file_documents(&path)?.unwrap_or_default();
                for (&vbucket, update) in &updates {
                    if let Some(existing) = documents.get(&vbucket)
                        && existing.bucket_uuid != update.bucket_uuid
                    {
                        return Err(bucket_mismatch(
                            vbucket,
                            &update.bucket_uuid,
                            &existing.bucket_uuid,
                        ));
                    }
                }
                documents.extend(updates);
                write_file_documents(&path, &documents)
            })
            .await
            .map_err(|error| checkpoint_task_error(&error))??;
            Ok(())
        })
    }

    fn clear<'a>(
        &'a self,
        bucket_uuid: &'a str,
        vbuckets: &'a [u16],
    ) -> CheckpointStoreFuture<'a, ()> {
        Box::pin(async move {
            validate_vbuckets(vbuckets)?;
            if vbuckets.is_empty() {
                return Ok(());
            }
            let guard = Arc::clone(&self.gate).lock_owned().await;
            let path = self.path.clone();
            let bucket_uuid = bucket_uuid.to_owned();
            let vbuckets = vbuckets.to_vec();
            task::spawn_blocking(move || {
                let _guard = guard;
                let Some(mut documents) = read_file_documents(&path)? else {
                    return Ok(());
                };
                for vbucket in vbuckets {
                    if let Some(document) = documents.get(&vbucket)
                        && document.bucket_uuid != bucket_uuid
                    {
                        return Err(bucket_mismatch(
                            vbucket,
                            &bucket_uuid,
                            &document.bucket_uuid,
                        ));
                    }
                    documents.remove(&vbucket);
                }
                if documents.is_empty() {
                    match fs::remove_file(&path) {
                        Ok(()) => sync_directory(path.parent().unwrap_or_else(|| Path::new("."))),
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                        Err(error) => Err(store_error(format!(
                            "cannot remove checkpoint file {}: {error}",
                            path.display()
                        ))),
                    }
                } else {
                    write_file_documents(&path, &documents)
                }
            })
            .await
            .map_err(|error| checkpoint_task_error(&error))??;
            Ok(())
        })
    }
}

/// Raw Couchbase collection operations required by the checkpoint store.
///
/// `upsert_xattr` must create the backing document when it is absent.
/// `remove_document` must treat an absent document as success. This small
/// adapter keeps checkpoint policy independent of a particular Couchbase SDK.
pub trait CouchbaseCheckpointCollection: Send + Sync {
    /// Reads one XATTR; returns `None` when the document or XATTR is absent.
    fn get_xattr<'a>(
        &'a self,
        key: &'a str,
        xattr: &'a str,
    ) -> CheckpointStoreFuture<'a, Option<Bytes>>;

    /// Creates the document if necessary and upserts one XATTR value.
    fn upsert_xattr<'a>(
        &'a self,
        key: &'a str,
        xattr: &'a str,
        value: Bytes,
    ) -> CheckpointStoreFuture<'a, ()>;

    /// Removes one metadata document idempotently.
    fn remove_document<'a>(&'a self, key: &'a str) -> CheckpointStoreFuture<'a, ()>;
}

/// Couchbase XATTR checkpoint store compatible with go-dcp v1.3.1 keys and
/// document schema.
pub struct CouchbaseCheckpointStore {
    collection: Arc<dyn CouchbaseCheckpointCollection>,
    group: String,
}

impl std::fmt::Debug for CouchbaseCheckpointStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CouchbaseCheckpointStore")
            .field("group", &self.group)
            .finish_non_exhaustive()
    }
}

impl CouchbaseCheckpointStore {
    /// Creates a go-dcp-compatible Couchbase metadata store.
    ///
    /// # Errors
    ///
    /// Rejects an empty group or a group containing `.`, matching go-dcp's
    /// metadata key constraint.
    pub fn new(
        collection: Arc<dyn CouchbaseCheckpointCollection>,
        group: impl Into<String>,
    ) -> Result<Self> {
        let group = group.into();
        if group.trim().is_empty() || group.contains('.') {
            return Err(DcpError::InvalidConfiguration(
                "checkpoint group must be non-empty and must not contain '.'".into(),
            ));
        }
        Ok(Self { collection, group })
    }

    /// Returns the exact metadata document key for a vBucket.
    #[must_use]
    pub fn checkpoint_key(&self, vbucket: u16) -> String {
        format!("{GO_DCP_KEY_PREFIX}{}:checkpoint:{vbucket}", self.group)
    }
}

impl CheckpointStore for CouchbaseCheckpointStore {
    fn load<'a>(
        &'a self,
        bucket_uuid: &'a str,
        vbuckets: &'a [u16],
    ) -> CheckpointStoreFuture<'a, BTreeMap<u16, DcpCheckpoint>> {
        Box::pin(async move {
            validate_vbuckets(vbuckets)?;
            let mut pending = FuturesUnordered::new();
            for &vbucket in vbuckets {
                let collection = Arc::clone(&self.collection);
                let key = self.checkpoint_key(vbucket);
                pending
                    .push(async move { (vbucket, collection.get_xattr(&key, GO_DCP_XATTR).await) });
            }

            let mut checkpoints = BTreeMap::new();
            while let Some((vbucket, result)) = pending.next().await {
                if let Some(value) = result? {
                    let document = serde_json::from_slice::<StoredCheckpointDocument>(&value)
                        .map_err(|error| {
                            store_error(format!(
                                "invalid Couchbase checkpoint for vBucket {vbucket}: {error}"
                            ))
                        })?;
                    checkpoints.insert(vbucket, document.into_checkpoint(vbucket, bucket_uuid)?);
                }
            }
            Ok(checkpoints)
        })
    }

    fn save<'a>(&'a self, checkpoints: &'a [DcpCheckpoint]) -> CheckpointStoreFuture<'a, ()> {
        Box::pin(async move {
            let documents = serialize_checkpoint_batch(checkpoints)?;
            let mut pending = FuturesUnordered::new();
            for (vbucket, document) in documents {
                let collection = Arc::clone(&self.collection);
                let key = self.checkpoint_key(vbucket);
                let expected_bucket_uuid = document.bucket_uuid.clone();
                let value = Bytes::from(serde_json::to_vec(&document).map_err(|error| {
                    store_error(format!(
                        "cannot serialize Couchbase checkpoint for vBucket {vbucket}: {error}"
                    ))
                })?);
                pending.push(async move {
                    let result = async {
                        if let Some(existing) = collection.get_xattr(&key, GO_DCP_XATTR).await? {
                            let existing = serde_json::from_slice::<StoredCheckpointDocument>(
                                &existing,
                            )
                            .map_err(|error| {
                                store_error(format!(
                                    "invalid existing Couchbase checkpoint for vBucket {vbucket}: {error}"
                                ))
                            })?;
                            if existing.bucket_uuid != expected_bucket_uuid {
                                return Err(bucket_mismatch(
                                    vbucket,
                                    &expected_bucket_uuid,
                                    &existing.bucket_uuid,
                                ));
                            }
                        }
                        collection
                            .upsert_xattr(&key, GO_DCP_XATTR, value)
                            .await
                    }
                    .await;
                    (vbucket, result)
                });
            }
            collect_couchbase_writes("save", &mut pending).await
        })
    }

    fn clear<'a>(
        &'a self,
        bucket_uuid: &'a str,
        vbuckets: &'a [u16],
    ) -> CheckpointStoreFuture<'a, ()> {
        Box::pin(async move {
            validate_vbuckets(vbuckets)?;
            let mut pending = FuturesUnordered::new();
            for &vbucket in vbuckets {
                let collection = Arc::clone(&self.collection);
                let key = self.checkpoint_key(vbucket);
                let bucket_uuid = bucket_uuid.to_owned();
                pending.push(async move {
                    let result = async {
                        if let Some(existing) = collection.get_xattr(&key, GO_DCP_XATTR).await? {
                            let existing = serde_json::from_slice::<StoredCheckpointDocument>(
                                &existing,
                            )
                            .map_err(|error| {
                                store_error(format!(
                                    "invalid existing Couchbase checkpoint for vBucket {vbucket}: {error}"
                                ))
                            })?;
                            if existing.bucket_uuid != bucket_uuid {
                                return Err(bucket_mismatch(
                                    vbucket,
                                    &bucket_uuid,
                                    &existing.bucket_uuid,
                                ));
                            }
                        }
                        collection.remove_document(&key).await
                    }
                    .await;
                    (vbucket, result)
                });
            }
            collect_couchbase_writes("clear", &mut pending).await
        })
    }
}

async fn collect_couchbase_writes<F>(
    operation: &str,
    pending: &mut FuturesUnordered<F>,
) -> Result<()>
where
    F: Future<Output = (u16, Result<()>)>,
{
    let mut failures = Vec::new();
    while let Some((vbucket, result)) = pending.next().await {
        if let Err(error) = result {
            failures.push(format!("vBucket {vbucket}: {error}"));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(store_error(format!(
            "Couchbase checkpoint {operation} failed: {}",
            failures.join("; ")
        )))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct StoredCheckpointDocument {
    checkpoint: StoredCheckpointPosition,
    bucket_uuid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    manifest_uid: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredCheckpointPosition {
    snapshot: StoredSnapshot,
    #[serde(rename = "vbuuid")]
    vbucket_uuid: u64,
    #[serde(rename = "seqno")]
    seqno: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct StoredSnapshot {
    #[serde(rename = "startSeqno")]
    start_seqno: u64,
    #[serde(rename = "endSeqno")]
    end_seqno: u64,
}

impl StoredCheckpointDocument {
    fn from_checkpoint(checkpoint: &DcpCheckpoint) -> Result<Self> {
        checkpoint.validate()?;
        let bucket_uuid = checkpoint.bucket_uuid.clone().ok_or_else(|| {
            DcpError::Checkpoint(format!(
                "vBucket {} checkpoint has no bucket UUID",
                checkpoint.vbucket
            ))
        })?;
        Ok(Self {
            checkpoint: StoredCheckpointPosition {
                snapshot: StoredSnapshot {
                    start_seqno: checkpoint.snapshot_start,
                    end_seqno: checkpoint.snapshot_end,
                },
                vbucket_uuid: checkpoint.vbucket_uuid,
                seqno: checkpoint.seqno,
            },
            bucket_uuid,
            manifest_uid: checkpoint.manifest_uid,
        })
    }

    fn into_checkpoint(self, vbucket: u16, expected_bucket_uuid: &str) -> Result<DcpCheckpoint> {
        if self.bucket_uuid != expected_bucket_uuid {
            return Err(bucket_mismatch(
                vbucket,
                expected_bucket_uuid,
                &self.bucket_uuid,
            ));
        }
        let checkpoint = DcpCheckpoint {
            bucket_uuid: Some(self.bucket_uuid),
            vbucket,
            vbucket_uuid: self.checkpoint.vbucket_uuid,
            seqno: self.checkpoint.seqno,
            snapshot_start: self.checkpoint.snapshot.start_seqno,
            snapshot_end: self.checkpoint.snapshot.end_seqno,
            manifest_uid: self.manifest_uid,
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }
}

fn serialize_checkpoint_batch(
    checkpoints: &[DcpCheckpoint],
) -> Result<BTreeMap<u16, StoredCheckpointDocument>> {
    let mut documents = BTreeMap::new();
    let mut bucket_uuid = None;
    for checkpoint in checkpoints {
        let observed = checkpoint.bucket_uuid.as_deref().ok_or_else(|| {
            DcpError::Checkpoint(format!(
                "vBucket {} checkpoint has no bucket UUID",
                checkpoint.vbucket
            ))
        })?;
        if let Some(expected) = bucket_uuid
            && expected != observed
        {
            return Err(DcpError::Checkpoint(format!(
                "checkpoint batch mixes bucket UUID {expected} and {observed}"
            )));
        }
        bucket_uuid = Some(observed);
        if documents
            .insert(
                checkpoint.vbucket,
                StoredCheckpointDocument::from_checkpoint(checkpoint)?,
            )
            .is_some()
        {
            return Err(DcpError::Checkpoint(format!(
                "duplicate vBucket {} in checkpoint batch",
                checkpoint.vbucket
            )));
        }
    }
    Ok(documents)
}

fn validate_vbuckets(vbuckets: &[u16]) -> Result<()> {
    let unique = vbuckets.iter().copied().collect::<BTreeSet<_>>();
    if unique.len() != vbuckets.len() {
        return Err(DcpError::Checkpoint(
            "checkpoint vBucket list contains duplicates".into(),
        ));
    }
    Ok(())
}

fn read_file_documents(path: &Path) -> Result<Option<BTreeMap<u16, StoredCheckpointDocument>>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(store_error(format!(
                "cannot read checkpoint file {}: {error}",
                path.display()
            )));
        }
    };
    serde_json::from_slice(&bytes).map(Some).map_err(|error| {
        store_error(format!(
            "invalid checkpoint JSON in {}: {error}",
            path.display()
        ))
    })
}

fn write_file_documents(
    path: &Path,
    documents: &BTreeMap<u16, StoredCheckpointDocument>,
) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(documents)
        .map_err(|error| store_error(format!("cannot serialize checkpoint JSON: {error}")))?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|error| {
        store_error(format!(
            "cannot create checkpoint directory {}: {error}",
            parent.display()
        ))
    })?;
    let mut random = [0_u8; 8];
    getrandom::fill(&mut random)
        .map_err(|error| store_error(format!("cannot generate temporary file name: {error}")))?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            store_error(format!(
                "checkpoint path {} has a non-UTF-8 file name",
                path.display()
            ))
        })?;
    let temporary = parent.join(format!(
        ".{file_name}.tmp-{}-{}",
        std::process::id(),
        u64::from_be_bytes(random)
    ));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|error| {
                store_error(format!(
                    "cannot create temporary checkpoint file {}: {error}",
                    temporary.display()
                ))
            })?;
        file.write_all(&bytes).map_err(|error| {
            store_error(format!(
                "cannot write temporary checkpoint file {}: {error}",
                temporary.display()
            ))
        })?;
        file.sync_all().map_err(|error| {
            store_error(format!(
                "cannot sync temporary checkpoint file {}: {error}",
                temporary.display()
            ))
        })?;
        fs::rename(&temporary, path).map_err(|error| {
            store_error(format!(
                "cannot atomically replace checkpoint file {}: {error}",
                path.display()
            ))
        })?;
        sync_directory(parent)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

#[cfg(unix)]
fn sync_directory(directory: &Path) -> Result<()> {
    fs::File::open(directory)
        .and_then(|file| file.sync_all())
        .map_err(|error| {
            store_error(format!(
                "cannot sync checkpoint directory {}: {error}",
                directory.display()
            ))
        })
}

#[cfg(not(unix))]
fn sync_directory(_directory: &Path) -> Result<()> {
    Ok(())
}

fn checkpoint_task_error(error: &task::JoinError) -> DcpError {
    store_error(format!("checkpoint filesystem task failed: {error}"))
}

fn bucket_mismatch(vbucket: u16, expected: &str, observed: &str) -> DcpError {
    DcpError::Checkpoint(format!(
        "vBucket {vbucket} checkpoint bucket UUID {observed} does not match current bucket {expected}"
    ))
}

fn store_error(message: String) -> DcpError {
    DcpError::CheckpointStore(message)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static NEXT_TEMP_FILE: AtomicU64 = AtomicU64::new(1);

    #[derive(Default)]
    struct MemoryCollection {
        documents: std::sync::Mutex<BTreeMap<String, BTreeMap<String, Bytes>>>,
    }

    impl MemoryCollection {
        fn xattr(&self, key: &str, xattr: &str) -> Option<Bytes> {
            self.documents
                .lock()
                .unwrap()
                .get(key)
                .and_then(|document| document.get(xattr))
                .cloned()
        }
    }

    impl CouchbaseCheckpointCollection for MemoryCollection {
        fn get_xattr<'a>(
            &'a self,
            key: &'a str,
            xattr: &'a str,
        ) -> CheckpointStoreFuture<'a, Option<Bytes>> {
            Box::pin(async move { Ok(self.xattr(key, xattr)) })
        }

        fn upsert_xattr<'a>(
            &'a self,
            key: &'a str,
            xattr: &'a str,
            value: Bytes,
        ) -> CheckpointStoreFuture<'a, ()> {
            Box::pin(async move {
                self.documents
                    .lock()
                    .unwrap()
                    .entry(key.to_owned())
                    .or_default()
                    .insert(xattr.to_owned(), value);
                Ok(())
            })
        }

        fn remove_document<'a>(&'a self, key: &'a str) -> CheckpointStoreFuture<'a, ()> {
            Box::pin(async move {
                self.documents.lock().unwrap().remove(key);
                Ok(())
            })
        }
    }

    fn checkpoint(vbucket: u16, seqno: u64, bucket_uuid: &str) -> DcpCheckpoint {
        DcpCheckpoint {
            bucket_uuid: Some(bucket_uuid.into()),
            vbucket,
            vbucket_uuid: 0xaaaa + u64::from(vbucket),
            seqno,
            snapshot_start: seqno.saturating_sub(1),
            snapshot_end: seqno,
            manifest_uid: Some(0xff),
        }
    }

    fn temporary_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "rust-dcp-{label}-{}-{}.json",
            std::process::id(),
            NEXT_TEMP_FILE.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[tokio::test]
    async fn file_store_is_atomic_go_compatible_and_bucket_scoped() {
        let path = temporary_path("checkpoint");
        let store = FileCheckpointStore::new(path.clone()).unwrap();
        store
            .save(&[
                checkpoint(7, 10, "bucket-id"),
                checkpoint(8, 20, "bucket-id"),
            ])
            .await
            .unwrap();

        let value: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(value["7"]["bucketUuid"], "bucket-id");
        assert_eq!(value["7"]["checkpoint"]["vbuuid"], 0xaab1_u64);
        assert_eq!(value["7"]["checkpoint"]["seqno"], 10);
        assert_eq!(value["7"]["checkpoint"]["snapshot"]["startSeqno"], 9);
        let temporary_prefix = format!(".{}.tmp-", path.file_name().unwrap().to_string_lossy());
        assert!(
            fs::read_dir(path.parent().unwrap())
                .unwrap()
                .filter_map(std::result::Result::ok)
                .all(|entry| !entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(&temporary_prefix))
        );

        let loaded = store.load("bucket-id", &[8, 7]).await.unwrap();
        assert_eq!(loaded[&7].seqno, 10);
        assert_eq!(loaded[&8].manifest_uid, Some(0xff));
        assert!(store.load("recreated-bucket", &[7]).await.is_err());
        assert!(
            store
                .save(&[checkpoint(7, 11, "recreated-bucket")])
                .await
                .is_err()
        );

        store.clear("bucket-id", &[7]).await.unwrap();
        assert_eq!(store.load("bucket-id", &[7, 8]).await.unwrap().len(), 1);
        store.clear("bucket-id", &[8]).await.unwrap();
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn malformed_file_is_reported_instead_of_silently_reset() {
        let path = temporary_path("malformed-checkpoint");
        fs::write(&path, b"not-json").unwrap();
        let store = FileCheckpointStore::new(path.clone()).unwrap();

        assert!(matches!(
            store.load("bucket-id", &[7]).await,
            Err(DcpError::CheckpointStore(_))
        ));
        fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn noop_store_always_loads_empty_and_accepts_writes_and_clears() {
        let store = NoopCheckpointStore;

        assert!(store.load("bucket-id", &[7, 8]).await.unwrap().is_empty());
        store.save(&[checkpoint(7, 10, "bucket-id")]).await.unwrap();
        store.clear("bucket-id", &[7]).await.unwrap();
        assert!(store.load("bucket-id", &[7]).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn read_only_store_delegates_load_but_suppresses_save_and_clear() {
        let path = temporary_path("read-only-checkpoint");
        let writable = Arc::new(FileCheckpointStore::new(path.clone()).unwrap());
        writable
            .save(&[checkpoint(7, 10, "bucket-id")])
            .await
            .unwrap();
        let store = ReadOnlyCheckpointStore::new(writable.clone());

        assert_eq!(store.load("bucket-id", &[7]).await.unwrap()[&7].seqno, 10);
        store.save(&[checkpoint(7, 20, "bucket-id")]).await.unwrap();
        store.clear("bucket-id", &[7]).await.unwrap();

        assert_eq!(
            writable.load("bucket-id", &[7]).await.unwrap()[&7].seqno,
            10
        );
        writable.clear("bucket-id", &[7]).await.unwrap();
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn couchbase_store_uses_go_dcp_key_and_xattr_schema() {
        let collection = Arc::new(MemoryCollection::default());
        let store = CouchbaseCheckpointStore::new(collection.clone(), "group1").unwrap();
        assert_eq!(
            store.checkpoint_key(7),
            "_connector:cbgo:group1:checkpoint:7"
        );

        store.save(&[checkpoint(7, 10, "bucket-id")]).await.unwrap();
        let raw = collection
            .xattr("_connector:cbgo:group1:checkpoint:7", "cbgo")
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(value["checkpoint"]["seqno"], 10);
        assert_eq!(value["bucketUuid"], "bucket-id");

        let loaded = store.load("bucket-id", &[7, 8]).await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[&7].vbucket_uuid, 0xaab1);
        assert!(
            store
                .save(&[checkpoint(7, 11, "recreated-bucket")])
                .await
                .is_err()
        );
        assert!(store.clear("recreated-bucket", &[7]).await.is_err());
        store.clear("bucket-id", &[7]).await.unwrap();
        assert!(store.load("bucket-id", &[7]).await.unwrap().is_empty());
    }

    #[test]
    fn couchbase_group_matches_go_dcp_constraint() {
        let collection = Arc::new(MemoryCollection::default());
        assert!(CouchbaseCheckpointStore::new(collection.clone(), "").is_err());
        assert!(CouchbaseCheckpointStore::new(collection, "group.with.dot").is_err());
    }
}
