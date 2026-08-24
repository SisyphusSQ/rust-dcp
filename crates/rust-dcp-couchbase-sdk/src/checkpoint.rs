use std::{future::Future, pin::Pin, sync::Arc};

use bytes::Bytes;
use couchbase::{
    collection::Collection,
    error::{self, ErrorKind},
    options::kv_options::{MutateInOptions, StoreSemantics},
    subdoc::{
        lookup_in_specs::{GetSpecOptions, LookupInSpec},
        mutate_in_specs::{MutateInSpec, UpsertSpecOptions},
    },
};
use rust_dcp_core::{CheckpointStoreFuture, CouchbaseCheckpointCollection, DcpError};

type SdkFuture<'a, T> = Pin<Box<dyn Future<Output = error::Result<T>> + Send + 'a>>;

trait SdkCheckpointClient: Send + Sync {
    fn get_xattr<'a>(&'a self, key: &'a str, xattr: &'a str) -> SdkFuture<'a, Bytes>;

    fn upsert_xattr<'a>(&'a self, key: &'a str, xattr: &'a str, value: Bytes) -> SdkFuture<'a, ()>;

    fn remove_document<'a>(&'a self, key: &'a str) -> SdkFuture<'a, ()>;
}

struct OfficialCheckpointClient {
    collection: Collection,
}

impl SdkCheckpointClient for OfficialCheckpointClient {
    fn get_xattr<'a>(&'a self, key: &'a str, xattr: &'a str) -> SdkFuture<'a, Bytes> {
        Box::pin(async move {
            let result = self
                .collection
                .lookup_in(key, &[checkpoint_lookup_spec(xattr)], None)
                .await?;
            result.content_as_raw(0)
        })
    }

    fn upsert_xattr<'a>(&'a self, key: &'a str, xattr: &'a str, value: Bytes) -> SdkFuture<'a, ()> {
        Box::pin(async move {
            self.collection
                .mutate_in(
                    key,
                    &[checkpoint_upsert_spec(xattr, &value)],
                    checkpoint_upsert_options(),
                )
                .await?;
            Ok(())
        })
    }

    fn remove_document<'a>(&'a self, key: &'a str) -> SdkFuture<'a, ()> {
        Box::pin(async move {
            self.collection.remove(key, None).await?;
            Ok(())
        })
    }
}

/// Checkpoint XATTR adapter backed by an official SDK [`Collection`].
///
/// Cluster connection, authentication, TLS, retry, and KV routing remain owned by the supplied
/// official SDK collection. DCP streaming continues to use rust-dcp's Tokio implementation.
#[derive(Clone)]
pub struct CouchbaseSdkCheckpointCollection {
    client: Arc<dyn SdkCheckpointClient>,
}

impl CouchbaseSdkCheckpointCollection {
    /// Wraps an official SDK collection used for checkpoint metadata.
    #[must_use]
    pub fn new(collection: Collection) -> Self {
        Self {
            client: Arc::new(OfficialCheckpointClient { collection }),
        }
    }

    #[cfg(test)]
    fn with_client(client: Arc<dyn SdkCheckpointClient>) -> Self {
        Self { client }
    }
}

impl std::fmt::Debug for CouchbaseSdkCheckpointCollection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CouchbaseSdkCheckpointCollection")
            .finish_non_exhaustive()
    }
}

impl CouchbaseCheckpointCollection for CouchbaseSdkCheckpointCollection {
    fn get_xattr<'a>(
        &'a self,
        key: &'a str,
        xattr: &'a str,
    ) -> CheckpointStoreFuture<'a, Option<Bytes>> {
        Box::pin(async move {
            match self.client.get_xattr(key, xattr).await {
                Ok(value) => Ok(Some(value)),
                Err(error) if is_missing_xattr(&error) => Ok(None),
                Err(error) => Err(checkpoint_error(&error)),
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
            self.client
                .upsert_xattr(key, xattr, value)
                .await
                .map_err(|error| checkpoint_error(&error))
        })
    }

    fn remove_document<'a>(&'a self, key: &'a str) -> CheckpointStoreFuture<'a, ()> {
        Box::pin(async move {
            match self.client.remove_document(key).await {
                Ok(()) => Ok(()),
                Err(error) if *error.kind() == ErrorKind::DocumentNotFound => Ok(()),
                Err(error) => Err(checkpoint_error(&error)),
            }
        })
    }
}

fn checkpoint_lookup_spec(xattr: &str) -> LookupInSpec {
    LookupInSpec::get(xattr, GetSpecOptions::new().xattr(true))
}

fn checkpoint_upsert_spec(xattr: &str, value: &Bytes) -> MutateInSpec {
    MutateInSpec::upsert_raw(xattr, value.to_vec(), UpsertSpecOptions::new().xattr(true))
}

fn checkpoint_upsert_options() -> MutateInOptions {
    MutateInOptions::new().store_semantics(StoreSemantics::Upsert)
}

fn is_missing_xattr(error: &error::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::DocumentNotFound | ErrorKind::PathNotFound
    )
}

fn checkpoint_error(error: &error::Error) -> DcpError {
    DcpError::CheckpointStore(format!(
        "official Couchbase SDK checkpoint operation failed: {error}"
    ))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use couchbase::error::{Error, ErrorKind};

    use super::*;

    #[derive(Default)]
    struct FakeClient {
        gets: Mutex<VecDeque<error::Result<Bytes>>>,
        upserts: Mutex<VecDeque<error::Result<()>>>,
        removes: Mutex<VecDeque<error::Result<()>>>,
        calls: Mutex<Vec<Call>>,
    }

    #[derive(Debug, Eq, PartialEq)]
    enum Call {
        Get {
            key: String,
            xattr: String,
        },
        Upsert {
            key: String,
            xattr: String,
            value: Bytes,
        },
        Remove {
            key: String,
        },
    }

    impl FakeClient {
        fn with_gets(gets: impl IntoIterator<Item = error::Result<Bytes>>) -> Self {
            Self {
                gets: Mutex::new(gets.into_iter().collect()),
                ..Self::default()
            }
        }
    }

    impl SdkCheckpointClient for FakeClient {
        fn get_xattr<'a>(&'a self, key: &'a str, xattr: &'a str) -> SdkFuture<'a, Bytes> {
            self.calls.lock().unwrap().push(Call::Get {
                key: key.to_owned(),
                xattr: xattr.to_owned(),
            });
            let result = self.gets.lock().unwrap().pop_front().unwrap();
            Box::pin(async move { result })
        }

        fn upsert_xattr<'a>(
            &'a self,
            key: &'a str,
            xattr: &'a str,
            value: Bytes,
        ) -> SdkFuture<'a, ()> {
            self.calls.lock().unwrap().push(Call::Upsert {
                key: key.to_owned(),
                xattr: xattr.to_owned(),
                value,
            });
            let result = self.upserts.lock().unwrap().pop_front().unwrap();
            Box::pin(async move { result })
        }

        fn remove_document<'a>(&'a self, key: &'a str) -> SdkFuture<'a, ()> {
            self.calls.lock().unwrap().push(Call::Remove {
                key: key.to_owned(),
            });
            let result = self.removes.lock().unwrap().pop_front().unwrap();
            Box::pin(async move { result })
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn missing_document_or_xattr_loads_as_absent_but_other_errors_surface() {
        let client = Arc::new(FakeClient::with_gets([
            Err(Error::new(ErrorKind::DocumentNotFound)),
            Err(Error::new(ErrorKind::PathNotFound)),
            Ok(Bytes::from_static(br#"{"seqno":42}"#)),
            Err(Error::new(ErrorKind::AuthenticationFailure)),
        ]));
        let collection = CouchbaseSdkCheckpointCollection::with_client(client.clone());

        assert_eq!(collection.get_xattr("key", "cbgo").await.unwrap(), None);
        assert_eq!(collection.get_xattr("key", "cbgo").await.unwrap(), None);
        assert_eq!(
            collection.get_xattr("key", "cbgo").await.unwrap(),
            Some(Bytes::from_static(br#"{"seqno":42}"#))
        );
        assert!(collection.get_xattr("key", "cbgo").await.is_err());
        assert_eq!(client.calls.lock().unwrap().len(), 4);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn writes_delegate_exact_values_and_only_missing_deletes_are_idempotent() {
        let client = Arc::new(FakeClient {
            upserts: Mutex::new(
                [Ok(()), Err(Error::new(ErrorKind::AuthenticationFailure))]
                    .into_iter()
                    .collect(),
            ),
            removes: Mutex::new(
                [
                    Err(Error::new(ErrorKind::DocumentNotFound)),
                    Ok(()),
                    Err(Error::new(ErrorKind::AuthenticationFailure)),
                ]
                .into_iter()
                .collect(),
            ),
            ..FakeClient::default()
        });
        let collection = CouchbaseSdkCheckpointCollection::with_client(client.clone());
        let value = Bytes::from_static(br#"{"seqno":42}"#);

        collection
            .upsert_xattr("checkpoint", "cbgo", value.clone())
            .await
            .unwrap();
        assert!(
            collection
                .upsert_xattr("checkpoint", "cbgo", value.clone())
                .await
                .is_err()
        );
        collection.remove_document("checkpoint").await.unwrap();
        collection.remove_document("checkpoint").await.unwrap();
        assert!(collection.remove_document("checkpoint").await.is_err());

        assert_eq!(
            client.calls.lock().unwrap()[0],
            Call::Upsert {
                key: "checkpoint".into(),
                xattr: "cbgo".into(),
                value,
            }
        );
    }

    #[test]
    fn official_sdk_specs_preserve_raw_xattr_and_create_the_backing_document() {
        let lookup = checkpoint_lookup_spec("cbgo");
        assert_eq!(lookup.path, "cbgo");
        assert!(lookup.is_xattr);

        let value = Bytes::from_static(br#"{"seqno":42}"#);
        let upsert = checkpoint_upsert_spec("cbgo", &value);
        assert_eq!(upsert.path, "cbgo");
        assert_eq!(upsert.value, value);
        assert!(upsert.is_xattr);
        assert_eq!(
            checkpoint_upsert_options().store_semantics,
            Some(StoreSemantics::Upsert)
        );
    }
}
