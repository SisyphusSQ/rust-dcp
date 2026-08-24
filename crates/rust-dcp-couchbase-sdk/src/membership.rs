use std::{future::Future, pin::Pin, sync::Arc};

use bytes::Bytes;
use couchbase::{
    collection::Collection,
    error::{self, ErrorKind},
    options::kv_options::ReplaceOptions,
    transcoding::raw_json,
};
use rust_dcp_membership_couchbase::{
    CouchbaseMembershipError, MembershipStore, MembershipStoreFuture, StoreWriteResult,
    StoredRegistryDocument,
};

type SdkFuture<'a, T> = Pin<Box<dyn Future<Output = error::Result<T>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
struct SdkDocument {
    value: Bytes,
    cas: u64,
}

trait SdkMembershipClient: Send + Sync {
    fn load<'a>(&'a self, key: &'a str) -> SdkFuture<'a, SdkDocument>;

    fn create<'a>(&'a self, key: &'a str, value: Bytes) -> SdkFuture<'a, ()>;

    fn replace<'a>(&'a self, key: &'a str, value: Bytes, cas: u64) -> SdkFuture<'a, ()>;
}

struct OfficialMembershipClient {
    collection: Collection,
}

impl SdkMembershipClient for OfficialMembershipClient {
    fn load<'a>(&'a self, key: &'a str) -> SdkFuture<'a, SdkDocument> {
        Box::pin(async move {
            let result = self.collection.get(key, None).await?;
            let (value, _) = result.content_as_raw();
            Ok(SdkDocument {
                value: Bytes::copy_from_slice(value),
                cas: result.cas(),
            })
        })
    }

    fn create<'a>(&'a self, key: &'a str, value: Bytes) -> SdkFuture<'a, ()> {
        Box::pin(async move {
            let (value, flags) = raw_json::encode(&value)?;
            self.collection.insert_raw(key, value, flags, None).await?;
            Ok(())
        })
    }

    fn replace<'a>(&'a self, key: &'a str, value: Bytes, cas: u64) -> SdkFuture<'a, ()> {
        Box::pin(async move {
            let (value, flags) = raw_json::encode(&value)?;
            self.collection
                .replace_raw(key, value, flags, ReplaceOptions::new().cas(cas))
                .await?;
            Ok(())
        })
    }
}

/// Membership CAS-document adapter backed by an official SDK [`Collection`].
///
/// The supplied official SDK collection owns cluster connections, TLS, retries, and KV routing.
/// The membership runtime still owns heartbeat, stale-member pruning, CAS retries, and vBucket
/// assignment fencing.
#[derive(Clone)]
pub struct CouchbaseSdkMembershipStore {
    client: Arc<dyn SdkMembershipClient>,
}

impl CouchbaseSdkMembershipStore {
    /// Wraps an official SDK collection used for the shared membership registry.
    #[must_use]
    pub fn new(collection: Collection) -> Self {
        Self {
            client: Arc::new(OfficialMembershipClient { collection }),
        }
    }

    #[cfg(test)]
    fn with_client(client: Arc<dyn SdkMembershipClient>) -> Self {
        Self { client }
    }
}

impl std::fmt::Debug for CouchbaseSdkMembershipStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CouchbaseSdkMembershipStore")
            .finish_non_exhaustive()
    }
}

impl MembershipStore for CouchbaseSdkMembershipStore {
    fn load<'a>(
        &'a self,
        key: &'a str,
    ) -> MembershipStoreFuture<'a, Option<StoredRegistryDocument>> {
        Box::pin(async move {
            match self.client.load(key).await {
                Ok(document) => Ok(Some(StoredRegistryDocument {
                    value: document.value,
                    cas: document.cas,
                })),
                Err(error) if *error.kind() == ErrorKind::DocumentNotFound => Ok(None),
                Err(error) => Err(membership_error(&error)),
            }
        })
    }

    fn create<'a>(
        &'a self,
        key: &'a str,
        value: Bytes,
    ) -> MembershipStoreFuture<'a, StoreWriteResult> {
        Box::pin(async move {
            match self.client.create(key, value).await {
                Ok(()) => Ok(StoreWriteResult::Stored),
                Err(error) if *error.kind() == ErrorKind::DocumentExists => {
                    Ok(StoreWriteResult::Conflict)
                }
                Err(error) => Err(membership_error(&error)),
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
            match self.client.replace(key, value, cas).await {
                Ok(()) => Ok(StoreWriteResult::Stored),
                Err(error)
                    if matches!(
                        error.kind(),
                        ErrorKind::CasMismatch | ErrorKind::DocumentNotFound
                    ) =>
                {
                    Ok(StoreWriteResult::Conflict)
                }
                Err(error) => Err(membership_error(&error)),
            }
        })
    }
}

fn membership_error(error: &error::Error) -> CouchbaseMembershipError {
    CouchbaseMembershipError::Store(format!(
        "official Couchbase SDK membership operation failed: {error}"
    ))
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Mutex};

    use couchbase::error::{Error, ErrorKind};

    use super::*;

    #[derive(Default)]
    struct FakeClient {
        loads: Mutex<VecDeque<error::Result<SdkDocument>>>,
        creates: Mutex<VecDeque<error::Result<()>>>,
        replaces: Mutex<VecDeque<error::Result<()>>>,
        calls: Mutex<Vec<Call>>,
    }

    #[derive(Debug, Eq, PartialEq)]
    enum Call {
        Load { key: String },
        Create { key: String, value: Bytes },
        Replace { key: String, value: Bytes, cas: u64 },
    }

    impl SdkMembershipClient for FakeClient {
        fn load<'a>(&'a self, key: &'a str) -> SdkFuture<'a, SdkDocument> {
            self.calls.lock().unwrap().push(Call::Load {
                key: key.to_owned(),
            });
            let result = self.loads.lock().unwrap().pop_front().unwrap();
            Box::pin(async move { result })
        }

        fn create<'a>(&'a self, key: &'a str, value: Bytes) -> SdkFuture<'a, ()> {
            self.calls.lock().unwrap().push(Call::Create {
                key: key.to_owned(),
                value,
            });
            let result = self.creates.lock().unwrap().pop_front().unwrap();
            Box::pin(async move { result })
        }

        fn replace<'a>(&'a self, key: &'a str, value: Bytes, cas: u64) -> SdkFuture<'a, ()> {
            self.calls.lock().unwrap().push(Call::Replace {
                key: key.to_owned(),
                value,
                cas,
            });
            let result = self.replaces.lock().unwrap().pop_front().unwrap();
            Box::pin(async move { result })
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn missing_registry_loads_as_absent_but_other_errors_surface() {
        let client = Arc::new(FakeClient {
            loads: Mutex::new(
                [
                    Err(Error::new(ErrorKind::DocumentNotFound)),
                    Ok(SdkDocument {
                        value: Bytes::from_static(br#"{"generation":1}"#),
                        cas: 42,
                    }),
                    Err(Error::new(ErrorKind::AuthenticationFailure)),
                ]
                .into_iter()
                .collect(),
            ),
            ..FakeClient::default()
        });
        let store = CouchbaseSdkMembershipStore::with_client(client.clone());

        assert_eq!(store.load("registry").await.unwrap(), None);
        assert_eq!(
            store.load("registry").await.unwrap(),
            Some(StoredRegistryDocument {
                value: Bytes::from_static(br#"{"generation":1}"#),
                cas: 42,
            })
        );
        assert!(store.load("registry").await.is_err());
        assert_eq!(client.calls.lock().unwrap().len(), 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn conditional_writes_preserve_conflicts_and_delegate_exact_cas() {
        let client = Arc::new(FakeClient {
            creates: Mutex::new(
                [
                    Err(Error::new(ErrorKind::DocumentExists)),
                    Ok(()),
                    Err(Error::new(ErrorKind::AuthenticationFailure)),
                ]
                .into_iter()
                .collect(),
            ),
            replaces: Mutex::new(
                [
                    Err(Error::new(ErrorKind::CasMismatch)),
                    Err(Error::new(ErrorKind::DocumentNotFound)),
                    Ok(()),
                    Err(Error::new(ErrorKind::DocumentExists)),
                ]
                .into_iter()
                .collect(),
            ),
            ..FakeClient::default()
        });
        let store = CouchbaseSdkMembershipStore::with_client(client.clone());
        let value = Bytes::from_static(br#"{"generation":1}"#);

        assert_eq!(
            store.create("registry", value.clone()).await.unwrap(),
            StoreWriteResult::Conflict
        );
        assert_eq!(
            store.create("registry", value.clone()).await.unwrap(),
            StoreWriteResult::Stored
        );
        assert!(store.create("registry", value.clone()).await.is_err());
        assert_eq!(
            store.replace("registry", value.clone(), 42).await.unwrap(),
            StoreWriteResult::Conflict
        );
        assert_eq!(
            store.replace("registry", value.clone(), 42).await.unwrap(),
            StoreWriteResult::Conflict
        );
        assert_eq!(
            store.replace("registry", value.clone(), 42).await.unwrap(),
            StoreWriteResult::Stored
        );
        assert!(store.replace("registry", value.clone(), 42).await.is_err());

        assert_eq!(
            client.calls.lock().unwrap()[3],
            Call::Replace {
                key: "registry".into(),
                value,
                cas: 42,
            }
        );
    }
}
