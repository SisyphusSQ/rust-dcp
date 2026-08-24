//! Couchbase-backed Tokio membership and vBucket assignment extension.

#![forbid(unsafe_code)]

mod error;
mod model;
mod runtime;
mod store;

pub use error::{CouchbaseMembershipError, Result};
pub use model::{MemberIdentity, MemberInfo, MembershipSnapshot, RegistryDocument};
pub use runtime::{CouchbaseMembership, CouchbaseMembershipConfig};
pub use store::{
    CouchbaseKvMembershipStore, CouchbaseRegistryCollection, MembershipStore,
    MembershipStoreFuture, StoreWriteResult, StoredRegistryDocument,
};

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Mutex as StdMutex,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use bytes::Bytes;

    use super::*;

    struct MemoryStore {
        state: StdMutex<MemoryStoreState>,
        fail_loads: AtomicBool,
    }

    impl Default for MemoryStore {
        fn default() -> Self {
            Self {
                state: StdMutex::new(MemoryStoreState::default()),
                fail_loads: AtomicBool::new(false),
            }
        }
    }

    #[derive(Default)]
    struct MemoryStoreState {
        document: Option<StoredRegistryDocument>,
        next_cas: u64,
        conflicts_remaining: usize,
    }

    impl MemoryStore {
        fn with_conflicts(conflicts_remaining: usize) -> Self {
            Self {
                state: StdMutex::new(MemoryStoreState {
                    conflicts_remaining,
                    ..MemoryStoreState::default()
                }),
                fail_loads: AtomicBool::new(false),
            }
        }

        fn set_load_failure(&self, failed: bool) {
            self.fail_loads.store(failed, Ordering::SeqCst);
        }

        fn registered_member_count(&self) -> usize {
            let state = self.state.lock().unwrap();
            let Some(document) = &state.document else {
                return 0;
            };
            serde_json::from_slice::<serde_json::Value>(&document.value).unwrap()["members"]
                .as_object()
                .unwrap()
                .len()
        }

        fn write(&self, value: Bytes, expected_cas: Option<u64>) -> StoreWriteResult {
            let mut state = self.state.lock().unwrap();
            if state.conflicts_remaining != 0 {
                state.conflicts_remaining -= 1;
                return StoreWriteResult::Conflict;
            }
            let matches = match (expected_cas, state.document.as_ref()) {
                (None, None) => true,
                (Some(expected), Some(document)) => document.cas == expected,
                _ => false,
            };
            if !matches {
                return StoreWriteResult::Conflict;
            }
            state.next_cas += 1;
            let cas = state.next_cas;
            state.document = Some(StoredRegistryDocument { value, cas });
            StoreWriteResult::Stored
        }
    }

    impl MembershipStore for MemoryStore {
        fn load<'a>(
            &'a self,
            _key: &'a str,
        ) -> MembershipStoreFuture<'a, Option<StoredRegistryDocument>> {
            Box::pin(async move {
                if self.fail_loads.load(Ordering::SeqCst) {
                    return Err(CouchbaseMembershipError::Store(
                        "injected load failure".into(),
                    ));
                }
                Ok(self.state.lock().unwrap().document.clone())
            })
        }

        fn create<'a>(
            &'a self,
            _key: &'a str,
            value: Bytes,
        ) -> MembershipStoreFuture<'a, StoreWriteResult> {
            Box::pin(async move { Ok(self.write(value, None)) })
        }

        fn replace<'a>(
            &'a self,
            _key: &'a str,
            value: Bytes,
            cas: u64,
        ) -> MembershipStoreFuture<'a, StoreWriteResult> {
            Box::pin(async move { Ok(self.write(value, Some(cas))) })
        }
    }

    fn runtime_config(member: &str, incarnation: &str) -> CouchbaseMembershipConfig {
        CouchbaseMembershipConfig::new(
            "runtime-test",
            MemberIdentity::new(member, incarnation).unwrap(),
            8,
        )
        .unwrap()
        .heartbeat_interval(Duration::from_millis(10))
        .monitor_interval(Duration::from_millis(10))
        .stale_after(Duration::from_secs(2))
        .rebalance_delay(Duration::ZERO)
        .operation_timeout(Duration::from_secs(1))
    }

    #[test]
    fn registry_generation_changes_only_for_membership_fences() {
        let mut registry = RegistryDocument::new(10, 100).unwrap();
        let alice = MemberIdentity::new("alice", "incarnation-a").unwrap();
        let bob = MemberIdentity::new("bob", "incarnation-b").unwrap();

        registry.register(&alice, 10).unwrap();
        registry.register(&bob, 20).unwrap();
        assert_eq!(registry.generation(), 2);
        registry.heartbeat(&alice, 30).unwrap();
        assert_eq!(registry.generation(), 2);

        let alice_view = registry.snapshot(&alice).unwrap();
        let bob_view = registry.snapshot(&bob).unwrap();
        assert_eq!(
            alice_view.assignment().vbuckets().collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4]
        );
        assert_eq!(
            bob_view.assignment().vbuckets().collect::<Vec<_>>(),
            vec![5, 6, 7, 8, 9]
        );

        registry.heartbeat(&bob, 150).unwrap();
        assert_eq!(registry.prune_stale(150).unwrap(), 1);
        assert_eq!(registry.generation(), 3);
        assert_eq!(
            registry
                .snapshot(&bob)
                .unwrap()
                .assignment()
                .vbuckets()
                .collect::<Vec<_>>(),
            (0..10).collect::<Vec<_>>()
        );
    }

    #[test]
    fn live_duplicate_is_rejected_but_stale_incarnation_is_fenced() {
        let mut registry = RegistryDocument::new(16, 100).unwrap();
        let original = MemberIdentity::new("worker", "old").unwrap();
        let replacement = MemberIdentity::new("worker", "new").unwrap();

        registry.register(&original, 10).unwrap();
        assert!(matches!(
            registry.register(&replacement, 50),
            Err(CouchbaseMembershipError::DuplicateMember { .. })
        ));
        registry.register(&replacement, 111).unwrap();

        assert_eq!(registry.generation(), 2);
        assert!(matches!(
            registry.heartbeat(&original, 112),
            Err(CouchbaseMembershipError::Fenced { .. })
        ));
    }

    #[test]
    fn generation_overflow_never_partially_mutates_the_registry() {
        let identity = MemberIdentity::new("worker", "incarnation").unwrap();
        let mut registry: RegistryDocument = serde_json::from_value(serde_json::json!({
            "schemaVersion": 1,
            "vbucketCount": 8,
            "staleAfterMillis": 100,
            "generation": u64::MAX,
            "members": {
                "worker": {
                    "memberId": "worker",
                    "incarnation": "incarnation",
                    "joinedAtMillis": 1,
                    "heartbeatAtMillis": 1
                }
            }
        }))
        .unwrap();
        let before = registry.clone();

        assert!(registry.prune_stale(101).is_err());
        assert_eq!(registry, before);
        assert!(registry.remove(&identity).is_err());
        assert_eq!(registry, before);

        let mut empty: RegistryDocument = serde_json::from_value(serde_json::json!({
            "schemaVersion": 1,
            "vbucketCount": 8,
            "staleAfterMillis": 100,
            "generation": u64::MAX,
            "members": {}
        }))
        .unwrap();
        let before = empty.clone();
        assert!(empty.register(&identity, 10).is_err());
        assert_eq!(empty, before);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tokio_runtime_retries_a_cas_conflict_before_publishing_assignment() {
        let store = Arc::new(MemoryStore::with_conflicts(1));
        let membership =
            CouchbaseMembership::with_store(runtime_config("worker-a", "incarnation-a"), store)
                .await
                .unwrap();

        assert_eq!(membership.snapshot().assignment().generation(), 1);
        assert_eq!(
            membership
                .snapshot()
                .assignment()
                .vbuckets()
                .collect::<Vec<_>>(),
            (0..8).collect::<Vec<_>>()
        );
        membership.close().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tokio_members_observe_non_overlapping_rebalanced_assignments_and_graceful_leave() {
        let store = Arc::new(MemoryStore::default());
        let alice = CouchbaseMembership::with_store(
            runtime_config("alice", "incarnation-a"),
            store.clone(),
        )
        .await
        .unwrap();
        let bob = CouchbaseMembership::with_store(runtime_config("bob", "incarnation-b"), store)
            .await
            .unwrap();

        let mut alice_updates = alice.subscribe();
        tokio::time::timeout(Duration::from_secs(1), async {
            while alice_updates.borrow().members().len() != 2 {
                alice_updates.changed().await.unwrap();
            }
        })
        .await
        .unwrap();

        let alice_vbuckets = alice_updates
            .borrow()
            .assignment()
            .vbuckets()
            .collect::<Vec<_>>();
        let bob_vbuckets = bob.snapshot().assignment().vbuckets().collect::<Vec<_>>();
        assert_eq!(alice_vbuckets, vec![0, 1, 2, 3]);
        assert_eq!(bob_vbuckets, vec![4, 5, 6, 7]);

        bob.close().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while alice_updates.borrow().members().len() != 1 {
                alice_updates.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        assert_eq!(
            alice_updates
                .borrow()
                .assignment()
                .vbuckets()
                .collect::<Vec<_>>(),
            (0..8).collect::<Vec<_>>()
        );
        alice.close().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn live_duplicate_incarnation_is_rejected_by_the_tokio_runtime() {
        let store = Arc::new(MemoryStore::default());
        let original =
            CouchbaseMembership::with_store(runtime_config("worker", "original"), store.clone())
                .await
                .unwrap();

        let duplicate =
            CouchbaseMembership::with_store(runtime_config("worker", "duplicate"), store)
                .await
                .unwrap_err();
        assert!(matches!(
            duplicate,
            CouchbaseMembershipError::DuplicateMember { .. }
        ));
        original.close().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cas_retry_budget_is_bounded() {
        let store = Arc::new(MemoryStore::with_conflicts(3));
        let config = runtime_config("worker", "incarnation")
            .max_cas_attempts(std::num::NonZeroUsize::new(2).unwrap());

        let error = CouchbaseMembership::with_store(config, store)
            .await
            .unwrap_err();
        assert!(matches!(error, CouchbaseMembershipError::Store(_)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn heartbeat_writes_do_not_publish_false_assignment_changes() {
        let membership = CouchbaseMembership::with_store(
            runtime_config("worker", "incarnation"),
            Arc::new(MemoryStore::default()),
        )
        .await
        .unwrap();
        let updates = membership.subscribe();

        tokio::time::sleep(Duration::from_millis(35)).await;
        assert!(!updates.has_changed().unwrap());
        membership.close().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn transient_store_failure_recovers_before_the_membership_lease_expires() {
        let store = Arc::new(MemoryStore::default());
        let config =
            runtime_config("worker", "incarnation").stale_after(Duration::from_millis(100));
        let membership = CouchbaseMembership::with_store(config, store.clone())
            .await
            .unwrap();
        let updates = membership.subscribe();

        store.set_load_failure(true);
        tokio::time::sleep(Duration::from_millis(35)).await;
        store.set_load_failure(false);
        tokio::time::sleep(Duration::from_millis(120)).await;

        assert!(updates.has_changed().is_ok());
        membership.close().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn persistent_store_failure_expires_the_membership_lease_and_closes_updates() {
        let store = Arc::new(MemoryStore::default());
        let config = runtime_config("worker", "incarnation").stale_after(Duration::from_millis(45));
        let membership = CouchbaseMembership::with_store(config, store.clone())
            .await
            .unwrap();
        let mut updates = membership.subscribe();
        store.set_load_failure(true);

        tokio::time::timeout(Duration::from_secs(1), async {
            while updates.changed().await.is_ok() {}
        })
        .await
        .unwrap();
        let error = membership.close().await.unwrap_err();
        assert!(matches!(
            error,
            CouchbaseMembershipError::LeaseExpired { .. }
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelling_connect_during_rebalance_delay_removes_the_registered_incarnation() {
        let store = Arc::new(MemoryStore::default());
        let config =
            runtime_config("worker", "incarnation").rebalance_delay(Duration::from_millis(200));
        let connect = tokio::spawn(CouchbaseMembership::with_store(config, store.clone()));

        tokio::time::timeout(Duration::from_secs(1), async {
            while store.registered_member_count() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        connect.abort();
        let _ = connect.await;

        tokio::time::timeout(Duration::from_secs(1), async {
            while store.registered_member_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
