//! High-level asynchronous client API contract.

use std::sync::Arc;

use futures_util::Stream;
use rust_dcp::{
    AssignmentMode, CheckpointStore, ClusterTopology, CouchbaseCheckpointCollectionSpec,
    CouchbaseCheckpointStore, CouchbaseKvCheckpointCollection, DcpClient, DcpConfig, DcpDelivery,
    DcpHealth, DcpHealthStatus, DcpMetrics, DcpSubscription, DcpSubscriptionSpec,
    FileCheckpointStore, RollbackMitigationConfig, TopologyNetwork,
};

fn assert_subscription_stream<S>()
where
    S: Stream<Item = rust_dcp::Result<DcpDelivery>>,
{
}

#[test]
fn umbrella_crate_exposes_the_tokio_client_lifecycle() {
    let store: Arc<dyn CheckpointStore> = Arc::new(
        FileCheckpointStore::new(std::env::temp_dir().join("rust-dcp-public-api.json")).unwrap(),
    );
    let spec = DcpSubscriptionSpec::standalone(store).stream_id(Some(7));

    assert!(matches!(spec.assignment(), AssignmentMode::Standalone));
    assert_eq!(spec.stream_id_value(), Some(7));
    assert_subscription_stream::<DcpSubscription>();
    let _ = DcpClient::connect;
    let _ = std::any::type_name::<DcpDelivery>();
    let _ = std::any::type_name::<DcpHealth>();
    let _ = std::any::type_name::<DcpMetrics>();
    let _ = std::any::type_name::<RollbackMitigationConfig>();
    let _ = std::any::type_name::<ClusterTopology>();
    let _ = std::any::type_name::<TopologyNetwork>();
    let _ = std::any::type_name::<CouchbaseCheckpointCollectionSpec>();
    let _ = std::any::type_name::<CouchbaseKvCheckpointCollection>();
    let _: fn(DcpConfig, String) -> rust_dcp::Result<CouchbaseCheckpointStore> =
        CouchbaseCheckpointStore::from_config;
    let _: fn(
        DcpConfig,
        CouchbaseCheckpointCollectionSpec,
        String,
    ) -> rust_dcp::Result<CouchbaseCheckpointStore> =
        CouchbaseCheckpointStore::from_config_in_collection;
    assert_ne!(DcpHealthStatus::Starting, DcpHealthStatus::Stopped);
}
