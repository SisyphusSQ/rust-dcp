//! High-level asynchronous client API contract.

use std::sync::Arc;

use futures_util::Stream;
use rust_dcp::{
    AssignmentMode, CheckpointStore, ClusterTopology, DcpClient, DcpDelivery, DcpHealth,
    DcpHealthStatus, DcpMetrics, DcpSubscription, DcpSubscriptionSpec, FileCheckpointStore,
    TopologyNetwork,
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
    let _ = std::any::type_name::<ClusterTopology>();
    let _ = std::any::type_name::<TopologyNetwork>();
    assert_ne!(DcpHealthStatus::Starting, DcpHealthStatus::Stopped);
}
