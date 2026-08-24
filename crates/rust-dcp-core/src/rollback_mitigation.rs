use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc, time::Duration};

use futures_util::{StreamExt, stream::FuturesUnordered};
use tokio::{
    sync::{Mutex, watch},
    task::JoinHandle,
    time::{self, Instant},
};

use crate::{
    ClusterTopology, DcpCheckpoint, DcpConfig, DcpError, DcpEvent, KvSession, NodeId, Result,
    RollbackMitigationConfig, bootstrap_kv_connection,
};
use rust_dcp_protocol::{ObserveSeqNoResponse, observe_seqno, parse_observe_seqno};

const MITIGATION_CLIENT_NAME: &str = concat!("rust-dcp-mitigation/", env!("CARGO_PKG_VERSION"));

pub(crate) type MitigationSourceFuture<'a> =
    Pin<Box<dyn Future<Output = ObservationBatch> + Send + 'a>>;
pub(crate) type ObservationBatch = BTreeMap<u16, ObservationOutcome>;
type RawObservation = (
    ObservationTarget,
    std::result::Result<ObserveSeqNoResponse, String>,
);
type NodeObservationBatch = (NodeId, Option<KvSession>, Vec<RawObservation>);

pub(crate) trait MitigationSource: Send + Sync {
    fn observe(&self) -> MitigationSourceFuture<'_>;
}

#[derive(Clone, Debug)]
struct NodeObservationPlan {
    address: String,
    targets: Vec<ObservationTarget>,
}

struct TokioMitigationSource {
    config: DcpConfig,
    expected_copies: BTreeMap<u16, usize>,
    nodes: BTreeMap<NodeId, NodeObservationPlan>,
    sessions: Mutex<BTreeMap<NodeId, KvSession>>,
}

impl TokioMitigationSource {
    fn new(
        mut config: DcpConfig,
        topology: &ClusterTopology,
        branches: &BTreeMap<u16, u64>,
    ) -> Result<Self> {
        let mut expected_copies = BTreeMap::new();
        let mut nodes = BTreeMap::<NodeId, NodeObservationPlan>::new();
        for (&vbucket, &expected_vbucket_uuid) in branches {
            if expected_vbucket_uuid == 0 {
                return Err(DcpError::RollbackMitigation {
                    vbucket,
                    message: "effective checkpoint has no history branch UUID".into(),
                });
            }
            let copies = std::iter::once(topology.active_node(vbucket)?.clone())
                .chain(topology.replica_nodes(vbucket)?.iter().cloned())
                .collect::<std::collections::BTreeSet<_>>();
            expected_copies.insert(vbucket, copies.len());
            for node in copies {
                let endpoint = topology.endpoints().get(&node).ok_or_else(|| {
                    DcpError::Topology(format!(
                        "rollback mitigation node {node} for vBucket {vbucket} has no endpoint"
                    ))
                })?;
                nodes
                    .entry(node)
                    .or_insert_with(|| NodeObservationPlan {
                        address: endpoint.address().to_owned(),
                        targets: Vec::new(),
                    })
                    .targets
                    .push(ObservationTarget {
                        vbucket,
                        expected_vbucket_uuid,
                    });
            }
        }
        for plan in nodes.values_mut() {
            plan.targets.sort_unstable_by_key(|target| target.vbucket);
        }
        config.connect_timeout = config.rollback_mitigation.request_timeout;
        Ok(Self {
            config,
            expected_copies,
            nodes,
            sessions: Mutex::new(BTreeMap::new()),
        })
    }

    async fn observe_cycle(&self) -> ObservationBatch {
        let mut available_sessions = {
            let mut sessions = self.sessions.lock().await;
            std::mem::take(&mut *sessions)
        };
        let mut pending = FuturesUnordered::new();
        for (node, plan) in &self.nodes {
            pending.push(observe_node(
                self.config.clone(),
                node.clone(),
                plan.clone(),
                available_sessions.remove(node),
            ));
        }

        let mut reusable_sessions = BTreeMap::new();
        let mut observations = Vec::new();
        while let Some((node, session, node_observations)) = pending.next().await {
            if let Some(session) = session {
                reusable_sessions.insert(node, session);
            }
            observations.extend(node_observations);
        }
        self.sessions.lock().await.extend(reusable_sessions);
        aggregate_observations(&self.expected_copies, observations)
    }
}

async fn observe_node(
    config: DcpConfig,
    node: NodeId,
    plan: NodeObservationPlan,
    session: Option<KvSession>,
) -> NodeObservationBatch {
    let request_timeout = config.rollback_mitigation.request_timeout;
    let targets = plan.targets.clone();
    if let Ok(result) = time::timeout(
        request_timeout,
        observe_node_inner(config, node.clone(), plan, session),
    )
    .await
    {
        result
    } else {
        let mut observations = Vec::with_capacity(targets.len());
        push_node_failure(
            &mut observations,
            &targets,
            &format!("node observation exceeded {request_timeout:?}"),
        );
        (node, None, observations)
    }
}

async fn observe_node_inner(
    mut config: DcpConfig,
    node: NodeId,
    plan: NodeObservationPlan,
    session: Option<KvSession>,
) -> NodeObservationBatch {
    let mut observations = Vec::with_capacity(plan.targets.len());
    let mut session = if let Some(session) = session {
        session
    } else {
        let seed: crate::SeedAddress = match plan.address.parse() {
            Ok(seed) => seed,
            Err(error) => {
                push_node_failure(&mut observations, &plan.targets, &error.to_string());
                return (node, None, observations);
            }
        };
        config.seeds = vec![seed];
        match bootstrap_kv_connection(&config, MITIGATION_CLIENT_NAME).await {
            Ok(session) => session,
            Err(error) => {
                push_node_failure(&mut observations, &plan.targets, &error.to_string());
                return (node, None, observations);
            }
        }
    };
    let requests = plan
        .targets
        .iter()
        .map(|target| observe_seqno(target.vbucket, target.expected_vbucket_uuid, 0))
        .collect();
    match session.connection_mut().request_batch(requests).await {
        Ok(responses) => {
            observations.extend(plan.targets.iter().copied().zip(
                responses.iter().map(|response| {
                    parse_observe_seqno(response).map_err(|error| error.to_string())
                }),
            ));
            (node, Some(session), observations)
        }
        Err(error) => {
            push_node_failure(&mut observations, &plan.targets, &error.to_string());
            (node, None, observations)
        }
    }
}

impl MitigationSource for TokioMitigationSource {
    fn observe(&self) -> MitigationSourceFuture<'_> {
        Box::pin(self.observe_cycle())
    }
}

fn push_node_failure(
    observations: &mut Vec<RawObservation>,
    targets: &[ObservationTarget],
    message: &str,
) {
    observations.extend(
        targets
            .iter()
            .copied()
            .map(|target| (target, Err(message.to_owned()))),
    );
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ObservationOutcome {
    Persisted {
        vbucket_uuid: u64,
        persisted_seqno: u64,
    },
    Transient {
        message: String,
    },
    BranchChanged {
        observed_vbucket_uuid: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ObservationTarget {
    vbucket: u16,
    expected_vbucket_uuid: u64,
}

fn aggregate_observations(
    expected_copies: &BTreeMap<u16, usize>,
    observations: Vec<RawObservation>,
) -> ObservationBatch {
    let mut grouped = BTreeMap::<u16, Vec<RawObservation>>::new();
    for (target, result) in observations {
        grouped
            .entry(target.vbucket)
            .or_default()
            .push((target, result));
    }

    expected_copies
        .iter()
        .map(|(&vbucket, &expected_count)| {
            let results = grouped.remove(&vbucket).unwrap_or_default();
            let mut minimum = None::<u64>;
            let mut expected_uuid = None;
            let mut transient = None;
            let mut changed_branch = None;
            for (target, result) in &results {
                expected_uuid.get_or_insert(target.expected_vbucket_uuid);
                match result {
                    Ok(observed)
                        if observed.did_failover
                            || observed.vbucket_uuid != target.expected_vbucket_uuid =>
                    {
                        changed_branch = Some(observed.vbucket_uuid);
                    }
                    Ok(observed) if observed.vbucket != vbucket => {
                        transient = Some(format!(
                            "node returned vBucket {} for requested vBucket {vbucket}",
                            observed.vbucket
                        ));
                    }
                    Ok(observed) if observed.persisted_seqno > observed.current_seqno => {
                        transient = Some(format!(
                            "persisted seqno {} exceeds current seqno {}",
                            observed.persisted_seqno, observed.current_seqno
                        ));
                    }
                    Ok(observed) => {
                        minimum = Some(minimum.map_or(observed.persisted_seqno, |current| {
                            current.min(observed.persisted_seqno)
                        }));
                    }
                    Err(message) => transient = Some(message.clone()),
                }
            }
            let outcome = if let Some(observed_vbucket_uuid) = changed_branch {
                ObservationOutcome::BranchChanged {
                    observed_vbucket_uuid,
                }
            } else if results.len() != expected_count {
                ObservationOutcome::Transient {
                    message: format!(
                        "received {} of {expected_count} required copy observations",
                        results.len()
                    ),
                }
            } else if let Some(message) = transient {
                ObservationOutcome::Transient { message }
            } else if let (Some(vbucket_uuid), Some(persisted_seqno)) = (expected_uuid, minimum) {
                ObservationOutcome::Persisted {
                    vbucket_uuid,
                    persisted_seqno,
                }
            } else {
                ObservationOutcome::Transient {
                    message: "observation cycle contained no successful copy result".into(),
                }
            };
            (vbucket, outcome)
        })
        .collect()
}

#[derive(Clone, Debug)]
struct PartitionState {
    expected_vbucket_uuid: u64,
    persisted_seqno: Option<u64>,
    last_error: Option<String>,
}

#[derive(Clone, Debug)]
struct MitigationSnapshot {
    partitions: BTreeMap<u16, PartitionState>,
}

impl MitigationSnapshot {
    fn new(branches: BTreeMap<u16, u64>) -> Result<Self> {
        if branches.is_empty() {
            return Err(DcpError::InvalidConfiguration(
                "rollback mitigation requires at least one vBucket".into(),
            ));
        }
        Ok(Self {
            partitions: branches
                .into_iter()
                .map(|(vbucket, expected_vbucket_uuid)| {
                    (
                        vbucket,
                        PartitionState {
                            expected_vbucket_uuid,
                            persisted_seqno: None,
                            last_error: None,
                        },
                    )
                })
                .collect(),
        })
    }

    fn apply(&mut self, batch: &ObservationBatch) {
        for (&vbucket, state) in &mut self.partitions {
            let Some(outcome) = batch.get(&vbucket) else {
                state.last_error = Some("observation cycle omitted the vBucket".into());
                continue;
            };
            match outcome {
                ObservationOutcome::Persisted {
                    vbucket_uuid,
                    persisted_seqno,
                } => {
                    if *vbucket_uuid != state.expected_vbucket_uuid {
                        state.persisted_seqno = None;
                        state.last_error = Some(format!(
                            "expected history branch {}, observed {vbucket_uuid}",
                            state.expected_vbucket_uuid
                        ));
                        continue;
                    }
                    if let Some(previous) = state
                        .persisted_seqno
                        .filter(|previous| *persisted_seqno < *previous)
                    {
                        state.persisted_seqno = None;
                        state.last_error = Some(format!(
                            "persisted sequence number regressed from {previous} to {persisted_seqno}"
                        ));
                        continue;
                    }
                    state.persisted_seqno = Some(*persisted_seqno);
                    state.last_error = None;
                }
                ObservationOutcome::Transient { message } => {
                    state.last_error = Some(message.clone());
                }
                ObservationOutcome::BranchChanged {
                    observed_vbucket_uuid,
                } => {
                    state.persisted_seqno = None;
                    state.last_error = Some(format!(
                        "expected history branch {}, observed {observed_vbucket_uuid}",
                        state.expected_vbucket_uuid
                    ));
                }
            }
        }
    }
}

pub(crate) struct RollbackMitigator {
    config: RollbackMitigationConfig,
    snapshot: watch::Receiver<MitigationSnapshot>,
    cancel: watch::Sender<bool>,
    worker: Option<JoinHandle<()>>,
}

impl RollbackMitigator {
    pub(crate) fn spawn(
        config: RollbackMitigationConfig,
        source: Arc<dyn MitigationSource>,
        branches: BTreeMap<u16, u64>,
    ) -> Result<Self> {
        let initial = MitigationSnapshot::new(branches)?;
        let (snapshot_sender, snapshot) = watch::channel(initial);
        let (cancel, cancel_receiver) = watch::channel(false);
        let poll_interval = config.poll_interval;
        let worker = tokio::spawn(run_observer(
            source,
            snapshot_sender,
            cancel_receiver,
            poll_interval,
        ));
        Ok(Self {
            config,
            snapshot,
            cancel,
            worker: Some(worker),
        })
    }

    pub(crate) async fn wait_until_safe(&mut self, vbucket: u16, seqno: u64) -> Result<bool> {
        if seqno == 0 {
            return Ok(false);
        }
        let deadline = Instant::now() + self.config.maximum_stall;
        let mut delayed = false;
        loop {
            let safe = {
                let snapshot = self.snapshot.borrow();
                let state = snapshot.partitions.get(&vbucket).ok_or_else(|| {
                    DcpError::RollbackMitigation {
                        vbucket,
                        message: "vBucket is absent from the mitigation generation".into(),
                    }
                })?;
                state
                    .persisted_seqno
                    .is_some_and(|persisted| persisted >= seqno)
            };
            if safe {
                return Ok(delayed);
            }
            if Instant::now() >= deadline {
                let context = self
                    .snapshot
                    .borrow()
                    .partitions
                    .get(&vbucket)
                    .and_then(|state| state.last_error.clone())
                    .unwrap_or_else(|| format!("persisted sequence number did not reach {seqno}"));
                return Err(DcpError::RollbackMitigation {
                    vbucket,
                    message: format!(
                        "delivery did not become persistence-safe within {:?}: {context}",
                        self.config.maximum_stall
                    ),
                });
            }
            delayed = true;
            tokio::select! {
                changed = self.snapshot.changed() => {
                    if changed.is_err() {
                        return Err(DcpError::Cancelled);
                    }
                }
                () = time::sleep_until(deadline) => {}
            }
        }
    }

    pub(crate) async fn close(mut self) -> Result<()> {
        let _ = self.cancel.send(true);
        if let Some(worker) = self.worker.take() {
            worker.await.map_err(|error| DcpError::RollbackMitigation {
                vbucket: 0,
                message: format!("observer task failed: {error}"),
            })?;
        }
        Ok(())
    }
}

pub(crate) fn spawn_tokio_mitigator(
    config: &DcpConfig,
    topology: &ClusterTopology,
    checkpoints: &BTreeMap<u16, DcpCheckpoint>,
) -> Result<Option<RollbackMitigator>> {
    if !config.rollback_mitigation.enabled {
        return Ok(None);
    }
    let branches = checkpoints
        .iter()
        .map(|(&vbucket, checkpoint)| (vbucket, checkpoint.vbucket_uuid))
        .collect::<BTreeMap<_, _>>();
    let source = Arc::new(TokioMitigationSource::new(
        config.clone(),
        topology,
        &branches,
    )?);
    RollbackMitigator::spawn(config.rollback_mitigation.clone(), source, branches).map(Some)
}

pub(crate) fn mitigation_position(event: &DcpEvent) -> Option<(u16, u64)> {
    match event {
        DcpEvent::SnapshotMarker(marker) => Some((marker.vbucket, marker.start_seqno)),
        event => event.seqno().map(|seqno| (event.vbucket(), seqno)),
    }
}

impl Drop for RollbackMitigator {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
        if let Some(worker) = self.worker.take() {
            worker.abort();
        }
    }
}

async fn run_observer(
    source: Arc<dyn MitigationSource>,
    snapshots: watch::Sender<MitigationSnapshot>,
    mut cancel: watch::Receiver<bool>,
    poll_interval: Duration,
) {
    loop {
        let observation = source.observe();
        let batch = tokio::select! {
            biased;
            changed = cancel.changed() => {
                if changed.is_err() || *cancel.borrow() {
                    return;
                }
                continue;
            }
            batch = observation => batch,
        };
        let mut next = snapshots.borrow().clone();
        next.apply(&batch);
        snapshots.send_replace(next);
        tokio::select! {
            biased;
            changed = cancel.changed() => {
                if changed.is_err() || *cancel.borrow() {
                    return;
                }
            }
            () = time::sleep(poll_interval) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use super::*;
    use rust_dcp_protocol::ObserveSeqNoResponse;

    #[derive(Default)]
    struct MutableSource {
        batch: Mutex<ObservationBatch>,
    }

    impl MutableSource {
        fn set_persisted(&self, vbucket: u16, vbucket_uuid: u64, persisted_seqno: u64) {
            self.batch.lock().unwrap().insert(
                vbucket,
                ObservationOutcome::Persisted {
                    vbucket_uuid,
                    persisted_seqno,
                },
            );
        }

        fn set_branch_changed(&self, vbucket: u16, observed_vbucket_uuid: u64) {
            self.batch.lock().unwrap().insert(
                vbucket,
                ObservationOutcome::BranchChanged {
                    observed_vbucket_uuid,
                },
            );
        }
    }

    impl MitigationSource for MutableSource {
        fn observe(&self) -> MitigationSourceFuture<'_> {
            let batch = self.batch.lock().unwrap().clone();
            Box::pin(async move { batch })
        }
    }

    struct PendingSource;

    impl MitigationSource for PendingSource {
        fn observe(&self) -> MitigationSourceFuture<'_> {
            Box::pin(std::future::pending())
        }
    }

    fn mitigation_config() -> crate::RollbackMitigationConfig {
        crate::RollbackMitigationConfig {
            enabled: true,
            poll_interval: Duration::from_millis(1),
            request_timeout: Duration::from_millis(10),
            maximum_stall: Duration::from_secs(1),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn delivery_waits_until_every_copy_has_persisted_the_event() {
        let source = Arc::new(MutableSource::default());
        source.set_persisted(7, 99, 40);
        let mut mitigation = RollbackMitigator::spawn(
            mitigation_config(),
            source.clone(),
            BTreeMap::from([(7, 99)]),
        )
        .unwrap();

        assert!(
            tokio::time::timeout(Duration::from_millis(20), mitigation.wait_until_safe(7, 41))
                .await
                .is_err()
        );
        source.set_persisted(7, 99, 41);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), mitigation.wait_until_safe(7, 41))
                .await
                .unwrap()
                .unwrap()
        );
        mitigation.close().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn changed_history_branch_fails_the_wait_without_releasing_the_event() {
        let source = Arc::new(MutableSource::default());
        source.set_branch_changed(7, 100);
        let mut config = mitigation_config();
        config.request_timeout = Duration::from_millis(5);
        config.maximum_stall = Duration::from_millis(30);
        let mut mitigation =
            RollbackMitigator::spawn(config, source, BTreeMap::from([(7, 99)])).unwrap();

        let error = tokio::time::timeout(Duration::from_secs(1), mitigation.wait_until_safe(7, 41))
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(error, crate::DcpError::RollbackMitigation { .. }));
        mitigation.close().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn changed_history_invalidates_a_previously_safe_watermark() {
        let source = Arc::new(MutableSource::default());
        source.set_persisted(7, 99, 50);
        let mut config = mitigation_config();
        config.request_timeout = Duration::from_millis(5);
        config.maximum_stall = Duration::from_millis(30);
        let mut mitigation =
            RollbackMitigator::spawn(config, source.clone(), BTreeMap::from([(7, 99)])).unwrap();
        mitigation.wait_until_safe(7, 45).await.unwrap();

        source.set_branch_changed(7, 100);
        tokio::time::timeout(Duration::from_millis(100), async {
            loop {
                if mitigation.snapshot.borrow().partitions[&7]
                    .last_error
                    .is_some()
                {
                    break;
                }
                mitigation.snapshot.changed().await.unwrap();
            }
        })
        .await
        .unwrap();

        assert!(mitigation.wait_until_safe(7, 45).await.is_err());
        mitigation.close().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stagnant_persistence_fails_within_the_configured_delivery_stall() {
        let source = Arc::new(MutableSource::default());
        source.set_persisted(7, 99, 40);
        let mut config = mitigation_config();
        config.request_timeout = Duration::from_millis(5);
        config.maximum_stall = Duration::from_millis(30);
        let mut mitigation =
            RollbackMitigator::spawn(config, source, BTreeMap::from([(7, 99)])).unwrap();

        let error = tokio::time::timeout(
            Duration::from_millis(250),
            mitigation.wait_until_safe(7, 41),
        )
        .await
        .expect("the bounded mitigation wait must not hang")
        .unwrap_err();

        assert!(matches!(error, crate::DcpError::RollbackMitigation { .. }));
        mitigation.close().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn close_cancels_an_in_flight_observation() {
        let mitigation = RollbackMitigator::spawn(
            mitigation_config(),
            Arc::new(PendingSource),
            BTreeMap::from([(7, 99)]),
        )
        .unwrap();

        tokio::time::timeout(Duration::from_millis(100), mitigation.close())
            .await
            .expect("closing mitigation must cancel the source future")
            .unwrap();
    }

    #[test]
    fn active_and_replica_observations_use_the_minimum_persisted_seqno() {
        let target = ObservationTarget {
            vbucket: 7,
            expected_vbucket_uuid: 99,
        };
        let observed = |persisted_seqno| ObserveSeqNoResponse {
            did_failover: false,
            vbucket: 7,
            vbucket_uuid: 99,
            persisted_seqno,
            current_seqno: 50,
            old_vbucket_uuid: None,
            last_seqno: None,
        };
        let batch = aggregate_observations(
            &BTreeMap::from([(7, 3)]),
            vec![
                (target, Ok(observed(45))),
                (target, Ok(observed(42))),
                (target, Ok(observed(44))),
            ],
        );

        assert_eq!(
            batch.get(&7),
            Some(&ObservationOutcome::Persisted {
                vbucket_uuid: 99,
                persisted_seqno: 42,
            })
        );
    }

    #[test]
    fn incomplete_copy_observation_is_transient() {
        let target = ObservationTarget {
            vbucket: 7,
            expected_vbucket_uuid: 99,
        };
        let observed = ObserveSeqNoResponse {
            did_failover: false,
            vbucket: 7,
            vbucket_uuid: 99,
            persisted_seqno: 45,
            current_seqno: 50,
            old_vbucket_uuid: None,
            last_seqno: None,
        };

        let batch = aggregate_observations(&BTreeMap::from([(7, 2)]), vec![(target, Ok(observed))]);

        assert!(matches!(
            batch.get(&7),
            Some(ObservationOutcome::Transient { .. })
        ));
    }

    #[test]
    fn observation_plan_includes_active_and_every_available_replica() {
        let topology = ClusterTopology::from_json(
            br#"{
              "rev": 1,
              "name": "travel",
              "uuid": "bucket-uuid",
              "nodeLocator": "vbucket",
              "nodesExt": [
                {"hostname":"node-a","nodeUUID":"a","services":{"kv":11210}},
                {"hostname":"node-b","nodeUUID":"b","services":{"kv":11210}},
                {"hostname":"node-c","nodeUUID":"c","services":{"kv":11210}}
              ],
              "vBucketServerMap": {
                "hashAlgorithm": "CRC",
                "numReplicas": 2,
                "serverList": ["node-a:11210","node-b:11210","node-c:11210"],
                "vBucketMap": [[0,1,2],[1,2,-1]]
              }
            }"#,
            "node-a:11210",
            false,
            &crate::TopologyNetwork::Default,
        )
        .unwrap();
        let config = DcpConfig::builder(crate::Credentials::new("alice", "secret"), "travel")
            .seed("node-a:11210")
            .unwrap()
            .build()
            .unwrap();

        let source =
            TokioMitigationSource::new(config, &topology, &BTreeMap::from([(0, 99), (1, 100)]))
                .unwrap();

        assert_eq!(source.expected_copies, BTreeMap::from([(0, 3), (1, 2)]));
        assert_eq!(source.nodes.len(), 3);
        assert_eq!(
            source
                .nodes
                .values()
                .map(|plan| plan.targets.len())
                .sum::<usize>(),
            5
        );
    }

    #[test]
    fn mitigation_positions_cover_control_progress_without_gating_stream_end() {
        let marker = DcpEvent::SnapshotMarker(crate::SnapshotMarker {
            vbucket: 7,
            start_seqno: 41,
            end_seqno: 45,
            flags: crate::SnapshotFlags::MEMORY,
            high_completed_seqno: None,
            max_visible_seqno: None,
            purge_seqno: None,
        });
        let advanced = DcpEvent::SeqNoAdvanced(crate::SeqNoAdvanced {
            vbucket: 7,
            seqno: 45,
        });
        let ended = DcpEvent::StreamEnd(crate::StreamEnd {
            vbucket: 7,
            reason: crate::StreamEndReason::Ok,
        });

        assert_eq!(mitigation_position(&marker), Some((7, 41)));
        assert_eq!(mitigation_position(&advanced), Some((7, 45)));
        assert_eq!(mitigation_position(&ended), None);
    }
}
