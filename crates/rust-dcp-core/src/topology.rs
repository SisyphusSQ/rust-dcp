use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use rust_dcp_protocol::{
    FailoverEntry, Frame, Opcode, ProtocolError, VBucketState, get_cluster_config,
    get_failover_log, get_vbucket_seqnos, parse_failover_log, parse_vbucket_seqnos,
};
use serde::Deserialize;

use crate::{DcpError, KvConnection, Result};

/// Stable identity for one KV node across topology revisions.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeId(String);

impl NodeId {
    /// Opaque node identity, normally a server UUID with a canonical endpoint
    /// fallback for older Couchbase versions.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Selected KV endpoint for one topology node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvEndpoint {
    id: NodeId,
    address: String,
    canonical_address: String,
    server_group: Option<String>,
}

impl KvEndpoint {
    /// Stable node identity.
    #[must_use]
    pub const fn id(&self) -> &NodeId {
        &self.id
    }

    /// Dial address selected for the configured network and TLS mode.
    #[must_use]
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Default-network address for diagnostics and identity reconciliation.
    #[must_use]
    pub fn canonical_address(&self) -> &str {
        &self.canonical_address
    }

    /// Couchbase server group, when advertised by the cluster.
    #[must_use]
    pub fn server_group(&self) -> Option<&str> {
        self.server_group.as_deref()
    }
}

/// CCCP revision tuple used to reject stale topology updates.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TopologyRevision {
    epoch: u64,
    revision: u64,
}

impl TopologyRevision {
    /// Creates a revision tuple.
    #[must_use]
    pub const fn new(epoch: u64, revision: u64) -> Self {
        Self { epoch, revision }
    }

    /// Revision epoch, compared before the revision number.
    #[must_use]
    pub const fn epoch(self) -> u64 {
        self.epoch
    }

    /// Revision number within the epoch.
    #[must_use]
    pub const fn revision(self) -> u64 {
        self.revision
    }

    /// Couchbase-compatible freshness comparison.
    ///
    /// Revision zero is unversioned and is accepted within an equal epoch.
    #[must_use]
    pub const fn is_newer_than(self, current: Self) -> bool {
        self.epoch > current.epoch
            || (self.epoch == current.epoch
                && (self.revision == 0 || self.revision > current.revision))
    }
}

/// Network address set selected from CCCP `alternateAddresses`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum TopologyNetwork {
    /// Use the source endpoint to choose default or external addresses.
    #[default]
    Auto,
    /// Use canonical/default node addresses.
    Default,
    /// Use the conventional `external` alternate address set.
    External,
    /// Use another named alternate address set.
    Named(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VBucketRoute {
    active: NodeId,
    replicas: Vec<NodeId>,
}

/// Validated Couchbase bucket topology and active vBucket routes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterTopology {
    bucket: String,
    bucket_uuid: Option<String>,
    revision: TopologyRevision,
    network: String,
    endpoints: BTreeMap<NodeId, KvEndpoint>,
    vbuckets: BTreeMap<u16, VBucketRoute>,
}

impl ClusterTopology {
    /// Parses and validates one CCCP bucket-config JSON payload.
    ///
    /// # Errors
    ///
    /// Returns a topology error for malformed JSON, unsupported bucket types,
    /// missing addresses, invalid node indexes, or incomplete vBucket maps.
    pub fn from_json(
        payload: &[u8],
        source_address: &str,
        tls: bool,
        network: &TopologyNetwork,
    ) -> Result<Self> {
        let raw: RawBucketConfig = serde_json::from_slice(payload)
            .map_err(|error| DcpError::Topology(format!("invalid CCCP bucket config: {error}")))?;
        raw.build(source_address, tls, network)
    }

    /// Bucket name carried by this config.
    #[must_use]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Stable bucket UUID, when present on the server version.
    #[must_use]
    pub fn bucket_uuid(&self) -> Option<&str> {
        self.bucket_uuid.as_deref()
    }

    /// CCCP revision tuple.
    #[must_use]
    pub const fn revision(&self) -> TopologyRevision {
        self.revision
    }

    /// Resolved address-set name (`default`, `external`, or custom).
    #[must_use]
    pub fn network(&self) -> &str {
        &self.network
    }

    /// Ordered KV endpoints indexed by stable node identity.
    #[must_use]
    pub const fn endpoints(&self) -> &BTreeMap<NodeId, KvEndpoint> {
        &self.endpoints
    }

    /// Number of vBuckets in the bucket map.
    #[must_use]
    pub fn num_vbuckets(&self) -> usize {
        self.vbuckets.len()
    }

    /// Active node identity for one vBucket.
    ///
    /// # Errors
    ///
    /// Returns a topology error when `vbucket` is outside the current map.
    pub fn active_node(&self, vbucket: u16) -> Result<&NodeId> {
        self.vbuckets
            .get(&vbucket)
            .map(|route| &route.active)
            .ok_or_else(|| DcpError::Topology(format!("vBucket {vbucket} is not in the topology")))
    }

    /// Replica node identities for one vBucket, in server preference order.
    ///
    /// # Errors
    ///
    /// Returns a topology error when `vbucket` is outside the current map.
    pub fn replica_nodes(&self, vbucket: u16) -> Result<&[NodeId]> {
        self.vbuckets
            .get(&vbucket)
            .map(|route| route.replicas.as_slice())
            .ok_or_else(|| DcpError::Topology(format!("vBucket {vbucket} is not in the topology")))
    }

    /// Endpoint currently active for one vBucket.
    ///
    /// # Errors
    ///
    /// Returns a topology error when the vBucket or its node is missing.
    pub fn active_endpoint(&self, vbucket: u16) -> Result<&KvEndpoint> {
        let node = self.active_node(vbucket)?;
        self.endpoints.get(node).ok_or_else(|| {
            DcpError::Topology(format!(
                "active node {node} for vBucket {vbucket} has no endpoint"
            ))
        })
    }

    /// Groups a requested vBucket set by active KV node.
    ///
    /// # Errors
    ///
    /// Returns a topology error if any requested vBucket is absent.
    pub fn active_vbuckets_by_node(
        &self,
        vbuckets: impl IntoIterator<Item = u16>,
    ) -> Result<BTreeMap<NodeId, Vec<u16>>> {
        let mut grouped = BTreeMap::<NodeId, Vec<u16>>::new();
        for vbucket in vbuckets {
            grouped
                .entry(self.active_node(vbucket)?.clone())
                .or_default()
                .push(vbucket);
        }
        for partitions in grouped.values_mut() {
            partitions.sort_unstable();
            partitions.dedup();
        }
        Ok(grouped)
    }

    /// Groups every vBucket by active KV node.
    #[must_use]
    pub fn all_active_vbuckets_by_node(&self) -> BTreeMap<NodeId, Vec<u16>> {
        let mut grouped = BTreeMap::<NodeId, Vec<u16>>::new();
        for (vbucket, route) in &self.vbuckets {
            grouped
                .entry(route.active.clone())
                .or_default()
                .push(*vbucket);
        }
        grouped
    }
}

/// Reconnection and stream-reopen work produced by a topology update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopologyChange {
    generation: u64,
    added_nodes: BTreeSet<NodeId>,
    removed_nodes: BTreeSet<NodeId>,
    reconnect_nodes: BTreeSet<NodeId>,
    rerouted_vbuckets: BTreeSet<u16>,
}

impl TopologyChange {
    /// Monotonic local topology generation after the update.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Nodes newly present in the config.
    #[must_use]
    pub const fn added_nodes(&self) -> &BTreeSet<NodeId> {
        &self.added_nodes
    }

    /// Nodes no longer present in the config.
    #[must_use]
    pub const fn removed_nodes(&self) -> &BTreeSet<NodeId> {
        &self.removed_nodes
    }

    /// New nodes and existing nodes whose selected dial address changed.
    #[must_use]
    pub const fn reconnect_nodes(&self) -> &BTreeSet<NodeId> {
        &self.reconnect_nodes
    }

    /// vBuckets whose active node changed, appeared, or disappeared.
    #[must_use]
    pub const fn rerouted_vbuckets(&self) -> &BTreeSet<u16> {
        &self.rerouted_vbuckets
    }
}

/// Current topology plus a monotonic local generation fence.
#[derive(Clone, Debug)]
pub struct TopologyState {
    generation: u64,
    topology: ClusterTopology,
}

impl TopologyState {
    /// Creates state at local generation one.
    #[must_use]
    pub const fn new(topology: ClusterTopology) -> Self {
        Self {
            generation: 1,
            topology,
        }
    }

    /// Current local generation.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Current accepted topology.
    #[must_use]
    pub const fn topology(&self) -> &ClusterTopology {
        &self.topology
    }

    /// Applies a newer config and returns the exact connection/stream impact.
    /// Stale or identical revisions return `Ok(None)`.
    ///
    /// # Errors
    ///
    /// Returns a topology error if the candidate belongs to another bucket or
    /// the local generation counter overflows.
    pub fn apply(&mut self, candidate: ClusterTopology) -> Result<Option<TopologyChange>> {
        validate_bucket_identity(&self.topology, &candidate)?;
        if !candidate.revision.is_newer_than(self.topology.revision) {
            return Ok(None);
        }

        let current_nodes = self
            .topology
            .endpoints
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let candidate_nodes = candidate.endpoints.keys().cloned().collect::<BTreeSet<_>>();
        let added_nodes = candidate_nodes
            .difference(&current_nodes)
            .cloned()
            .collect::<BTreeSet<_>>();
        let removed_nodes = current_nodes
            .difference(&candidate_nodes)
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut reconnect_nodes = added_nodes.clone();
        for node in current_nodes.intersection(&candidate_nodes) {
            let old = &self.topology.endpoints[node];
            let new = &candidate.endpoints[node];
            if old.address != new.address {
                reconnect_nodes.insert(node.clone());
            }
        }

        let all_vbuckets = self
            .topology
            .vbuckets
            .keys()
            .chain(candidate.vbuckets.keys())
            .copied()
            .collect::<BTreeSet<_>>();
        let rerouted_vbuckets = all_vbuckets
            .into_iter()
            .filter(|vbucket| {
                self.topology
                    .vbuckets
                    .get(vbucket)
                    .map(|route| &route.active)
                    != candidate.vbuckets.get(vbucket).map(|route| &route.active)
            })
            .collect();

        self.generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| DcpError::Topology("topology generation overflow".into()))?;
        self.topology = candidate;

        Ok(Some(TopologyChange {
            generation: self.generation,
            added_nodes,
            removed_nodes,
            reconnect_nodes,
            rerouted_vbuckets,
        }))
    }
}

/// Fetches and parses the current CCCP config on an authenticated KV
/// connection.
///
/// # Errors
///
/// Returns a request, server-status, JSON, bucket-identity, or topology error.
pub async fn discover_topology(
    connection: &mut KvConnection,
    expected_bucket: &str,
    tls: bool,
    network: &TopologyNetwork,
) -> Result<ClusterTopology> {
    let source_address = connection.peer().to_owned();
    let response = connection.request(get_cluster_config(0)).await?;
    ensure_success_response(
        &response,
        Opcode::GET_CLUSTER_CONFIG,
        "cluster configuration",
    )?;
    let topology = ClusterTopology::from_json(&response.value, &source_address, tls, network)?;
    if topology.bucket != expected_bucket {
        return Err(DcpError::Topology(format!(
            "cluster config bucket {:?} does not match selected bucket {expected_bucket:?}",
            topology.bucket
        )));
    }
    Ok(topology)
}

/// Fetches active high sequence numbers visible from one KV node.
///
/// # Errors
///
/// Returns a request, server-status, malformed-response, or duplicate-vBucket
/// error.
pub async fn fetch_active_high_seqnos(
    connection: &mut KvConnection,
    collection_id: Option<u32>,
) -> Result<BTreeMap<u16, u64>> {
    let response = connection
        .request(get_vbucket_seqnos(VBucketState::Active, collection_id, 0))
        .await?;
    ensure_success_response(
        &response,
        Opcode::GET_ALL_VB_SEQNOS,
        "active high sequence numbers",
    )?;
    let mut result = BTreeMap::new();
    for entry in parse_vbucket_seqnos(&response)? {
        if result.insert(entry.vbucket, entry.seqno).is_some() {
            return Err(ProtocolError::MalformedDcp(format!(
                "duplicate high-seqno entry for vBucket {}",
                entry.vbucket
            ))
            .into());
        }
    }
    Ok(result)
}

/// Fetches the newest-first failover log for one active vBucket.
///
/// # Errors
///
/// Returns a request, server-status, malformed-response, or empty-log error.
pub async fn fetch_failover_log(
    connection: &mut KvConnection,
    vbucket: u16,
) -> Result<Vec<FailoverEntry>> {
    let response = connection.request(get_failover_log(vbucket, 0)).await?;
    ensure_success_response(
        &response,
        Opcode::DCP_GET_FAILOVER_LOG,
        "vBucket failover log",
    )?;
    let entries = parse_failover_log(&response)?;
    if entries.is_empty() {
        return Err(DcpError::Topology(format!(
            "server returned an empty failover log for vBucket {vbucket}"
        )));
    }
    Ok(entries)
}

fn validate_bucket_identity(current: &ClusterTopology, candidate: &ClusterTopology) -> Result<()> {
    if current.bucket != candidate.bucket {
        return Err(DcpError::Topology(format!(
            "topology bucket changed from {:?} to {:?}",
            current.bucket, candidate.bucket
        )));
    }
    if let (Some(current_uuid), Some(candidate_uuid)) =
        (&current.bucket_uuid, &candidate.bucket_uuid)
        && current_uuid != candidate_uuid
    {
        return Err(DcpError::Topology(format!(
            "topology bucket UUID changed from {current_uuid} to {candidate_uuid}"
        )));
    }
    Ok(())
}

fn ensure_success_response(response: &Frame, opcode: Opcode, context: &str) -> Result<()> {
    if !response.magic.is_response() || response.opcode != opcode {
        return Err(ProtocolError::MalformedFrame(format!(
            "expected response opcode 0x{:02x}, got magic 0x{:02x} opcode 0x{:02x}",
            opcode.as_u8(),
            response.magic.as_u8(),
            response.opcode.as_u8()
        ))
        .into());
    }
    if response.status.is_success() {
        return Ok(());
    }
    Err(DcpError::ServerStatus {
        status: response.status.as_u16(),
        opcode: response.opcode.as_u8(),
        message: if response.value.is_empty() {
            context.to_owned()
        } else {
            format!("{context}: {}", String::from_utf8_lossy(&response.value))
        },
    })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawBucketConfig {
    #[serde(default)]
    rev: i64,
    #[serde(default)]
    rev_epoch: i64,
    #[serde(default)]
    name: String,
    #[serde(default)]
    uuid: String,
    #[serde(default)]
    node_locator: String,
    #[serde(default)]
    nodes_ext: Vec<RawNodeExt>,
    v_bucket_server_map: RawVBucketServerMap,
}

impl RawBucketConfig {
    fn build(
        self,
        source_address: &str,
        tls: bool,
        requested_network: &TopologyNetwork,
    ) -> Result<ClusterTopology> {
        if self.rev < 0 || self.rev_epoch < 0 {
            return Err(DcpError::Topology(format!(
                "negative topology revision {}/{}",
                self.rev_epoch, self.rev
            )));
        }
        if self.name.trim().is_empty() {
            return Err(DcpError::Topology(
                "cluster config has no bucket name".into(),
            ));
        }
        if self.node_locator != "vbucket" {
            return Err(DcpError::Topology(format!(
                "unsupported node locator {:?}; DCP requires vbucket routing",
                self.node_locator
            )));
        }
        if self.v_bucket_server_map.hash_algorithm != "CRC" {
            return Err(DcpError::Topology(format!(
                "unsupported vBucket hash algorithm {:?}",
                self.v_bucket_server_map.hash_algorithm
            )));
        }

        let source_host = host_from_address(source_address)?;
        let network = resolve_network(
            requested_network,
            &self.nodes_ext,
            self.v_bucket_server_map.server_list.len(),
            source_address,
            &source_host,
            tls,
        )?;
        let endpoint_list = build_endpoints(
            &self.nodes_ext,
            &self.v_bucket_server_map.server_list,
            &source_host,
            tls,
            &network,
        )?;
        let endpoints = endpoint_list
            .iter()
            .cloned()
            .map(|endpoint| (endpoint.id.clone(), endpoint))
            .collect::<BTreeMap<_, _>>();
        if endpoints.len() != endpoint_list.len() {
            return Err(DcpError::Topology(
                "cluster config contains duplicate KV node identities".into(),
            ));
        }

        let routes = build_vbucket_routes(&self.v_bucket_server_map, &endpoint_list)?;
        let revision = u64::try_from(self.rev)
            .map_err(|error| DcpError::Topology(format!("invalid revision: {error}")))?;
        let revision_epoch = u64::try_from(self.rev_epoch)
            .map_err(|error| DcpError::Topology(format!("invalid revision epoch: {error}")))?;
        Ok(ClusterTopology {
            bucket: self.name,
            bucket_uuid: (!self.uuid.is_empty()).then_some(self.uuid),
            revision: TopologyRevision::new(revision_epoch, revision),
            network,
            endpoints,
            vbuckets: routes,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawVBucketServerMap {
    #[serde(default)]
    hash_algorithm: String,
    #[serde(default)]
    num_replicas: i64,
    #[serde(default)]
    server_list: Vec<String>,
    #[serde(default)]
    v_bucket_map: Vec<Vec<i64>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawNodeExt {
    #[serde(default)]
    hostname: Option<String>,
    #[serde(default)]
    services: RawNodeServices,
    #[serde(default)]
    alternate_addresses: Option<BTreeMap<String, RawAlternateAddress>>,
    #[serde(default, rename = "nodeUUID")]
    node_uuid: Option<String>,
    #[serde(default)]
    server_group: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct RawNodeServices {
    #[serde(default)]
    kv: u16,
    #[serde(default, rename = "kvSSL")]
    kv_ssl: u16,
}

#[derive(Debug, Deserialize)]
struct RawAlternateAddress {
    #[serde(default)]
    hostname: Option<String>,
    #[serde(default)]
    ports: Option<RawNodeServices>,
}

fn resolve_network(
    requested: &TopologyNetwork,
    nodes: &[RawNodeExt],
    server_count: usize,
    source_address: &str,
    source_host: &str,
    tls: bool,
) -> Result<String> {
    match requested {
        TopologyNetwork::Default => Ok("default".into()),
        TopologyNetwork::External => Ok("external".into()),
        TopologyNetwork::Named(name) => {
            validate_network_name(name)?;
            Ok(name.clone())
        }
        TopologyNetwork::Auto => {
            if nodes.is_empty() {
                return Ok("default".into());
            }
            let kv_nodes = nodes
                .iter()
                .filter(|node| node.services.kv != 0 || node.services.kv_ssl != 0)
                .take(server_count)
                .collect::<Vec<_>>();
            let source_is_default = kv_nodes.iter().any(|node| {
                node_endpoint(node, "default", source_host, tls)
                    .is_ok_and(|address| addresses_equal(&address, source_address))
            });
            if source_is_default {
                return Ok("default".into());
            }
            if kv_nodes.len() == server_count
                && kv_nodes.iter().all(|node| {
                    node.alternate_addresses
                        .as_ref()
                        .is_some_and(|addresses| addresses.contains_key("external"))
                })
            {
                return Ok("external".into());
            }
            Ok("default".into())
        }
    }
}

fn validate_network_name(name: &str) -> Result<()> {
    if name.trim().is_empty() || name.chars().any(char::is_whitespace) {
        return Err(DcpError::Topology(
            "alternate network name must be non-empty and contain no whitespace".into(),
        ));
    }
    Ok(())
}

fn build_endpoints(
    nodes: &[RawNodeExt],
    server_list: &[String],
    source_host: &str,
    tls: bool,
    network: &str,
) -> Result<Vec<KvEndpoint>> {
    if server_list.is_empty() {
        return Err(DcpError::Topology(
            "cluster config has no KV servers".into(),
        ));
    }
    if nodes.is_empty() {
        if tls {
            return Err(DcpError::Topology(
                "TLS topology requires nodesExt with kvSSL ports".into(),
            ));
        }
        if network != "default" {
            return Err(DcpError::Topology(format!(
                "network {network:?} requires nodesExt alternate addresses"
            )));
        }
        return server_list
            .iter()
            .map(|address| {
                let address = validated_endpoint(address)?;
                Ok(KvEndpoint {
                    id: NodeId(address.clone()),
                    canonical_address: address.clone(),
                    address,
                    server_group: None,
                })
            })
            .collect();
    }
    let kv_nodes = nodes
        .iter()
        .filter(|node| node.services.kv != 0 || node.services.kv_ssl != 0)
        .collect::<Vec<_>>();
    if kv_nodes.len() < server_list.len() {
        return Err(DcpError::Topology(format!(
            "nodesExt has {} KV nodes but vBucket serverList has {}",
            kv_nodes.len(),
            server_list.len()
        )));
    }

    kv_nodes
        .into_iter()
        .zip(server_list)
        .map(|(node, legacy_identity)| {
            let canonical_address = node_endpoint(node, "default", source_host, tls)?;
            let address = node_endpoint(node, network, source_host, tls)?;
            let node_uuid = node.node_uuid.as_deref().unwrap_or_default();
            let id = if node_uuid.trim().is_empty() {
                NodeId(validated_endpoint(legacy_identity)?)
            } else {
                NodeId(node_uuid.to_owned())
            };
            Ok(KvEndpoint {
                id,
                address,
                canonical_address,
                server_group: node
                    .server_group
                    .as_ref()
                    .filter(|group| !group.is_empty())
                    .cloned(),
            })
        })
        .collect()
}

fn node_endpoint(node: &RawNodeExt, network: &str, source_host: &str, tls: bool) -> Result<String> {
    let (hostname, services) = if network == "default" {
        (node.hostname.as_deref().unwrap_or_default(), &node.services)
    } else {
        let alternate = node
            .alternate_addresses
            .as_ref()
            .and_then(|addresses| addresses.get(network))
            .ok_or_else(|| {
                DcpError::Topology(format!(
                    "KV node {:?} has no alternate address for network {network:?}",
                    node.node_uuid
                ))
            })?;
        (
            alternate.hostname.as_deref().unwrap_or_default(),
            alternate.ports.as_ref().unwrap_or(&node.services),
        )
    };
    let hostname = if hostname.is_empty() {
        source_host
    } else {
        hostname
    };
    let port = if tls { services.kv_ssl } else { services.kv };
    if port == 0 {
        return Err(DcpError::Topology(format!(
            "KV node {:?} has no {} port for network {network:?}",
            node.node_uuid,
            if tls { "kvSSL" } else { "kv" }
        )));
    }
    format_endpoint(hostname, port)
}

fn build_vbucket_routes(
    map: &RawVBucketServerMap,
    endpoints: &[KvEndpoint],
) -> Result<BTreeMap<u16, VBucketRoute>> {
    if map.num_replicas < 0 {
        return Err(DcpError::Topology(format!(
            "negative replica count {}",
            map.num_replicas
        )));
    }
    let replica_count = usize::try_from(map.num_replicas)
        .map_err(|error| DcpError::Topology(format!("invalid replica count: {error}")))?;
    if map.v_bucket_map.is_empty() {
        return Err(DcpError::Topology(
            "cluster config has no vBucket map".into(),
        ));
    }
    if map.v_bucket_map.len() > usize::from(u16::MAX) + 1 {
        return Err(DcpError::Topology(format!(
            "vBucket map has {} entries, exceeding the u16 identifier space",
            map.v_bucket_map.len()
        )));
    }

    map.v_bucket_map
        .iter()
        .enumerate()
        .map(|(vbucket, row)| {
            if row.len() != replica_count + 1 {
                return Err(DcpError::Topology(format!(
                    "vBucket {vbucket} has {} route entries; expected {}",
                    row.len(),
                    replica_count + 1
                )));
            }
            let active = route_node(row[0], endpoints, vbucket, "active")?.ok_or_else(|| {
                DcpError::Topology(format!("vBucket {vbucket} has no active node"))
            })?;
            let replicas = row[1..]
                .iter()
                .map(|index| route_node(*index, endpoints, vbucket, "replica"))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect();
            let vbucket = u16::try_from(vbucket)
                .map_err(|error| DcpError::Topology(format!("invalid vBucket ID: {error}")))?;
            Ok((vbucket, VBucketRoute { active, replicas }))
        })
        .collect()
}

fn route_node(
    index: i64,
    endpoints: &[KvEndpoint],
    vbucket: usize,
    role: &str,
) -> Result<Option<NodeId>> {
    if index == -1 {
        return Ok(None);
    }
    let index = usize::try_from(index).map_err(|_| {
        DcpError::Topology(format!(
            "vBucket {vbucket} has invalid {role} node index {index}"
        ))
    })?;
    endpoints
        .get(index)
        .map(|endpoint| Some(endpoint.id.clone()))
        .ok_or_else(|| {
            DcpError::Topology(format!(
                "vBucket {vbucket} {role} node index {index} exceeds {} endpoints",
                endpoints.len()
            ))
        })
}

fn host_from_address(address: &str) -> Result<String> {
    let (host, _) = split_endpoint(address)?;
    Ok(host.to_owned())
}

fn validated_endpoint(address: &str) -> Result<String> {
    let (host, port) = split_endpoint(address)?;
    format_endpoint(host, port)
}

fn split_endpoint(address: &str) -> Result<(&str, u16)> {
    if let Some(stripped) = address.strip_prefix('[') {
        let end = stripped
            .find(']')
            .ok_or_else(|| DcpError::Topology(format!("invalid bracketed endpoint {address:?}")))?;
        let host = &stripped[..end];
        let port = stripped[end + 1..]
            .strip_prefix(':')
            .ok_or_else(|| DcpError::Topology(format!("endpoint {address:?} has no port")))?;
        return parse_host_port(host, port, address);
    }
    let (host, port) = address
        .rsplit_once(':')
        .ok_or_else(|| DcpError::Topology(format!("endpoint {address:?} has no port")))?;
    if host.contains(':') {
        return Err(DcpError::Topology(format!(
            "IPv6 endpoint {address:?} must use brackets"
        )));
    }
    parse_host_port(host, port, address)
}

fn parse_host_port<'a>(host: &'a str, port: &str, original: &str) -> Result<(&'a str, u16)> {
    if host.is_empty() || host.chars().any(char::is_whitespace) {
        return Err(DcpError::Topology(format!(
            "endpoint {original:?} has an invalid host"
        )));
    }
    let port = port.parse::<u16>().map_err(|error| {
        DcpError::Topology(format!(
            "endpoint {original:?} has an invalid port: {error}"
        ))
    })?;
    if port == 0 {
        return Err(DcpError::Topology(format!(
            "endpoint {original:?} has port zero"
        )));
    }
    Ok((host, port))
}

fn format_endpoint(host: &str, port: u16) -> Result<String> {
    let host = host
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(host);
    if host.is_empty() || host.chars().any(char::is_whitespace) {
        return Err(DcpError::Topology(format!(
            "invalid KV endpoint hostname {host:?}"
        )));
    }
    if host.contains(':') {
        Ok(format!("[{host}]:{port}"))
    } else {
        Ok(format!("{host}:{port}"))
    }
}

fn addresses_equal(left: &str, right: &str) -> bool {
    match (split_endpoint(left), split_endpoint(right)) {
        (Ok((left_host, left_port)), Ok((right_host, right_port))) => {
            left_port == right_port && left_host.eq_ignore_ascii_case(right_host)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::{BufMut, Bytes, BytesMut};
    use futures_util::{SinkExt, StreamExt};
    use rust_dcp_protocol::{Frame, FrameCodec, Status};
    use tokio::io::duplex;
    use tokio_util::codec::Framed;

    use super::*;

    const CONFIG: &str = r#"{
        "rev": 7,
        "revEpoch": 2,
        "name": "travel",
        "uuid": "bucket-uuid",
        "nodeLocator": "vbucket",
        "nodesExt": [
          {
            "hostname": "10.0.0.1",
            "nodeUUID": "node-a",
            "serverGroup": "group-a",
            "services": {"kv": 11210, "kvSSL": 11207},
            "alternateAddresses": {"external": {"hostname": "proxy.test", "ports": {"kv": 30101, "kvSSL": 31101}}}
          },
          {
            "hostname": "10.0.0.2",
            "nodeUUID": "node-b",
            "serverGroup": "group-b",
            "services": {"kv": 11210, "kvSSL": 11207},
            "alternateAddresses": {"external": {"hostname": "proxy.test", "ports": {"kv": 30102, "kvSSL": 31102}}}
          }
        ],
        "vBucketServerMap": {
          "hashAlgorithm": "CRC",
          "numReplicas": 1,
          "serverList": ["10.0.0.1:11210", "10.0.0.2:11210"],
          "vBucketMap": [[0, 1], [1, 0], [0, 1], [1, 0]]
        }
      }"#;

    fn config_with_revision_and_first_route(revision: u64, active: usize) -> Vec<u8> {
        CONFIG
            .replace("\"rev\": 7", &format!("\"rev\": {revision}"))
            .replace("[[0, 1]", &format!("[[{active}, {}]", 1 - active))
            .into_bytes()
    }

    #[test]
    fn default_topology_routes_vbuckets_by_stable_node_id() {
        let topology = ClusterTopology::from_json(
            CONFIG.as_bytes(),
            "10.0.0.1:11210",
            false,
            &TopologyNetwork::Auto,
        )
        .unwrap();

        assert_eq!(topology.network(), "default");
        assert_eq!(topology.revision(), TopologyRevision::new(2, 7));
        assert_eq!(topology.active_node(0).unwrap().as_str(), "node-a");
        assert_eq!(
            topology.active_endpoint(1).unwrap().address(),
            "10.0.0.2:11210"
        );
        assert_eq!(topology.replica_nodes(0).unwrap()[0].as_str(), "node-b");
        assert_eq!(
            topology.all_active_vbuckets_by_node()[&NodeId("node-a".into())],
            vec![0, 2]
        );
    }

    #[test]
    fn auto_network_uses_external_tls_addresses_for_external_source() {
        let topology = ClusterTopology::from_json(
            CONFIG.as_bytes(),
            "proxy.test:31101",
            true,
            &TopologyNetwork::Auto,
        )
        .unwrap();

        assert_eq!(topology.network(), "external");
        assert_eq!(
            topology.endpoints()[&NodeId("node-a".into())].address(),
            "proxy.test:31101"
        );
        assert_eq!(
            topology.endpoints()[&NodeId("node-a".into())].canonical_address(),
            "10.0.0.1:11207"
        );
    }

    #[test]
    fn nullable_hostnames_and_non_kv_nodes_preserve_server_indexes() {
        let config = CONFIG
            .replacen("\"hostname\": \"10.0.0.1\"", "\"hostname\": null", 1)
            .replacen(
                "\"nodesExt\": [",
                "\"nodesExt\": [{\"hostname\": \"query.test\", \"services\": {}},",
                1,
            );
        let topology = ClusterTopology::from_json(
            config.as_bytes(),
            "10.0.0.1:11210",
            false,
            &TopologyNetwork::Default,
        )
        .unwrap();

        assert_eq!(topology.active_node(0).unwrap().as_str(), "node-a");
        assert_eq!(
            topology.endpoints()[&NodeId("node-a".into())].address(),
            "10.0.0.1:11210"
        );
    }

    #[test]
    fn topology_state_rejects_stale_revision_and_reports_reroutes() {
        let current = ClusterTopology::from_json(
            CONFIG.as_bytes(),
            "10.0.0.1:11210",
            false,
            &TopologyNetwork::Default,
        )
        .unwrap();
        let stale = ClusterTopology::from_json(
            &config_with_revision_and_first_route(6, 1),
            "10.0.0.1:11210",
            false,
            &TopologyNetwork::Default,
        )
        .unwrap();
        let newer = ClusterTopology::from_json(
            &config_with_revision_and_first_route(8, 1),
            "10.0.0.1:11210",
            false,
            &TopologyNetwork::Default,
        )
        .unwrap();
        let mut tracker = TopologyState::new(current);

        assert_eq!(tracker.apply(stale).unwrap(), None);
        let change = tracker.apply(newer).unwrap().unwrap();
        assert_eq!(change.generation(), 2);
        assert_eq!(change.rerouted_vbuckets(), &BTreeSet::from([0]));
        assert!(change.reconnect_nodes().is_empty());
    }

    #[test]
    fn invalid_active_node_index_is_rejected() {
        let invalid = CONFIG.replace("[[0, 1]", "[[9, 1]");
        assert!(
            ClusterTopology::from_json(
                invalid.as_bytes(),
                "10.0.0.1:11210",
                false,
                &TopologyNetwork::Default,
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn asynchronous_metadata_queries_validate_and_decode_responses() {
        let (client_io, server_io) = duplex(32 * 1024);
        let mut connection =
            KvConnection::from_io(client_io, "10.0.0.1:11210", Duration::from_secs(1));
        let server = tokio::spawn(async move {
            let mut framed = Framed::new(server_io, FrameCodec::default());
            while let Some(request) = framed.next().await.transpose().unwrap() {
                let mut response = Frame::success_response_to(&request);
                match request.opcode {
                    Opcode::GET_CLUSTER_CONFIG => {
                        response.value = Bytes::from_static(CONFIG.as_bytes());
                    }
                    Opcode::GET_ALL_VB_SEQNOS => {
                        let mut value = BytesMut::new();
                        value.put_u16(0);
                        value.put_u64(101);
                        value.put_u16(2);
                        value.put_u64(202);
                        response.value = value.freeze();
                    }
                    Opcode::DCP_GET_FAILOVER_LOG => {
                        assert_eq!(request.vbucket, 2);
                        let mut value = BytesMut::new();
                        value.put_u64(0xfeed);
                        value.put_u64(88);
                        response.value = value.freeze();
                        framed.send(response).await.unwrap();
                        break;
                    }
                    opcode => panic!("unexpected metadata opcode {opcode:?}"),
                }
                framed.send(response).await.unwrap();
            }
        });

        let topology =
            discover_topology(&mut connection, "travel", false, &TopologyNetwork::Default)
                .await
                .unwrap();
        let seqnos = fetch_active_high_seqnos(&mut connection, None)
            .await
            .unwrap();
        let failover = fetch_failover_log(&mut connection, 2).await.unwrap();

        assert_eq!(topology.num_vbuckets(), 4);
        assert_eq!(seqnos, BTreeMap::from([(0, 101), (2, 202)]));
        assert_eq!(
            failover,
            vec![FailoverEntry {
                vbucket_uuid: 0xfeed,
                seqno: 88,
            }]
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn high_seqno_query_rejects_duplicate_vbuckets() {
        let (client_io, server_io) = duplex(4_096);
        let mut connection =
            KvConnection::from_io(client_io, "10.0.0.1:11210", Duration::from_secs(1));
        let server = tokio::spawn(async move {
            let mut framed = Framed::new(server_io, FrameCodec::default());
            let request = framed.next().await.unwrap().unwrap();
            let mut response = Frame::success_response_to(&request);
            let mut value = BytesMut::new();
            for seqno in [1, 2] {
                value.put_u16(9);
                value.put_u64(seqno);
            }
            response.value = value.freeze();
            framed.send(response).await.unwrap();
        });

        assert!(
            fetch_active_high_seqnos(&mut connection, None)
                .await
                .is_err()
        );
        server.await.unwrap();
    }

    #[test]
    fn non_success_metadata_status_is_not_treated_as_valid_data() {
        let mut response = Frame::response(Opcode::GET_CLUSTER_CONFIG, Status::NOT_SUPPORTED);
        response.value = Bytes::from_static(b"not supported");

        assert!(matches!(
            ensure_success_response(&response, Opcode::GET_CLUSTER_CONFIG, "config"),
            Err(DcpError::ServerStatus { status: 0x83, .. })
        ));
    }
}
