//! Prometheus integration for `rust-dcp` observability handles.
//!
//! This crate only implements a synchronous, in-memory
//! [`prometheus::core::Collector`]. The integrating application owns its
//! [`prometheus::Registry`] and any HTTP exposition endpoint.

use prometheus::{
    Registry,
    core::{Collector, Desc},
    proto::{Counter, Gauge, LabelPair, Metric, MetricFamily, MetricType},
};
use rust_dcp_core::{
    DcpHealth, DcpHealthSnapshot, DcpHealthStatus, DcpMetrics, DcpMetricsSnapshot,
};

/// Prometheus collector backed by cloneable `rust-dcp` metrics and health
/// handles.
///
/// Collection reads point-in-time in-memory snapshots and performs no network
/// or checkpoint I/O. It can therefore be registered with an application-owned
/// registry regardless of which Tokio HTTP stack the application uses.
#[derive(Clone, Debug)]
pub struct DcpPrometheusCollector {
    metrics: DcpMetrics,
    health: DcpHealth,
    descriptors: Descriptors,
}

impl DcpPrometheusCollector {
    /// Creates a collector for one DCP client or subscription.
    ///
    /// # Errors
    ///
    /// Returns a Prometheus descriptor error if the crate's static metric
    /// schema is invalid.
    pub fn new(metrics: DcpMetrics, health: DcpHealth) -> prometheus::Result<Self> {
        Ok(Self {
            metrics,
            health,
            descriptors: Descriptors::new()?,
        })
    }

    /// Registers this collector in an application-owned Prometheus registry.
    ///
    /// # Errors
    ///
    /// Returns the registry error, including duplicate or inconsistent metric
    /// descriptors.
    pub fn register(self, registry: &Registry) -> prometheus::Result<()> {
        registry.register(Box::new(self))
    }
}

impl Collector for DcpPrometheusCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descriptors.all()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let metrics = self.metrics.snapshot();
        let health = self.health.snapshot();
        let mut families = collect_lifecycle(&self.descriptors, &metrics);
        families.extend(collect_delivery_and_recovery(&self.descriptors, &metrics));
        families.extend(collect_health(&self.descriptors, &health));
        families
    }
}

fn collect_lifecycle(descriptors: &Descriptors, metrics: &DcpMetricsSnapshot) -> Vec<MetricFamily> {
    vec![
        counter_family(
            &descriptors.bootstrap_attempts,
            &[(number(metrics.bootstrap_attempts), &[])],
        ),
        counter_family(
            &descriptors.bootstrap_successes,
            &[(number(metrics.bootstrap_successes), &[])],
        ),
        counter_family(
            &descriptors.bootstrap_failures,
            &[(number(metrics.bootstrap_failures), &[])],
        ),
        counter_family(
            &descriptors.topology_updates,
            &[(number(metrics.topology_updates), &[])],
        ),
        counter_family(
            &descriptors.reconnects,
            &[(number(metrics.reconnects), &[])],
        ),
        counter_family(
            &descriptors.health_checks,
            &[(number(metrics.health_checks), &[])],
        ),
        counter_family(
            &descriptors.health_failures,
            &[(number(metrics.health_failures), &[])],
        ),
        gauge_family(
            &descriptors.active_connections,
            &[(number(metrics.active_connections), &[])],
        ),
        gauge_family(
            &descriptors.assigned_vbuckets,
            &[(number(metrics.assigned_vbuckets), &[])],
        ),
    ]
}

fn collect_delivery_and_recovery(
    descriptors: &Descriptors,
    metrics: &DcpMetricsSnapshot,
) -> Vec<MetricFamily> {
    vec![
        counter_family(
            &descriptors.events_delivered,
            &[(number(metrics.delivered_events), &[])],
        ),
        counter_family(
            &descriptors.event_deliveries,
            &[
                (number(metrics.mutations), &["mutation"]),
                (number(metrics.deletions), &["deletion"]),
                (number(metrics.expirations), &["expiration"]),
                (number(metrics.snapshot_markers), &["snapshot_marker"]),
                (number(metrics.seqno_advanced_events), &["seqno_advanced"]),
                (number(metrics.system_events), &["system_event"]),
                (number(metrics.stream_ends), &["stream_end"]),
                (number(metrics.oso_snapshots), &["oso_snapshot"]),
            ],
        ),
        counter_family(
            &descriptors.events_processed,
            &[(number(metrics.processed_events), &[])],
        ),
        counter_family(
            &descriptors.unknown_frames,
            &[(number(metrics.unknown_frames), &[])],
        ),
        counter_family(
            &descriptors.stream_errors,
            &[(number(metrics.stream_errors), &[])],
        ),
        counter_family(
            &descriptors.stale_generation_drops,
            &[(number(metrics.stale_generation_drops), &[])],
        ),
        counter_family(&descriptors.rollbacks, &[(number(metrics.rollbacks), &[])]),
        counter_family(
            &descriptors.rollback_mitigation_delays,
            &[(number(metrics.rollback_mitigation_delays), &[])],
        ),
        counter_family(
            &descriptors.rollback_mitigation_failures,
            &[(number(metrics.rollback_mitigation_failures), &[])],
        ),
    ]
}

fn collect_health(descriptors: &Descriptors, health: &DcpHealthSnapshot) -> Vec<MetricFamily> {
    vec![
        gauge_family(
            &descriptors.health_status,
            &[
                (
                    status_value(health.status, DcpHealthStatus::Starting),
                    &["starting"],
                ),
                (
                    status_value(health.status, DcpHealthStatus::Healthy),
                    &["healthy"],
                ),
                (
                    status_value(health.status, DcpHealthStatus::Degraded),
                    &["degraded"],
                ),
                (
                    status_value(health.status, DcpHealthStatus::Stopped),
                    &["stopped"],
                ),
            ],
        ),
        gauge_family(
            &descriptors.health_consecutive_failures,
            &[(number(health.consecutive_failures), &[])],
        ),
        gauge_family(
            &descriptors.health_topology_generation,
            &[(number(health.topology_generation), &[])],
        ),
        gauge_family(
            &descriptors.health_last_check_timestamp_seconds,
            &[((health.last_check.map_or(0.0, unix_timestamp_seconds)), &[])],
        ),
    ]
}

#[derive(Clone, Debug)]
struct Descriptors {
    bootstrap_attempts: Desc,
    bootstrap_successes: Desc,
    bootstrap_failures: Desc,
    topology_updates: Desc,
    reconnects: Desc,
    health_checks: Desc,
    health_failures: Desc,
    active_connections: Desc,
    assigned_vbuckets: Desc,
    events_delivered: Desc,
    event_deliveries: Desc,
    events_processed: Desc,
    unknown_frames: Desc,
    stream_errors: Desc,
    stale_generation_drops: Desc,
    rollbacks: Desc,
    rollback_mitigation_delays: Desc,
    rollback_mitigation_failures: Desc,
    health_status: Desc,
    health_consecutive_failures: Desc,
    health_topology_generation: Desc,
    health_last_check_timestamp_seconds: Desc,
}

impl Descriptors {
    fn new() -> prometheus::Result<Self> {
        let (
            bootstrap_attempts,
            bootstrap_successes,
            bootstrap_failures,
            topology_updates,
            reconnects,
            health_checks,
            health_failures,
            active_connections,
            assigned_vbuckets,
        ) = lifecycle_descriptors()?;
        let (
            events_delivered,
            event_deliveries,
            events_processed,
            unknown_frames,
            stream_errors,
            stale_generation_drops,
            rollbacks,
            rollback_mitigation_delays,
            rollback_mitigation_failures,
        ) = delivery_descriptors()?;
        let (
            health_status,
            health_consecutive_failures,
            health_topology_generation,
            health_last_check_timestamp_seconds,
        ) = health_descriptors()?;

        Ok(Self {
            bootstrap_attempts,
            bootstrap_successes,
            bootstrap_failures,
            topology_updates,
            reconnects,
            health_checks,
            health_failures,
            active_connections,
            assigned_vbuckets,
            events_delivered,
            event_deliveries,
            events_processed,
            unknown_frames,
            stream_errors,
            stale_generation_drops,
            rollbacks,
            rollback_mitigation_delays,
            rollback_mitigation_failures,
            health_status,
            health_consecutive_failures,
            health_topology_generation,
            health_last_check_timestamp_seconds,
        })
    }

    fn all(&self) -> Vec<&Desc> {
        vec![
            &self.bootstrap_attempts,
            &self.bootstrap_successes,
            &self.bootstrap_failures,
            &self.topology_updates,
            &self.reconnects,
            &self.health_checks,
            &self.health_failures,
            &self.active_connections,
            &self.assigned_vbuckets,
            &self.events_delivered,
            &self.event_deliveries,
            &self.events_processed,
            &self.unknown_frames,
            &self.stream_errors,
            &self.stale_generation_drops,
            &self.rollbacks,
            &self.rollback_mitigation_delays,
            &self.rollback_mitigation_failures,
            &self.health_status,
            &self.health_consecutive_failures,
            &self.health_topology_generation,
            &self.health_last_check_timestamp_seconds,
        ]
    }
}

type LifecycleDescriptors = (Desc, Desc, Desc, Desc, Desc, Desc, Desc, Desc, Desc);
type DeliveryDescriptors = (Desc, Desc, Desc, Desc, Desc, Desc, Desc, Desc, Desc);
type HealthDescriptors = (Desc, Desc, Desc, Desc);

fn lifecycle_descriptors() -> prometheus::Result<LifecycleDescriptors> {
    Ok((
        desc(
            "rust_dcp_bootstrap_attempts_total",
            "Total DCP cluster bootstrap attempts.",
            &[],
        )?,
        desc(
            "rust_dcp_bootstrap_successes_total",
            "Total successful DCP cluster bootstraps.",
            &[],
        )?,
        desc(
            "rust_dcp_bootstrap_failures_total",
            "Total failed DCP cluster bootstraps or bootstrap health probes.",
            &[],
        )?,
        desc(
            "rust_dcp_topology_updates_total",
            "Total accepted DCP cluster topology revisions.",
            &[],
        )?,
        desc(
            "rust_dcp_reconnects_total",
            "Total DCP stream generation reconnects.",
            &[],
        )?,
        desc(
            "rust_dcp_health_checks_total",
            "Total DCP client health probe attempts.",
            &[],
        )?,
        desc(
            "rust_dcp_health_failures_total",
            "Total failed DCP client health probes.",
            &[],
        )?,
        desc(
            "rust_dcp_active_connections",
            "Current open node-level DCP connections.",
            &[],
        )?,
        desc(
            "rust_dcp_assigned_vbuckets",
            "Current vBuckets owned by the active DCP subscription.",
            &[],
        )?,
    ))
}

fn delivery_descriptors() -> prometheus::Result<DeliveryDescriptors> {
    Ok((
        desc(
            "rust_dcp_events_delivered_total",
            "Total DCP events delivered to the application.",
            &[],
        )?,
        desc(
            "rust_dcp_event_deliveries_total",
            "Total DCP events delivered to the application by event type.",
            &["event_type"],
        )?,
        desc(
            "rust_dcp_events_processed_total",
            "Total DCP deliveries explicitly marked processed by the application.",
            &[],
        )?,
        desc(
            "rust_dcp_unknown_frames_total",
            "Total future protocol frames observed by the high-level DCP runtime.",
            &[],
        )?,
        desc(
            "rust_dcp_stream_errors_total",
            "Total DCP runtime stream failures.",
            &[],
        )?,
        desc(
            "rust_dcp_stale_generation_drops_total",
            "Total DCP events discarded by a connection-generation fence.",
            &[],
        )?,
        desc(
            "rust_dcp_rollbacks_total",
            "Total explicit DCP rollback rewinds accepted by policy.",
            &[],
        )?,
        desc(
            "rust_dcp_rollback_mitigation_delays_total",
            "Total deliveries delayed by replica-persistence rollback mitigation.",
            &[],
        )?,
        desc(
            "rust_dcp_rollback_mitigation_failures_total",
            "Total stalls or history changes reported by rollback mitigation.",
            &[],
        )?,
    ))
}

fn health_descriptors() -> prometheus::Result<HealthDescriptors> {
    Ok((
        desc(
            "rust_dcp_health_status",
            "Current one-hot DCP health status.",
            &["status"],
        )?,
        desc(
            "rust_dcp_health_consecutive_failures",
            "Current consecutive DCP health or runtime failures.",
            &[],
        )?,
        desc(
            "rust_dcp_health_topology_generation",
            "Current local DCP topology generation observed by health checks.",
            &[],
        )?,
        desc(
            "rust_dcp_health_last_check_timestamp_seconds",
            "Unix timestamp of the latest DCP health check or state transition, or zero before the first check.",
            &[],
        )?,
    ))
}

fn desc(name: &str, help: &str, variable_labels: &[&str]) -> prometheus::Result<Desc> {
    Desc::new(
        name.to_owned(),
        help.to_owned(),
        variable_labels
            .iter()
            .map(|label| (*label).to_owned())
            .collect(),
        std::collections::HashMap::new(),
    )
}

fn counter_family(desc: &Desc, samples: &[(f64, &[&str])]) -> MetricFamily {
    family(desc, MetricType::COUNTER, samples)
}

fn gauge_family(desc: &Desc, samples: &[(f64, &[&str])]) -> MetricFamily {
    family(desc, MetricType::GAUGE, samples)
}

fn family(desc: &Desc, metric_type: MetricType, samples: &[(f64, &[&str])]) -> MetricFamily {
    let mut family = MetricFamily::default();
    family.set_name(desc.fq_name.clone());
    family.set_help(desc.help.clone());
    family.set_field_type(metric_type);
    family.set_metric(
        samples
            .iter()
            .map(|(value, labels)| metric(desc, metric_type, *value, labels))
            .collect(),
    );
    family
}

fn metric(desc: &Desc, metric_type: MetricType, value: f64, labels: &[&str]) -> Metric {
    debug_assert_eq!(desc.variable_labels.len(), labels.len());
    let mut pairs = desc
        .variable_labels
        .iter()
        .zip(labels)
        .map(|(name, value)| {
            let mut pair = LabelPair::default();
            pair.set_name(name.clone());
            pair.set_value((*value).to_owned());
            pair
        })
        .chain(desc.const_label_pairs.iter().cloned())
        .collect::<Vec<_>>();
    pairs.sort();

    let mut metric = Metric::from_label(pairs);
    match metric_type {
        MetricType::COUNTER => {
            let mut counter = Counter::default();
            counter.set_value(value);
            metric.set_counter(counter);
        }
        MetricType::GAUGE => {
            let mut gauge = Gauge::default();
            gauge.set_value(value);
            metric.set_gauge(gauge);
        }
        _ => unreachable!("collector only defines counter and gauge descriptors"),
    }
    metric
}

#[allow(clippy::cast_precision_loss)]
fn number(value: u64) -> f64 {
    value as f64
}

fn status_value(actual: DcpHealthStatus, expected: DcpHealthStatus) -> f64 {
    if actual == expected { 1.0 } else { 0.0 }
}

fn unix_timestamp_seconds(value: std::time::SystemTime) -> f64 {
    value
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |duration| duration.as_secs_f64())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use prometheus::{Registry, proto::MetricType};
    use rust_dcp_core::{DcpHealth, DcpMetrics};

    use super::DcpPrometheusCollector;

    #[test]
    fn collector_registers_complete_zero_snapshot_without_an_http_exporter() {
        let registry = Registry::new();
        DcpPrometheusCollector::new(DcpMetrics::default(), DcpHealth::default())
            .expect("static metric descriptors must be valid")
            .register(&registry)
            .expect("collector must register in an application registry");

        let families = registry
            .gather()
            .into_iter()
            .map(|family| (family.name().to_owned(), family))
            .collect::<BTreeMap<_, _>>();

        for name in [
            "rust_dcp_bootstrap_attempts_total",
            "rust_dcp_bootstrap_successes_total",
            "rust_dcp_bootstrap_failures_total",
            "rust_dcp_topology_updates_total",
            "rust_dcp_reconnects_total",
            "rust_dcp_health_checks_total",
            "rust_dcp_health_failures_total",
            "rust_dcp_events_delivered_total",
            "rust_dcp_event_deliveries_total",
            "rust_dcp_events_processed_total",
            "rust_dcp_unknown_frames_total",
            "rust_dcp_stream_errors_total",
            "rust_dcp_stale_generation_drops_total",
            "rust_dcp_rollbacks_total",
            "rust_dcp_rollback_mitigation_delays_total",
            "rust_dcp_rollback_mitigation_failures_total",
        ] {
            assert_eq!(families[name].get_field_type(), MetricType::COUNTER);
        }

        for name in [
            "rust_dcp_active_connections",
            "rust_dcp_assigned_vbuckets",
            "rust_dcp_health_status",
            "rust_dcp_health_consecutive_failures",
            "rust_dcp_health_topology_generation",
            "rust_dcp_health_last_check_timestamp_seconds",
        ] {
            assert_eq!(families[name].get_field_type(), MetricType::GAUGE);
        }
    }

    #[test]
    fn starting_health_is_exported_as_a_one_hot_status_vector() {
        let registry = Registry::new();
        DcpPrometheusCollector::new(DcpMetrics::default(), DcpHealth::default())
            .unwrap()
            .register(&registry)
            .unwrap();

        let status = registry
            .gather()
            .into_iter()
            .find(|family| family.name() == "rust_dcp_health_status")
            .expect("health status family must be present");
        let values = status
            .get_metric()
            .iter()
            .map(|metric| {
                let label = metric
                    .get_label()
                    .iter()
                    .find(|label| label.name() == "status")
                    .expect("status label must be present");
                (label.value().to_owned(), metric.get_gauge().value())
            })
            .collect::<BTreeMap<_, _>>();

        assert_eq!(
            values,
            BTreeMap::from([
                ("degraded".to_owned(), 0.0),
                ("healthy".to_owned(), 0.0),
                ("starting".to_owned(), 1.0),
                ("stopped".to_owned(), 0.0),
            ])
        );
    }
}
