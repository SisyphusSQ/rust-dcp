use std::{
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::SystemTime,
};

use crate::DcpEvent;

/// Lock-free lifecycle and delivery counters owned by one DCP client.
#[derive(Clone, Debug, Default)]
pub struct DcpMetrics {
    inner: Arc<MetricCounters>,
}

/// Point-in-time DCP metrics suitable for an application-provided exporter.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DcpMetricsSnapshot {
    /// Cluster bootstrap attempts.
    pub bootstrap_attempts: u64,
    /// Successful cluster bootstraps.
    pub bootstrap_successes: u64,
    /// Failed cluster bootstraps or health probes.
    pub bootstrap_failures: u64,
    /// Accepted topology revisions.
    pub topology_updates: u64,
    /// Stream-generation reconnects.
    pub reconnects: u64,
    /// Health probe attempts.
    pub health_checks: u64,
    /// Failed health probes.
    pub health_failures: u64,
    /// Currently open node-level DCP connections.
    pub active_connections: u64,
    /// vBuckets owned by the active subscription.
    pub assigned_vbuckets: u64,
    /// Events yielded to the application.
    pub delivered_events: u64,
    /// Mutation deliveries.
    pub mutations: u64,
    /// Deletion deliveries.
    pub deletions: u64,
    /// Expiration deliveries.
    pub expirations: u64,
    /// Snapshot-marker deliveries.
    pub snapshot_markers: u64,
    /// Filtered progress deliveries.
    pub seqno_advanced_events: u64,
    /// Collection/scope system-event deliveries.
    pub system_events: u64,
    /// Stream-end deliveries.
    pub stream_ends: u64,
    /// OSO markers observed even though OSO is not enabled by this SDK.
    pub oso_snapshots: u64,
    /// Deliveries explicitly marked processed by the application.
    pub processed_events: u64,
    /// Raw future frames observed by the high-level runtime. The low-level
    /// [`crate::DcpStream`] API retains the original frame for callers that
    /// need custom forward-compatibility handling.
    pub unknown_frames: u64,
    /// Runtime stream failures.
    pub stream_errors: u64,
    /// Events discarded by a connection-generation fence.
    pub stale_generation_drops: u64,
    /// Explicit rollback rewinds accepted by policy.
    pub rollbacks: u64,
    /// Deliveries delayed until every available vBucket copy persisted them.
    pub rollback_mitigation_delays: u64,
    /// Bounded stalls or history changes reported by rollback mitigation.
    pub rollback_mitigation_failures: u64,
}

#[derive(Debug, Default)]
struct MetricCounters {
    bootstrap_attempts: AtomicU64,
    bootstrap_successes: AtomicU64,
    bootstrap_failures: AtomicU64,
    topology_updates: AtomicU64,
    reconnects: AtomicU64,
    health_checks: AtomicU64,
    health_failures: AtomicU64,
    active_connections: AtomicU64,
    assigned_vbuckets: AtomicU64,
    delivered_events: AtomicU64,
    mutations: AtomicU64,
    deletions: AtomicU64,
    expirations: AtomicU64,
    snapshot_markers: AtomicU64,
    seqno_advanced_events: AtomicU64,
    system_events: AtomicU64,
    stream_ends: AtomicU64,
    oso_snapshots: AtomicU64,
    processed_events: AtomicU64,
    unknown_frames: AtomicU64,
    stream_errors: AtomicU64,
    stale_generation_drops: AtomicU64,
    rollbacks: AtomicU64,
    rollback_mitigation_delays: AtomicU64,
    rollback_mitigation_failures: AtomicU64,
}

impl DcpMetrics {
    /// Captures all counters and gauges without blocking the DCP runtime.
    #[must_use]
    pub fn snapshot(&self) -> DcpMetricsSnapshot {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        DcpMetricsSnapshot {
            bootstrap_attempts: load(&self.inner.bootstrap_attempts),
            bootstrap_successes: load(&self.inner.bootstrap_successes),
            bootstrap_failures: load(&self.inner.bootstrap_failures),
            topology_updates: load(&self.inner.topology_updates),
            reconnects: load(&self.inner.reconnects),
            health_checks: load(&self.inner.health_checks),
            health_failures: load(&self.inner.health_failures),
            active_connections: load(&self.inner.active_connections),
            assigned_vbuckets: load(&self.inner.assigned_vbuckets),
            delivered_events: load(&self.inner.delivered_events),
            mutations: load(&self.inner.mutations),
            deletions: load(&self.inner.deletions),
            expirations: load(&self.inner.expirations),
            snapshot_markers: load(&self.inner.snapshot_markers),
            seqno_advanced_events: load(&self.inner.seqno_advanced_events),
            system_events: load(&self.inner.system_events),
            stream_ends: load(&self.inner.stream_ends),
            oso_snapshots: load(&self.inner.oso_snapshots),
            processed_events: load(&self.inner.processed_events),
            unknown_frames: load(&self.inner.unknown_frames),
            stream_errors: load(&self.inner.stream_errors),
            stale_generation_drops: load(&self.inner.stale_generation_drops),
            rollbacks: load(&self.inner.rollbacks),
            rollback_mitigation_delays: load(&self.inner.rollback_mitigation_delays),
            rollback_mitigation_failures: load(&self.inner.rollback_mitigation_failures),
        }
    }

    pub(crate) fn record_bootstrap_attempt(&self) {
        increment(&self.inner.bootstrap_attempts);
    }

    pub(crate) fn record_bootstrap_success(&self) {
        increment(&self.inner.bootstrap_successes);
    }

    pub(crate) fn record_bootstrap_failure(&self) {
        increment(&self.inner.bootstrap_failures);
    }

    pub(crate) fn record_topology_update(&self) {
        increment(&self.inner.topology_updates);
    }

    pub(crate) fn record_reconnect(&self) {
        increment(&self.inner.reconnects);
    }

    pub(crate) fn record_health_check(&self) {
        increment(&self.inner.health_checks);
    }

    pub(crate) fn record_health_failure(&self) {
        increment(&self.inner.health_failures);
    }

    pub(crate) fn set_active_connections(&self, connections: u64) {
        self.inner
            .active_connections
            .store(connections, Ordering::Relaxed);
    }

    pub(crate) fn set_assigned_vbuckets(&self, vbuckets: u64) {
        self.inner
            .assigned_vbuckets
            .store(vbuckets, Ordering::Relaxed);
    }

    pub(crate) fn record_delivery(&self, event: &DcpEvent) {
        increment(&self.inner.delivered_events);
        let counter = match event {
            DcpEvent::Mutation(_) => &self.inner.mutations,
            DcpEvent::Deletion(_) => &self.inner.deletions,
            DcpEvent::Expiration(_) => &self.inner.expirations,
            DcpEvent::SnapshotMarker(_) => &self.inner.snapshot_markers,
            DcpEvent::StreamEnd(_) => &self.inner.stream_ends,
            DcpEvent::SeqNoAdvanced(_) => &self.inner.seqno_advanced_events,
            DcpEvent::SystemEvent(_) => &self.inner.system_events,
            DcpEvent::OsoSnapshot(_) => &self.inner.oso_snapshots,
        };
        increment(counter);
    }

    pub(crate) fn record_processed(&self) {
        increment(&self.inner.processed_events);
    }

    pub(crate) fn record_unknown_frame(&self) {
        increment(&self.inner.unknown_frames);
    }

    pub(crate) fn record_stream_error(&self) {
        increment(&self.inner.stream_errors);
    }

    pub(crate) fn record_stale_generation_drop(&self) {
        increment(&self.inner.stale_generation_drops);
    }

    pub(crate) fn record_rollbacks(&self, count: usize) {
        let count = u64::try_from(count).unwrap_or(u64::MAX);
        let _ =
            self.inner
                .rollbacks
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(current.saturating_add(count))
                });
    }

    pub(crate) fn record_rollback_mitigation_delay(&self) {
        increment(&self.inner.rollback_mitigation_delays);
    }

    pub(crate) fn record_rollback_mitigation_failure(&self) {
        increment(&self.inner.rollback_mitigation_failures);
    }
}

fn increment(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

/// Overall health state of the client and its current subscription.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DcpHealthStatus {
    /// Initial bootstrap or subscription setup is in progress.
    #[default]
    Starting,
    /// The most recent probe succeeded.
    Healthy,
    /// The most recent probe or stream generation failed.
    Degraded,
    /// The client was explicitly closed.
    Stopped,
}

/// Point-in-time health data with no built-in HTTP server or exporter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DcpHealthSnapshot {
    /// Current health classification.
    pub status: DcpHealthStatus,
    /// Wall-clock time of the most recent probe or state transition.
    pub last_check: Option<SystemTime>,
    /// Consecutive probe/runtime failures since the last success.
    pub consecutive_failures: u64,
    /// Currently open node-level DCP connections.
    pub active_connections: u64,
    /// Current local topology generation.
    pub topology_generation: u64,
    /// Most recent failure or stop context.
    pub message: Option<String>,
}

/// Cloneable health handle polled by application-provided status exporters.
#[derive(Clone, Debug)]
pub struct DcpHealth {
    inner: Arc<Mutex<DcpHealthSnapshot>>,
}

impl Default for DcpHealth {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(DcpHealthSnapshot {
                status: DcpHealthStatus::Starting,
                last_check: None,
                consecutive_failures: 0,
                active_connections: 0,
                topology_generation: 0,
                message: None,
            })),
        }
    }
}

impl DcpHealth {
    /// Captures the latest health state.
    #[must_use]
    pub fn snapshot(&self) -> DcpHealthSnapshot {
        self.lock().clone()
    }

    pub(crate) fn record_success(
        &self,
        checked_at: SystemTime,
        active_connections: u64,
        topology_generation: u64,
    ) {
        let mut state = self.lock();
        state.status = DcpHealthStatus::Healthy;
        state.last_check = Some(checked_at);
        state.consecutive_failures = 0;
        state.active_connections = active_connections;
        state.topology_generation = topology_generation;
        state.message = None;
    }

    pub(crate) fn record_failure(&self, checked_at: SystemTime, message: String) {
        let mut state = self.lock();
        state.status = DcpHealthStatus::Degraded;
        state.last_check = Some(checked_at);
        state.consecutive_failures = state.consecutive_failures.saturating_add(1);
        state.message = Some(message);
    }

    pub(crate) fn record_stopped(&self, checked_at: SystemTime) {
        let mut state = self.lock();
        state.status = DcpHealthStatus::Stopped;
        state.last_check = Some(checked_at);
        state.active_connections = 0;
        state.message = Some("client stopped".into());
    }

    fn lock(&self) -> MutexGuard<'_, DcpHealthSnapshot> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use super::*;
    use crate::{DcpEvent, SeqNoAdvanced};

    #[test]
    fn metrics_snapshot_separates_lifecycle_delivery_and_processing_counters() {
        let metrics = DcpMetrics::default();
        metrics.record_bootstrap_attempt();
        metrics.record_bootstrap_success();
        metrics.set_active_connections(2);
        metrics.set_assigned_vbuckets(16);
        metrics.record_delivery(&DcpEvent::SeqNoAdvanced(SeqNoAdvanced {
            vbucket: 7,
            seqno: 42,
        }));
        metrics.record_processed();
        metrics.record_reconnect();
        metrics.record_stale_generation_drop();
        metrics.record_rollback_mitigation_delay();
        metrics.record_rollback_mitigation_failure();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.bootstrap_attempts, 1);
        assert_eq!(snapshot.bootstrap_successes, 1);
        assert_eq!(snapshot.active_connections, 2);
        assert_eq!(snapshot.assigned_vbuckets, 16);
        assert_eq!(snapshot.delivered_events, 1);
        assert_eq!(snapshot.seqno_advanced_events, 1);
        assert_eq!(snapshot.processed_events, 1);
        assert_eq!(snapshot.reconnects, 1);
        assert_eq!(snapshot.stale_generation_drops, 1);
        assert_eq!(snapshot.rollback_mitigation_delays, 1);
        assert_eq!(snapshot.rollback_mitigation_failures, 1);
    }

    #[test]
    fn health_state_records_failures_recovery_and_stop() {
        let health = DcpHealth::default();
        assert_eq!(health.snapshot().status, DcpHealthStatus::Starting);

        let checked_at = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(10);
        health.record_success(checked_at, 3, 7);
        let healthy = health.snapshot();
        assert_eq!(healthy.status, DcpHealthStatus::Healthy);
        assert_eq!(healthy.last_check, Some(checked_at));
        assert_eq!(healthy.active_connections, 3);
        assert_eq!(healthy.topology_generation, 7);

        health.record_failure(checked_at, "probe timed out".into());
        let degraded = health.snapshot();
        assert_eq!(degraded.status, DcpHealthStatus::Degraded);
        assert_eq!(degraded.consecutive_failures, 1);
        assert_eq!(degraded.message.as_deref(), Some("probe timed out"));

        health.record_success(checked_at, 2, 8);
        assert_eq!(health.snapshot().consecutive_failures, 0);
        health.record_stopped(checked_at);
        assert_eq!(health.snapshot().status, DcpHealthStatus::Stopped);
    }

    #[test]
    fn rollback_metric_saturates_instead_of_wrapping() {
        let metrics = DcpMetrics::default();

        metrics.record_rollbacks(usize::MAX);
        let before = metrics.snapshot().rollbacks;
        metrics.record_rollbacks(1);

        assert_eq!(metrics.snapshot().rollbacks, before.saturating_add(1));
    }
}
