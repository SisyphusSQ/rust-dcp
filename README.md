# rust-dcp

An asynchronous, embeddable Rust SDK for building reliable Couchbase Database Change Protocol (DCP) consumers on Tokio.

The frozen first-release feature scope is implemented and covered by deterministic unit and mock-transport tests. Live Couchbase Server E2E validation is intentionally tracked as a separate phase; see the [compatibility matrix](docs/compatibility.md) for the exact boundary.

## Capabilities

- password authentication with SASL PLAIN or SCRAM, plus TCP or TLS with platform and custom root CAs;
- bucket, scope, collection, whole-scope, and server-side multi-collection streams;
- mutation, deletion, expiration, snapshot marker, `SeqNoAdvanced`, system event, stream-end, and OSO marker models;
- optional go-dcp-compatible `listener.skipUntil` filtering with checkpoint-safe internal progress;
- earliest, latest, and durable-checkpoint starts in finite or infinite mode;
- CCCP topology discovery, active-vBucket routing, failover logs, high sequence numbers, topology refresh, and stream reopen;
- DCP flow control, NOOP handling, dead-connection detection, bounded queues, and generation fencing;
- manual or automatic per-vBucket checkpoints backed by a file, Couchbase XATTR documents, noop/read-only adapters, or a custom async store;
- optional official Couchbase Rust SDK adapters for modern-server checkpoint XATTR and membership KV/CAS operations, without replacing the DCP transport;
- explicit rollback policy and active-plus-replica persistence rollback mitigation;
- DCP priority, optional Couchbase Change Streams, Snappy decompression, datatype flags, and raw XATTR framing;
- standalone or externally fenced assignments, with optional Couchbase and Kubernetes membership crates;
- application-owned Prometheus collection, health snapshots, and `tracing` instrumentation without a forced HTTP server.

OSO packets are parsed and remain visible, but rust-dcp does not request OSO enablement. This is deliberate until an OSO-aware checkpoint/recovery contract is defined.

## Quick start

```rust,no_run
use std::sync::Arc;

use futures_util::StreamExt;
use rust_dcp::{
    CheckpointStore, Credentials, DcpClient, DcpConfig, DcpEvent, DcpSubscriptionSpec,
    FileCheckpointStore,
};

struct Target;

impl Target {
    async fn apply(&self, _event: &DcpEvent) -> rust_dcp::Result<()> {
        // Commit to the downstream system here.
        Ok(())
    }
}

#[tokio::main]
async fn main() -> rust_dcp::Result<()> {
    let config = DcpConfig::builder(Credentials::new("user", "password"), "source-bucket")
        .seed("127.0.0.1")?
        .build()?;
    let client = DcpClient::connect(config).await?;
    let checkpoint_store: Arc<dyn CheckpointStore> =
        Arc::new(FileCheckpointStore::new("./checkpoints.json")?);
    let spec = DcpSubscriptionSpec::standalone(checkpoint_store);
    let mut subscription = client.subscribe(spec).await?;
    let target = Target;

    while let Some(delivery) = subscription.next().await.transpose()? {
        target.apply(delivery.event()).await?;
        delivery.mark_processed().await?;
    }

    subscription.close().await?;
    client.close().await?;
    Ok(())
}
```

`DcpSubscription` implements `futures_util::Stream<Item = rust_dcp::Result<DcpDelivery>>`. A `DcpDelivery` is consumed by `mark_processed`; dropping it without marking it processed does not advance the durable checkpoint.

## Delivery and recovery contract

Delivery is ordered within each vBucket and at least once. rust-dcp does not claim cluster-wide ordering or exactly-once downstream effects.

Network flow control, application processing, and checkpoint persistence are separate lifecycle points:

```text
network buffer credit returned
        != application processing completed
        != checkpoint durably persisted
```

Network credit is based on bounded runtime admission and cannot be delayed indefinitely by the application. `mark_processed` advances only contiguous application progress. Checkpoint coordinator flushes—during required initialization, an explicit `DcpSubscription::flush`, the automatic scheduler, or final shutdown—make that progress durable.

Rollback is never silently hidden. `RollbackPolicy::StopAndReport` is the default. `RewindAndReplay` must be selected explicitly, and `DelegateToHandler` requires an application callback. Rollback mitigation is enabled by default: delivery waits for the active and every available replica to persist the required history position, polling every 1 second with a 5-second node-batch timeout and a 60-second maximum delivery stall.

### Listener cutoff

`ListenerConfig::skip_until` matches go-dcp's `dcp.listener.skipUntil`: mutation, deletion, and expiration CAS values are interpreted as nanoseconds since the Unix epoch and truncated to whole seconds before comparison. Events strictly before the cutoff are not delivered; an event at the cutoff second is delivered. Snapshot markers, `SeqNoAdvanced`, system events, stream ends, and OSO markers remain visible.

```rust,no_run
use std::time::{Duration, UNIX_EPOCH};

use rust_dcp::{Credentials, DcpConfig, ListenerConfig};

let config = DcpConfig::builder(Credentials::new("user", "password"), "source-bucket")
    .seed("127.0.0.1")?
    .listener(ListenerConfig {
        skip_until: Some(UNIX_EPOCH + Duration::from_secs(1_700_000_000)),
    })
    .build()?;
# Ok::<(), rust_dcp::DcpError>(())
```

Skipped document events still pass rollback mitigation and are internally acknowledged by the checkpoint coordinator. They therefore advance contiguous progress without requiring an application callback and are not counted as delivered or application-processed events.

## Checkpoint stores

- `FileCheckpointStore` atomically replaces a go-dcp-compatible JSON file.
- `CouchbaseCheckpointStore::from_config` uses the built-in Tokio KV/XATTR adapter and go-dcp v1.3.1 metadata keys, XATTR name, and document schema.
- `CouchbaseCheckpointStore::from_config_in_collection` places metadata in a named scope and collection.
- `CouchbaseSdkCheckpointCollection` in `rust-dcp-couchbase-sdk` lets a caller reuse an official SDK `Collection` for checkpoint XATTR operations on server/toolchain combinations supported by that SDK.
- `NoopCheckpointStore` always starts from the configured fallback and accepts save/clear calls without persistence.
- `ReadOnlyCheckpointStore` loads from a wrapped store but suppresses save/clear, so replay/debug sessions cannot alter the source checkpoint.
- Implement `CheckpointStore` for a fully custom asynchronous backend, or implement `CouchbaseCheckpointCollection` to reuse the go-dcp-compatible Couchbase metadata policy with another KV adapter.

Every store is bucket-UUID scoped. A checkpoint from a recreated bucket is reported as an error instead of being silently reused.

Noop and read-only modes deliberately do not retain new progress across restarts. Their successful no-op writes prevent repeated in-process flush attempts; choose a writable store whenever restart continuity is required.

## Assignment and membership

`DcpSubscriptionSpec::standalone` owns every current vBucket. `DcpSubscriptionSpec::external` accepts a `VBucketAssignment` with a monotonic generation fence for applications that own scheduling and leases.

Optional coordination runtimes are separate crates:

- `rust-dcp-membership-couchbase`: CAS-fenced registry, heartbeats, stale-member pruning, and deterministic rebalance, with a built-in Tokio KV store;
- `rust-dcp-couchbase-sdk`: optional official SDK `Collection` adapter for the Couchbase membership registry on modern supported servers;
- `rust-dcp-membership-kubernetes`: StatefulSet ordinal assignment or a Tokio Kubernetes Pod watcher with UID fencing and ready/running membership rules.

Membership updates produce assignments; the integrating application owns subscription replacement at an assignment boundary.

## Observability

`DcpClient::metrics` and `DcpSubscription::metrics` return cloneable counters/gauges with snapshot APIs. Health handles expose bootstrap, probe, topology-generation, connection, failure, and stopped state. Runtime operations emit `tracing` spans and events.

Enable the umbrella crate's `prometheus` feature to register those live handles in an application-owned registry:

```rust,no_run
use prometheus::Registry;
use rust_dcp::{DcpClient, DcpPrometheusCollector};

fn register_dcp_metrics(
    client: &DcpClient,
    registry: &Registry,
) -> prometheus::Result<()> {
    DcpPrometheusCollector::new(client.metrics(), client.health())?.register(registry)
}
```

The collector emits `rust_dcp_*` counters and gauges for bootstrap, topology, reconnect, health, connections, assignments, deliveries by event type, processing, stream errors, generation fencing, rollback, and rollback mitigation. Each `Registry::gather` reads the latest in-memory snapshot and performs no network or checkpoint I/O. `rust-dcp` does not create an HTTP listener or select an HTTP framework; the integrating Tokio application owns text encoding, routing, authentication, and endpoint lifecycle.

## Crates

| Crate | Responsibility |
|---|---|
| `rust-dcp` | Umbrella public API |
| `rust-dcp-couchbase-sdk` | Official Couchbase Rust SDK adapters for checkpoint XATTR and membership KV/CAS metadata operations; no DCP transport |
| `rust-dcp-core` | Tokio transport, topology, stream lifecycle, checkpoints, rollback, collections, and client API |
| `rust-dcp-protocol` | Memcached/DCP framing, commands, parsers, and event codecs |
| `rust-dcp-prometheus` | Standard Prometheus `Collector` over live SDK metrics and health handles; no HTTP server |
| `rust-dcp-membership-couchbase` | Couchbase-backed membership and assignment extension |
| `rust-dcp-membership-kubernetes` | Kubernetes-backed membership and assignment extension |

The SDK owns Couchbase protocol, topology, and stream correctness. The application owns downstream durability, assignment orchestration, deployment, and exporter choices.

## Compatibility and validation

The behavioral baseline is [go-dcp v1.3.1](https://github.com/Trendyol/go-dcp/tree/v1.3.1), with low-level wire behavior checked against [gocbcore v10.7.1](https://github.com/couchbase/gocbcore/tree/v10.7.1).

See [docs/compatibility.md](docs/compatibility.md) for feature-by-feature behavior, intentional differences, Server capability gates, deterministic validation evidence, and the deferred live E2E matrix. The versioned decision about what can safely reuse `couchbase-rs` is documented in [docs/official-sdk-boundary.md](docs/official-sdk-boundary.md).

The base DCP/protocol/membership crates require Rust 1.85 or newer and use the 2024 edition. The optional `rust-dcp-couchbase-sdk` crate declares Rust 1.90 because the official Couchbase Rust SDK 1.0 support policy starts at Rust 1.90.

## Status

The complete frozen feature scope is present on `main`. The public API is versioned as `0.1.0` and may still evolve before a stable semver release. Live Couchbase Server E2E, performance characterization, packaging, and release publication are outside the completed implementation/unit-test boundary.

## License

No license has been selected yet. Until a license is added, reuse of this repository is subject to the default rights reserved by copyright law.
