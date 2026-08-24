# rust-dcp

An asynchronous, embeddable Rust SDK for building reliable Couchbase Database Change Protocol (DCP) consumers on Tokio.

The frozen first-release feature scope is implemented and covered by deterministic unit and mock-transport tests. Live Couchbase Server E2E validation is intentionally tracked as a separate phase; see the [compatibility matrix](docs/compatibility.md) for the exact boundary.

## Capabilities

- password authentication with SASL PLAIN or SCRAM, plus TCP or TLS with platform and custom root CAs;
- bucket, scope, collection, whole-scope, and server-side multi-collection streams;
- mutation, deletion, expiration, snapshot marker, `SeqNoAdvanced`, system event, stream-end, and OSO marker models;
- earliest, latest, and durable-checkpoint starts in finite or infinite mode;
- CCCP topology discovery, active-vBucket routing, failover logs, high sequence numbers, topology refresh, and stream reopen;
- DCP flow control, NOOP handling, dead-connection detection, bounded queues, and generation fencing;
- manual or automatic per-vBucket checkpoints backed by a file, Couchbase XATTR documents, or a custom async store;
- explicit rollback policy and active-plus-replica persistence rollback mitigation;
- DCP priority, optional Couchbase Change Streams, Snappy decompression, datatype flags, and raw XATTR framing;
- standalone or externally fenced assignments, with optional Couchbase and Kubernetes membership crates;
- application-owned metrics export, health snapshots, and `tracing` instrumentation without a forced HTTP server.

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

## Checkpoint stores

- `FileCheckpointStore` atomically replaces a go-dcp-compatible JSON file.
- `CouchbaseCheckpointStore::from_config` uses the built-in Tokio KV/XATTR adapter and go-dcp v1.3.1 metadata keys, XATTR name, and document schema.
- `CouchbaseCheckpointStore::from_config_in_collection` places metadata in a named scope and collection.
- Implement `CheckpointStore` for a fully custom asynchronous backend, or implement `CouchbaseCheckpointCollection` to reuse the go-dcp-compatible Couchbase metadata policy with another KV adapter.

Every store is bucket-UUID scoped. A checkpoint from a recreated bucket is reported as an error instead of being silently reused.

## Assignment and membership

`DcpSubscriptionSpec::standalone` owns every current vBucket. `DcpSubscriptionSpec::external` accepts a `VBucketAssignment` with a monotonic generation fence for applications that own scheduling and leases.

Optional coordination runtimes are separate crates:

- `rust-dcp-membership-couchbase`: CAS-fenced registry, heartbeats, stale-member pruning, and deterministic rebalance, with a built-in Tokio KV store;
- `rust-dcp-membership-kubernetes`: StatefulSet ordinal assignment or a Tokio Kubernetes Pod watcher with UID fencing and ready/running membership rules.

Membership updates produce assignments; the integrating application owns subscription replacement at an assignment boundary.

## Observability

`DcpClient::metrics` and `DcpSubscription::metrics` return cloneable counters/gauges with snapshot APIs. Health handles expose bootstrap, probe, topology-generation, connection, failure, and stopped state. Runtime operations emit `tracing` spans and events. Export format, HTTP endpoints, and OpenTelemetry/Prometheus integration remain application choices.

## Crates

| Crate | Responsibility |
|---|---|
| `rust-dcp` | Umbrella public API |
| `rust-dcp-core` | Tokio transport, topology, stream lifecycle, checkpoints, rollback, collections, and client API |
| `rust-dcp-protocol` | Memcached/DCP framing, commands, parsers, and event codecs |
| `rust-dcp-membership-couchbase` | Couchbase-backed membership and assignment extension |
| `rust-dcp-membership-kubernetes` | Kubernetes-backed membership and assignment extension |

The SDK owns Couchbase protocol, topology, and stream correctness. The application owns downstream durability, assignment orchestration, deployment, and exporter choices.

## Compatibility and validation

The behavioral baseline is [go-dcp v1.3.1](https://github.com/Trendyol/go-dcp/tree/v1.3.1), with low-level wire behavior checked against [gocbcore v10.7.1](https://github.com/couchbase/gocbcore/tree/v10.7.1).

See [docs/compatibility.md](docs/compatibility.md) for feature-by-feature behavior, intentional differences, Server capability gates, deterministic validation evidence, and the deferred live E2E matrix.

The workspace requires Rust 1.85 or newer and uses the 2024 edition.

## Status

The complete frozen feature scope is present on `main`. The public API is versioned as `0.1.0` and may still evolve before a stable semver release. Live Couchbase Server E2E, performance characterization, packaging, and release publication are outside the completed implementation/unit-test boundary.

## License

No license has been selected yet. Until a license is added, reuse of this repository is subject to the default rights reserved by copyright law.
