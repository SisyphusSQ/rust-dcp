# Compatibility matrix

This document separates implemented behavior from live deployment claims. The compatibility baseline is [go-dcp v1.3.1](https://github.com/Trendyol/go-dcp/tree/v1.3.1); low-level packet behavior is cross-checked against [gocbcore v10.7.1](https://github.com/couchbase/gocbcore/tree/v10.7.1).

## Status vocabulary

- **Implemented and unit-tested**: production code exists and deterministic unit/mock-transport tests cover the contract.
- **Capability-gated**: rust-dcp negotiates or probes the feature and either uses a compatible fallback or returns an explicit error.
- **Intentional difference**: behavior differs from go-dcp for an explicitly documented correctness or ownership reason.
- **Live E2E deferred**: no live Couchbase Server was contacted in the completed implementation phase.

An “implemented” row is not a claim that every listed Couchbase Server release has passed live certification.

## Core and metadata backend boundary

The base DCP path remains implemented on Tokio so the go-dcp-compatible Server 5.x–7.2 target is not narrowed. The official Couchbase Rust SDK 1.0 currently supports Rust 1.90+ and Couchbase Server 7.6–8.0; it marks Server 7.0–7.2 unsupported. Therefore official SDK reuse is isolated to the optional `rust-dcp-couchbase-sdk` metadata adapter crate and does not replace `rust-dcp-core` or `rust-dcp-protocol`.

| Path | Implementation | Declared toolchain/server boundary |
|---|---|---|
| DCP wire, bootstrap, topology, streaming, reconnect, rollback | `rust-dcp-protocol` + Tokio `rust-dcp-core` | Rust 1.85; Server 5.x and later behavioral target; live version certification deferred |
| Legacy checkpoint XATTR and membership KV/CAS | Built-in Tokio adapters | Preserves Server 5.x–7.2 paths; capability-gated; live E2E deferred |
| Modern checkpoint XATTR and membership KV/CAS | `rust-dcp-couchbase-sdk` over official `couchbase 1.0.2` `Collection` | Rust 1.90; use only within the official SDK server support matrix; live adapter E2E deferred |

See [official SDK reuse boundary](official-sdk-boundary.md) for the audited API gaps, backend selection, and the acceptance gate for any future DCP replacement.

## Behavioral compatibility

| Area | go-dcp v1.3.1 baseline | rust-dcp behavior | Status and boundary |
|---|---|---|---|
| Minimum legacy path | Supports Couchbase Server 5.x after go-dcp 1.1.16 | Default collection works without Collections negotiation; classic DCP/KV protocol remains available | Implemented and unit-tested; Server 5.x live E2E deferred |
| Authentication | Username/password through gocbcore | SASL mechanism negotiation with SCRAM-SHA512, SCRAM-SHA256, SCRAM-SHA1, and PLAIN fallback | Implemented and unit-tested |
| TLS | Secure connection and optional root CA | Tokio/rustls transport, platform roots, optional PEM roots, and explicit server name | Implemented and unit-tested; live certificate matrix deferred |
| Bucket bootstrap | HELLO, auth, bucket selection, DCP open, controls | Same sequence; required controls fail explicitly and optional controls are recorded as capabilities | Implemented and unit-tested |
| Topology and routing | vBucket map and node agents | CCCP parsing, stable node identity, default/alternate network selection, active routing, and topology revision fences | Implemented and unit-tested; live rebalance E2E deferred |
| Stream topology changes | Reopen streams after routing changes | Reconnects affected nodes/vBuckets from processed checkpoint state and drops stale generations | Implemented and unit-tested |
| Default collection | `_default._default` | Legacy no-prefix path when Collections is unavailable; collection ID zero when negotiated | Capability-gated and unit-tested |
| Named scope/collection | Server-side collection filters | Manifest and collection-ID resolution, whole-scope selection, named collection selection, and manifest UID fencing | Capability-gated and unit-tested; named collections require server support |
| Multiple collections | Multiple collection IDs per stream | One server-side filter with unique collection IDs; per-vBucket finite high sequence number is the maximum across selected collections | Implemented and unit-tested |
| Document events | Mutation, deletion, expiration | Typed mutation, deletion, and expiration events with key, value/tombstone, CAS, revision, vBucket, seqno, collection, and datatype | Implemented and unit-tested |
| Control/progress events | Snapshot, stream end, SeqNoAdvanced, system events, OSO callback | Snapshot marker, `StreamEnd`, `SeqNoAdvanced`, collection/scope system events, and visible OSO marker model | Implemented and unit-tested |
| OSO enablement | go-dcp exposes OSO callbacks | Parser/model only; rust-dcp deliberately does not request OSO enablement because checkpoint/recovery semantics are not frozen | Intentional disabled feature |
| Datatype and XATTR | DCP XATTR inclusion and raw event data | Negotiates XATTR, opens DCP with include-XATTR when supported, preserves datatype/future bits, and exposes raw XATTR-framed bytes | Implemented and unit-tested; XATTR body splitting remains application-owned |
| Snappy | SDK decompression | Negotiates Snappy/SnappyEverywhere, bounds decompressed size, decompresses before DCP parsing, and clears only the Snappy datatype bit | Implemented and unit-tested |
| Start position | Earliest/latest and persisted offsets | `StartPosition::Earliest`, `Latest`, or explicit checkpoint; durable store values take precedence per vBucket | Implemented and unit-tested |
| Listener skip-until | `dcp.listener.skipUntil` filters document events by CAS-derived event time | `ListenerConfig::skip_until` applies the same whole-second CAS cutoff to mutation/deletion/expiration; skipped documents are internally acknowledged so checkpoint progress does not replay forever | Implemented and unit-tested; control/progress/system events remain visible |
| Finite/infinite mode | Finite snapshot or continuous stream | Finite mode freezes initial high seqnos across reconnects; infinite mode continues until close/error | Implemented and unit-tested |
| High seqno and failover log | Reads partition metadata before/opening streams | Typed high-seqno and newest-first failover-log commands with response validation | Implemented and unit-tested |
| Flow control | Connection buffer ACKs | Buffer credit is returned after bounded runtime admission, independently from processing and checkpoint durability | Implemented and unit-tested; explicit lifecycle separation |
| NOOP and liveness | DCP NOOP and health checks | Required NOOP controls, producer NOOP responses, inbound-activity deadline, and periodic topology health probes | Implemented and unit-tested |
| Rollback policy | Library may rewind/recover according to go-dcp flow | Default `StopAndReport`; optional explicit `RewindAndReplay` or application `DelegateToHandler` | Intentional difference: no silent downstream rewind |
| Rollback mitigation | Poll persisted seqnos on active and replicas; default enabled, 1s interval | Same minimum-persisted watermark across active plus every available replica, with history-branch fencing and delivery gating; defaults are 1s poll, 5s node batch, and 60s maximum stall | Implemented and unit-tested |
| Mitigation stalls | go-dcp polling may wait without a delivery-level upper bound | Request timeout and maximum delivery stall are bounded; cancellation/topology changes interrupt waits and failures surface explicitly | Intentional safety difference |
| DCP priority | Low/medium/high control | `DcpPriority` maps to required `set_priority` control | Implemented and unit-tested |
| Change Streams | Enabled by default unless disabled; relevant to Server 7.2+ Magma history | Optional `change_streams=true` control, enabled by default when accepted and explicitly disableable | Capability-gated and unit-tested; Server 7.2+ live E2E deferred |
| Checkpoint progression | Per-vBucket checkpoint and interval/manual modes | Only contiguous events marked processed can advance a checkpoint; automatic Tokio scheduler or explicit `flush` makes it durable | Implemented and unit-tested |
| File checkpoint | File metadata backend | Atomic fsync/rename, go-dcp-compatible JSON schema, bucket UUID checks, and malformed-file errors | Implemented and unit-tested |
| Couchbase checkpoint | `_connector:cbgo:{group}:checkpoint:{vb}` document with `cbgo` XATTR | Same key, XATTR, and JSON schema; built-in Tokio KV routing preserves legacy support, while the optional official SDK adapter supports XATTR lookup/upsert/idempotent delete on modern supported servers | Both adapters implemented and unit-tested; live KV/XATTR E2E deferred |
| Custom checkpoint | Custom metadata implementation | Object-safe async `CheckpointStore`; lower-level `CouchbaseCheckpointCollection` adapter is also replaceable | Implemented and unit-tested |
| Noop checkpoint | `metadata.type: noop` returns empty state and accepts writes/clears | `NoopCheckpointStore` returns no stored vBuckets and accepts save/clear without persistence | Implemented and unit-tested |
| Read-only checkpoint | `metadata.readOnly: true` delegates load and suppresses save/clear | `ReadOnlyCheckpointStore` wraps any async store, delegates load, and suppresses save/clear | Implemented and unit-tested |
| Standalone assignment | One consumer can own the complete vBucket set | `AssignmentMode::Standalone` follows the current topology | Implemented and unit-tested |
| External/static assignment | Static/dynamic membership can provide a partition slice | `VBucketAssignment` carries an explicit monotonic generation fence | Implemented and unit-tested; subscription replacement remains orchestrator-owned |
| Couchbase membership | Couchbase-coordinated group membership | Separate crate with CAS registry, heartbeat, stale pruning, incarnation fencing, deterministic rebalance, and built-in Tokio KV store; optional official SDK KV/CAS store for modern supported servers | Both stores implemented and unit-tested; live coordination E2E deferred |
| Kubernetes membership | StatefulSet and Kubernetes membership discovery | Separate crate with StatefulSet ordinal mode and Pod watch mode using UID/readiness/termination fences | Implemented and unit-tested; live cluster E2E deferred |
| Metrics and health | Built-in HTTP/API and Prometheus collector | Optional `rust-dcp-prometheus` standard Collector over cloneable SDK metric/health handles; registry and HTTP surface are application-owned | Collector implemented and unit-tested; intentional HTTP embedding difference |
| Tracing | Optional external tracing integrations | `tracing` spans/events at bootstrap, subscription, processing, flush, and close boundaries | Implemented; exporter integration is application-owned |

## Server capability gates

| Server capability | rust-dcp behavior when present | Behavior when absent |
|---|---|---|
| Collections HELLO feature | Collection ID prefixes, manifest resolution, named/multi-collection filters, and optional stream IDs | Only `_default._default` is accepted; no key prefix or collection stream filter is sent |
| XATTR HELLO feature | DCP requests XATTR values; Couchbase checkpoint XATTR storage is available | DCP continues without include-XATTR; the built-in Couchbase checkpoint adapter returns an explicit unsupported-capability error |
| Snappy/SnappyEverywhere HELLO feature | Inbound compressed values are decompressed with a 32 MiB output bound | A compressed frame is rejected as an unnegotiated protocol violation |
| Change Streams DCP control | Capability is recorded and history events may be delivered according to server/bucket behavior | Bootstrap continues because the control is optional |
| Expiration opcode DCP control | Dedicated expiration events are decoded | Older server behavior remains representable as deletion semantics |
| Stream-end-on-close DCP control | Server stream end is awaited/decoded | The client synthesizes an explicit closed stream end during shutdown |
| Stream ID DCP control | Non-zero requested stream IDs are attached and fenced | A subscription requesting a stream ID is rejected explicitly |

## Validation evidence

The final code-bearing branch was validated before its commit/PR stage with:

```text
K8S_OPENAPI_ENABLED_VERSION=1.30 cargo test --workspace --all-features
199 tests passed; 0 failed

K8S_OPENAPI_ENABLED_VERSION=1.30 cargo clippy \
  --workspace --all-targets --all-features -- -D warnings

cargo fmt --all -- --check

K8S_OPENAPI_ENABLED_VERSION=1.30 RUSTDOCFLAGS='-D warnings' \
  cargo doc --workspace --all-features --no-deps
```

The test suite includes Tokio duplex/mock transports, exact packet layouts, malformed responses, SASL/SCRAM vectors, TLS root handling, Snappy limits, topology revisions, stream reopen and rollback paths, rollback mitigation, listener cutoff/checkpoint progress, checkpoint failure/races and noop/read-only adapters, official SDK checkpoint absence/XATTR mappings, official SDK membership CAS/conflict mappings, collection manifest/system events, lifecycle cancellation, health/metrics, and membership fencing.

No test in this evidence block contacted a live Couchbase Server.

## Deferred live E2E phase

The following is a separate acceptance phase and was not executed while completing the implementation goal:

| Live target | Primary acceptance |
|---|---|
| Couchbase Server 5.x | Password auth, optional TLS/root CA, default collection, mutation/deletion, finite/infinite streams, reconnect, file/custom/Couchbase checkpoint |
| Couchbase Server 7.x with named collections | Scope and multi-collection filters, system events, collection recreation/manifest fencing, XATTR, Snappy |
| Couchbase Server 7.2+ with Magma history | Change Streams control and historical snapshot behavior |
| Current supported server with replicas | Active-plus-all-available-replica rollback mitigation under persistence lag and failover |
| Server 7.6–8.0 with official Rust SDK adapter | Checkpoint XATTR load/upsert/delete, membership raw JSON/CAS conflict behavior, TLS, routing refresh, and shared SDK collection lifecycle |
| Live Couchbase membership group | CAS conflicts, heartbeat expiry, fenced duplicate incarnation, rebalance handoff |
| Live Kubernetes cluster | StatefulSet ordinal mode, Pod readiness/UID replacement, watch restart, assignment handoff |

Performance, soak, fault injection against real nodes, packaging, and release publication also remain outside the completed unit-test boundary.
