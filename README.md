# rust-dcp

An asynchronous Rust SDK for building reliable Couchbase Database Change Protocol (DCP) consumers.

## Purpose

`rust-dcp` is designed as an independent, embeddable library. It focuses on protocol correctness, resumable delivery, and clear recovery semantics while leaving application-specific scheduling and downstream storage to the integrating application.

The core delivery contract is:

- ordered delivery within each vBucket;
- at-least-once processing semantics;
- explicit checkpoints that retain the vBucket UUID, sequence number, and snapshot bounds;
- observable failover and rollback decisions;
- no implicit claim of cluster-wide ordering or exactly-once effects.

## Architecture

The implementation is organized into four boundaries:

```text
Application or integration adapter
        │  partition ownership, generation fencing,
        │  downstream commit, checkpoint persistence
        ▼
rust-dcp
        │  subscription API, events, delivery lifecycle
        ▼
rust-dcp-core
        │  topology, node connections, stream lifecycle,
        │  reconnect, failover, rollback, flow control
        ▼
rust-dcp-protocol
           binary framing, opcodes, message codecs
```

The SDK owns Couchbase topology and DCP stream behavior. The integrating application owns worker assignment, leases, deployment concerns, and the durability policy for its downstream system.

## Delivery and recovery semantics

Network flow control, application processing, and checkpoint persistence are separate operations:

```text
buffer credit returned
        ≠ application processing completed
        ≠ checkpoint durably persisted
```

A delivery is eligible for checkpoint advancement only after the application has completed processing and the checkpoint store has durably accepted the contiguous per-vBucket position. Out-of-order completion must not skip an earlier gap.

Rollback is surfaced as an explicit recovery event. The default policy is to stop and report so that the application can choose between replay, partition rebuild, or another verified repair path. Silent rewind is not assumed to be safe for every downstream system.

## Scope

The planned capability set covers:

1. binary framing, authentication, bucket selection, DCP session setup, and event codecs;
2. flow control, NOOP handling, snapshot markers, stream end, and sequence advancement;
3. vBucket topology discovery and multiplexed connections to the active nodes;
4. reconnect, failover-log evaluation, rollback handling, and generation fencing;
5. collections, scopes, system events, manifests, filters, compression, and durable checkpoint stores;
6. standalone consumption and integration hooks for applications that already manage partition assignment.

The initial implementation will keep OSO disabled until its recovery model is fully specified and tested.

## Status

This repository is the public home for the design and implementation of `rust-dcp`. The API and module boundaries may evolve as protocol behavior is validated against supported Couchbase Server versions.

## License

No license has been selected yet. Until a license is added, reuse of this repository is subject to the default rights reserved by copyright law.
