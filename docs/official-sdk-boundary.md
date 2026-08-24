# Official Couchbase Rust SDK reuse boundary

This decision was last audited on 2026-08-24 against `couchbase` and `couchbase-core` 1.0.2. It is versioned evidence, not a permanent claim about future Couchbase Rust SDK releases.

## Decision

rust-dcp uses the official Couchbase Rust SDK where its committed public API already matches an ordinary KV/sub-document contract:

- checkpoint XATTR lookup, upsert, and document removal;
- membership registry get, conditional insert, and CAS replace.

rust-dcp does not use `couchbase-core` as its DCP transport. DCP framing, bootstrap, topology, stream lifecycle, failover, rollback, flow control, and legacy compatibility remain in `rust-dcp-protocol` and the Tokio `rust-dcp-core` backend.

This is a dual-backend decision, not a fork in product behavior:

| Concern | Server 5.x–7.2 target | Modern server in official Rust SDK support matrix |
|---|---|---|
| DCP data stream | Tokio rust-dcp backend | Tokio rust-dcp backend |
| Checkpoint XATTR metadata | Built-in `CouchbaseKvCheckpointCollection` | Built-in adapter or `CouchbaseSdkCheckpointCollection` |
| Couchbase membership registry | Built-in `CouchbaseKvMembershipStore` | Built-in store or `CouchbaseSdkMembershipStore` |

The legacy adapters remain first-class. Selecting an official SDK metadata adapter never changes DCP event, ordering, rollback, checkpoint, or assignment semantics.

## Why the DCP core is not replaced

The official [Rust SDK compatibility matrix](https://docs.couchbase.com/rust-sdk/current/project-docs/compatibility.html) currently states:

- Rust SDK 1.0 supports Rust 1.90+;
- Couchbase Server 7.6–8.0 is supported;
- Couchbase Server 7.0–7.2 is unsupported.

Using that SDK as the only cluster core would therefore narrow rust-dcp's go-dcp-compatible Server 5.x–7.2 target before any live compatibility work even begins.

The public API is also not a Rust equivalent of `gocbcore.DCPAgent`. In the audited 1.0.2 source:

- the high-level `couchbase` crate exposes ordinary KV, sub-document, query, search, analytics, and management operations, but no DCP consumer API;
- `couchbase-core` describes itself as core networking/protocol code “not intended for direct use”;
- `memdx` is public, while configuration management, KV client lifecycle, TLS configuration, vBucket routing, and SCRAM internals needed for a complete DCP agent remain private in the versioned [`couchbase-core/src/lib.rs`](https://github.com/couchbase/couchbase-rs/blob/v1.0.2/sdk/couchbase-core/src/lib.rs);
- there is no public, stable operation set corresponding to DCP open/control, high-seqno, failover-log, stream request, buffer acknowledgement, NOOP response, and stream close.

Depending on private modules or reconstructing a second cluster lifecycle around public packet structs would keep the maintenance burden while adding an unstable dependency. It would not actually eliminate rust-dcp's DCP core.

## What is reused now

`rust-dcp-couchbase-sdk` depends on the high-level official `couchbase 1.0.2` crate and accepts an already configured `couchbase::collection::Collection`. The caller retains ownership of connection-string handling, credentials, TLS options, official SDK retry policy, and SDK cluster lifetime.

### Checkpoint adapter

`CouchbaseSdkCheckpointCollection` implements `rust_dcp_core::CouchbaseCheckpointCollection`:

- `lookup_in` reads the go-dcp-compatible XATTR as raw JSON bytes;
- `DocumentNotFound` and `PathNotFound` become an absent checkpoint;
- `mutate_in` uses XATTR plus upsert/MKDOC semantics so the backing document is created when absent;
- `remove` treats only `DocumentNotFound` as idempotent success;
- authentication, authorization, timeout, and all other SDK errors remain explicit checkpoint failures.

### Membership adapter

`CouchbaseSdkMembershipStore` implements `rust_dcp_membership_couchbase::MembershipStore`:

- `get` preserves the raw JSON registry bytes and CAS;
- `insert_raw` preserves create-if-absent semantics, mapping only `DocumentExists` to `Conflict`;
- CAS `replace_raw` maps only `CasMismatch` and `DocumentNotFound` to `Conflict`;
- other SDK failures remain explicit store failures.

The Tokio membership runtime still owns heartbeat scheduling, stale-member pruning, bounded CAS retry, lease expiry, incarnation fencing, and deterministic vBucket assignment.

## Selection examples

For Server 5.x–7.2 or one configuration surface shared with the DCP connection, retain the built-in adapters:

```rust,no_run
# use rust_dcp::{CouchbaseCheckpointStore, Credentials, DcpConfig};
# fn example() -> rust_dcp::Result<()> {
let config = DcpConfig::builder(Credentials::new("user", "password"), "source")
    .seed("127.0.0.1")?
    .build()?;
let checkpoint = CouchbaseCheckpointStore::from_config(config, "consumer-group")?;
# let _ = checkpoint;
# Ok(())
# }
```

For a modern supported server where the application already owns an official SDK `Cluster`, clone one SDK collection into the metadata adapters:

```rust,no_run
use std::sync::Arc;

use rust_dcp_couchbase_sdk::{
    CouchbaseSdkCheckpointCollection, CouchbaseSdkMembershipStore,
};
use rust_dcp_core::CouchbaseCheckpointStore;

# fn example(collection: couchbase::collection::Collection) -> rust_dcp_core::Result<()> {
let checkpoint_collection = Arc::new(CouchbaseSdkCheckpointCollection::new(collection.clone()));
let checkpoint = CouchbaseCheckpointStore::new(checkpoint_collection, "consumer-group")?;
let membership_store = Arc::new(CouchbaseSdkMembershipStore::new(collection));
# let _ = (checkpoint, membership_store);
# Ok(())
# }
```

The official SDK collection and rust-dcp DCP client use separate connection lifecycles. Closing or dropping one is not a signal to close the other; the integrating application owns both lifetimes.

## Gate for a future official DCP backend

A future `couchbase-rs` release may make more reuse safe. Replacing the Tokio backend requires all of the following before a support claim changes:

1. A public, documented DCP agent or raw-transport extension point with a usable stability classification.
2. Typed DCP open/control, high-seqno, failover-log, stream-request/rollback, buffer-ack, NOOP, and close operations.
3. Correct request-direction vBucket semantics, unsolicited producer-frame dispatch, opaque correlation, flexible framing extras, collection prefixes, exact received wire size, and bounded malformed-frame handling.
4. Public TLS/authentication and connection-lifecycle ownership that does not depend on private SDK modules.
5. Topology revision handling, active-vBucket routing, reconnect, failover history, and generation fencing compatible with rust-dcp's checkpoint contract.
6. Deterministic conformance tests for every existing unit/mock-transport behavior.
7. Live E2E against Server 5.x, Server 7.0/7.2, and current supported releases, or an explicit product decision to drop those older targets.

Until that gate is met, “use the official SDK” means reuse its stable high-level metadata operations, not replace a DCP core it does not publicly provide.

## Validation boundary

Both metadata adapters are implemented and unit-tested with deterministic fake SDK clients. The tests prove error classification, raw payload/CAS preservation, and exact adapter delegation. They do not prove network behavior against a live Couchbase cluster.

Live validation of official SDK TLS, routing refresh, XATTR support, collection behavior, and membership conflict handling remains part of the separately deferred live E2E phase.
