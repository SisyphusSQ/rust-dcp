//! Collection manifest and resolution contracts.

use std::{collections::BTreeSet, time::Duration};

use bytes::{BufMut, Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt, stream};
use rust_dcp_core::{
    BootstrapCapabilities, CollectionFilter, CollectionManifest, CollectionRegistry, DataType,
    DcpControlFeature, DcpDeletion, DcpError, DcpEvent, DcpExpiration, DcpMutation, DcpStreamItem,
    KvConnection, SaslMechanism, SystemEvent, SystemEventKind, fetch_collection_manifest,
    fetch_selection_high_seqnos, resolve_collection_id, resolve_collection_selection,
};
use rust_dcp_protocol::{Frame, FrameCodec, HelloFeature, Opcode, Status};
use tokio::io::duplex;
use tokio_util::codec::Framed;

fn airline_registry() -> CollectionRegistry {
    let manifest = CollectionManifest::parse(
        br#"{"uid":"2a","scopes":[{"uid":"8","name":"inventory","collections":[{"uid":"9","name":"airline"}]}]}"#,
    )
    .unwrap();
    CollectionRegistry::new(
        manifest
            .resolve(
                &CollectionFilter {
                    scope: "inventory".into(),
                    collections: vec!["airline".into()],
                },
                None,
            )
            .unwrap()
            .into(),
    )
}

#[test]
fn manifest_parses_hex_ids_and_optional_collection_properties() {
    let manifest = CollectionManifest::parse(
        br#"{
          "uid":"a",
          "scopes":[{
            "uid":"8",
            "name":"inventory",
            "collections":[
              {"uid":"9","name":"airline","maxTTL":60,"history":true},
              {"uid":"a","name":"airport","history":false}
            ]
          }]
        }"#,
    )
    .unwrap();

    assert_eq!(manifest.uid, 0xa);
    assert_eq!(manifest.scopes.len(), 1);
    let scope = &manifest.scopes[0];
    assert_eq!(scope.uid, 0x8);
    assert_eq!(scope.name, "inventory");
    assert_eq!(scope.collections.len(), 2);
    assert_eq!(scope.collections[0].uid, 0x9);
    assert_eq!(scope.collections[0].name, "airline");
    assert_eq!(scope.collections[0].max_ttl, 60);
    assert_eq!(scope.collections[0].history, Some(true));
    assert_eq!(scope.collections[1].max_ttl, 0);
    assert_eq!(scope.collections[1].history, Some(false));
}

#[test]
fn manifest_rejects_ambiguous_scope_and_collection_identity() {
    let duplicate_scope_name = br#"{
      "uid":"4",
      "scopes":[
        {"uid":"8","name":"inventory","collections":[]},
        {"uid":"9","name":"inventory","collections":[]}
      ]
    }"#;
    let duplicate_scope_id = br#"{
      "uid":"4",
      "scopes":[
        {"uid":"8","name":"inventory","collections":[]},
        {"uid":"8","name":"archive","collections":[]}
      ]
    }"#;
    let duplicate_collection_name = br#"{
      "uid":"4",
      "scopes":[{"uid":"8","name":"inventory","collections":[
        {"uid":"9","name":"airline"},
        {"uid":"a","name":"airline"}
      ]}]
    }"#;
    let duplicate_collection_id = br#"{
      "uid":"4",
      "scopes":[
        {"uid":"8","name":"inventory","collections":[{"uid":"9","name":"airline"}]},
        {"uid":"a","name":"archive","collections":[{"uid":"9","name":"old-airline"}]}
      ]
    }"#;

    for ambiguous in [
        &duplicate_scope_name[..],
        &duplicate_scope_id[..],
        &duplicate_collection_name[..],
        &duplicate_collection_id[..],
    ] {
        assert!(CollectionManifest::parse(ambiguous).is_err());
    }
}

#[test]
fn manifest_resolves_multi_collection_filter_at_one_uid() {
    let manifest = CollectionManifest::parse(
        br#"{
          "uid":"2a",
          "scopes":[{"uid":"8","name":"inventory","collections":[
            {"uid":"9","name":"airline"},
            {"uid":"a","name":"airport"},
            {"uid":"b","name":"hotel"}
          ]}]
        }"#,
    )
    .unwrap();
    let configured = CollectionFilter {
        scope: "inventory".into(),
        collections: vec!["airport".into(), "airline".into()],
    };

    let resolved = manifest.resolve(&configured, Some(7)).unwrap();

    assert_eq!(resolved.manifest_uid(), 0x2a);
    assert_eq!(resolved.scope_id(), 0x8);
    assert_eq!(resolved.collection_ids(), &[0xa, 0x9]);
    assert_eq!(resolved.collection_name(0xa), Some("airport"));
    assert_eq!(resolved.collection_name(0xb), None);
    assert_eq!(resolved.stream_filter().scope_id, None);
    assert_eq!(resolved.stream_filter().collection_ids, vec![0xa, 0x9]);
    assert_eq!(resolved.stream_filter().manifest_uid, None);
    assert_eq!(resolved.stream_filter().stream_id, Some(7));
}

#[test]
fn empty_collection_list_resolves_the_entire_scope() {
    let manifest = CollectionManifest::parse(
        br#"{
          "uid":"2a",
          "scopes":[{"uid":"8","name":"inventory","collections":[
            {"uid":"9","name":"airline"},
            {"uid":"a","name":"airport"}
          ]}]
        }"#,
    )
    .unwrap();
    let configured = CollectionFilter {
        scope: "inventory".into(),
        collections: Vec::new(),
    };

    let resolved = manifest.resolve(&configured, None).unwrap();

    assert!(resolved.collection_ids().is_empty());
    assert_eq!(resolved.collection_name(0x9), Some("airline"));
    assert_eq!(resolved.collection_name(0xa), Some("airport"));
    assert_eq!(resolved.stream_filter().scope_id, Some(0x8));
    assert!(resolved.stream_filter().collection_ids.is_empty());
    assert_eq!(resolved.stream_filter().manifest_uid, None);
}

#[tokio::test]
async fn collection_metadata_uses_the_tokio_kv_request_path() {
    let (client_io, server_io) = duplex(8 * 1024);
    let mut connection =
        KvConnection::from_io(client_io, "collection-unit-test", Duration::from_secs(1));
    let server = tokio::spawn(async move {
        let mut framed = Framed::new(server_io, FrameCodec::default());

        let manifest_request = framed.next().await.unwrap().unwrap();
        assert_eq!(manifest_request.opcode, Opcode::COLLECTIONS_GET_MANIFEST);
        let mut manifest_response =
            Frame::response(Opcode::COLLECTIONS_GET_MANIFEST, Status::SUCCESS);
        manifest_response.opaque = manifest_request.opaque;
        manifest_response.value = Bytes::from_static(
            br#"{"uid":"2a","scopes":[{"uid":"8","name":"inventory","collections":[{"uid":"9","name":"airline"}]}]}"#,
        );
        framed.send(manifest_response).await.unwrap();

        let id_request = framed.next().await.unwrap().unwrap();
        assert_eq!(id_request.opcode, Opcode::COLLECTIONS_GET_ID);
        assert_eq!(&id_request.value[..], b"inventory.airline");
        let mut id_response = Frame::response(Opcode::COLLECTIONS_GET_ID, Status::SUCCESS);
        id_response.opaque = id_request.opaque;
        let mut extras = BytesMut::new();
        extras.put_u64(0x2a);
        extras.put_u32(0x9);
        id_response.extras = extras.freeze();
        framed.send(id_response).await.unwrap();
    });

    let manifest = fetch_collection_manifest(&mut connection).await.unwrap();
    assert_eq!(manifest.uid, 0x2a);
    let collection = resolve_collection_id(&mut connection, "inventory", "airline")
        .await
        .unwrap();
    assert_eq!(collection.manifest_uid, 0x2a);
    assert_eq!(collection.collection_id, 0x9);
    server.await.unwrap();
}

#[tokio::test]
async fn legacy_server_only_allows_the_default_collection_without_a_wire_filter() {
    let (client_io, _server_io) = duplex(256);
    let mut connection =
        KvConnection::from_io(client_io, "legacy-unit-test", Duration::from_secs(1));
    let capabilities = BootstrapCapabilities {
        hello_features: BTreeSet::new(),
        sasl_mechanism: SaslMechanism::Plain,
        dcp_controls: BTreeSet::new(),
    };
    let default = CollectionFilter::default();

    let selection = resolve_collection_selection(&mut connection, &capabilities, &default, None)
        .await
        .unwrap();

    assert!(selection.stream_filter().is_none());
    assert_eq!(selection.collection_name(None), Some("_default"));
    assert_eq!(selection.collection_name(Some(0)), None);

    let custom = CollectionFilter {
        scope: "inventory".into(),
        collections: vec!["airline".into()],
    };
    assert!(matches!(
        resolve_collection_selection(&mut connection, &capabilities, &custom, None,).await,
        Err(DcpError::Unsupported(_))
    ));
}

#[tokio::test]
async fn stream_id_resolution_requires_the_negotiated_dcp_control() {
    let (client_io, _server_io) = duplex(256);
    let mut connection =
        KvConnection::from_io(client_io, "stream-id-unit-test", Duration::from_millis(10));
    let capabilities = BootstrapCapabilities {
        hello_features: BTreeSet::from([HelloFeature::Collections]),
        sasl_mechanism: SaslMechanism::Plain,
        dcp_controls: BTreeSet::<DcpControlFeature>::new(),
    };

    assert!(matches!(
        resolve_collection_selection(
            &mut connection,
            &capabilities,
            &CollectionFilter::default(),
            Some(7),
        )
        .await,
        Err(DcpError::Unsupported(_))
    ));
}

#[test]
fn registry_decorates_document_events_from_the_resolved_manifest() {
    let manifest = CollectionManifest::parse(
        br#"{
          "uid":"2a",
          "scopes":[{"uid":"8","name":"inventory","collections":[
            {"uid":"9","name":"airline"}
          ]}]
        }"#,
    )
    .unwrap();
    let resolved = manifest
        .resolve(
            &CollectionFilter {
                scope: "inventory".into(),
                collections: vec!["airline".into()],
            },
            None,
        )
        .unwrap();
    let registry = CollectionRegistry::new(resolved.into());
    let event = DcpEvent::Mutation(DcpMutation {
        vbucket: 7,
        seqno: 10,
        rev_seqno: 1,
        flags: 0,
        expiry: 0,
        lock_time: 0,
        cas: 99,
        datatype: DataType::JSON,
        collection_id: Some(0x9),
        collection_name: None,
        key: Bytes::from_static(b"flight::1"),
        value: Bytes::from_static(br#"{"type":"airline"}"#),
    });

    let DcpEvent::Mutation(decorated) = registry.decorate(event).unwrap() else {
        panic!("expected mutation");
    };

    assert_eq!(decorated.collection_id, Some(0x9));
    assert_eq!(decorated.collection_name.as_deref(), Some("airline"));
}

#[test]
fn registry_decorates_deletion_and_expiration_events() {
    let manifest = CollectionManifest::parse(
        br#"{"uid":"2a","scopes":[{"uid":"8","name":"inventory","collections":[{"uid":"9","name":"airline"}]}]}"#,
    )
    .unwrap();
    let registry = CollectionRegistry::new(
        manifest
            .resolve(
                &CollectionFilter {
                    scope: "inventory".into(),
                    collections: vec!["airline".into()],
                },
                None,
            )
            .unwrap()
            .into(),
    );
    let deletion = DcpEvent::Deletion(DcpDeletion {
        vbucket: 7,
        seqno: 11,
        rev_seqno: 2,
        delete_time: Some(100),
        cas: 101,
        collection_id: Some(0x9),
        collection_name: None,
        key: Bytes::from_static(b"flight::1"),
        value: Bytes::new(),
        datatype: DataType::default(),
    });
    let expiration = DcpEvent::Expiration(DcpExpiration {
        vbucket: 7,
        seqno: 12,
        rev_seqno: 3,
        delete_time: Some(200),
        cas: 102,
        collection_id: Some(0x9),
        collection_name: None,
        key: Bytes::from_static(b"flight::2"),
        value: Bytes::new(),
        datatype: DataType::default(),
    });

    let DcpEvent::Deletion(deletion) = registry.decorate(deletion).unwrap() else {
        panic!("expected deletion");
    };
    let DcpEvent::Expiration(expiration) = registry.decorate(expiration).unwrap() else {
        panic!("expected expiration");
    };

    assert_eq!(deletion.collection_name.as_deref(), Some("airline"));
    assert_eq!(expiration.collection_name.as_deref(), Some("airline"));
}

#[test]
fn registry_rejects_an_unmapped_collection_id() {
    let registry = airline_registry();
    let event = DcpEvent::Mutation(DcpMutation {
        vbucket: 7,
        seqno: 10,
        rev_seqno: 1,
        flags: 0,
        expiry: 0,
        lock_time: 0,
        cas: 99,
        datatype: DataType::JSON,
        collection_id: Some(0xa),
        collection_name: None,
        key: Bytes::from_static(b"flight::1"),
        value: Bytes::new(),
    });

    assert!(matches!(
        registry.decorate(event),
        Err(DcpError::Collection(_))
    ));
}

#[test]
fn scope_registry_applies_a_newer_collection_created_event() {
    let manifest = CollectionManifest::parse(
        br#"{"uid":"2a","scopes":[{"uid":"8","name":"inventory","collections":[{"uid":"9","name":"airline"}]}]}"#,
    )
    .unwrap();
    let registry = CollectionRegistry::new(
        manifest
            .resolve(
                &CollectionFilter {
                    scope: "inventory".into(),
                    collections: Vec::new(),
                },
                None,
            )
            .unwrap()
            .into(),
    );
    let creation = DcpEvent::SystemEvent(SystemEvent {
        vbucket: 7,
        seqno: 10,
        manifest_uid: 0x2b,
        version: 1,
        key: Bytes::from_static(b"hotel"),
        kind: SystemEventKind::CollectionCreated {
            scope_id: 0x8,
            collection_id: 0xa,
            max_ttl: Some(3600),
        },
    });
    registry.decorate(creation).unwrap();
    let hotel = DcpEvent::Mutation(DcpMutation {
        vbucket: 7,
        seqno: 11,
        rev_seqno: 1,
        flags: 0,
        expiry: 0,
        lock_time: 0,
        cas: 100,
        datatype: DataType::JSON,
        collection_id: Some(0xa),
        collection_name: None,
        key: Bytes::from_static(b"hotel::1"),
        value: Bytes::new(),
    });

    let DcpEvent::Mutation(hotel) = registry.decorate(hotel).unwrap() else {
        panic!("expected mutation");
    };
    assert_eq!(hotel.collection_name.as_deref(), Some("hotel"));
}

#[test]
fn registry_removes_a_collection_after_a_newer_drop_event() {
    let registry = airline_registry();
    let drop_event = DcpEvent::SystemEvent(SystemEvent {
        vbucket: 7,
        seqno: 12,
        manifest_uid: 0x2b,
        version: 0,
        key: Bytes::from_static(b"airline"),
        kind: SystemEventKind::CollectionDropped {
            scope_id: 0x8,
            collection_id: 0x9,
        },
    });
    registry.decorate(drop_event).unwrap();
    let stale_document = DcpEvent::Deletion(DcpDeletion {
        vbucket: 7,
        seqno: 13,
        rev_seqno: 2,
        delete_time: None,
        cas: 100,
        collection_id: Some(0x9),
        collection_name: None,
        key: Bytes::from_static(b"flight::1"),
        value: Bytes::new(),
        datatype: DataType::default(),
    });

    assert!(matches!(
        registry.decorate(stale_document),
        Err(DcpError::Collection(_))
    ));
}

#[test]
fn unknown_system_event_marks_the_registry_stale_but_remains_visible() {
    let registry = airline_registry();
    let unknown = DcpEvent::SystemEvent(SystemEvent {
        vbucket: 7,
        seqno: 14,
        manifest_uid: 0x2b,
        version: 0,
        key: Bytes::from_static(b"future"),
        kind: SystemEventKind::Unknown {
            code: 99,
            data: Bytes::from_static(b"future-data"),
        },
    });

    let DcpEvent::SystemEvent(visible) = registry.decorate(unknown).unwrap() else {
        panic!("expected system event");
    };
    let status = registry.status().unwrap();

    assert!(matches!(
        visible.kind,
        SystemEventKind::Unknown { code: 99, .. }
    ));
    assert_eq!(status.manifest_uid, Some(0x2b));
    assert!(status.stale);
}

#[tokio::test]
async fn collection_stream_decorates_events_and_preserves_control_items() {
    let registry = airline_registry();
    let source = stream::iter(vec![
        Ok(DcpStreamItem::TopologyConfig {
            source: "node-a".into(),
            payload: Bytes::from_static(b"config"),
        }),
        Ok(DcpStreamItem::Event(DcpEvent::Mutation(DcpMutation {
            vbucket: 7,
            seqno: 10,
            rev_seqno: 1,
            flags: 0,
            expiry: 0,
            lock_time: 0,
            cas: 99,
            datatype: DataType::JSON,
            collection_id: Some(0x9),
            collection_name: None,
            key: Bytes::from_static(b"flight::1"),
            value: Bytes::new(),
        }))),
    ]);
    let mut stream = registry.wrap(source);

    assert!(matches!(
        stream.next().await.unwrap().unwrap(),
        DcpStreamItem::TopologyConfig { .. }
    ));
    let DcpStreamItem::Event(DcpEvent::Mutation(mutation)) = stream.next().await.unwrap().unwrap()
    else {
        panic!("expected decorated mutation");
    };
    assert_eq!(mutation.collection_name.as_deref(), Some("airline"));
}

#[tokio::test]
async fn multi_collection_high_seqnos_take_the_per_vbucket_maximum() {
    let manifest = CollectionManifest::parse(
        br#"{"uid":"2a","scopes":[{"uid":"8","name":"inventory","collections":[{"uid":"9","name":"airline"},{"uid":"a","name":"airport"}]}]}"#,
    )
    .unwrap();
    let selection = manifest
        .resolve(
            &CollectionFilter {
                scope: "inventory".into(),
                collections: vec!["airline".into(), "airport".into()],
            },
            None,
        )
        .unwrap()
        .into();
    let (client_io, server_io) = duplex(8 * 1024);
    let mut connection =
        KvConnection::from_io(client_io, "seqno-unit-test", Duration::from_secs(1));
    let server = tokio::spawn(async move {
        let mut framed = Framed::new(server_io, FrameCodec::default());
        for (expected_id, entries) in [
            (0x9_u32, [(0_u16, 10_u64), (1, 20)]),
            (0xa_u32, [(0_u16, 15_u64), (1, 18)]),
        ] {
            let request = framed.next().await.unwrap().unwrap();
            assert_eq!(request.opcode, Opcode::GET_ALL_VB_SEQNOS);
            assert_eq!(
                u32::from_be_bytes(request.extras[4..8].try_into().unwrap()),
                expected_id
            );
            let mut response = Frame::response(Opcode::GET_ALL_VB_SEQNOS, Status::SUCCESS);
            response.opaque = request.opaque;
            let mut value = BytesMut::new();
            for (vbucket, seqno) in entries {
                value.put_u16(vbucket);
                value.put_u64(seqno);
            }
            response.value = value.freeze();
            framed.send(response).await.unwrap();
        }
    });

    let seqnos = fetch_selection_high_seqnos(&mut connection, &selection)
        .await
        .unwrap();

    assert_eq!(seqnos.get(&0), Some(&15));
    assert_eq!(seqnos.get(&1), Some(&20));
    server.await.unwrap();
}
