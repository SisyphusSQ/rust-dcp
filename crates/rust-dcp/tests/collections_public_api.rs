//! Public collection API contract for the umbrella crate.

use rust_dcp::{
    CollectionFilter, CollectionManifest, CollectionRegistry, CollectionRegistryStatus,
    CollectionSelection, CollectionStream, ManifestCollection, ManifestScope,
    ResolvedCollectionFilter, fetch_collection_manifest, fetch_selection_high_seqnos,
    resolve_collection_id, resolve_collection_selection,
};

#[test]
fn umbrella_crate_exposes_the_collection_runtime() {
    let manifest = CollectionManifest::parse(
        br#"{"uid":"2a","scopes":[{"uid":"8","name":"inventory","collections":[{"uid":"9","name":"airline"}]}]}"#,
    )
    .unwrap();
    let scope: &ManifestScope = &manifest.scopes[0];
    let collection: &ManifestCollection = &scope.collections[0];
    let resolved: ResolvedCollectionFilter = manifest
        .resolve(
            &CollectionFilter {
                scope: scope.name.clone(),
                collections: vec![collection.name.clone()],
            },
            None,
        )
        .unwrap();
    let selection: CollectionSelection = resolved.into();
    let registry = CollectionRegistry::new(selection);
    let status: CollectionRegistryStatus = registry.status().unwrap();

    assert_eq!(status.manifest_uid, Some(0x2a));
    let _ = std::any::type_name::<CollectionStream<std::iter::Empty<()>>>();
    let _ = fetch_collection_manifest;
    let _ = fetch_selection_high_seqnos;
    let _ = resolve_collection_id;
    let _ = resolve_collection_selection;
}
