//! Tests for version resolution and S3-style versioning.

use std::collections::BTreeMap;

use shoal_types::{MANIFEST_VERSION, Manifest, ObjectId};

use super::test_tree;

fn test_manifest(object_id: ObjectId) -> Manifest {
    Manifest {
        version: MANIFEST_VERSION,
        object_id,
        total_size: 100,
        chunk_size: 1024,
        chunks: vec![],
        created_at: 1000,
        metadata: BTreeMap::new(),
    }
}

#[test]
fn test_put_v1_put_v2_resolve_returns_v2() {
    let tree = test_tree(1);
    let oid1 = ObjectId::from([1u8; 32]);
    let oid2 = ObjectId::from([2u8; 32]);
    let m1 = test_manifest(oid1);
    let m2 = test_manifest(oid2);

    tree.append_put("b", "k", oid1, &m1).unwrap();
    tree.append_put("b", "k", oid2, &m2).unwrap();

    assert_eq!(tree.resolve("b", "k").unwrap(), Some(oid2));
}

#[test]
fn test_put_v1_delete_resolve_returns_none() {
    let tree = test_tree(1);
    let oid = ObjectId::from([1u8; 32]);
    let manifest = test_manifest(oid);

    tree.append_put("b", "k", oid, &manifest).unwrap();
    tree.append_delete("b", "k").unwrap();

    assert_eq!(tree.resolve("b", "k").unwrap(), None);
}

#[test]
fn test_put_v1_put_v2_versions_returns_both() {
    let tree = test_tree(1);
    let oid1 = ObjectId::from([1u8; 32]);
    let oid2 = ObjectId::from([2u8; 32]);
    let m1 = test_manifest(oid1);
    let m2 = test_manifest(oid2);

    tree.append_put("b", "k", oid1, &m1).unwrap();
    tree.append_put("b", "k", oid2, &m2).unwrap();

    let versions = tree.versions("b", "k").unwrap();
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0].manifest_id, oid2); // latest first
    assert_eq!(versions[1].manifest_id, oid1);
}

#[test]
fn test_put_v1_delete_versions_includes_delete_marker() {
    let tree = test_tree(1);
    let oid = ObjectId::from([1u8; 32]);
    let manifest = test_manifest(oid);

    tree.append_put("b", "k", oid, &manifest).unwrap();
    tree.append_delete("b", "k").unwrap();

    let versions = tree.versions("b", "k").unwrap();
    assert_eq!(versions.len(), 2);
    assert!(versions[0].deleted, "latest should be delete marker");
    assert!(!versions[1].deleted);
}

#[test]
fn test_resolve_version_by_hlc() {
    let tree = test_tree(1);
    let oid1 = ObjectId::from([1u8; 32]);
    let oid2 = ObjectId::from([2u8; 32]);
    let m1 = test_manifest(oid1);
    let m2 = test_manifest(oid2);

    let e1 = tree.append_put("b", "k", oid1, &m1).unwrap();
    let _e2 = tree.append_put("b", "k", oid2, &m2).unwrap();

    // Should be able to resolve v1 by its HLC even after v2 is written.
    let resolved = tree.resolve_version("b", "k", e1.hlc).unwrap();
    assert_eq!(resolved, Some(oid1));
}

#[test]
fn test_resolve_version_nonexistent_hlc() {
    let tree = test_tree(1);
    let oid = ObjectId::from([1u8; 32]);
    let manifest = test_manifest(oid);

    tree.append_put("b", "k", oid, &manifest).unwrap();

    let resolved = tree.resolve_version("b", "k", 999999999).unwrap();
    assert_eq!(resolved, None);
}

#[test]
fn test_put_delete_put_resolves_to_latest_put() {
    let tree = test_tree(1);
    let oid1 = ObjectId::from([1u8; 32]);
    let oid2 = ObjectId::from([2u8; 32]);
    let m1 = test_manifest(oid1);
    let m2 = test_manifest(oid2);

    tree.append_put("b", "k", oid1, &m1).unwrap();
    tree.append_delete("b", "k").unwrap();
    tree.append_put("b", "k", oid2, &m2).unwrap();

    // Should resolve to v2 (the latest put, after the delete).
    assert_eq!(tree.resolve("b", "k").unwrap(), Some(oid2));

    let versions = tree.versions("b", "k").unwrap();
    assert_eq!(versions.len(), 3);
    assert!(!versions[0].deleted); // latest put
    assert!(versions[1].deleted); // delete marker
    assert!(!versions[2].deleted); // original put
}
