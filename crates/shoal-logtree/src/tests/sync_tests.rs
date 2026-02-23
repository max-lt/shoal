//! Tests for LogTree sync operations (compute_delta, apply_sync_entries).

use std::collections::BTreeMap;

use shoal_types::{MANIFEST_VERSION, Manifest, ObjectId};

use super::{test_identity, test_tree};
use crate::store::LogTreeStore;
use crate::tree::LogTree;

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
fn test_compute_delta_with_common_ancestor() {
    let tree_a = test_tree(1);
    let oid1 = ObjectId::from([1u8; 32]);
    let oid2 = ObjectId::from([2u8; 32]);
    let m1 = test_manifest(oid1);
    let m2 = test_manifest(oid2);

    // Both entries.
    let e1 = tree_a.append_put("b", "k1", oid1, &m1).unwrap();
    let _e2 = tree_a.append_put("b", "k2", oid2, &m2).unwrap();

    // Peer has e1 as their tip — they need only e2.
    let delta = tree_a.compute_delta(&[e1.hash]).unwrap();
    assert_eq!(delta.len(), 1, "only e2 should be in delta");
    assert_eq!(delta[0].hash, _e2.hash);
}

#[test]
fn test_compute_delta_with_empty_peer_tips() {
    let tree = test_tree(1);
    let oid = ObjectId::from([1u8; 32]);
    let manifest = test_manifest(oid);

    tree.append_put("b", "k1", oid, &manifest).unwrap();
    tree.append_put("b", "k2", oid, &manifest).unwrap();
    tree.append_put("b", "k3", oid, &manifest).unwrap();

    // Empty peer tips = peer has nothing.
    let delta = tree.compute_delta(&[]).unwrap();
    assert_eq!(delta.len(), 3, "all entries should be in delta");
}

#[test]
fn test_apply_sync_entries_stores_and_updates() {
    let tree_a = test_tree(1);
    let tree_b = test_tree(2);

    let oid1 = ObjectId::from([1u8; 32]);
    let oid2 = ObjectId::from([2u8; 32]);
    let m1 = test_manifest(oid1);
    let m2 = test_manifest(oid2);

    // tree_a writes two entries.
    tree_a.append_put("b", "k1", oid1, &m1).unwrap();
    tree_a.append_put("b", "k2", oid2, &m2).unwrap();

    // Get delta for tree_b (empty tips).
    let delta = tree_a.compute_delta(&[]).unwrap();
    let manifests = vec![(oid1, m1), (oid2, m2)];

    // Apply to tree_b.
    let applied = tree_b.apply_sync_entries(&delta, &manifests).unwrap();
    assert_eq!(applied, 2);

    // tree_b should resolve both keys.
    assert_eq!(tree_b.resolve("b", "k1").unwrap(), Some(oid1));
    assert_eq!(tree_b.resolve("b", "k2").unwrap(), Some(oid2));
}

#[test]
fn test_new_node_syncs_from_scratch() {
    let tree_a = test_tree(1);

    let oid = ObjectId::from([1u8; 32]);
    let manifest = test_manifest(oid);

    for i in 0..5 {
        tree_a
            .append_put("b", &format!("k{i}"), oid, &manifest)
            .unwrap();
    }

    // Brand new node.
    let tree_new = test_tree(3);

    let delta = tree_a.compute_delta(&[]).unwrap();
    let manifests = vec![(oid, manifest)];
    let applied = tree_new.apply_sync_entries(&delta, &manifests).unwrap();

    assert_eq!(applied, 5);

    let keys = tree_new.list_keys("b", "").unwrap();
    assert_eq!(keys.len(), 5);
}

#[test]
fn test_delayed_node_syncs_and_gets_correct_state() {
    let (node_a, key_a) = test_identity(1);
    let (node_b, key_b) = test_identity(2);

    let store_a = LogTreeStore::in_memory();
    let tree_a = LogTree::new(store_a, node_a, key_a);

    let store_b = LogTreeStore::in_memory();
    let tree_b = LogTree::new(store_b, node_b, key_b);

    let oid1 = ObjectId::from([1u8; 32]);
    let oid2 = ObjectId::from([2u8; 32]);
    let m1 = test_manifest(oid1);
    let m2 = test_manifest(oid2);

    // tree_a writes v1.
    let e1 = tree_a.append_put("b", "k", oid1, &m1).unwrap();

    // tree_b receives e1.
    tree_b
        .apply_sync_entries(&[e1.clone()], &[(oid1, m1.clone())])
        .unwrap();
    assert_eq!(tree_b.resolve("b", "k").unwrap(), Some(oid1));

    // tree_a overwrites with v2.
    tree_a.append_put("b", "k", oid2, &m2).unwrap();

    // tree_b syncs delta.
    let delta = tree_a.compute_delta(&tree_b.tips().unwrap()).unwrap();
    let applied = tree_b.apply_sync_entries(&delta, &[(oid2, m2)]).unwrap();
    assert!(applied >= 1);

    // tree_b should now resolve to v2.
    assert_eq!(tree_b.resolve("b", "k").unwrap(), Some(oid2));
}
