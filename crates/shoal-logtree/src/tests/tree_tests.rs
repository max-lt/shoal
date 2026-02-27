//! Tests for LogTree core operations.

use std::collections::BTreeMap;

use shoal_types::{MANIFEST_VERSION, Manifest, ObjectId};

use super::{test_identity, test_tree};
use crate::entry::Action;
use crate::store::LogTreeStore;
use crate::tree::LogTree;

/// Create a minimal test manifest.
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
fn test_append_put_creates_entry() {
    let tree = test_tree(1);
    let oid = ObjectId::from([1u8; 32]);
    let manifest = test_manifest(oid);

    let entry = tree.append_put("b", "k", oid, &manifest).unwrap();

    assert!(entry.verify_hash());
    assert!(entry.verify_signature());

    match &entry.action {
        Action::Put {
            bucket,
            key,
            manifest_id,
        } => {
            assert_eq!(bucket, "b");
            assert_eq!(key, "k");
            assert_eq!(*manifest_id, oid);
        }
        _ => panic!("expected Put action"),
    }
}

#[test]
fn test_append_put_updates_tips() {
    let tree = test_tree(1);
    let oid = ObjectId::from([1u8; 32]);
    let manifest = test_manifest(oid);

    let entry = tree.append_put("b", "k", oid, &manifest).unwrap();
    let tips = tree.tips().unwrap();

    assert_eq!(tips.len(), 1);
    assert_eq!(tips[0], entry.hash);
}

#[test]
fn test_append_delete_adds_delete_marker() {
    let tree = test_tree(1);
    let oid = ObjectId::from([1u8; 32]);
    let manifest = test_manifest(oid);

    tree.append_put("b", "k", oid, &manifest).unwrap();
    let del_entry = tree.append_delete("b", "k").unwrap();

    match &del_entry.action {
        Action::Delete { bucket, key } => {
            assert_eq!(bucket, "b");
            assert_eq!(key, "k");
        }
        _ => panic!("expected Delete action"),
    }

    // Key should no longer resolve.
    assert_eq!(tree.resolve("b", "k").unwrap(), None);
}

#[test]
fn test_first_entry_has_empty_parents() {
    let tree = test_tree(1);
    let oid = ObjectId::from([1u8; 32]);
    let manifest = test_manifest(oid);

    let entry = tree.append_put("b", "k", oid, &manifest).unwrap();
    assert!(
        entry.parents.is_empty(),
        "first entry should have no parents"
    );
}

#[test]
fn test_second_entry_has_first_as_parent() {
    let tree = test_tree(1);
    let oid = ObjectId::from([1u8; 32]);
    let manifest = test_manifest(oid);

    let e1 = tree.append_put("b", "k1", oid, &manifest).unwrap();
    let e2 = tree.append_put("b", "k2", oid, &manifest).unwrap();

    assert_eq!(e2.parents.len(), 1);
    assert_eq!(e2.parents[0], e1.hash);
}

#[test]
fn test_hash_chain_5_entries() {
    let tree = test_tree(1);
    let oid = ObjectId::from([1u8; 32]);
    let manifest = test_manifest(oid);

    let mut prev_hash = None;

    for i in 0..5 {
        let entry = tree
            .append_put("b", &format!("k{i}"), oid, &manifest)
            .unwrap();

        if let Some(ph) = prev_hash {
            assert_eq!(entry.parents.len(), 1);
            assert_eq!(entry.parents[0], ph, "entry {i} parent should be prev hash");
        } else {
            assert!(entry.parents.is_empty());
        }

        prev_hash = Some(entry.hash);
    }

    let tips = tree.tips().unwrap();
    assert_eq!(tips.len(), 1);
}

#[test]
fn test_resolve_returns_latest_manifest_id() {
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
fn test_resolve_returns_none_after_delete() {
    let tree = test_tree(1);
    let oid = ObjectId::from([1u8; 32]);
    let manifest = test_manifest(oid);

    tree.append_put("b", "k", oid, &manifest).unwrap();
    tree.append_delete("b", "k").unwrap();

    assert_eq!(tree.resolve("b", "k").unwrap(), None);
}

#[test]
fn test_versions_returns_all_sorted() {
    let tree = test_tree(1);
    let oid1 = ObjectId::from([1u8; 32]);
    let oid2 = ObjectId::from([2u8; 32]);
    let m1 = test_manifest(oid1);
    let m2 = test_manifest(oid2);

    tree.append_put("b", "k", oid1, &m1).unwrap();
    tree.append_put("b", "k", oid2, &m2).unwrap();

    let versions = tree.versions("b", "k").unwrap();
    assert_eq!(versions.len(), 2);
    // Sorted by HLC desc — latest first.
    assert!(versions[0].hlc > versions[1].hlc);
    assert_eq!(versions[0].manifest_id, oid2);
    assert_eq!(versions[1].manifest_id, oid1);
}

#[test]
fn test_manifest_cache() {
    let tree = test_tree(1);
    let oid = ObjectId::from([42u8; 32]);
    let manifest = test_manifest(oid);

    tree.append_put("b", "k", oid, &manifest).unwrap();

    let cached = tree.get_manifest(&oid).unwrap();
    assert_eq!(cached, Some(manifest));
}

#[test]
fn test_list_keys_filters_deleted() {
    let tree = test_tree(1);
    let oid = ObjectId::from([1u8; 32]);
    let manifest = test_manifest(oid);

    tree.append_put("b", "alive", oid, &manifest).unwrap();
    tree.append_put("b", "dead", oid, &manifest).unwrap();
    tree.append_delete("b", "dead").unwrap();

    let keys: Vec<String> = tree
        .list_keys("b", "")
        .unwrap()
        .into_iter()
        .map(|o| o.key)
        .collect();
    assert_eq!(keys, vec!["alive"]);
}

// =========================================================================
// Multi-node tests
// =========================================================================

#[test]
fn test_two_nodes_fork_two_tips() {
    let tree_a = test_tree(1);
    let tree_b = test_tree(2);

    let oid_a = ObjectId::from([1u8; 32]);
    let oid_b = ObjectId::from([2u8; 32]);
    let m_a = test_manifest(oid_a);
    let m_b = test_manifest(oid_b);

    // Both nodes write independently.
    let _entry_a = tree_a.append_put("b", "ka", oid_a, &m_a).unwrap();
    let entry_b = tree_b.append_put("b", "kb", oid_b, &m_b).unwrap();

    // Merge both into tree_a: receive entry_b.
    // entry_b has empty parents (it's the first entry on tree_b).
    // entry_a also has empty parents. These are independent branches.
    tree_a.receive_entry(&entry_b, Some(&m_b)).unwrap();

    // After receiving, auto-merge should have been triggered.
    // Since entry_a and entry_b are both tips (no parent relationship),
    // there should now be a merge entry with 1 tip.
    let tips = tree_a.tips().unwrap();
    assert_eq!(tips.len(), 1, "auto-merge should converge to 1 tip");
}

#[test]
fn test_lww_concurrent_put_higher_hlc_wins() {
    // Simulate two concurrent puts from different nodes.
    let (node_a, key_a) = test_identity(1);
    let (node_b, key_b) = test_identity(2);

    let oid_a = ObjectId::from([1u8; 32]);
    let oid_b = ObjectId::from([2u8; 32]);
    let m_a = test_manifest(oid_a);
    let m_b = test_manifest(oid_b);

    let store_a = LogTreeStore::in_memory();
    let tree_a = LogTree::new(store_a, node_a, key_a);

    let store_b = LogTreeStore::in_memory();
    let tree_b = LogTree::new(store_b, node_b, key_b);

    // tree_a writes first (lower HLC).
    let entry_a = tree_a.append_put("b", "k", oid_a, &m_a).unwrap();

    // tree_b writes second (higher HLC due to wall clock advancing).
    let entry_b = tree_b.append_put("b", "k", oid_b, &m_b).unwrap();

    // Apply both to a third tree.
    let tree_c = test_tree(3);
    tree_c.receive_entry(&entry_a, Some(&m_a)).unwrap();
    tree_c.receive_entry(&entry_b, Some(&m_b)).unwrap();

    // Higher HLC should win.
    let resolved = tree_c.resolve("b", "k").unwrap().unwrap();

    if entry_b.hlc > entry_a.hlc {
        assert_eq!(resolved, oid_b);
    } else if entry_a.hlc > entry_b.hlc {
        assert_eq!(resolved, oid_a);
    } else {
        // Same HLC — higher NodeId wins.
        if node_b > node_a {
            assert_eq!(resolved, oid_b);
        } else {
            assert_eq!(resolved, oid_a);
        }
    }
}

#[test]
fn test_after_merge_both_nodes_identical_state() {
    let (node_a, key_a) = test_identity(1);
    let (node_b, key_b) = test_identity(2);

    let store_a = LogTreeStore::in_memory();
    let tree_a = LogTree::new(store_a, node_a, key_a);

    let store_b = LogTreeStore::in_memory();
    let tree_b = LogTree::new(store_b, node_b, key_b);

    let oid_a = ObjectId::from([1u8; 32]);
    let oid_b = ObjectId::from([2u8; 32]);
    let m_a = test_manifest(oid_a);
    let m_b = test_manifest(oid_b);

    // Each writes independently.
    let entry_a = tree_a.append_put("b", "file.txt", oid_a, &m_a).unwrap();
    let entry_b = tree_b.append_put("b", "file.txt", oid_b, &m_b).unwrap();

    // Cross-receive.
    tree_a.receive_entry(&entry_b, Some(&m_b)).unwrap();
    tree_b.receive_entry(&entry_a, Some(&m_a)).unwrap();

    // Both should resolve to the same value (LWW).
    let resolved_a = tree_a.resolve("b", "file.txt").unwrap();
    let resolved_b = tree_b.resolve("b", "file.txt").unwrap();
    assert_eq!(resolved_a, resolved_b);
}
