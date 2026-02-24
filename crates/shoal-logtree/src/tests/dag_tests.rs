//! Comprehensive DAG tests for compute_delta and apply_sync_entries.
//!
//! These tests exercise diamond merges, deep chains, wide fan-in,
//! partial overlap, out-of-order application, and other non-trivial
//! DAG topologies that the simpler sync_tests.rs doesn't cover.

use std::collections::BTreeMap;

use ed25519_dalek::SigningKey;
use shoal_types::{MANIFEST_VERSION, Manifest, NodeId, ObjectId};

use super::{test_identity, test_tree};
use crate::entry::{Action, LogEntry};
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

/// Helper: create a signed Put entry with explicit parents.
fn make_entry(
    hlc: u64,
    node_id: NodeId,
    key: &SigningKey,
    bucket: &str,
    obj_key: &str,
    oid: ObjectId,
    parents: Vec<[u8; 32]>,
) -> LogEntry {
    let action = Action::Put {
        bucket: bucket.to_string(),
        key: obj_key.to_string(),
        manifest_id: oid,
    };
    LogEntry::new_signed(hlc, node_id, action, parents, key)
}

/// Helper: create a signed Merge entry with explicit parents.
fn make_merge(hlc: u64, node_id: NodeId, key: &SigningKey, parents: Vec<[u8; 32]>) -> LogEntry {
    LogEntry::new_signed(hlc, node_id, Action::Merge, parents, key)
}

// =========================================================================
// Diamond DAG tests
// =========================================================================

/// Diamond: A → B, A → C, B+C → D (merge).
///
/// ```text
///     A
///    / \
///   B   C
///    \ /
///     D (merge)
/// ```
///
/// compute_delta must return [A, B, C, D] in topological order
/// (A before B/C, B/C before D).
#[test]
fn test_compute_delta_diamond_dag() {
    let (node_id, signing_key) = test_identity(1);
    let store = LogTreeStore::in_memory();
    let tree = LogTree::new(store, node_id, signing_key.clone());

    let oid = ObjectId::from([1u8; 32]);
    let manifest = test_manifest(oid);

    // A: root entry.
    let a = make_entry(1, node_id, &signing_key, "b", "k1", oid, vec![]);
    tree.store().put_entry(&a).unwrap();
    tree.store().put_manifest(&manifest).unwrap();

    // B: child of A.
    let b = make_entry(2, node_id, &signing_key, "b", "k2", oid, vec![a.hash]);
    tree.store().put_entry(&b).unwrap();

    // C: child of A (divergent branch).
    let c = make_entry(3, node_id, &signing_key, "b", "k3", oid, vec![a.hash]);
    tree.store().put_entry(&c).unwrap();

    // D: merge of B and C.
    let d = make_merge(4, node_id, &signing_key, vec![b.hash, c.hash]);
    tree.store().put_entry(&d).unwrap();

    // Set tips to just D.
    tree.store().add_tip(d.hash).unwrap();

    // Peer has nothing → full delta.
    let delta = tree.compute_delta(&[]).unwrap();
    assert_eq!(delta.len(), 4, "diamond should yield 4 entries");

    // Check topological order: A must come before B and C; B and C before D.
    let pos = |hash: [u8; 32]| delta.iter().position(|e| e.hash == hash).unwrap();
    assert!(pos(a.hash) < pos(b.hash), "A must precede B");
    assert!(pos(a.hash) < pos(c.hash), "A must precede C");
    assert!(pos(b.hash) < pos(d.hash), "B must precede D");
    assert!(pos(c.hash) < pos(d.hash), "C must precede D");
}

/// Diamond DAG: apply_sync_entries correctly stores all entries.
#[test]
fn test_apply_sync_entries_diamond_dag() {
    let (node_id, signing_key) = test_identity(1);
    let store = LogTreeStore::in_memory();
    let source = LogTree::new(store, node_id, signing_key.clone());

    let oid1 = ObjectId::from([1u8; 32]);
    let oid2 = ObjectId::from([2u8; 32]);
    let oid3 = ObjectId::from([3u8; 32]);
    let m1 = test_manifest(oid1);
    let m2 = test_manifest(oid2);
    let m3 = test_manifest(oid3);

    // Build diamond: A → (B, C) → D(merge).
    let a = make_entry(1, node_id, &signing_key, "b", "k1", oid1, vec![]);
    source.store().put_entry(&a).unwrap();
    source.store().put_manifest(&m1).unwrap();

    let b = make_entry(2, node_id, &signing_key, "b", "k2", oid2, vec![a.hash]);
    source.store().put_entry(&b).unwrap();
    source.store().put_manifest(&m2).unwrap();

    let c = make_entry(3, node_id, &signing_key, "b", "k3", oid3, vec![a.hash]);
    source.store().put_entry(&c).unwrap();
    source.store().put_manifest(&m3).unwrap();

    let d = make_merge(4, node_id, &signing_key, vec![b.hash, c.hash]);
    source.store().put_entry(&d).unwrap();
    source.store().add_tip(d.hash).unwrap();

    // Compute delta for empty peer.
    let delta = source.compute_delta(&[]).unwrap();
    assert_eq!(delta.len(), 4);

    // Apply to a fresh tree.
    let target = test_tree(2);
    let manifests = vec![(oid1, m1), (oid2, m2), (oid3, m3)];
    let applied = target.apply_sync_entries(&delta, &manifests).unwrap();
    assert_eq!(applied, 4, "all 4 diamond entries should apply");

    // Target should have all keys.
    assert_eq!(target.resolve("b", "k1").unwrap(), Some(oid1));
    assert_eq!(target.resolve("b", "k2").unwrap(), Some(oid2));
    assert_eq!(target.resolve("b", "k3").unwrap(), Some(oid3));
}

/// Peer already has A in a diamond → delta should only be [B, C, D].
#[test]
fn test_compute_delta_diamond_peer_has_root() {
    let (node_id, signing_key) = test_identity(1);
    let store = LogTreeStore::in_memory();
    let tree = LogTree::new(store, node_id, signing_key.clone());

    let oid = ObjectId::from([1u8; 32]);

    let a = make_entry(1, node_id, &signing_key, "b", "k1", oid, vec![]);
    tree.store().put_entry(&a).unwrap();

    let b = make_entry(2, node_id, &signing_key, "b", "k2", oid, vec![a.hash]);
    tree.store().put_entry(&b).unwrap();

    let c = make_entry(3, node_id, &signing_key, "b", "k3", oid, vec![a.hash]);
    tree.store().put_entry(&c).unwrap();

    let d = make_merge(4, node_id, &signing_key, vec![b.hash, c.hash]);
    tree.store().put_entry(&d).unwrap();
    tree.store().add_tip(d.hash).unwrap();

    // Peer already has A.
    let delta = tree.compute_delta(&[a.hash]).unwrap();
    assert_eq!(delta.len(), 3, "A is excluded since peer has it");

    // Topological order: B and C before D.
    let pos = |hash: [u8; 32]| delta.iter().position(|e| e.hash == hash).unwrap();
    assert!(pos(b.hash) < pos(d.hash));
    assert!(pos(c.hash) < pos(d.hash));
}

// =========================================================================
// Deep chain tests
// =========================================================================

/// 100-entry linear chain: compute_delta returns all in order.
#[test]
fn test_compute_delta_deep_chain_100() {
    let (node_id, signing_key) = test_identity(1);
    let store = LogTreeStore::in_memory();
    let tree = LogTree::new(store, node_id, signing_key.clone());

    let oid = ObjectId::from([1u8; 32]);
    let manifest = test_manifest(oid);
    tree.store().put_manifest(&manifest).unwrap();

    let mut prev_hash: Option<[u8; 32]> = None;
    let mut hashes = Vec::new();

    for i in 0..100u64 {
        let parents = prev_hash.map_or(vec![], |h| vec![h]);
        let entry = make_entry(
            i + 1,
            node_id,
            &signing_key,
            "b",
            &format!("k{i}"),
            oid,
            parents,
        );
        tree.store().put_entry(&entry).unwrap();
        hashes.push(entry.hash);
        prev_hash = Some(entry.hash);
    }

    tree.store().add_tip(*hashes.last().unwrap()).unwrap();

    let delta = tree.compute_delta(&[]).unwrap();
    assert_eq!(delta.len(), 100);

    // Verify strict topological order: each entry comes after its parent.
    for i in 1..100 {
        let pos_parent = delta.iter().position(|e| e.hash == hashes[i - 1]).unwrap();
        let pos_child = delta.iter().position(|e| e.hash == hashes[i]).unwrap();
        assert!(
            pos_parent < pos_child,
            "entry {i} must come after entry {}",
            i - 1
        );
    }
}

/// Deep chain: apply_sync_entries handles 100 entries correctly.
#[test]
fn test_apply_sync_entries_deep_chain() {
    let (node_id, signing_key) = test_identity(1);
    let store = LogTreeStore::in_memory();
    let source = LogTree::new(store, node_id, signing_key.clone());

    let oid = ObjectId::from([1u8; 32]);
    let manifest = test_manifest(oid);
    source.store().put_manifest(&manifest).unwrap();

    let mut prev_hash: Option<[u8; 32]> = None;
    for i in 0..100u64 {
        let parents = prev_hash.map_or(vec![], |h| vec![h]);
        let entry = make_entry(
            i + 1,
            node_id,
            &signing_key,
            "b",
            &format!("k{i}"),
            oid,
            parents,
        );
        source.store().put_entry(&entry).unwrap();
        prev_hash = Some(entry.hash);
    }

    source.store().add_tip(prev_hash.unwrap()).unwrap();

    let delta = source.compute_delta(&[]).unwrap();
    let target = test_tree(2);
    let applied = target
        .apply_sync_entries(&delta, &[(oid, manifest)])
        .unwrap();
    assert_eq!(applied, 100);

    let keys = target.list_keys("b", "").unwrap();
    assert_eq!(keys.len(), 100);
}

/// Peer has the first 50 entries → delta is entries 50..100.
#[test]
fn test_compute_delta_partial_chain() {
    let (node_id, signing_key) = test_identity(1);
    let store = LogTreeStore::in_memory();
    let tree = LogTree::new(store, node_id, signing_key.clone());

    let oid = ObjectId::from([1u8; 32]);
    let manifest = test_manifest(oid);
    tree.store().put_manifest(&manifest).unwrap();

    let mut hashes = Vec::new();
    let mut prev_hash: Option<[u8; 32]> = None;

    for i in 0..100u64 {
        let parents = prev_hash.map_or(vec![], |h| vec![h]);
        let entry = make_entry(
            i + 1,
            node_id,
            &signing_key,
            "b",
            &format!("k{i}"),
            oid,
            parents,
        );
        tree.store().put_entry(&entry).unwrap();
        hashes.push(entry.hash);
        prev_hash = Some(entry.hash);
    }

    tree.store().add_tip(*hashes.last().unwrap()).unwrap();

    // Peer has entry #49 (0-indexed) as their tip → needs entries 50..99.
    let delta = tree.compute_delta(&[hashes[49]]).unwrap();
    assert_eq!(delta.len(), 50, "entries 50..99 should be in delta");

    // First entry in delta should be entry #50.
    assert_eq!(delta[0].hash, hashes[50]);
    // Last entry should be entry #99.
    assert_eq!(delta[49].hash, hashes[99]);
}

// =========================================================================
// Wide merge (many parents)
// =========================================================================

/// Entry with 10 parents from independent branches.
///
/// ```text
///   r0 r1 r2 r3 r4 r5 r6 r7 r8 r9
///    \  |  |  |  |  |  |  |  |  /
///              M (merge)
/// ```
#[test]
fn test_compute_delta_wide_merge() {
    let (node_id, signing_key) = test_identity(1);
    let store = LogTreeStore::in_memory();
    let tree = LogTree::new(store, node_id, signing_key.clone());

    let oid = ObjectId::from([1u8; 32]);

    // 10 independent root entries.
    let mut roots = Vec::new();
    for i in 0..10u64 {
        let entry = make_entry(
            i + 1,
            node_id,
            &signing_key,
            "b",
            &format!("k{i}"),
            oid,
            vec![],
        );
        tree.store().put_entry(&entry).unwrap();
        roots.push(entry);
    }

    // Merge all 10.
    let parent_hashes: Vec<[u8; 32]> = roots.iter().map(|e| e.hash).collect();
    let merge = make_merge(11, node_id, &signing_key, parent_hashes);
    tree.store().put_entry(&merge).unwrap();
    tree.store().add_tip(merge.hash).unwrap();

    let delta = tree.compute_delta(&[]).unwrap();
    assert_eq!(delta.len(), 11, "10 roots + 1 merge");

    // All roots must precede the merge.
    let merge_pos = delta.iter().position(|e| e.hash == merge.hash).unwrap();
    for root in &roots {
        let root_pos = delta.iter().position(|e| e.hash == root.hash).unwrap();
        assert!(root_pos < merge_pos, "root must precede merge");
    }
}

/// Apply a wide merge (10 parents) through sync.
#[test]
fn test_apply_sync_entries_wide_merge() {
    let (node_id, signing_key) = test_identity(1);
    let store = LogTreeStore::in_memory();
    let source = LogTree::new(store, node_id, signing_key.clone());

    let oid = ObjectId::from([1u8; 32]);
    let manifest = test_manifest(oid);
    source.store().put_manifest(&manifest).unwrap();

    let mut roots = Vec::new();
    for i in 0..10u64 {
        let entry = make_entry(
            i + 1,
            node_id,
            &signing_key,
            "b",
            &format!("k{i}"),
            oid,
            vec![],
        );
        source.store().put_entry(&entry).unwrap();
        roots.push(entry);
    }

    let parent_hashes: Vec<[u8; 32]> = roots.iter().map(|e| e.hash).collect();
    let merge = make_merge(11, node_id, &signing_key, parent_hashes);
    source.store().put_entry(&merge).unwrap();
    source.store().add_tip(merge.hash).unwrap();

    let delta = source.compute_delta(&[]).unwrap();
    let target = test_tree(2);
    let applied = target
        .apply_sync_entries(&delta, &[(oid, manifest)])
        .unwrap();
    assert_eq!(applied, 11);
}

// =========================================================================
// Multiple independent roots
// =========================================================================

/// Two independent entries with no parents (multiple roots in delta).
#[test]
fn test_compute_delta_multiple_roots() {
    let (node_id, signing_key) = test_identity(1);
    let store = LogTreeStore::in_memory();
    let tree = LogTree::new(store, node_id, signing_key.clone());

    let oid1 = ObjectId::from([1u8; 32]);
    let oid2 = ObjectId::from([2u8; 32]);

    // Two independent entries — no parent relationship.
    let a = make_entry(1, node_id, &signing_key, "b", "k1", oid1, vec![]);
    tree.store().put_entry(&a).unwrap();
    tree.store().add_tip(a.hash).unwrap();

    let b = make_entry(2, node_id, &signing_key, "b", "k2", oid2, vec![]);
    tree.store().put_entry(&b).unwrap();
    tree.store().add_tip(b.hash).unwrap();

    let delta = tree.compute_delta(&[]).unwrap();
    assert_eq!(
        delta.len(),
        2,
        "both independent entries should be in delta"
    );

    // Both should be present (order between them is arbitrary).
    let hashes: Vec<[u8; 32]> = delta.iter().map(|e| e.hash).collect();
    assert!(hashes.contains(&a.hash));
    assert!(hashes.contains(&b.hash));
}

// =========================================================================
// Out-of-order application
// =========================================================================

/// apply_sync_entries rejects entries whose in-delta parents haven't
/// been applied yet (entries are not in topological order).
#[test]
fn test_apply_sync_entries_out_of_order_skips_children() {
    let (node_id, signing_key) = test_identity(1);

    let oid1 = ObjectId::from([1u8; 32]);
    let oid2 = ObjectId::from([2u8; 32]);
    let m1 = test_manifest(oid1);
    let m2 = test_manifest(oid2);

    // A → B (linear chain).
    let a = make_entry(1, node_id, &signing_key, "b", "k1", oid1, vec![]);
    let b = make_entry(2, node_id, &signing_key, "b", "k2", oid2, vec![a.hash]);

    // Apply in WRONG order: B first, then A.
    let target = test_tree(2);
    let applied = target
        .apply_sync_entries(&[b.clone(), a.clone()], &[(oid1, m1), (oid2, m2)])
        .unwrap();

    // B should be skipped because its in-delta parent A hasn't been applied yet.
    // A should be applied.
    assert_eq!(applied, 1, "only A should apply when order is wrong");
    assert_eq!(target.resolve("b", "k1").unwrap(), Some(oid1));
    assert_eq!(target.resolve("b", "k2").unwrap(), None);
}

// =========================================================================
// Duplicate entries in batch
// =========================================================================

/// Same entry appearing twice in sync batch → only counted once.
#[test]
fn test_apply_sync_entries_duplicate_in_batch() {
    let (node_id, signing_key) = test_identity(1);

    let oid = ObjectId::from([1u8; 32]);
    let manifest = test_manifest(oid);

    let a = make_entry(1, node_id, &signing_key, "b", "k1", oid, vec![]);

    let target = test_tree(2);
    let applied = target
        .apply_sync_entries(&[a.clone(), a.clone()], &[(oid, manifest)])
        .unwrap();

    // First instance applies, second is skipped (already in store).
    assert_eq!(applied, 1);
}

// =========================================================================
// Peer tips at non-leaf (ancestor) positions
// =========================================================================

/// Peer has an ancestor tip (not a leaf) → delta includes all descendants.
#[test]
fn test_compute_delta_peer_tip_at_ancestor() {
    let (node_id, signing_key) = test_identity(1);
    let store = LogTreeStore::in_memory();
    let tree = LogTree::new(store, node_id, signing_key.clone());

    let oid = ObjectId::from([1u8; 32]);

    // Chain: A → B → C.
    let a = make_entry(1, node_id, &signing_key, "b", "k1", oid, vec![]);
    tree.store().put_entry(&a).unwrap();

    let b = make_entry(2, node_id, &signing_key, "b", "k2", oid, vec![a.hash]);
    tree.store().put_entry(&b).unwrap();

    let c = make_entry(3, node_id, &signing_key, "b", "k3", oid, vec![b.hash]);
    tree.store().put_entry(&c).unwrap();
    tree.store().add_tip(c.hash).unwrap();

    // Peer has A → delta should be [B, C].
    let delta = tree.compute_delta(&[a.hash]).unwrap();
    assert_eq!(delta.len(), 2);
    assert_eq!(delta[0].hash, b.hash, "B should come first");
    assert_eq!(delta[1].hash, c.hash, "C should come second");
}

// =========================================================================
// Nested diamond (double diamond)
// =========================================================================

/// Double diamond:
/// ```text
///       A
///      / \
///     B   C
///      \ /
///       D
///      / \
///     E   F
///      \ /
///       G (merge)
/// ```
#[test]
fn test_compute_delta_double_diamond() {
    let (node_id, signing_key) = test_identity(1);
    let store = LogTreeStore::in_memory();
    let tree = LogTree::new(store, node_id, signing_key.clone());

    let oid = ObjectId::from([1u8; 32]);

    let a = make_entry(1, node_id, &signing_key, "b", "k1", oid, vec![]);
    tree.store().put_entry(&a).unwrap();

    let b = make_entry(2, node_id, &signing_key, "b", "k2", oid, vec![a.hash]);
    tree.store().put_entry(&b).unwrap();

    let c = make_entry(3, node_id, &signing_key, "b", "k3", oid, vec![a.hash]);
    tree.store().put_entry(&c).unwrap();

    let d = make_merge(4, node_id, &signing_key, vec![b.hash, c.hash]);
    tree.store().put_entry(&d).unwrap();

    let e = make_entry(5, node_id, &signing_key, "b", "k4", oid, vec![d.hash]);
    tree.store().put_entry(&e).unwrap();

    let f = make_entry(6, node_id, &signing_key, "b", "k5", oid, vec![d.hash]);
    tree.store().put_entry(&f).unwrap();

    let g = make_merge(7, node_id, &signing_key, vec![e.hash, f.hash]);
    tree.store().put_entry(&g).unwrap();
    tree.store().add_tip(g.hash).unwrap();

    let delta = tree.compute_delta(&[]).unwrap();
    assert_eq!(delta.len(), 7, "all 7 entries in double diamond");

    let pos = |hash: [u8; 32]| delta.iter().position(|e| e.hash == hash).unwrap();

    // First diamond.
    assert!(pos(a.hash) < pos(b.hash));
    assert!(pos(a.hash) < pos(c.hash));
    assert!(pos(b.hash) < pos(d.hash));
    assert!(pos(c.hash) < pos(d.hash));

    // Second diamond.
    assert!(pos(d.hash) < pos(e.hash));
    assert!(pos(d.hash) < pos(f.hash));
    assert!(pos(e.hash) < pos(g.hash));
    assert!(pos(f.hash) < pos(g.hash));
}

/// Double diamond: sync to a fresh target.
#[test]
fn test_apply_sync_entries_double_diamond() {
    let (node_id, signing_key) = test_identity(1);
    let store = LogTreeStore::in_memory();
    let source = LogTree::new(store, node_id, signing_key.clone());

    let oid = ObjectId::from([1u8; 32]);
    let manifest = test_manifest(oid);
    source.store().put_manifest(&manifest).unwrap();

    let a = make_entry(1, node_id, &signing_key, "b", "k1", oid, vec![]);
    source.store().put_entry(&a).unwrap();
    let b = make_entry(2, node_id, &signing_key, "b", "k2", oid, vec![a.hash]);
    source.store().put_entry(&b).unwrap();
    let c = make_entry(3, node_id, &signing_key, "b", "k3", oid, vec![a.hash]);
    source.store().put_entry(&c).unwrap();
    let d = make_merge(4, node_id, &signing_key, vec![b.hash, c.hash]);
    source.store().put_entry(&d).unwrap();
    let e = make_entry(5, node_id, &signing_key, "b", "k4", oid, vec![d.hash]);
    source.store().put_entry(&e).unwrap();
    let f = make_entry(6, node_id, &signing_key, "b", "k5", oid, vec![d.hash]);
    source.store().put_entry(&f).unwrap();
    let g = make_merge(7, node_id, &signing_key, vec![e.hash, f.hash]);
    source.store().put_entry(&g).unwrap();
    source.store().add_tip(g.hash).unwrap();

    let delta = source.compute_delta(&[]).unwrap();
    let target = test_tree(2);
    let applied = target
        .apply_sync_entries(&delta, &[(oid, manifest)])
        .unwrap();
    assert_eq!(applied, 7);
}

// =========================================================================
// Cross-node fork and merge sync
// =========================================================================

/// Two nodes fork independently, then sync.
///
/// Node A: root_a → a1 → a2.
/// Node B: root_b → b1 → b2.
/// Both share the same initial entry (root) — simulate by syncing root first.
///
/// After sync, the receiving tree should have all entries and auto-merge.
#[test]
fn test_cross_node_fork_then_sync() {
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

    // Both start with a shared root (sync root from A to B).
    let root = tree_a.append_put("b", "shared", oid_a, &m_a).unwrap();
    tree_b
        .apply_sync_entries(&[root.clone()], &[(oid_a, m_a.clone())])
        .unwrap();

    // A writes two more entries independently.
    let _a1 = tree_a.append_put("b", "a1", oid_a, &m_a).unwrap();
    let _a2 = tree_a.append_put("b", "a2", oid_a, &m_a).unwrap();

    // B writes two more entries independently.
    let _b1 = tree_b.append_put("b", "b1", oid_b, &m_b).unwrap();
    let _b2 = tree_b.append_put("b", "b2", oid_b, &m_b).unwrap();

    // Sync A → B: B should get A's entries.
    // B's tip is b2, which A doesn't know. A's BFS walks back from a2
    // and never hits b2, so the delta includes root, a1, a2 = 3 entries.
    // (root is included because B's tip b2 is unknown to compute_delta's
    // stop condition — it only stops at entries in peer_tips.)
    let tips_b = tree_b.tips().unwrap();
    let delta_from_a = tree_a.compute_delta(&tips_b).unwrap();
    assert_eq!(
        delta_from_a.len(),
        3,
        "A sends root+a1+a2 (B's tip b2 is unknown to A)"
    );

    let applied = tree_b
        .apply_sync_entries(&delta_from_a, &[(oid_a, m_a.clone())])
        .unwrap();
    // root already exists in B → skipped. a1+a2 applied.
    assert_eq!(applied, 2);

    // B should now know all 5 keys.
    let keys = tree_b.list_keys("b", "").unwrap();
    assert_eq!(keys.len(), 5, "shared + a1 + a2 + b1 + b2");

    // After auto-merge, B should have 1 tip.
    let tips = tree_b.tips().unwrap();
    assert_eq!(tips.len(), 1, "auto-merge should converge");
}

/// Bidirectional sync: after syncing both ways, both trees converge.
#[test]
fn test_bidirectional_sync_convergence() {
    let (node_a, key_a) = test_identity(1);
    let (node_b, key_b) = test_identity(2);

    let store_a = LogTreeStore::in_memory();
    let tree_a = LogTree::new(store_a, node_a, key_a);

    let store_b = LogTreeStore::in_memory();
    let tree_b = LogTree::new(store_b, node_b, key_b);

    let oid = ObjectId::from([1u8; 32]);
    let manifest = test_manifest(oid);

    // A writes 3 entries.
    for i in 0..3 {
        tree_a
            .append_put("b", &format!("a{i}"), oid, &manifest)
            .unwrap();
    }

    // B writes 3 entries.
    for i in 0..3 {
        tree_b
            .append_put("b", &format!("b{i}"), oid, &manifest)
            .unwrap();
    }

    // Sync A → B.
    let tips_b = tree_b.tips().unwrap();
    let delta_a_to_b = tree_a.compute_delta(&tips_b).unwrap();
    tree_b
        .apply_sync_entries(&delta_a_to_b, &[(oid, manifest.clone())])
        .unwrap();

    // Sync B → A.
    let tips_a = tree_a.tips().unwrap();
    let delta_b_to_a = tree_b.compute_delta(&tips_a).unwrap();
    tree_a
        .apply_sync_entries(&delta_b_to_a, &[(oid, manifest)])
        .unwrap();

    // Both should now have 6 keys.
    let keys_a = tree_a.list_keys("b", "").unwrap();
    let keys_b = tree_b.list_keys("b", "").unwrap();
    assert_eq!(keys_a.len(), 6);
    assert_eq!(keys_b.len(), 6);

    // Both should resolve the same keys.
    let mut sorted_a = keys_a.clone();
    let mut sorted_b = keys_b.clone();
    sorted_a.sort();
    sorted_b.sort();
    assert_eq!(sorted_a, sorted_b);
}

// =========================================================================
// Edge case: missing manifest for Put entry
// =========================================================================

/// apply_sync_entries with a Put entry whose manifest is not provided.
/// Entry should still apply, but the manifest won't be cached.
#[test]
fn test_apply_sync_entries_missing_manifest() {
    let (node_id, signing_key) = test_identity(1);

    let oid = ObjectId::from([1u8; 32]);

    let a = make_entry(1, node_id, &signing_key, "b", "k1", oid, vec![]);

    let target = test_tree(2);
    // Empty manifest list — no manifest provided for oid.
    let applied = target.apply_sync_entries(&[a], &[]).unwrap();
    assert_eq!(applied, 1, "entry should apply even without manifest");

    // Key should resolve (versions are tracked), but manifest isn't cached.
    assert_eq!(target.resolve("b", "k1").unwrap(), Some(oid));
    // Manifest lookup returns None.
    assert!(target.get_manifest(&oid).unwrap().is_none());
}

// =========================================================================
// Idempotency
// =========================================================================

/// Applying the same delta twice doesn't double-count or break state.
#[test]
fn test_apply_sync_entries_idempotent() {
    let tree_a = test_tree(1);
    let oid = ObjectId::from([1u8; 32]);
    let manifest = test_manifest(oid);

    tree_a.append_put("b", "k1", oid, &manifest).unwrap();
    tree_a.append_put("b", "k2", oid, &manifest).unwrap();

    let delta = tree_a.compute_delta(&[]).unwrap();
    let manifests = vec![(oid, manifest)];

    let target = test_tree(2);

    let first = target.apply_sync_entries(&delta, &manifests).unwrap();
    assert_eq!(first, 2);

    let second = target.apply_sync_entries(&delta, &manifests).unwrap();
    assert_eq!(second, 0, "no new entries on second apply");

    let keys = target.list_keys("b", "").unwrap();
    assert_eq!(keys.len(), 2, "still exactly 2 keys");
}

// =========================================================================
// Empty delta
// =========================================================================

/// compute_delta when peer has same tips → empty delta.
#[test]
fn test_compute_delta_same_tips_empty() {
    let tree = test_tree(1);
    let oid = ObjectId::from([1u8; 32]);
    let manifest = test_manifest(oid);

    tree.append_put("b", "k1", oid, &manifest).unwrap();

    let tips = tree.tips().unwrap();
    let delta = tree.compute_delta(&tips).unwrap();
    assert!(delta.is_empty(), "same tips should yield empty delta");
}

/// apply_sync_entries with empty slice → 0 applied.
#[test]
fn test_apply_sync_entries_empty() {
    let target = test_tree(1);
    let applied = target.apply_sync_entries(&[], &[]).unwrap();
    assert_eq!(applied, 0);
}
