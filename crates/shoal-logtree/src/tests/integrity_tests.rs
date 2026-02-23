//! Tests for LogTree integrity checks (hash/signature rejection).

use std::collections::BTreeMap;

use shoal_types::{MANIFEST_VERSION, Manifest, ObjectId};

use super::{test_identity, test_tree};
use crate::entry::{Action, LogEntry};

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
fn test_receive_rejects_invalid_hash() {
    let tree = test_tree(1);

    let (node_id, signing_key) = test_identity(2);
    let action = Action::Put {
        bucket: "b".into(),
        key: "k".into(),
        manifest_id: ObjectId::from([1u8; 32]),
    };

    let mut entry = LogEntry::new_signed(100, node_id, action, vec![], &signing_key);
    entry.hash[0] ^= 0xff; // corrupt hash

    let result = tree.receive_entry(&entry, None);
    assert!(
        matches!(result, Err(crate::error::LogTreeError::InvalidHash)),
        "should reject entry with invalid hash"
    );
}

#[test]
fn test_receive_rejects_invalid_signature() {
    let tree = test_tree(1);

    let (node_id, signing_key) = test_identity(2);
    let action = Action::Put {
        bucket: "b".into(),
        key: "k".into(),
        manifest_id: ObjectId::from([1u8; 32]),
    };

    let mut entry = LogEntry::new_signed(100, node_id, action, vec![], &signing_key);
    entry.signature_r[0] ^= 0xff; // corrupt signature

    let result = tree.receive_entry(&entry, None);
    assert!(
        matches!(result, Err(crate::error::LogTreeError::InvalidSignature)),
        "should reject entry with invalid signature"
    );
}

#[test]
fn test_receive_rejects_unknown_parent() {
    let tree = test_tree(1);

    let (node_id, signing_key) = test_identity(2);
    let action = Action::Merge;
    let fake_parent = [99u8; 32]; // doesn't exist

    let entry = LogEntry::new_signed(100, node_id, action, vec![fake_parent], &signing_key);

    let result = tree.receive_entry(&entry, None);
    assert!(
        matches!(result, Err(crate::error::LogTreeError::MissingParents(_))),
        "should reject entry with unknown parent"
    );
}

#[test]
fn test_receive_duplicate_entry_returns_false() {
    let tree = test_tree(1);
    let oid = ObjectId::from([1u8; 32]);
    let manifest = test_manifest(oid);

    let (node_id, signing_key) = test_identity(2);
    let action = Action::Put {
        bucket: "b".into(),
        key: "k".into(),
        manifest_id: oid,
    };

    let entry = LogEntry::new_signed(100, node_id, action, vec![], &signing_key);

    let first = tree.receive_entry(&entry, Some(&manifest)).unwrap();
    assert!(first, "first receive should return true");

    let second = tree.receive_entry(&entry, Some(&manifest)).unwrap();
    assert!(!second, "duplicate receive should return false");
}

#[test]
fn test_receive_valid_entry_succeeds() {
    let tree = test_tree(1);
    let oid = ObjectId::from([1u8; 32]);
    let manifest = test_manifest(oid);

    let (node_id, signing_key) = test_identity(2);
    let action = Action::Put {
        bucket: "b".into(),
        key: "k".into(),
        manifest_id: oid,
    };

    let entry = LogEntry::new_signed(100, node_id, action, vec![], &signing_key);

    let result = tree.receive_entry(&entry, Some(&manifest)).unwrap();
    assert!(result, "valid entry should be accepted");

    // Key should be resolvable.
    assert_eq!(tree.resolve("b", "k").unwrap(), Some(oid));
}
