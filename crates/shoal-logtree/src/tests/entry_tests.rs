//! Tests for LogEntry hash/sign/verify.

use crate::entry::{Action, LogEntry};
use shoal_types::ObjectId;

use super::test_identity;

#[test]
fn test_compute_hash_deterministic() {
    let (node_id, _) = test_identity(1);
    let action = Action::Put {
        bucket: "b".into(),
        key: "k".into(),
        manifest_id: ObjectId::from([1u8; 32]),
    };
    let parents = vec![];

    let h1 = LogEntry::compute_hash(100, node_id, &action, &parents);
    let h2 = LogEntry::compute_hash(100, node_id, &action, &parents);
    assert_eq!(h1, h2, "same inputs must produce same hash");
}

#[test]
fn test_verify_hash_passes() {
    let (node_id, signing_key) = test_identity(1);
    let action = Action::Put {
        bucket: "b".into(),
        key: "k".into(),
        manifest_id: ObjectId::from([1u8; 32]),
    };

    let entry = LogEntry::new_signed(100, node_id, action, vec![], &signing_key);
    assert!(entry.verify_hash(), "valid entry should pass hash check");
}

#[test]
fn test_verify_hash_fails_on_tamper() {
    let (node_id, signing_key) = test_identity(1);
    let action = Action::Put {
        bucket: "b".into(),
        key: "k".into(),
        manifest_id: ObjectId::from([1u8; 32]),
    };

    let mut entry = LogEntry::new_signed(100, node_id, action, vec![], &signing_key);
    entry.hlc = 999; // tamper with the HLC
    assert!(
        !entry.verify_hash(),
        "tampered entry should fail hash check"
    );
}

#[test]
fn test_verify_signature_passes() {
    let (node_id, signing_key) = test_identity(1);
    let action = Action::Delete {
        bucket: "b".into(),
        key: "k".into(),
    };

    let entry = LogEntry::new_signed(200, node_id, action, vec![], &signing_key);
    assert!(
        entry.verify_signature(),
        "valid signature should pass verification"
    );
}

#[test]
fn test_verify_signature_rejects_tampered_hash() {
    let (node_id, signing_key) = test_identity(1);
    let action = Action::Merge;

    let mut entry = LogEntry::new_signed(300, node_id, action, vec![], &signing_key);
    entry.hash[0] ^= 0xff; // flip a bit in the hash
    assert!(
        !entry.verify_signature(),
        "tampered hash should fail signature check"
    );
}

#[test]
fn test_verify_signature_rejects_wrong_signer() {
    let (node_id_1, signing_key_1) = test_identity(1);
    let (_node_id_2, signing_key_2) = test_identity(2);

    // Sign with key 1 but claim node_id 2.
    let action = Action::Merge;
    let mut entry = LogEntry::new_signed(400, node_id_1, action.clone(), vec![], &signing_key_1);

    // Forge: create entry as node_id_2 but reuse node_id_1's signature.
    let (node_id_2, _) = test_identity(2);
    entry.node_id = node_id_2;
    assert!(
        !entry.verify_signature(),
        "wrong signer should fail verification"
    );

    // Also verify that a correctly-signed entry from node 2 passes.
    let entry2 = LogEntry::new_signed(500, node_id_2, action, vec![], &signing_key_2);
    assert!(entry2.verify_signature());
}

#[test]
fn test_entry_postcard_roundtrip() {
    let (node_id, signing_key) = test_identity(1);
    let action = Action::Put {
        bucket: "photos".into(),
        key: "cat.jpg".into(),
        manifest_id: ObjectId::from([42u8; 32]),
    };

    let entry = LogEntry::new_signed(123456, node_id, action, vec![[7u8; 32]], &signing_key);
    let bytes = postcard::to_allocvec(&entry).unwrap();
    let decoded: LogEntry = postcard::from_bytes(&bytes).unwrap();

    assert_eq!(entry, decoded);
    assert!(decoded.verify_hash());
    assert!(decoded.verify_signature());
}

#[test]
fn test_different_actions_produce_different_hashes() {
    let (node_id, _) = test_identity(1);
    let parents = vec![];

    let h1 = LogEntry::compute_hash(
        100,
        node_id,
        &Action::Put {
            bucket: "b".into(),
            key: "k".into(),
            manifest_id: ObjectId::from([1u8; 32]),
        },
        &parents,
    );

    let h2 = LogEntry::compute_hash(
        100,
        node_id,
        &Action::Delete {
            bucket: "b".into(),
            key: "k".into(),
        },
        &parents,
    );

    assert_ne!(h1, h2, "different actions must produce different hashes");
}

#[test]
fn test_different_parents_produce_different_hashes() {
    let (node_id, _) = test_identity(1);
    let action = Action::Merge;

    let h1 = LogEntry::compute_hash(100, node_id, &action, &vec![]);
    let h2 = LogEntry::compute_hash(100, node_id, &action, &vec![[1u8; 32]]);

    assert_ne!(h1, h2, "different parents must produce different hashes");
}
