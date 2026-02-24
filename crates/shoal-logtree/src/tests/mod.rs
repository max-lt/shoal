//! Tests for the LogTree crate.

mod dag_tests;
mod entry_tests;
mod integrity_tests;
mod snapshot_tests;
mod sync_tests;
mod tree_tests;
mod versioning_tests;

use ed25519_dalek::SigningKey;
use shoal_types::NodeId;

use crate::store::LogTreeStore;
use crate::tree::LogTree;

/// Create a test (NodeId, SigningKey) pair from a seed.
///
/// The NodeId is derived from the ed25519 public key so that
/// `LogEntry::verify_signature()` works.
fn test_identity(seed: u8) -> (NodeId, SigningKey) {
    let signing_key = SigningKey::from_bytes(&[seed; 32]);
    let verifying_key = signing_key.verifying_key();
    let node_id = NodeId::from(verifying_key.to_bytes());
    (node_id, signing_key)
}

/// Create a test LogTree with an in-memory store.
fn test_tree(seed: u8) -> LogTree {
    let (node_id, signing_key) = test_identity(seed);
    let store = LogTreeStore::in_memory();
    LogTree::new(store, node_id, signing_key)
}
