//! Log entry types for the LogTree DAG.

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use shoal_types::{NodeId, ObjectId};

/// A single entry in the LogTree DAG.
///
/// Each entry records a cluster mutation (put, delete, merge, snapshot),
/// references its parent entries by hash, and is signed by the authoring node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    /// Hybrid logical clock timestamp.
    pub hlc: u64,
    /// Node that created this entry.
    pub node_id: NodeId,
    /// The mutation being recorded.
    pub action: Action,
    /// Hashes of parent entries (DAG edges).
    pub parents: Vec<[u8; 32]>,
    /// blake3 hash of `(hlc, node_id, action, parents)`.
    pub hash: [u8; 32],
    /// ed25519 signature over the hash, by `node_id`.
    /// Stored as two 32-byte halves for serde compatibility (serde doesn't
    /// derive for `[u8; 64]` out of the box).
    pub signature_r: [u8; 32],
    pub signature_s: [u8; 32],
}

/// A cluster mutation action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action {
    /// Store or overwrite an object.
    Put {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// ObjectId of the manifest.
        manifest_id: ObjectId,
    },
    /// Delete an object.
    Delete {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
    },
    /// Merge entry — converges multiple DAG tips into one.
    Merge,
    /// Snapshot — records a materialized state hash for pruning.
    Snapshot {
        /// blake3 hash of the serialized materialized state.
        state_hash: [u8; 32],
    },
    /// Create an API key.
    ///
    /// Only the access key ID is stored in the DAG for auditability.
    /// The secret is pulled via QUIC point-to-point.
    CreateApiKey { access_key_id: String },
    /// Delete an API key.
    DeleteApiKey { access_key_id: String },
    /// Set tags on an object (replaces all existing tags).
    SetTags {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Tag key-value pairs.
        tags: BTreeMap<String, String>,
    },
    /// Delete all tags from an object.
    DeleteTags {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
    },
}

/// A versioned record for a specific `(bucket, key)` pair.
///
/// Multiple versions are kept sorted by HLC descending. The first
/// non-deleted entry is the "current" version (LWW semantics).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Version {
    /// HLC timestamp when this version was created.
    pub hlc: u64,
    /// The manifest ObjectId (zero for delete markers).
    pub manifest_id: ObjectId,
    /// Whether this version is a delete marker.
    pub deleted: bool,
    /// NodeId of the author, used as tiebreak when HLCs match.
    pub node_id: NodeId,
}

/// Hashable content of a [`LogEntry`] (excludes `hash` and `signature`).
#[derive(Serialize)]
struct HashableContent<'a> {
    hlc: u64,
    node_id: NodeId,
    action: &'a Action,
    parents: &'a Vec<[u8; 32]>,
}

impl LogEntry {
    /// Compute the blake3 hash of the entry's content (hlc, node_id, action, parents).
    pub fn compute_hash(
        hlc: u64,
        node_id: NodeId,
        action: &Action,
        parents: &Vec<[u8; 32]>,
    ) -> [u8; 32] {
        let content = HashableContent {
            hlc,
            node_id,
            action,
            parents,
        };
        let bytes = postcard::to_allocvec(&content).expect("serialization should not fail");
        blake3::hash(&bytes).into()
    }

    /// Verify that the stored hash matches the entry's content.
    pub fn verify_hash(&self) -> bool {
        let expected = Self::compute_hash(self.hlc, self.node_id, &self.action, &self.parents);
        self.hash == expected
    }

    /// Reconstruct the 64-byte signature from its two halves.
    pub fn signature_bytes(&self) -> [u8; 64] {
        let mut sig = [0u8; 64];
        sig[..32].copy_from_slice(&self.signature_r);
        sig[32..].copy_from_slice(&self.signature_s);
        sig
    }

    /// Verify the ed25519 signature over the hash.
    ///
    /// Reconstructs the `VerifyingKey` from the `node_id` bytes and checks
    /// the signature. Returns `false` if the key bytes are invalid or the
    /// signature doesn't match.
    pub fn verify_signature(&self) -> bool {
        let Ok(verifying_key) = VerifyingKey::from_bytes(self.node_id.as_bytes()) else {
            return false;
        };
        let signature = Signature::from_bytes(&self.signature_bytes());
        verifying_key.verify(&self.hash, &signature).is_ok()
    }

    /// Create a new signed entry.
    pub fn new_signed(
        hlc: u64,
        node_id: NodeId,
        action: Action,
        parents: Vec<[u8; 32]>,
        signing_key: &SigningKey,
    ) -> Self {
        let hash = Self::compute_hash(hlc, node_id, &action, &parents);
        let signature: Signature = signing_key.sign(&hash);
        let sig_bytes = signature.to_bytes();
        let mut signature_r = [0u8; 32];
        let mut signature_s = [0u8; 32];
        signature_r.copy_from_slice(&sig_bytes[..32]);
        signature_s.copy_from_slice(&sig_bytes[32..]);

        Self {
            hlc,
            node_id,
            action,
            parents,
            hash,
            signature_r,
            signature_s,
        }
    }
}
