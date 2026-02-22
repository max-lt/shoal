//! Shared types and identifiers for Shoal.
//!
//! This crate defines all core types used across the Shoal workspace:
//! identifiers ([`ShardId`], [`ChunkId`], [`ObjectId`], [`NodeId`]),
//! data structures ([`Manifest`], [`ChunkMeta`], [`ShardMeta`]),
//! cluster types ([`Member`], [`MemberState`], [`ClusterEvent`]),
//! and configuration ([`NodeConfig`], [`ErasureConfig`], [`StorageBackend`]).

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// ID types
// ---------------------------------------------------------------------------

macro_rules! define_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
        pub struct $name([u8; 32]);

        impl $name {
            /// Create an ID by hashing arbitrary data with BLAKE3.
            pub fn from_data(data: &[u8]) -> Self {
                Self(blake3::hash(data).into())
            }

            /// Return the raw 32-byte representation.
            pub fn as_bytes(&self) -> &[u8; 32] {
                &self.0
            }
        }

        impl From<[u8; 32]> for $name {
            fn from(bytes: [u8; 32]) -> Self {
                Self(bytes)
            }
        }

        impl AsRef<[u8]> for $name {
            fn as_ref(&self) -> &[u8] {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                for byte in &self.0 {
                    write!(f, "{byte:02x}")?;
                }
                Ok(())
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self)
            }
        }
    };
}

define_id!(
    /// Content-addressed identifier for a shard: `blake3(shard_data)`.
    ShardId
);

define_id!(
    /// Content-addressed identifier for a chunk: `blake3(chunk_data)`.
    ChunkId
);

define_id!(
    /// Content-addressed identifier for an object: `blake3(serialized_manifest)`.
    ObjectId
);

define_id!(
    /// Identifier for a cluster node, derived from its iroh endpoint key.
    NodeId
);

// ---------------------------------------------------------------------------
// Core data structures
// ---------------------------------------------------------------------------

/// Top-level manifest describing a stored object.
///
/// A manifest records how an object was chunked and erasure-coded,
/// allowing any node to reconstruct the original data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Unique identifier for this object (blake3 of the serialized manifest content).
    pub object_id: ObjectId,
    /// Total size of the original object in bytes.
    pub total_size: u64,
    /// Size of each chunk (last chunk may be smaller).
    pub chunk_size: u32,
    /// Per-chunk metadata including shard information.
    pub chunks: Vec<ChunkMeta>,
    /// Unix timestamp (seconds) when the object was created.
    pub created_at: u64,
    /// User-supplied metadata (e.g. content-type, custom headers).
    pub metadata: BTreeMap<String, String>,
}

/// Metadata for a single chunk within a manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkMeta {
    /// Content-addressed identifier for this chunk.
    pub chunk_id: ChunkId,
    /// Byte offset of this chunk within the original object.
    pub offset: u64,
    /// Size of this chunk in bytes.
    pub size: u32,
    /// Reed-Solomon shards produced from this chunk.
    pub shards: Vec<ShardMeta>,
}

/// Metadata for a single erasure-coded shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardMeta {
    /// Content-addressed identifier for this shard.
    pub shard_id: ShardId,
    /// Position in the Reed-Solomon coding (0..k+m).
    pub index: u8,
    /// Size of this shard in bytes.
    pub size: u32,
}

// ---------------------------------------------------------------------------
// Cluster types
// ---------------------------------------------------------------------------

/// A member of the Shoal cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    /// Unique identifier for this node.
    pub node_id: NodeId,
    /// Available storage capacity in bytes.
    pub capacity: u64,
    /// Current membership state.
    pub state: MemberState,
    /// Incarnation number, incremented on each restart.
    pub generation: u64,
}

/// Membership state of a cluster node as determined by the SWIM protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemberState {
    /// Node is responsive and healthy.
    Alive,
    /// Node has not responded to probes; under investigation.
    Suspect,
    /// Node has been declared unreachable.
    Dead,
}

/// Events that propagate through the cluster via gossip.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterEvent {
    /// A new node has joined the cluster.
    NodeJoined(Member),
    /// A node has gracefully left the cluster.
    NodeLeft(NodeId),
    /// A node has been declared dead by the failure detector.
    NodeDead(NodeId),
    /// A shard has been successfully stored on a node.
    ShardStored(ShardId, NodeId),
    /// A shard needs repair (under-replicated or corrupted).
    RepairNeeded(ShardId),
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Storage backend selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageBackend {
    /// In-memory storage (volatile, for testing or caching).
    Memory,
    /// File-based storage with one file per shard.
    File,
    /// Direct I/O (io_uring) — reserved for future use.
    Direct,
}

/// Erasure coding parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErasureConfig {
    /// Number of data shards.
    pub k: u8,
    /// Number of parity shards.
    pub m: u8,
    /// Size of each chunk in bytes before erasure coding.
    pub chunk_size: u32,
}

impl Default for ErasureConfig {
    fn default() -> Self {
        Self {
            k: 4,
            m: 2,
            chunk_size: 1_048_576, // 1 MB
        }
    }
}

/// Adaptive node configuration that adjusts to available hardware.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Which storage backend to use.
    pub storage_backend: StorageBackend,
    /// Chunk size in bytes (256 KB on RPi, up to 4 MB on datacenter).
    pub chunk_size: u32,
    /// Number of data shards for erasure coding.
    pub erasure_k: u8,
    /// Number of parity shards for erasure coding.
    pub erasure_m: u8,
    /// Maximum bandwidth (bytes/sec) allocated to repair traffic.
    pub repair_max_bandwidth: u64,
    /// Number of concurrent repair transfers.
    pub repair_concurrent_transfers: u16,
    /// Interval in milliseconds between gossip rounds.
    pub gossip_interval_ms: u32,
    /// Objects smaller than this threshold are stored inline in metadata.
    pub inline_threshold: u32,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            storage_backend: StorageBackend::File,
            chunk_size: 1_048_576, // 1 MB
            erasure_k: 4,
            erasure_m: 2,
            repair_max_bandwidth: 104_857_600, // 100 MB/s
            repair_concurrent_transfers: 8,
            gossip_interval_ms: 1_000,
            inline_threshold: 4_096, // 4 KB
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_id_from_data_deterministic() {
        let data = b"hello world";
        let id1 = ShardId::from_data(data);
        let id2 = ShardId::from_data(data);
        assert_eq!(id1, id2, "same data must produce same ShardId");
    }

    #[test]
    fn test_shard_id_different_data_different_id() {
        let id1 = ShardId::from_data(b"hello");
        let id2 = ShardId::from_data(b"world");
        assert_ne!(id1, id2, "different data must produce different ShardId");
    }

    #[test]
    fn test_chunk_id_deterministic() {
        let id1 = ChunkId::from_data(b"chunk data");
        let id2 = ChunkId::from_data(b"chunk data");
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_object_id_deterministic() {
        let id1 = ObjectId::from_data(b"object");
        let id2 = ObjectId::from_data(b"object");
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_node_id_deterministic() {
        let id1 = NodeId::from_data(b"node key");
        let id2 = NodeId::from_data(b"node key");
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_id_from_bytes() {
        let bytes = [42u8; 32];
        let id = ShardId::from(bytes);
        assert_eq!(id.as_bytes(), &bytes);
    }

    #[test]
    fn test_id_as_ref() {
        let id = ShardId::from_data(b"test");
        let slice: &[u8] = id.as_ref();
        assert_eq!(slice.len(), 32);
    }

    #[test]
    fn test_display_outputs_hex() {
        let bytes = [
            0x0a, 0x1b, 0x2c, 0x3d, 0x4e, 0x5f, 0x60, 0x71, 0x82, 0x93, 0xa4, 0xb5, 0xc6, 0xd7,
            0xe8, 0xf9, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb,
            0xcc, 0xdd, 0xee, 0xff,
        ];
        let id = ShardId::from(bytes);
        let hex = id.to_string();
        assert_eq!(
            hex,
            "0a1b2c3d4e5f60718293a4b5c6d7e8f900112233445566778899aabbccddeeff"
        );
        assert_eq!(hex.len(), 64);
    }

    #[test]
    fn test_debug_format() {
        let id = ShardId::from([0u8; 32]);
        let debug = format!("{id:?}");
        assert!(debug.starts_with("ShardId("));
        assert!(debug.ends_with(')'));
    }

    #[test]
    fn test_id_ordering() {
        let id_low = ShardId::from([0u8; 32]);
        let id_high = ShardId::from([0xffu8; 32]);
        assert!(id_low < id_high);
    }

    #[test]
    fn test_id_hash() {
        use std::collections::HashSet;
        let id1 = ShardId::from_data(b"a");
        let id2 = ShardId::from_data(b"b");
        let mut set = HashSet::new();
        set.insert(id1);
        set.insert(id2);
        set.insert(id1); // duplicate
        assert_eq!(set.len(), 2);
    }

    // --- Postcard round-trip tests ---

    #[test]
    fn test_shard_id_roundtrip_postcard() {
        let id = ShardId::from_data(b"shard content");
        let encoded = postcard::to_allocvec(&id).unwrap();
        let decoded: ShardId = postcard::from_bytes(&encoded).unwrap();
        assert_eq!(id, decoded);
    }

    #[test]
    fn test_chunk_id_roundtrip_postcard() {
        let id = ChunkId::from_data(b"chunk content");
        let encoded = postcard::to_allocvec(&id).unwrap();
        let decoded: ChunkId = postcard::from_bytes(&encoded).unwrap();
        assert_eq!(id, decoded);
    }

    #[test]
    fn test_object_id_roundtrip_postcard() {
        let id = ObjectId::from_data(b"object content");
        let encoded = postcard::to_allocvec(&id).unwrap();
        let decoded: ObjectId = postcard::from_bytes(&encoded).unwrap();
        assert_eq!(id, decoded);
    }

    #[test]
    fn test_node_id_roundtrip_postcard() {
        let id = NodeId::from_data(b"node key");
        let encoded = postcard::to_allocvec(&id).unwrap();
        let decoded: NodeId = postcard::from_bytes(&encoded).unwrap();
        assert_eq!(id, decoded);
    }

    #[test]
    fn test_manifest_roundtrip_postcard() {
        let manifest = Manifest {
            object_id: ObjectId::from_data(b"test object"),
            total_size: 5000,
            chunk_size: 1024,
            chunks: vec![ChunkMeta {
                chunk_id: ChunkId::from_data(b"chunk 0"),
                offset: 0,
                size: 1024,
                shards: vec![
                    ShardMeta {
                        shard_id: ShardId::from_data(b"shard 0-0"),
                        index: 0,
                        size: 512,
                    },
                    ShardMeta {
                        shard_id: ShardId::from_data(b"shard 0-1"),
                        index: 1,
                        size: 512,
                    },
                ],
            }],
            created_at: 1700000000,
            metadata: BTreeMap::from([(
                "content-type".to_string(),
                "application/octet-stream".to_string(),
            )]),
        };

        let encoded = postcard::to_allocvec(&manifest).unwrap();
        let decoded: Manifest = postcard::from_bytes(&encoded).unwrap();
        assert_eq!(manifest, decoded);
    }

    #[test]
    fn test_chunk_meta_roundtrip_postcard() {
        let chunk = ChunkMeta {
            chunk_id: ChunkId::from_data(b"chunk"),
            offset: 4096,
            size: 1024,
            shards: vec![],
        };
        let encoded = postcard::to_allocvec(&chunk).unwrap();
        let decoded: ChunkMeta = postcard::from_bytes(&encoded).unwrap();
        assert_eq!(chunk, decoded);
    }

    #[test]
    fn test_shard_meta_roundtrip_postcard() {
        let shard = ShardMeta {
            shard_id: ShardId::from_data(b"shard"),
            index: 3,
            size: 512,
        };
        let encoded = postcard::to_allocvec(&shard).unwrap();
        let decoded: ShardMeta = postcard::from_bytes(&encoded).unwrap();
        assert_eq!(shard, decoded);
    }

    #[test]
    fn test_member_roundtrip_postcard() {
        let member = Member {
            node_id: NodeId::from_data(b"node-1"),
            capacity: 1_000_000_000,
            state: MemberState::Alive,
            generation: 1,
        };
        let encoded = postcard::to_allocvec(&member).unwrap();
        let decoded: Member = postcard::from_bytes(&encoded).unwrap();
        assert_eq!(member, decoded);
    }

    #[test]
    fn test_member_state_roundtrip_postcard() {
        for state in [MemberState::Alive, MemberState::Suspect, MemberState::Dead] {
            let encoded = postcard::to_allocvec(&state).unwrap();
            let decoded: MemberState = postcard::from_bytes(&encoded).unwrap();
            assert_eq!(state, decoded);
        }
    }

    #[test]
    fn test_cluster_event_roundtrip_postcard() {
        let events = vec![
            ClusterEvent::NodeJoined(Member {
                node_id: NodeId::from_data(b"node-1"),
                capacity: 500_000,
                state: MemberState::Alive,
                generation: 1,
            }),
            ClusterEvent::NodeLeft(NodeId::from_data(b"node-2")),
            ClusterEvent::NodeDead(NodeId::from_data(b"node-3")),
            ClusterEvent::ShardStored(ShardId::from_data(b"shard-1"), NodeId::from_data(b"node-1")),
            ClusterEvent::RepairNeeded(ShardId::from_data(b"shard-2")),
        ];

        for event in &events {
            let encoded = postcard::to_allocvec(event).unwrap();
            let decoded: ClusterEvent = postcard::from_bytes(&encoded).unwrap();
            assert_eq!(event, &decoded);
        }
    }

    #[test]
    fn test_storage_backend_roundtrip_postcard() {
        for backend in [
            StorageBackend::Memory,
            StorageBackend::File,
            StorageBackend::Direct,
        ] {
            let encoded = postcard::to_allocvec(&backend).unwrap();
            let decoded: StorageBackend = postcard::from_bytes(&encoded).unwrap();
            assert_eq!(backend, decoded);
        }
    }

    #[test]
    fn test_erasure_config_roundtrip_postcard() {
        let config = ErasureConfig {
            k: 4,
            m: 2,
            chunk_size: 1_048_576,
        };
        let encoded = postcard::to_allocvec(&config).unwrap();
        let decoded: ErasureConfig = postcard::from_bytes(&encoded).unwrap();
        assert_eq!(config, decoded);
    }

    #[test]
    fn test_node_config_roundtrip_postcard() {
        let config = NodeConfig::default();
        let encoded = postcard::to_allocvec(&config).unwrap();
        let decoded: NodeConfig = postcard::from_bytes(&encoded).unwrap();
        assert_eq!(config, decoded);
    }

    #[test]
    fn test_erasure_config_default() {
        let config = ErasureConfig::default();
        assert_eq!(config.k, 4);
        assert_eq!(config.m, 2);
        assert_eq!(config.chunk_size, 1_048_576);
    }

    #[test]
    fn test_node_config_default() {
        let config = NodeConfig::default();
        assert_eq!(config.storage_backend, StorageBackend::File);
        assert_eq!(config.chunk_size, 1_048_576);
        assert_eq!(config.erasure_k, 4);
        assert_eq!(config.erasure_m, 2);
        assert_eq!(config.repair_max_bandwidth, 104_857_600);
        assert_eq!(config.repair_concurrent_transfers, 8);
        assert_eq!(config.gossip_interval_ms, 1_000);
        assert_eq!(config.inline_threshold, 4_096);
    }
}
