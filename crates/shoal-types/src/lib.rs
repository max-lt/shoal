//! Shared types and identifiers for Shoal.
//!
//! This crate defines all core types used across the Shoal workspace:
//! identifiers ([`ShardId`], [`ChunkId`], [`ObjectId`], [`NodeId`]),
//! data structures ([`Manifest`], [`ChunkMeta`], [`ShardMeta`]),
//! cluster types ([`Member`], [`MemberState`], [`ClusterEvent`]),
//! and configuration ([`NodeConfig`], [`ErasureConfig`], [`StorageBackend`]).

pub mod events;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

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

/// Current manifest format version.
pub const MANIFEST_VERSION: u8 = 1;

/// Top-level manifest describing a stored object.
///
/// A manifest records how an object was chunked and erasure-coded,
/// allowing any node to reconstruct the original data.
///
/// The `version` field enables safe format evolution. All code that reads
/// a manifest must check this field and reject unknown versions rather than
/// silently misinterpreting data (lesson from Ceph's "fast EC" incident).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Format version. Current version is [`MANIFEST_VERSION`] (1).
    pub version: u8,
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

/// Compression algorithm used for a stored chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Compression {
    /// No compression — chunk stored as-is.
    None,
    /// Zstandard compression.
    Zstd,
}

/// Metadata for a single chunk within a manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkMeta {
    /// Content-addressed identifier for this chunk (`blake3(raw_data)`).
    pub chunk_id: ChunkId,
    /// Byte offset of this chunk within the original object.
    pub offset: u64,
    /// Size of this chunk in bytes before compression.
    pub raw_length: u32,
    /// Size of this chunk after compression (equals `raw_length` when uncompressed).
    pub stored_length: u32,
    /// Compression algorithm applied to this chunk before erasure coding.
    pub compression: Compression,
    /// Reed-Solomon shards produced from the (possibly compressed) chunk data.
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
// Topology
// ---------------------------------------------------------------------------

/// Physical location of a node in the infrastructure hierarchy.
///
/// Used for failure-domain-aware shard placement and zone-scoped replication.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeTopology {
    /// Region name (e.g. "eu-west", "us-east").
    pub region: String,
    /// Datacenter within the region (e.g. "ovh-rbx", "aws-euw1a").
    pub datacenter: String,
    /// Machine identifier within the datacenter (e.g. "node-07").
    pub machine: String,
}

impl Default for NodeTopology {
    fn default() -> Self {
        Self {
            region: "default".to_string(),
            datacenter: "default".to_string(),
            machine: "default".to_string(),
        }
    }
}

/// Where erasure coding stops and full replication begins.
///
/// Within a zone, shards are spread via erasure coding (space-efficient).
/// Across zones, full chunk data is replicated (each zone EC-encodes independently).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationBoundary {
    /// EC stays within a single datacenter. Full replica across DCs.
    Datacenter,
    /// EC can span DCs within a region. Full replica across regions.
    /// This is the default — good for multi-DC single-region setups.
    #[default]
    Region,
}

/// When to ACK a write to the client.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ZoneWriteAck {
    /// ACK as soon as the local zone has stored all shards. Fast, but
    /// there is a window where data exists in only one zone.
    #[default]
    Local,
    /// ACK after N zones have confirmed. E.g. `Quorum(2)` with 3 zones.
    Quorum(u8),
    /// ACK after ALL zones have confirmed. Slowest, maximum durability.
    All,
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
    /// Physical location in the infrastructure hierarchy.
    pub topology: NodeTopology,
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
    /// The local node is now active in the cluster (foca considers it joined).
    ///
    /// Emitted once the SWIM protocol has confirmed this node's membership.
    /// Components can wait for this event before starting work that depends
    /// on cluster participation (e.g. rebalancing, shard transfers).
    NodeReady(NodeId),
}

/// Payload types that travel through the gossip broadcast layer.
///
/// All cluster-wide dissemination goes through gossip (epidemic broadcast).
/// Point-to-point request/response (shard pull, manifest sync) stays on
/// direct QUIC streams.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GossipPayload {
    /// A cluster membership event.
    Event(ClusterEvent),
    /// A manifest broadcast (object stored/updated).
    ManifestPut {
        /// Target bucket.
        bucket: String,
        /// Object key within the bucket.
        key: String,
        /// Postcard-serialized [`Manifest`].
        manifest_bytes: Vec<u8>,
    },
    /// A log tree entry broadcast (DAG-based mutation tracking).
    LogEntry {
        /// Postcard-serialized [`LogEntry`](shoal_logtree::LogEntry).
        entry_bytes: Vec<u8>,
        /// Optional postcard-serialized [`Manifest`] referenced by the entry.
        manifest_bytes: Option<Vec<u8>>,
    },
}

/// Wire envelope for gossip messages.
///
/// Wraps a [`GossipPayload`] with a random nonce so that PlumTree
/// (iroh-gossip) never deduplicates two distinct broadcasts that happen
/// to have identical payload bytes (e.g. repeated `NodeDead` events).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GossipMessage {
    /// Random nonce to guarantee byte-level uniqueness.
    pub nonce: u64,
    /// The actual payload.
    pub payload: GossipPayload,
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
            chunk_size: 262_144, // 256 KB
        }
    }
}

/// Adaptive node configuration that adjusts to available hardware.
///
/// Every object goes through the same pipeline regardless of size:
/// chunking → erasure coding → shard distribution via the placement ring.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Which storage backend to use.
    pub storage_backend: StorageBackend,
    /// Chunk size in bytes (128 KB on RPi, 256 KB default, up to 4 MB on datacenter).
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
    /// Zone boundary for erasure coding vs full replication.
    pub replication_boundary: ReplicationBoundary,
    /// When to ACK writes to the client relative to zone replication.
    pub zone_write_ack: ZoneWriteAck,
    /// Circuit breaker settings for repair and rebalancing.
    pub repair_circuit_breaker: RepairCircuitBreaker,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            storage_backend: StorageBackend::File,
            chunk_size: 262_144, // 256 KB
            erasure_k: 4,
            erasure_m: 2,
            repair_max_bandwidth: 104_857_600, // 100 MB/s
            repair_concurrent_transfers: 8,
            gossip_interval_ms: 1_000,
            replication_boundary: ReplicationBoundary::default(),
            zone_write_ack: ZoneWriteAck::default(),
            repair_circuit_breaker: RepairCircuitBreaker::default(),
        }
    }
}

/// Circuit breaker for repair and rebalancing operations.
///
/// Prevents cascade meltdowns where repair activity overwhelms the cluster
/// (lesson from Ceph's 2018 rebalancing storm: one OSD restart triggered
/// cascading OOM kills across the cluster).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RepairCircuitBreaker {
    /// If more than this fraction of nodes are down, suspend all repair/rebalance.
    /// Default: 0.5 (50%).
    pub max_down_fraction: f64,
    /// Throttle aggressively when repair queue exceeds this many shards.
    /// Default: 10,000.
    pub queue_pressure_threshold: usize,
    /// Seconds to wait after a node comes back before starting rebalance.
    /// Prevents flapping nodes from triggering repeated rebalance storms.
    /// Default: 60 seconds.
    pub rebalance_cooldown_secs: u64,
}

impl Default for RepairCircuitBreaker {
    fn default() -> Self {
        Self {
            max_down_fraction: 0.5,
            queue_pressure_threshold: 10_000,
            rebalance_cooldown_secs: 60,
        }
    }
}

// ---------------------------------------------------------------------------
// Hybrid Logical Clock
// ---------------------------------------------------------------------------

/// A hybrid logical clock combining wall-clock time with a logical counter.
///
/// Produces monotonically increasing timestamps (nanoseconds since UNIX epoch)
/// that are always at least as large as the wall clock and strictly increasing
/// even when the wall clock hasn't advanced. Thread-safe via `AtomicU64`.
pub struct HybridClock {
    last: AtomicU64,
}

impl HybridClock {
    /// Create a new clock initialised to the current wall-clock time.
    pub fn new() -> Self {
        let now = wall_clock_nanos();
        Self {
            last: AtomicU64::new(now),
        }
    }

    /// Advance and return a new unique timestamp.
    ///
    /// The returned value is `max(wall_clock, last) + 1`, guaranteeing strict
    /// monotonicity even under rapid successive calls or clock skew.
    pub fn tick(&self) -> u64 {
        loop {
            let prev = self.last.load(Ordering::SeqCst);
            let now = wall_clock_nanos();
            let candidate = prev.max(now) + 1;

            if self
                .last
                .compare_exchange(prev, candidate, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return candidate;
            }
        }
    }

    /// Witness a remote HLC timestamp, advancing the local clock if necessary.
    ///
    /// After witnessing, `last = max(last, remote_hlc)`. The next [`tick`](Self::tick)
    /// will produce a value strictly greater than both.
    pub fn witness(&self, remote_hlc: u64) {
        self.last.fetch_max(remote_hlc, Ordering::SeqCst);
    }

    /// Return the current clock value without advancing it.
    pub fn current(&self) -> u64 {
        self.last.load(Ordering::SeqCst)
    }
}

impl Default for HybridClock {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for HybridClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HybridClock")
            .field("last", &self.last.load(Ordering::SeqCst))
            .finish()
    }
}

/// Current wall-clock time in nanoseconds since UNIX epoch.
fn wall_clock_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
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
            version: MANIFEST_VERSION,
            object_id: ObjectId::from_data(b"test object"),
            total_size: 5000,
            chunk_size: 1024,
            chunks: vec![ChunkMeta {
                chunk_id: ChunkId::from_data(b"chunk 0"),
                offset: 0,
                raw_length: 1024,
                stored_length: 1024,
                compression: Compression::None,
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
            raw_length: 1024,
            stored_length: 1024,
            compression: Compression::None,
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
    fn test_compression_roundtrip_postcard() {
        for comp in [Compression::None, Compression::Zstd] {
            let encoded = postcard::to_allocvec(&comp).unwrap();
            let decoded: Compression = postcard::from_bytes(&encoded).unwrap();
            assert_eq!(comp, decoded);
        }
    }

    #[test]
    fn test_member_roundtrip_postcard() {
        let member = Member {
            node_id: NodeId::from_data(b"node-1"),
            capacity: 1_000_000_000,
            state: MemberState::Alive,
            generation: 1,
            topology: NodeTopology {
                region: "eu-west".to_string(),
                datacenter: "ovh-rbx".to_string(),
                machine: "node-07".to_string(),
            },
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
                topology: NodeTopology::default(),
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
        assert_eq!(config.chunk_size, 262_144);
    }

    #[test]
    fn test_node_config_default() {
        let config = NodeConfig::default();
        assert_eq!(config.storage_backend, StorageBackend::File);
        assert_eq!(config.chunk_size, 262_144);
        assert_eq!(config.erasure_k, 4);
        assert_eq!(config.erasure_m, 2);
        assert_eq!(config.repair_max_bandwidth, 104_857_600);
        assert_eq!(config.repair_concurrent_transfers, 8);
        assert_eq!(config.gossip_interval_ms, 1_000);
        assert_eq!(config.replication_boundary, ReplicationBoundary::Region);
        assert_eq!(config.zone_write_ack, ZoneWriteAck::Local);
    }

    // --- Zone-aware type tests ---

    #[test]
    fn test_node_topology_roundtrip_postcard() {
        let topo = NodeTopology {
            region: "us-east".to_string(),
            datacenter: "aws-use1a".to_string(),
            machine: "i-abc123".to_string(),
        };
        let encoded = postcard::to_allocvec(&topo).unwrap();
        let decoded: NodeTopology = postcard::from_bytes(&encoded).unwrap();
        assert_eq!(topo, decoded);
    }

    #[test]
    fn test_node_topology_default() {
        let topo = NodeTopology::default();
        assert_eq!(topo.region, "default");
        assert_eq!(topo.datacenter, "default");
        assert_eq!(topo.machine, "default");
    }

    #[test]
    fn test_replication_boundary_roundtrip_postcard() {
        for boundary in [ReplicationBoundary::Datacenter, ReplicationBoundary::Region] {
            let encoded = postcard::to_allocvec(&boundary).unwrap();
            let decoded: ReplicationBoundary = postcard::from_bytes(&encoded).unwrap();
            assert_eq!(boundary, decoded);
        }
    }

    #[test]
    fn test_zone_write_ack_roundtrip_postcard() {
        for ack in [
            ZoneWriteAck::Local,
            ZoneWriteAck::Quorum(2),
            ZoneWriteAck::All,
        ] {
            let encoded = postcard::to_allocvec(&ack).unwrap();
            let decoded: ZoneWriteAck = postcard::from_bytes(&encoded).unwrap();
            assert_eq!(ack, decoded);
        }
    }

    #[test]
    fn test_member_with_topology_roundtrip() {
        let member = Member {
            node_id: NodeId::from_data(b"topo-node"),
            capacity: 10_000_000_000,
            state: MemberState::Alive,
            generation: 3,
            topology: NodeTopology {
                region: "ap-southeast".to_string(),
                datacenter: "sg-1".to_string(),
                machine: "rack-04-slot-12".to_string(),
            },
        };
        let encoded = postcard::to_allocvec(&member).unwrap();
        let decoded: Member = postcard::from_bytes(&encoded).unwrap();
        assert_eq!(member, decoded);
        assert_eq!(decoded.topology.region, "ap-southeast");
        assert_eq!(decoded.topology.datacenter, "sg-1");
        assert_eq!(decoded.topology.machine, "rack-04-slot-12");
    }

    // --- Production hardening tests ---

    #[test]
    fn test_manifest_version_field() {
        let manifest = Manifest {
            version: MANIFEST_VERSION,
            object_id: ObjectId::from_data(b"versioned"),
            total_size: 100,
            chunk_size: 1024,
            chunks: vec![],
            created_at: 0,
            metadata: BTreeMap::new(),
        };
        assert_eq!(manifest.version, 1);
        let encoded = postcard::to_allocvec(&manifest).unwrap();
        let decoded: Manifest = postcard::from_bytes(&encoded).unwrap();
        assert_eq!(decoded.version, 1);
    }

    #[test]
    fn test_repair_circuit_breaker_default() {
        let cb = RepairCircuitBreaker::default();
        assert!((cb.max_down_fraction - 0.5).abs() < f64::EPSILON);
        assert_eq!(cb.queue_pressure_threshold, 10_000);
        assert_eq!(cb.rebalance_cooldown_secs, 60);
    }

    #[test]
    fn test_repair_circuit_breaker_roundtrip_postcard() {
        let cb = RepairCircuitBreaker {
            max_down_fraction: 0.3,
            queue_pressure_threshold: 5000,
            rebalance_cooldown_secs: 120,
        };
        let encoded = postcard::to_allocvec(&cb).unwrap();
        let decoded: RepairCircuitBreaker = postcard::from_bytes(&encoded).unwrap();
        assert!((decoded.max_down_fraction - 0.3).abs() < f64::EPSILON);
        assert_eq!(decoded.queue_pressure_threshold, 5000);
        assert_eq!(decoded.rebalance_cooldown_secs, 120);
    }

    #[test]
    fn test_node_config_includes_circuit_breaker() {
        let config = NodeConfig::default();
        assert!((config.repair_circuit_breaker.max_down_fraction - 0.5).abs() < f64::EPSILON);
    }

    // --- HybridClock tests ---

    #[test]
    fn test_hlc_tick_monotonic() {
        let clock = HybridClock::new();
        let mut prev = clock.tick();

        for _ in 0..1000 {
            let next = clock.tick();
            assert!(next > prev, "tick must be strictly increasing");
            prev = next;
        }
    }

    #[test]
    fn test_hlc_tick_advances_beyond_wall_clock() {
        let clock = HybridClock::new();
        // Rapid ticks should produce values beyond the wall clock.
        let t1 = clock.tick();
        let t2 = clock.tick();
        let t3 = clock.tick();
        assert!(t2 > t1);
        assert!(t3 > t2);
    }

    #[test]
    fn test_hlc_witness_advances_local() {
        let clock = HybridClock::new();
        let far_future = clock.current() + 1_000_000_000; // 1 second ahead
        clock.witness(far_future);
        assert!(
            clock.current() >= far_future,
            "witness should advance local clock"
        );
        let next = clock.tick();
        assert!(next > far_future, "tick after witness should exceed remote");
    }

    #[test]
    fn test_hlc_witness_no_retreat() {
        let clock = HybridClock::new();
        let current = clock.tick();
        let past = current.saturating_sub(1_000_000);
        clock.witness(past);
        assert!(
            clock.current() >= current,
            "witness with past value should not retreat"
        );
    }

    #[test]
    fn test_hlc_concurrent_ticks_unique() {
        use std::collections::HashSet;
        use std::sync::Arc;

        let clock = Arc::new(HybridClock::new());
        let n_threads = 4;
        let ticks_per_thread = 1000;

        let mut handles = Vec::new();

        for _ in 0..n_threads {
            let clock = clock.clone();
            handles.push(std::thread::spawn(move || {
                let mut values = Vec::with_capacity(ticks_per_thread);

                for _ in 0..ticks_per_thread {
                    values.push(clock.tick());
                }

                values
            }));
        }

        let mut all_values = HashSet::new();

        for h in handles {
            for v in h.join().unwrap() {
                assert!(
                    all_values.insert(v),
                    "concurrent tick produced duplicate value"
                );
            }
        }

        assert_eq!(all_values.len(), n_threads * ticks_per_thread);
    }
}
