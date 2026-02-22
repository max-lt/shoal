# Shoal — Distributed Object Storage Engine

## How to Use This Document

This is the **complete architecture and implementation plan** for Shoal. Read the whole thing first to understand the vision, then implement **one milestone at a time**.

When asked to work on a milestone:

1. Read the milestone requirements carefully
2. Implement all checkboxes
3. Write all specified tests
4. Run `cargo test` for the affected crates — everything must pass
5. Run `cargo clippy -- -D warnings` — must be clean
6. Run `cargo fmt`
7. **STOP.** Do not proceed to the next milestone unless explicitly asked.

---

## What is Shoal?

Shoal is a distributed, self-healing object storage engine written in **100% pure Rust**. Think of it as a lightweight, peer-to-peer alternative to S3-compatible storage that can run anywhere — from a Raspberry Pi with an SD card to a 128-core datacenter server with NVMe arrays.

Nodes discover each other automatically, form a cluster, distribute data using erasure coding across available nodes, and self-repair when nodes join or leave. The system exposes an S3-compatible HTTP API on top.

The name "Shoal" comes from a shoal of fish — lightweight individual units that move together, self-organize, and reform when members disappear.

## Core Design Principles

1. **Pure Rust** — Zero C/C++ bindings. Zero `cc` build scripts. Cross-compilation must be trivial.
2. **Adaptive** — Same binary runs on a Raspberry Pi 4 and a datacenter monster. Config adapts to available resources.
3. **Self-healing** — Nodes join and leave. The cluster detects failures, rebalances, and repairs automatically.
4. **Content-addressed** — All data is identified by its BLAKE3 hash. Integrity is verifiable at every layer.
5. **Deterministic placement** — Every node can independently compute where any shard belongs. No central coordinator.

## Architecture Overview

```
┌─────────────────────────────┐
│   S3 HTTP API (axum)        │  shoal-s3
├─────────────────────────────┤
│   Engine (orchestrator)     │  shoal-engine
├──────────┬──────────────────┤
│ Repair   │  Cluster         │  shoal-repair / shoal-cluster
│ Scheduler│  Membership      │
├──────────┼──────────────────┤
│ Erasure  │  Placement       │  shoal-erasure / shoal-placement
│ Coding   │  (hash ring)     │
├──────────┴──────────────────┤
│   Metadata Store (Fjall)    │  shoal-meta
├─────────────────────────────┤
│   Content Addressing        │  shoal-cas
├─────────────────────────────┤
│   Shard Store (trait)       │  shoal-store
├─────────────────────────────┤
│   Network (iroh + gossip)   │  shoal-net
└─────────────────────────────┘
```

## Technology Stack

All dependencies are pure Rust:

| Layer            | Crate                  | Purpose                                                                           |
| ---------------- | ---------------------- | --------------------------------------------------------------------------------- |
| Hashing          | `blake3`               | Content addressing, integrity verification                                        |
| Metadata         | `fjall` v3             | LSM-tree embedded KV store (manifests, shard map, membership state, repair queue) |
| Erasure coding   | `reed-solomon-simd` v3 | Reed-Solomon with runtime SIMD detection (AVX2, SSSE3, Neon)                      |
| Networking       | `iroh` 0.35 (stable)   | QUIC connections, hole punching, relay fallback                                   |
| Gossip/broadcast | `iroh-gossip`          | HyParView + PlumTree epidemic broadcast for cluster events                        |
| Membership       | `foca`                 | SWIM protocol for failure detection (plugged into iroh transport)                 |
| Serialization    | `postcard` + `serde`   | Compact binary serialization                                                      |
| HTTP             | `axum` + `hyper`       | S3-compatible API                                                                 |
| Async            | `tokio`                | Runtime                                                                           |
| Observability    | `tracing` + `metrics`  | Logging and metrics                                                               |

### Why these choices?

- **Fjall over redb**: LSM-tree is write-optimized — critical during rebalancing when thousands of shard locations update. Keyspaces give clean separation. Fjall is a local cache/index, not the source of truth — everything is reconstructible from the cluster.
- **foca over custom SWIM**: Production-grade SWIM+Inf.+Susp. implementation, `no_std` compatible, pluggable transport. We pipe foca messages over iroh connections.
- **iroh-gossip over foca for broadcast**: foca handles membership/failure detection; iroh-gossip handles event dissemination (new shard available, repair needed, etc). Different tools for different jobs.
- **Custom shard transfer over iroh-blobs**: iroh-blobs post-0.35 is marked "not production quality". We build a simple shard transfer protocol on iroh QUIC streams instead.
- **Consistent hash ring is hand-written**: ~150 lines. No existing crate handles the ring diff computation (which shards must move when membership changes) that we need for rebalancing.

## Monorepo Structure

```
shoal/
├── Cargo.toml                        (workspace)
├── crates/
│   ├── shoal-types/                  Shared types, IDs, configs
│   ├── shoal-store/                  Trait ShardStore + backends
│   ├── shoal-cas/                    Content addressing, chunking, manifests
│   ├── shoal-meta/                   Metadata store (wraps Fjall)
│   ├── shoal-erasure/                Reed-Solomon (wraps reed-solomon-simd)
│   ├── shoal-placement/              Consistent hashing ring
│   ├── shoal-cluster/                Membership (foca) + gossip (iroh-gossip)
│   ├── shoal-repair/                 Auto-repair & rebalancing
│   ├── shoal-net/                    Network protocol on iroh
│   ├── shoal-engine/                 Node orchestrator
│   ├── shoal-s3/                     S3 HTTP API
│   └── shoal-cli/                    Binary entrypoint
├── tests/
│   ├── integration/
│   └── chaos/
└── benches/
```

## Key Data Types

```rust
// === Identifiers (all 32 bytes, all BLAKE3) ===
pub struct ShardId([u8; 32]);      // blake3(shard_data)
pub struct ChunkId([u8; 32]);      // blake3(chunk_data)
pub struct ObjectId([u8; 32]);     // blake3(manifest_serialized)
pub struct NodeId([u8; 32]);       // derived from iroh endpoint key

// === Core structures ===
pub struct Manifest {
    pub object_id: ObjectId,
    pub total_size: u64,
    pub chunk_size: u32,
    pub chunks: Vec<ChunkMeta>,
    pub created_at: u64,
    pub metadata: BTreeMap<String, String>,  // user metadata (content-type, etc)
}

pub struct ChunkMeta {
    pub chunk_id: ChunkId,
    pub offset: u64,
    pub size: u32,
    pub shards: Vec<ShardMeta>,
}

pub struct ShardMeta {
    pub shard_id: ShardId,
    pub index: u8,           // position in RS coding (0..k+m)
    pub size: u32,
}

// === Cluster ===
pub struct Member {
    pub node_id: NodeId,
    pub addr: NodeAddr,       // iroh address
    pub capacity: u64,        // available storage bytes
    pub state: MemberState,   // Alive | Suspect | Dead
    pub generation: u64,      // incremented on restart
}

// === Adaptive config ===
pub struct NodeConfig {
    pub storage_backend: StorageBackend,   // Memory | File | Direct(io_uring)
    pub chunk_size: u32,                    // 128KB (RPi) .. 256KB (default) .. 4MB (datacenter)
    pub erasure_k: u8,                      // data shards (2..16)
    pub erasure_m: u8,                      // parity shards (1..8)
    pub repair_max_bandwidth: u64,          // bytes/sec for repair traffic
    pub repair_concurrent_transfers: u16,   // 1 (RPi) .. 64 (datacenter)
    pub gossip_interval_ms: u32,
}
```

## Detailed Behavior

### Write Path

1. Client sends PUT via S3 API
2. Object data is chunked into fixed-size chunks (size depends on config)
3. Determine local zone from this node's topology + `ReplicationBoundary`
4. Each chunk → Reed-Solomon encode → k data shards + m parity shards
5. Each shard is BLAKE3-hashed to get its ShardId
6. Distribute shards via local zone's ring
7. Build manifest → gossip broadcast to ALL nodes (all zones)
8. ACK to client per `ZoneWriteAck` policy
9. Background (`ZoneReplicator` in `shoal-engine`, Milestone 11):
   → Send full chunk data to other zones
   → Each remote zone EC-encodes independently via its own ring
10. ObjectId (blake3 of serialized manifest) is stored in local Fjall index: `objects[bucket/key] → ObjectId`, `manifests[ObjectId] → Manifest`
11. Every object, regardless of size, goes through this same pipeline — no inline shortcut

### Read Path

1. Client sends GET via S3 API
2. Look up manifest in local Fjall (always available via gossip)
3. Fetch shards from LOCAL zone's ring only
4. EC decode → stream reconstructed chunks to client
5. Never crosses zone boundaries for normal reads

### Node Join

1. New node starts, connects to seed node(s) via iroh
2. foca SWIM protocol propagates membership change
3. All nodes recompute placement ring
4. Ring diff identifies shards that must move to the new node
5. Rebalancing transfers shards gradually (throttled)
6. As manifest shards arrive, node decodes them and populates its local Fjall index
7. Node is fully operational once rebalancing completes

### Node Failure

1. foca detects unresponsive node (ping → indirect ping → suspect → dead)
2. Membership change propagated via gossip
3. Ring recomputed, under-replicated shards identified
4. Repair scheduler prioritizes: shards with fewest remaining copies first
5. Repair executor fetches shards from surviving owners, reconstructs via RS decode if needed, writes to new owners

### Anti-Entropy (Background)

- Each node periodically verifies: "Do I hold all shards the ring assigns me?"
- Each node periodically verifies shard integrity: re-hash and compare to ShardId
- Corrupted or missing shards → added to repair queue

---

# Implementation Plan

Work through these milestones **in order**. After each milestone, **run all tests** and make sure everything passes before moving on. Commit after each milestone.

---

## Milestone 0 — Workspace Setup ✅

Set up the Rust workspace and all crate skeletons.

- [x] Create `shoal/Cargo.toml` workspace with all member crates
- [x] Create each crate directory with `Cargo.toml` and `src/lib.rs` (or `src/main.rs` for `shoal-cli`)
- [x] Set up `[workspace.dependencies]` with all shared deps (blake3, fjall, reed-solomon-simd, iroh, iroh-gossip, foca, postcard, serde, axum, tokio, tracing, thiserror, anyhow, bytes)
- [x] Verify the entire workspace compiles: `cargo build`
- [x] Verify iroh 0.35 and iroh-gossip version compatibility (iroh-gossip 0.35 with `net` feature)

**Test**: `cargo build` succeeds with no errors.

---

## Milestone 1 — `shoal-types` ✅

All shared types and identifiers.

- [x] Define `ShardId`, `ChunkId`, `ObjectId` as newtype wrappers around `[u8; 32]`
- [x] Implement `Display` (hex), `Debug`, `From<[u8; 32]>`, `AsRef<[u8]>`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash`, `Ord`, `PartialOrd` for all ID types
- [x] Implement `Serialize` / `Deserialize` (via serde) for all ID types
- [x] Define `NodeId` (also `[u8; 32]`, derived from iroh key)
- [x] Define `Manifest`, `ChunkMeta`, `ShardMeta` structs with serde derives
- [x] Define `Member` and `MemberState` enum (`Alive`, `Suspect`, `Dead`)
- [x] Define `NodeConfig` with all adaptive fields and sensible defaults
- [x] Define `StorageBackend` enum (`Memory`, `File`, `Direct`)
- [x] Define `ErasureConfig` struct (`k`, `m`, `chunk_size`)
- [x] Define `ClusterEvent` enum: `NodeJoined(Member)`, `NodeLeft(NodeId)`, `NodeDead(NodeId)`, `ShardStored(ShardId, NodeId)`, `RepairNeeded(ShardId)`
- [x] Helper functions: `ShardId::from_data(data: &[u8]) -> ShardId` (blake3 hash), same for `ChunkId`, `ObjectId`
- [x] ID types also implement `as_bytes() -> &[u8; 32]` accessor

**Tests** (26 tests):

- [x] Round-trip serialize/deserialize all types with postcard
- [x] Verify ID generation: same data → same ID, different data → different ID
- [x] Verify Display outputs hex correctly
- [x] `cargo test -p shoal-types` passes

---

## Milestone 2 — `shoal-store` ✅

The shard storage trait and in-memory backend.

- [x] Define the `ShardStore` trait using `Bytes` for zero-copy:

  ```rust
  #[async_trait]
  pub trait ShardStore: Send + Sync {
      async fn put(&self, id: ShardId, data: Bytes) -> Result<()>;
      async fn get(&self, id: ShardId) -> Result<Option<Bytes>>;
      async fn delete(&self, id: ShardId) -> Result<()>;
      async fn contains(&self, id: ShardId) -> Result<bool>;
      async fn list(&self) -> Result<Vec<ShardId>>;
      async fn capacity(&self) -> Result<StorageCapacity>;
      async fn verify(&self, id: ShardId) -> Result<bool>;
  }
  ```

- [x] Implement `MemoryStore` using `RwLock<HashMap<ShardId, Bytes>>`
- [x] `MemoryStore::capacity()` tracked via `AtomicU64` counter (O(1) per operation)
- [x] `MemoryStore::verify()` re-hashes stored data and compares to ShardId
- [x] Implement `FileStore` — one file per shard, 2-level fan-out directory layout
  - Atomic writes: write to temp file, then rename (crash-safe)
  - `contains()` uses async `tokio::fs::metadata` (not blocking `path.exists()`)
  - `capacity()` uses `statvfs` via `spawn_blocking` (not blocking the async runtime)

**Tests** (24 tests):

- [x] `MemoryStore`: put/get round-trip, get nonexistent, delete, contains, list, verify, capacity, overwrite
- [x] `FileStore`: same suite + fan-out structure verification + atomic write (no .tmp left)
- [x] `cargo test -p shoal-store` passes

---

## Milestone 3 — `shoal-cas` ✅

Content addressing: chunking objects into chunks, building manifests.

- [x] Implement fixed-size chunker with `Chunk.data: Bytes` (zero-copy through pipeline)
- [x] `Chunk.id` = `blake3(chunk.data)`, last chunk may be smaller
- [x] Implement streaming chunker (`chunk_stream` takes `impl AsyncRead + Unpin`)
- [x] Implement `build_manifest` and `build_manifest_with_timestamp` (deterministic testing)
  - `ObjectId` = blake3 of postcard-serialized content (without object_id field)
- [x] Implement manifest serialization/deserialization with postcard

**Tests** (13 tests):

- [x] Chunking: 0 bytes, exact size, size+1, 3.5x size
- [x] ChunkId deterministic, deduplication (identical chunks → same ID)
- [x] Streaming chunker matches sync chunker
- [x] Manifest round-trip, ObjectId deterministic, ObjectId changes with content
- [x] `cargo test -p shoal-cas` passes

---

## Milestone 4 — `shoal-erasure` ✅

Erasure coding wrapper around reed-solomon-simd.

- [x] Implement `ErasureEncoder::encode(chunk) -> (Vec<Shard>, original_size)`
  - `Shard` has `id: ShardId`, `index: u8` (0..k for data, k..k+m for parity), `data: Bytes`
  - Automatic padding: `ceil(len/k)` rounded up to even (reed-solomon-simd requirement)
- [x] Implement `decode(k, m, shards, original_size) -> Vec<u8>`
  - Fast-path: if all k data shards present, skip RS decode (just concatenate)
  - Full RS decode when missing data shards
- [x] Implement `suggest_config(node_count) -> (k, m)`:
  - 1→(1,0), 2→(1,1), 3→(2,1), 4→(2,2), 5→(3,2), 6-11→(4,2), 12+→(8,4)
  - Guarantee: k+m never exceeds node_count

**Tests** (21 tests):

- [x] Encode/decode: all shards, data-only, parity-only, mixed, all combinations for k=2/m=2
- [x] Padding (non-divisible chunk), 1MB chunk, deterministic IDs
- [x] Fewer than k shards → error, empty chunk → error
- [x] Adaptive config: all node counts 0-50, never exceeds nodes
- [x] `cargo test -p shoal-erasure` passes

---

## Milestone 5 — `shoal-placement` ✅

Consistent hashing ring for deterministic shard placement (~150 lines).

- [x] `Ring` struct with `BTreeMap<u64, NodeId>` vnodes, `HashMap<NodeId, NodeInfo>` metadata
- [x] vnode positions: `blake3(node_id ++ vnode_index)` truncated to u64
- [x] `Ring::owners(&ShardId, replication_factor) -> Vec<NodeId>`: clockwise walk, distinct physical nodes
- [x] `Ring::add_node`, `add_node_with_weight`, `remove_node`
- [x] `Ring::diff(old, new, shard_ids, replication_factor) -> Vec<Migration>`
- [x] Weighted nodes: `add_node_with_weight` for capacity-proportional vnode counts

**Tests** (12 tests):

- [x] Single node ownership, balanced 2-node distribution (~50/50)
- [x] Consistent hashing: ~1/N moves on add, only removed node's shards redistribute
- [x] Replication factor 3 → 3 distinct owners, replication > nodes → all nodes
- [x] Weighted 2x → ~2x shards, diff identifies migrations to new node
- [x] Deterministic placement, empty ring, re-add updates weight
- [x] `cargo test -p shoal-placement` passes

---

## Milestone 6 — `shoal-meta`

Metadata store wrapping Fjall for persistent state.

**Important**: Fjall is a **local cache and index**, never the source of truth for user data.
Everything in Fjall is reconstructible from the cluster. Manifests are themselves stored
as regular erasure-coded objects in the cluster. If Fjall is lost, the node can reconstruct
its index by scanning manifest shards from the cluster.

- [ ] Initialize Fjall `Database` with the following keyspaces:
  - `objects` — user key (bucket/key string) → ObjectId — **CACHE**, reconstructible from manifests in the cluster
  - `manifests` — ObjectId → serialized Manifest — **CACHE**, the manifest itself is stored as a regular erasure-coded object in the cluster
  - `shardmap` — ShardId → serialized list of NodeIds (current owners) — **CACHE**, derived from placement ring computation
  - `membership` — NodeId → serialized Member — **CACHE**, derived from foca/gossip
  - `repair_queue` — ShardId → priority (u64, lower = more urgent) — **LOCAL**, transient, rebuilt on restart by anti-entropy scan
- [ ] Implement typed accessors for each keyspace:

  ```rust
  pub struct MetaStore { db: fjall::Database, /* keyspaces */ }
  impl MetaStore {
      // Manifests (local cache)
      pub fn put_manifest(&self, manifest: &Manifest) -> Result<()>;
      pub fn get_manifest(&self, id: &ObjectId) -> Result<Option<Manifest>>;

      // Object key mapping (local cache)
      pub fn put_object_key(&self, bucket: &str, key: &str, id: &ObjectId) -> Result<()>;
      pub fn get_object_key(&self, bucket: &str, key: &str) -> Result<Option<ObjectId>>;
      pub fn list_objects(&self, bucket: &str, prefix: &str) -> Result<Vec<String>>;
      pub fn delete_object_key(&self, bucket: &str, key: &str) -> Result<()>;

      // Shard map (local cache)
      pub fn put_shard_owners(&self, id: &ShardId, owners: &[NodeId]) -> Result<()>;
      pub fn get_shard_owners(&self, id: &ShardId) -> Result<Option<Vec<NodeId>>>;

      // Membership (local cache)
      pub fn put_member(&self, member: &Member) -> Result<()>;
      pub fn get_member(&self, id: &NodeId) -> Result<Option<Member>>;
      pub fn list_members(&self) -> Result<Vec<Member>>;
      pub fn remove_member(&self, id: &NodeId) -> Result<()>;

      // Repair queue (local, transient)
      pub fn enqueue_repair(&self, id: &ShardId, priority: u64) -> Result<()>;
      pub fn dequeue_repair(&self) -> Result<Option<ShardId>>;
      pub fn repair_queue_len(&self) -> Result<usize>;
  }
  ```

- [ ] All serialization via postcard
- [ ] `MetaStore::open(path)` and `MetaStore::open_temporary()` (in-memory for tests)

**Tests**:

- [ ] Manifest put/get round-trip
- [ ] Object key put/get/list/delete
- [ ] List objects with prefix filtering
- [ ] Shard owners put/get
- [ ] Member CRUD
- [ ] Repair queue: enqueue, dequeue in priority order, len
- [ ] Persistence: write, drop store, reopen, read back
- [ ] `cargo test -p shoal-meta` passes

---

## Milestone 7 — Full Local Pipeline (Integration)

Connect store + cas + erasure + placement + meta into a working local write/read pipeline. No networking yet.

- [ ] Create an integration test in `tests/integration/local_pipeline.rs`:
  1. Create a `NodeConfig` with chunk_size=1024, k=2, m=1
  2. Create 3 `MemoryStore` instances (simulating 3 nodes)
  3. Create a `Ring` with 3 nodes
  4. Create a `MetaStore` (temporary)
  5. **Write path**: take a test object (e.g., 5000 bytes of random data)
     - Chunk it → 5 chunks (4 × 1024 + 1 × 904)
     - Erasure encode each chunk → 3 shards per chunk (k=2, m=1)
     - Use ring to determine owner for each shard
     - Store each shard in the corresponding MemoryStore
     - Build and persist Manifest in MetaStore
  6. **Read path**: given the object key
     - Look up Manifest
     - For each chunk, fetch k shards from their stores
     - Erasure decode
     - Concatenate → verify equals original data
  7. **Failure path**: delete 1 shard per chunk (simulating a node loss)
     - Read should still succeed (k shards remain)
  8. **Total failure**: delete 2 shards per chunk
     - Read should fail gracefully (fewer than k shards)

- [ ] All assertions pass
- [ ] `cargo test --test local_pipeline` passes

---

## Milestone 8 — `shoal-net`

Network protocol on top of iroh.

- [ ] Define protocol messages (postcard-serialized):

  ```rust
  pub enum ShoalMessage {
      // Shard transfer
      ShardPush { shard_id: ShardId, data: Vec<u8> },
      ShardRequest { shard_id: ShardId },
      ShardResponse { shard_id: ShardId, data: Option<Vec<u8>> },

      // Cluster events (broadcast via gossip)
      ClusterEvent(ClusterEvent),

      // Membership (piggybacked on foca)
      MembershipUpdate(Vec<Member>),

      // Health
      Ping { timestamp: u64 },
      Pong { timestamp: u64 },
  }
  ```

- [ ] Implement `ShoalTransport`:
  - Wraps an `iroh::Endpoint`
  - `send_message(node_id, message)` — opens a QUIC stream, sends postcard-encoded message
  - `recv_message(stream)` — reads and decodes
  - Handles connection pooling (reuse connections to same node)
- [ ] Implement shard transfer:
  - `push_shard(node_id, shard_id, data)` — sends shard to a node
  - `pull_shard(node_id, shard_id)` — requests a shard from a node
  - Receiver verifies blake3 hash matches shard_id before accepting
- [ ] Define ALPN protocol identifier: `b"shoal/0"`
- [ ] Implement request handler that dispatches incoming messages

**Tests**:

- [ ] Spin up 2 iroh endpoints in-process
- [ ] Send a shard from node A to node B, verify it arrives intact
- [ ] Pull a shard from node B, verify it matches
- [ ] Send corrupted shard (wrong data for the shard_id) → receiver rejects
- [ ] `cargo test -p shoal-net` passes

---

## Milestone 9 — `shoal-cluster`

Membership management combining foca (SWIM) and iroh-gossip.

- [ ] Implement foca integration:
  - Implement foca's `Identity` trait for `Member`
  - Implement foca's `Runtime` trait using tokio timers + iroh transport
  - Wrap foca in a `MembershipService` that:
    - Runs the SWIM protocol loop
    - Emits `ClusterEvent::NodeJoined`, `NodeLeft`, `NodeDead` when membership changes
    - Persists membership state to MetaStore
- [ ] Implement gossip integration:
  - Create a gossip topic for the cluster (derived from a shared cluster secret/key)
  - Broadcast `ClusterEvent`s via iroh-gossip
  - Receive and process events from other nodes
- [ ] Implement `ClusterState`:

  ```rust
  pub struct ClusterState {
      members: RwLock<HashMap<NodeId, Member>>,
      ring: RwLock<Ring>,
      local_node_id: NodeId,
      event_tx: broadcast::Sender<ClusterEvent>,
  }
  ```

  - When membership changes → recompute ring
  - Expose `subscribe()` for other components to react to cluster events

**Tests**:

- [ ] Spin up 3 nodes in-process, verify they discover each other via foca
- [ ] All 3 nodes have consistent membership view within 5 seconds
- [ ] Kill one node → other 2 detect it as Dead within configurable timeout
- [ ] New node joins → all existing nodes learn about it
- [ ] ClusterState ring is recomputed on membership change
- [ ] Events are received by subscribers
- [ ] `cargo test -p shoal-cluster` passes

---

## Milestone 10 — `shoal-repair`

Auto-repair and rebalancing.

- [ ] Implement `RepairDetector`:
  - Subscribes to `ClusterEvent`s
  - On `NodeDead`: compute which shards were owned by dead node → enqueue in repair queue
  - Periodically: scan local shards, verify they match ring placement, report anomalies
- [ ] Implement `RepairScheduler`:
  - Reads from repair queue (MetaStore)
  - Prioritizes: shards with fewer surviving copies get higher priority
  - Respects rate limits: `repair_max_bandwidth`, `repair_concurrent_transfers`
  - On RPi: 1 concurrent transfer, 1MB/s max
  - On datacenter: 64 concurrent transfers, 1GB/s max
- [ ] Implement `RepairExecutor`:
  - For each shard to repair:
    1. Find nodes that still have copies (from shard map)
    2. Fetch the shard (or reconstruct via erasure decoding from sibling shards of the same chunk)
    3. Push to new owner (determined by current ring)
    4. Update shard map in MetaStore
- [ ] Implement `Throttle`:
  - Token bucket rate limiter for repair bandwidth
  - Adaptive: reduce repair rate when node is under read/write load

**Tests**:

- [ ] 3 nodes, store an object, kill 1 node → repair detector enqueues shards
- [ ] Scheduler dequeues in priority order
- [ ] Executor successfully repairs a shard by fetching from surviving node
- [ ] Executor successfully repairs a shard by RS reconstruction when direct copy unavailable
- [ ] Rate limiting: repair doesn't exceed configured bandwidth
- [ ] After repair: object is fully readable again
- [ ] `cargo test -p shoal-repair` passes

---

## Milestone 11 — `shoal-engine`

The node orchestrator that ties everything together.

- [ ] Implement `ShoalNode`:
  ```rust
  pub struct ShoalNode {
      config: NodeConfig,
      node_id: NodeId,
      store: Arc<dyn ShardStore>,
      meta: Arc<MetaStore>,
      cluster: Arc<ClusterState>,
      transport: Arc<ShoalTransport>,
      // background task handles
  }
  ```
- [ ] `ShoalNode::start(config)`:
  1. Initialize iroh endpoint
  2. Open MetaStore
  3. Initialize ShardStore (Memory or File based on config)
  4. Start foca membership service
  5. Start gossip
  6. Start repair detector + scheduler + executor as background tasks
  7. Start incoming message handler
- [ ] `ShoalNode::put_object(bucket, key, data, metadata)`:
  - Full write path: chunk → erasure → distribute shards → build manifest → store manifest as erasure-coded object → update local Fjall index
- [ ] `ShoalNode::get_object(bucket, key)`:
  - Full read path: lookup ObjectId in Fjall → fetch manifest shards → decode manifest → fetch data shards → decode → return
- [ ] `ShoalNode::delete_object(bucket, key)`:
  - Remove manifest and key mapping
  - Enqueue shard cleanup (background)
- [ ] Graceful shutdown: persist state, notify cluster of departure

**Tests**:

- [ ] Start a single node, put object, get object → matches
- [ ] Start 3 nodes, put object, get from any node → matches
- [ ] Start 3 nodes, put object, stop 1 node, get from surviving node → works (after repair)
- [ ] Small objects: put/get a tiny object (e.g. 10 bytes) → same pipeline, still works
- [ ] Delete object: key no longer resolves
- [ ] Graceful shutdown and restart: data persists
- [ ] `cargo test -p shoal-engine` passes

---

## Milestone 12 — `shoal-s3`

S3-compatible HTTP API.

- [ ] Implement axum router with S3 endpoints:
  - `PUT /{bucket}/{key}` → PutObject
  - `GET /{bucket}/{key}` → GetObject
  - `DELETE /{bucket}/{key}` → DeleteObject
  - `HEAD /{bucket}/{key}` → HeadObject (metadata only)
  - `GET /{bucket}?list-type=2&prefix=...` → ListObjectsV2
  - `PUT /{bucket}` → CreateBucket (just a namespace in the key mapping)
- [ ] Implement AWS SigV4 authentication (or start with a simple shared-secret auth)
- [ ] Proper S3 XML responses (ListObjectsV2 result, error responses)
- [ ] Multipart upload support:
  - `POST /{bucket}/{key}?uploads` → InitiateMultipartUpload
  - `PUT /{bucket}/{key}?partNumber=N&uploadId=X` → UploadPart
  - `POST /{bucket}/{key}?uploadId=X` → CompleteMultipartUpload
- [ ] Content-Type and user metadata pass-through
- [ ] ETags (blake3 hex of object)

**Tests**:

- [ ] PutObject + GetObject via HTTP client (reqwest)
- [ ] HeadObject returns correct content-length and metadata
- [ ] ListObjectsV2 with prefix filtering
- [ ] DeleteObject then GetObject → 404
- [ ] Multipart upload: 3-part upload → complete → get → matches
- [ ] Test with `aws-cli` or `s3cmd` against the running server
- [ ] `cargo test -p shoal-s3` passes

---

## Milestone 13 — `shoal-cli`

The binary that brings it all together.

- [ ] TOML config file:

  ```toml
  [node]
  data_dir = "/var/lib/shoal"
  listen_addr = "0.0.0.0:4820"
  s3_listen_addr = "0.0.0.0:4821"

  [cluster]
  secret = "my-cluster-secret"
  seeds = ["node-id-hex@192.168.1.10:4820"]

  [storage]
  backend = "file"  # or "memory"
  chunk_size = 262144

  [erasure]
  k = 4
  m = 2

  [repair]
  max_bandwidth = "100MB/s"
  concurrent_transfers = 8

  [s3]
  access_key = "shoal"
  secret_key = "shoalsecret"
  ```

- [ ] CLI commands:
  - `shoal start` — start the node
  - `shoal status` — show cluster status (members, shard distribution)
  - `shoal repair status` — show repair queue
  - `shoal benchmark` — run a quick read/write benchmark
- [ ] Auto-detection: if no config specified, detect available RAM and disk, set sensible defaults
- [ ] Tracing subscriber setup with configurable log level

**Tests**:

- [ ] Config parsing from TOML
- [ ] Start node, verify it binds to ports
- [ ] `cargo test -p shoal-cli` passes

---

## Milestone 14 — End-to-End Integration Tests

Full cluster tests with real networking.

- [ ] `tests/integration/cluster_formation.rs`:
  - Start 5 nodes in-process with different ports
  - Verify all 5 discover each other within 10 seconds
  - Verify consistent ring across all nodes

- [ ] `tests/integration/write_read.rs`:
  - 5-node cluster
  - Write 100 objects of varying sizes (1KB to 10MB)
  - Read each object from a random node → verify matches
  - Write and immediately read → consistency check

- [ ] `tests/integration/node_failure.rs`:
  - 5-node cluster, write 50 objects
  - Kill 1 node
  - Wait for repair to complete
  - Read all 50 objects → all succeed
  - Kill another node
  - Read all 50 objects → all still succeed (k=4, m=2 tolerates 2 failures)

- [ ] `tests/integration/rebalancing.rs`:
  - 3-node cluster, write 50 objects
  - Add 2 more nodes
  - Wait for rebalancing
  - Verify shard distribution is roughly even across 5 nodes
  - Read all objects → succeed

- [ ] `tests/integration/stress.rs`:
  - 5-node cluster
  - Concurrent writers (10 threads) writing 1000 objects total
  - Concurrent readers (10 threads) reading random objects
  - Verify no data corruption

- [ ] All integration tests pass: `cargo test --test '*'`

---

## Milestone 15 — Chaos Tests

- [ ] `tests/chaos/random_kill.rs`:
  - 7-node cluster
  - Background: continuously write and read objects
  - Every 5 seconds: kill a random node, wait 2 seconds, restart it
  - After 60 seconds: verify all objects are readable
  - No data loss allowed

- [ ] `tests/chaos/network_partition.rs`:
  - 6-node cluster
  - Simulate network partition (3 vs 3)
  - Both sides should remain operational for reads of local data
  - Heal partition
  - Verify cluster reconverges and all data is consistent

---

## Notes for Implementation

### Error Handling

- Use `thiserror` for library error types in each crate
- Use `anyhow` only in `shoal-cli` (the binary)
- Every error type should be descriptive and include context

### Logging

- Use `tracing` throughout, with structured fields
- Important events: shard stored, shard transferred, node joined, node died, repair started, repair completed
- Debug level: individual message sends/receives
- Trace level: ring computation details

### Performance Considerations

- On writes: pipeline the erasure encoding — start encoding chunk N+1 while chunk N's shards are being distributed
- On reads: fetch shards in parallel across nodes
- Repair: always respect the rate limiter, never starve client traffic
- All objects, regardless of size, go through the same pipeline (no inline shortcut)

### Small Object Packing

Small objects (smaller than `chunk_size`) are packed together into shared chunks to avoid wasting space and erasure coding overhead on tiny payloads.

**Design**: `ChunkMeta` already carries `offset` and `size` fields. For large objects, each chunk maps 1:1 to a data range. For small objects, multiple objects can share the same underlying chunk — each object's `ChunkMeta` references a `(chunk_id, offset, size)` slice within the shared chunk.

**Write path** (implemented in `shoal-engine`, Milestone 11):
- The engine maintains an `OpenChunk` buffer per node (or per bucket) that accumulates small objects.
- When an object arrives that is smaller than `chunk_size`, it is appended to the current `OpenChunk` buffer at the next available offset.
- When the buffer is full (or a flush interval elapses), the entire chunk is erasure-coded and distributed like any regular chunk.
- Each small object gets its own `Manifest` whose single `ChunkMeta` points to the shared chunk at its `(offset, size)`.

**Read path**: Fetch the shared chunk (or reconstruct it from shards), then extract the byte range `[offset..offset+size]`.

**Deletion and compaction**: Deleting a small object only removes its manifest and key mapping. The underlying shared chunk remains until a background compaction pass detects that all (or most) objects referencing it are deleted, at which point the chunk can be garbage-collected. This is a late-stage optimization (post-Milestone 11).

**No code changes needed now** — the existing `ChunkMeta` struct already supports this via its `offset` and `size` fields. The packing logic will be implemented in `shoal-engine` (Milestone 11).

### Things NOT to do yet

- No encryption at rest (later)
- No bucket-level ACLs (later)
- No versioning (later)
- No lifecycle policies (later)
- No `io_uring` / `O_DIRECT` backend (later, behind feature flag)
- No `ZoneReplicator` implementation yet (Milestone 11) — cross-zone replication is designed but not yet coded
