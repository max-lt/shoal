# Shoal — Architecture Reference

This document describes the internal architecture of the Shoal distributed object storage engine in detail. For a high-level overview see the [README](README.md). For the implementation roadmap see [plan.md](plan.md).

---

## Table of Contents

1. [Design Principles](#1-design-principles)
2. [Workspace Layout](#2-workspace-layout)
3. [Crate Dependency Graph](#3-crate-dependency-graph)
4. [Crate Descriptions](#4-crate-descriptions)
5. [Core Data Types](#5-core-data-types)
6. [Write Path](#6-write-path)
7. [Read Path](#7-read-path)
8. [Cluster Membership & Gossip](#8-cluster-membership--gossip)
9. [Repair System](#9-repair-system)
10. [Network Protocol](#10-network-protocol)
11. [Metadata Store](#11-metadata-store)
12. [Configuration](#12-configuration)
13. [Testing Strategy](#13-testing-strategy)

---

## 1. Design Principles

| Principle | Description |
|-----------|-------------|
| **Pure Rust** | Zero C/C++ bindings, zero `cc` build scripts. Cross-compilation is trivial. |
| **Content-addressed** | All data — shards, chunks, objects — is identified by its BLAKE3 hash. Integrity is verifiable at every layer. |
| **Deterministic placement** | Every node independently computes where any shard belongs using a consistent hash ring. No central coordinator required. |
| **Self-healing** | Nodes join and leave. The cluster detects failures via the SWIM protocol, identifies under-replicated shards, and repairs automatically. |
| **Adaptive** | The same binary runs on a Raspberry Pi 4 and a datacenter server. `NodeConfig` adapts chunk size, concurrency, and bandwidth limits to available hardware. |
| **Metadata is a cache** | The metadata store (Fjall) is a local cache/index, not the source of truth. Everything in it is reconstructible from the cluster state and object manifests. |

---

## 2. Workspace Layout

```
shoal/
├── Cargo.toml                  # Workspace manifest with shared dependency versions
├── crates/
│   ├── shoal-types/            # Shared types, IDs, config — no business logic
│   ├── shoal-store/            # ShardStore trait + MemoryStore, FileStore backends
│   ├── shoal-cas/              # Chunking and content-addressing (manifests)
│   ├── shoal-meta/             # Metadata persistence (Fjall LSM-tree)
│   ├── shoal-erasure/          # Reed-Solomon erasure coding (reed-solomon-simd)
│   ├── shoal-placement/        # Consistent hashing ring + topology-aware placement
│   ├── shoal-cluster/          # Cluster membership (foca SWIM) + gossip (iroh-gossip)
│   ├── shoal-net/              # Network protocol over iroh QUIC
│   ├── shoal-engine/           # Node orchestrator — ties all components together
│   ├── shoal-repair/           # Auto-repair, scrubbing, throttling, circuit breaker
│   ├── shoal-s3/               # S3-compatible HTTP API (axum)
│   └── shoal-cli/              # Binary entry point
└── tests/
    └── integration/            # Full pipeline integration tests
```

---

## 3. Crate Dependency Graph

```
shoal-types  (foundation — no internal deps)
    │
    ├──► shoal-store       (ShardStore trait, MemoryStore, FileStore)
    │
    ├──► shoal-cas         (Chunker, manifest builder)
    │
    ├──► shoal-meta        (MetaStore on top of Fjall)
    │
    ├──► shoal-erasure     (ErasureEncoder, decode())
    │
    ├──► shoal-placement   (Ring, topology-aware owners)
    │
    ├──► shoal-net         (ShoalTransport, ShoalMessage)
    │
    ├──► shoal-cluster ────► shoal-meta, shoal-placement, shoal-net
    │
    ├──► shoal-repair  ────► shoal-store, shoal-meta, shoal-erasure,
    │                        shoal-placement, shoal-net, shoal-cluster
    │
    └──► shoal-engine  ────► shoal-store, shoal-cas, shoal-meta,
                             shoal-erasure, shoal-placement,
                             shoal-cluster, shoal-repair, shoal-net
                                 │
                             shoal-s3  ────► shoal-engine
                                 │
                             shoal-cli ────► shoal-engine, shoal-s3
```

`shoal-types` is the only crate that every other crate depends on. `shoal-engine` is the integration point for all subsystems.

---

## 4. Crate Descriptions

### `shoal-types` — Shared types and identifiers

Contains all types shared across the workspace. Has no internal dependencies.

**Key types:**

| Type | Description |
|------|-------------|
| `ShardId` | `[u8; 32]` newtype, `blake3(shard_data)` |
| `ChunkId` | `[u8; 32]` newtype, `blake3(chunk_data)` |
| `ObjectId` | `[u8; 32]` newtype, `blake3(serialized_manifest)` |
| `NodeId` | `[u8; 32]` newtype, derived from the node's iroh endpoint key |
| `Manifest` | Full description of a stored object: version, chunks, shards, metadata |
| `ChunkMeta` | Per-chunk metadata: `ChunkId`, offset, size, shard list |
| `ShardMeta` | Per-shard metadata: `ShardId`, RS index (0..k+m), size |
| `Member` | A cluster node: `NodeId`, capacity, `MemberState`, generation, topology |
| `ClusterEvent` | Gossip events: `NodeJoined`, `NodeLeft`, `NodeDead`, `ShardStored`, `RepairNeeded` |
| `NodeConfig` | Runtime configuration: chunk size, erasure params, repair limits |
| `ErasureConfig` | `k` (data shards), `m` (parity shards), `chunk_size` |
| `NodeTopology` | Physical location: `region`, `datacenter`, `machine` |
| `RepairCircuitBreaker` | Thresholds for suspending repair during cascading failures |

All IDs are produced with BLAKE3 and serialized using `postcard` + `serde`. The `MANIFEST_VERSION` constant guards against silent format evolution.

---

### `shoal-store` — Shard persistence layer

Defines the `ShardStore` trait and provides two implementations.

```rust
#[async_trait]
pub trait ShardStore: Send + Sync {
    async fn put(&self, id: ShardId, data: &[u8]) -> Result<(), StoreError>;
    async fn get(&self, id: ShardId) -> Result<Option<Vec<u8>>, StoreError>;
    async fn delete(&self, id: ShardId) -> Result<(), StoreError>;
    async fn list(&self) -> Result<Vec<ShardId>, StoreError>;
    async fn capacity(&self) -> Result<StorageCapacity, StoreError>;
}
```

| Implementation | Description |
|----------------|-------------|
| `MemoryStore` | `DashMap<ShardId, Bytes>` — for tests and ephemeral nodes |
| `FileStore` | One file per shard under a configurable root directory; uses `libc` for disk space queries |

`StoreError` uses `thiserror` and includes `NotFound(ShardId)` and `Io(std::io::Error)`.

---

### `shoal-cas` — Content addressing and chunking

Splits raw object bytes into fixed-size chunks and builds the `Manifest`.

**`Chunker`** — splits data into `chunk_size`-byte pieces. Each `Chunk` carries a `ChunkId` (BLAKE3 of the chunk data), its byte offset, and its size.

**`build_manifest()`** — given chunks and their shards (after erasure encoding), assembles a `Manifest`, computes the `ObjectId` as `blake3(postcard::to_allocvec(&manifest_content))`, and returns the complete manifest.

**Manifest serialization** — `postcard` binary format with versioning. The `version` field in `Manifest` must be checked on deserialization to avoid silent data corruption.

---

### `shoal-meta` — Metadata persistence

A thin wrapper around Fjall (LSM-tree embedded KV store). Uses five separate keyspaces:

| Keyspace | Key | Value | Description |
|----------|-----|-------|-------------|
| `objects` | `bucket/key` | `ObjectId` | Maps a user-facing bucket + key to an `ObjectId` |
| `manifests` | `ObjectId` | `postcard(Manifest)` | Full object manifests |
| `shardmap` | `ShardId` | `postcard(Vec<NodeId>)` | Which nodes own each shard |
| `membership` | `NodeId` | `postcard(Member)` | Current cluster member state |
| `repair_queue` | `ShardId` | priority bytes | Transient repair queue; rebuilt on restart |

`MetaStore` is never the source of truth — every piece of data in it is reconstructible from the cluster. It is a cache and an index.

---

### `shoal-erasure` — Reed-Solomon erasure coding

Wraps `reed-solomon-simd` (runtime SIMD: AVX2, SSSE3, Neon).

**`ErasureEncoder`** — encodes a chunk of data into `k + m` shards:
1. Pads the chunk to an even multiple of `k * shard_size`.
2. Splits into `k` equally-sized data slices.
3. Generates `m` parity shards via Reed-Solomon.
4. Each shard is content-addressed: `ShardId = blake3(shard_data)`.

**`decode()`** — reconstructs original data from any `k` out of `k + m` shards. Returns `ErasureError::NotEnoughShards` if fewer than `k` shards are available.

**`suggest_config(node_count)`** — returns a sensible `ErasureConfig` for a given cluster size (e.g., k=2 m=1 for 3 nodes; k=4 m=2 for ≥6 nodes).

Default configuration: `k=4, m=2, chunk_size=256 KiB` — tolerates loss of any 2 shards per chunk.

---

### `shoal-placement` — Consistent hashing ring

A hand-written consistent hash ring (~150 lines) with virtual nodes (vnodes) and topology-aware placement.

**`Ring`**:
- Each physical node is represented by `vnodes_per_node` virtual tokens in the ring (default: 128).
- Token weight is proportional to node capacity, giving heavier nodes more tokens.
- `owners(shard_id, n)` returns the `n` nodes that should store a given shard, using the ring to walk clockwise from the shard's hash position.
- **Topology-aware placement**: uses progressive relaxation to maximize failure-domain separation. It first tries to place shards on nodes in different `machine` slots, then different `datacenter`s, then different `region`s.

**`Migration`** — describes shard movement when nodes join or leave (source node → target node).

`Ring` is deterministic: any node in the cluster, given the same member list, will compute the same placement.

---

### `shoal-cluster` — Membership and gossip

Combines two protocols for two distinct concerns:

| Concern | Protocol | Crate |
|---------|----------|-------|
| Failure detection | SWIM + Suspicion | `foca` |
| Event dissemination | HyParView + PlumTree epidemic broadcast | `iroh-gossip` |

**`ClusterState`** — owns the `Ring` and the current `HashMap<NodeId, Member>`. Provides:
- `add_node(member)` / `remove_node(node_id)` — updates both the ring and the member map.
- `get_members()` — returns all live members.

**`ClusterIdentity`** — wraps the node's `NodeId` and iroh `Endpoint` used as foca transport.

**`GossipService`** — runs the iroh-gossip loop, receives `ClusterEvent` messages from other nodes, and applies them to `ClusterState`.

**`MemberState`** lifecycle: `Alive → Suspect → Dead`. The `generation` counter on each `Member` prevents split-brain scenarios where a restarted node is confused with its previous incarnation.

---

### `shoal-net` — Network protocol

All inter-node communication goes over iroh QUIC connections (ALPN: `shoal/0`).

**`ShoalMessage`** — the wire message enum:

| Variant | Description |
|---------|-------------|
| `ShardPush { id, data }` | Push a shard to a remote node (repair, write forwarding) |
| `ShardRequest { id }` | Request a shard from a remote node |
| `ShardResponse { id, data }` | Reply to a `ShardRequest` |
| `ClusterEvent(ClusterEvent)` | Gossip event wrapped for direct delivery |
| `Ping` / `Pong` | Latency probing |

**`ShoalTransport`** — manages iroh `Endpoint`, accepts incoming connections, dispatches messages to registered handlers. Each message is `postcard`-serialized and includes an end-to-end BLAKE3 integrity check.

---

### `shoal-engine` — Node orchestrator

`ShoalNode` is the central object that owns all subsystems and exposes the public API:

```rust
impl ShoalNode {
    pub async fn put_object(...) -> Result<ObjectId, EngineError>;
    pub async fn get_object(...) -> Result<(Vec<u8>, Manifest), EngineError>;
    pub async fn delete_object(...) -> Result<(), EngineError>;
}
```

It is constructed from:
- A `ShoalNodeConfig` (chunk size, erasure params, virtual node count)
- A `Arc<dyn ShardStore>` (local shard storage)
- A `Arc<MetaStore>` (metadata)
- A `Arc<ClusterState>` (placement ring + membership)

---

### `shoal-repair` — Auto-repair and scrubbing

| Component | Description |
|-----------|-------------|
| `RepairDetector` | Listens for `ClusterEvent::NodeDead` and `RepairNeeded` events; identifies under-replicated shards and enqueues them |
| `RepairScheduler` | Pulls from the repair queue, respects the rate limiter (`Throttle`) and circuit breaker |
| `RepairExecutor` | Performs the actual repair using one of two strategies (see below) |
| `CircuitBreaker` | Suspends all repair/rebalance if > 50% of nodes are down |
| `Throttle` | Token-bucket rate limiter for repair bandwidth |
| `DeepScrubber` | Verifies shard integrity by majority-vote across replica holders |

**Repair strategies:**
1. **Direct copy** — fetch the exact shard from a surviving owner and store on the new owner.
2. **RS reconstruction** — fetch any `k` sibling shards, call `shoal-erasure::decode()` to reconstruct the chunk, re-encode, and place the missing shard on the new owner.

Strategy 2 is used when fewer than 1 surviving copy of the specific shard exists, but `k` other shards for the same chunk are reachable.

---

### `shoal-s3` — S3-compatible HTTP API

Built on `axum`. Exposes a subset of the S3 API for object storage operations (PUT, GET, DELETE, HEAD). Routes are mapped to `ShoalNode` calls.

---

### `shoal-cli` — Binary entry point

The `main.rs` wires together `ShoalNode` and the `shoal-s3` HTTP server. Uses `anyhow` for top-level error handling (allowed only in binary crates; library crates use `thiserror`).

---

## 5. Core Data Types

### Identifiers

All four ID types are `[u8; 32]` newtypes generated with BLAKE3:

```
ShardId  = blake3(shard_data)
ChunkId  = blake3(chunk_data)
ObjectId = blake3(postcard(manifest_fields_without_object_id))
NodeId   = blake3(iroh_endpoint_public_key)
```

They implement `Copy`, `Hash`, `Ord`, `Serialize`, `Deserialize`, `Display` (hex), and `Debug`.

### Manifest hierarchy

```
Manifest
├── version: u8          (format guard)
├── object_id: ObjectId
├── total_size: u64
├── chunk_size: u32
├── created_at: u64
├── metadata: BTreeMap<String, String>
└── chunks: Vec<ChunkMeta>
    ├── chunk_id: ChunkId
    ├── offset: u64
    ├── size: u32
    └── shards: Vec<ShardMeta>
        ├── shard_id: ShardId
        ├── index: u8       (0..k+m)
        └── size: u32
```

---

## 6. Write Path

```
Client PUT /bucket/key  (bytes)
        │
        ▼
┌──────────────────┐
│  Chunker         │  chunk_size slices, ChunkId = blake3(slice)
└──────┬───────────┘
       │ Vec<Chunk>
       ▼
┌──────────────────┐
│  ErasureEncoder  │  k data + m parity shards per chunk
└──────┬───────────┘
       │ Vec<(Chunk, Vec<EncodedShard>)>
       ▼
┌──────────────────┐
│  Ring::owners()  │  deterministic placement: replication_factor nodes per shard
└──────┬───────────┘
       │ shard → [NodeId]
       ▼
┌──────────────────┐
│  ShardStore::put │  local shards stored immediately
│  ShoalTransport  │  remote shards forwarded over QUIC (shoal-net)
└──────┬───────────┘
       │
       ▼
┌──────────────────┐
│  MetaStore       │  put_manifest(), put_shard_owners(), put_object_key()
└──────┬───────────┘
       │ ObjectId
       ▼
    200 OK
```

**Steps in detail:**

1. **Chunk** — `Chunker` splits the object into `ceil(size / chunk_size)` chunks. Default chunk size is 256 KiB. Each chunk gets a `ChunkId = blake3(chunk_data)`.

2. **Encode** — `ErasureEncoder::encode(chunk_data)` produces `k + m` shards. Each shard gets `ShardId = blake3(shard_data)`. The last chunk is zero-padded to `k * shard_size` before encoding.

3. **Place** — `Ring::owners(shard_id, replication_factor)` returns the list of `NodeId`s that should store the shard. `replication_factor = k + m`.

4. **Store** — Shards owned by the local node go into `ShardStore::put`. Shards owned by remote nodes are forwarded via `ShoalTransport::send(ShardPush {...})`.

5. **Persist metadata** — The manifest is saved, the shard-owner map is updated, and the `bucket/key → ObjectId` mapping is stored.

6. **ACK** — `ObjectId` is returned. The write ACK policy (local zone vs. quorum vs. all zones) is controlled by `ZoneWriteAck` in `NodeConfig`.

---

## 7. Read Path

```
Client GET /bucket/key
        │
        ▼
┌──────────────────┐
│  MetaStore       │  bucket/key → ObjectId → Manifest
└──────┬───────────┘
       │ Manifest (chunks list)
       ▼
┌──────────────────┐  for each chunk:
│  Ring::owners()  │    compute shard owners
│  ShardStore::get │    fetch ≥k shards locally
│  ShoalTransport  │    fetch missing shards via ShardRequest/Response
└──────┬───────────┘
       │ k shards per chunk
       ▼
┌──────────────────┐
│  decode()        │  reconstruct chunk data from k-of-(k+m) shards
└──────┬───────────┘
       │ chunk_data
       ▼
┌──────────────────┐
│  Concatenate     │  rejoin all chunks in offset order
└──────┬───────────┘
       │ object bytes
       ▼
    200 OK  (data, manifest)
```

**Notes:**
- Only `k` out of `k + m` shards are needed per chunk. Any `m` shards can be missing without data loss.
- If fewer than `k` shards are reachable, `EngineError::ReadFailed` is returned.
- Shard integrity is verified on read via BLAKE3 (`ShardId == blake3(fetched_data)`).

---

## 8. Cluster Membership & Gossip

Two separate protocols run in parallel:

### SWIM (failure detection) via `foca`

- Each node periodically probes a random peer with a `Ping`.
- If no `Pong` arrives within a deadline, the node is marked `Suspect`.
- Other nodes are asked to indirectly probe the suspect (`PingReq`).
- If indirect probes also fail, the suspect is declared `Dead` and a `NodeDead` event is gossiped.
- The `generation` counter on `Member` prevents a restarted node from being confused with its dead predecessor.

### Epidemic broadcast via `iroh-gossip`

- Uses HyParView (overlay management) + PlumTree (efficient broadcast).
- `ClusterEvent` variants propagated via gossip:
  - `NodeJoined(Member)` — new node arrives
  - `NodeLeft(NodeId)` — graceful departure
  - `NodeDead(NodeId)` — failure detected by SWIM
  - `ShardStored(ShardId, NodeId)` — new shard placement
  - `RepairNeeded(ShardId)` — shard is under-replicated

On receiving `NodeJoined` or `NodeDead`, `ClusterState` updates the consistent hash ring, triggering rebalance planning.

---

## 9. Repair System

```
NodeDead event
      │
      ▼
RepairDetector
  - identifies which shards were owned by the dead node
  - looks up each shard's chunk (via MetaStore or manifest scan)
  - enqueues under-replicated shards into repair_queue

      │
      ▼
RepairScheduler
  - token-bucket Throttle (100 MB/s default)
  - CircuitBreaker: suspend if > 50% of nodes are down
  - picks next shard from queue
  - dispatches to RepairExecutor

      │
      ▼
RepairExecutor
  - Strategy 1: Direct copy
      fetch surviving shard → place on new owner
  - Strategy 2: RS reconstruction
      fetch k sibling shards → decode chunk → re-encode → place missing shard

      │
      ▼
ShardStored gossip event broadcast
```

**Circuit breaker** (from `RepairCircuitBreaker` in `NodeConfig`):
- `max_down_fraction = 0.5` — suspend repair if > 50% of nodes are unreachable. Prevents repair storms during partial network partitions.
- `queue_pressure_threshold = 10_000` — aggressively throttle when queue grows large.
- `rebalance_cooldown_secs = 60` — wait before triggering rebalance after a node rejoins. Prevents flapping nodes from causing repeated storms.

**Deep scrubbing** (`DeepScrubber`): periodically fetches shards from all owners, verifies integrity by BLAKE3, and uses majority vote to detect silent corruption.

---

## 10. Network Protocol

- **Transport**: iroh QUIC (`iroh` 0.35), ALPN: `shoal/0`
- **Serialization**: `postcard` (compact binary, `no_std`-compatible)
- **Integrity**: every message payload includes a BLAKE3 hash verified on receipt

### Message types (`ShoalMessage`)

| Message | Direction | Purpose |
|---------|-----------|---------|
| `ShardPush { id, data }` | node → node | Push a shard (write forwarding or repair) |
| `ShardRequest { id }` | node → node | Request a shard |
| `ShardResponse { id, data }` | node → node | Reply to a shard request |
| `ClusterEvent(ClusterEvent)` | node → node | Direct delivery of a gossip event |
| `Ping` / `Pong` | node → node | Round-trip latency probing |

---

## 11. Metadata Store

`MetaStore` wraps Fjall (LSM-tree) with five keyspaces. All values are `postcard`-serialized.

| Keyspace | Key format | Value | Notes |
|----------|-----------|-------|-------|
| `objects` | `"{bucket}\0{key}"` | `ObjectId` | Reconstructible by scanning manifests |
| `manifests` | `ObjectId bytes` | `Manifest` | Source of truth for object structure |
| `shardmap` | `ShardId bytes` | `Vec<NodeId>` | Derived from placement ring; rebuilt on ring change |
| `membership` | `NodeId bytes` | `Member` | Authoritative copy of cluster membership |
| `repair_queue` | `ShardId bytes` | priority | Transient; rebuilt on restart from manifests |

The `objects` keyspace is the only one that cannot be fully reconstructed without client-side knowledge (the original bucket/key path). Everything else — manifests, shard locations, membership — is either gossip-maintained or derivable from the ring.

---

## 12. Configuration

All tunables live in `NodeConfig` (in `shoal-types`).

| Field | Default | Description |
|-------|---------|-------------|
| `storage_backend` | `File` | `Memory` or `File` (or future `Direct` for io_uring) |
| `chunk_size` | 256 KiB | Chunking granularity (128 KiB on RPi, up to 4 MiB on servers) |
| `erasure_k` | 4 | Data shards per chunk |
| `erasure_m` | 2 | Parity shards per chunk; tolerates loss of any `m` shards |
| `repair_max_bandwidth` | 100 MB/s | Token-bucket ceiling for repair traffic |
| `repair_concurrent_transfers` | 8 | Max simultaneous repair shard transfers |
| `gossip_interval_ms` | 1 000 | How often gossip rounds run |
| `replication_boundary` | `Region` | `Datacenter` or `Region` — where EC stops and full replication begins |
| `zone_write_ack` | `Local` | `Local`, `Quorum(n)`, or `All` |
| `repair_circuit_breaker` | see below | Thresholds for suspending repair |

`RepairCircuitBreaker` defaults: `max_down_fraction=0.5`, `queue_pressure_threshold=10_000`, `rebalance_cooldown_secs=60`.

---

## 13. Testing Strategy

| Scope | Location | Framework |
|-------|----------|-----------|
| Unit tests | Same file, `#[cfg(test)] mod tests` | `cargo test` |
| Async unit tests | Same file | `#[tokio::test]` |
| Integration tests | `tests/integration/` | `cargo test --test <name>` |
| Filesystem tests | Uses `tempfile::TempDir` | `tempfile` crate |

The main integration test (`tests/integration/local_pipeline.rs`) exercises the full write/read pipeline with a 3-node in-memory cluster:
- 5,000-byte object → 5 chunks of 1 KiB each
- k=2, m=1 (3 shards per chunk, 1 parity)
- Verifies chunk count, shard distribution across nodes, and exact data reconstruction

Each crate's unit tests cover its own logic in isolation. `shoal-engine` tests cover the full local pipeline. `shoal-erasure` tests include decode-with-missing-shards scenarios.

---

*This document reflects milestones 0–7 (complete). Milestones 8–15 (shoal-net, shoal-cluster, shoal-repair, shoal-engine full mode, shoal-s3, shoal-cli) will extend this document as they are implemented.*
