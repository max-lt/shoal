# Shoal — Distributed Object Storage Engine

## How to Use This Document

This is the **architecture and implementation plan** for Shoal. All 16 milestones (0-15) are complete.
For future work, follow the same workflow: implement, test, clippy, fmt, stop.

---

## What is Shoal?

Shoal is a distributed, self-healing object storage engine written in **100% pure Rust**. A lightweight, peer-to-peer alternative to S3-compatible storage that runs anywhere — from a Raspberry Pi to a datacenter server.

Nodes discover each other automatically, form a cluster, distribute data using erasure coding, and self-repair when nodes join or leave. S3-compatible HTTP API on top.

## Core Design Principles

1. **Pure Rust** — Zero C/C++ bindings. Zero `cc` build scripts.
2. **Adaptive** — Same binary on RPi and datacenter. Config adapts to resources.
3. **Self-healing** — Cluster detects failures, rebalances, and repairs automatically.
4. **Content-addressed** — All data identified by BLAKE3 hash. Integrity at every layer.
5. **Deterministic placement** — Every node independently computes shard placement. No coordinator.

## Architecture Overview

```
┌─────────────────────────────┐
│   S3 HTTP API (axum)        │  shoal-s3
├─────────────────────────────┤
│   Daemon (shoald)           │  shoald
├─────────────────────────────┤
│   Engine (orchestrator)     │  shoal-engine
├──────────┬──────────────────┤
│ Repair   │  Cluster         │  shoal-repair / shoal-cluster
│ Scheduler│  Membership      │
├──────────┼──────────────────┤
│ Erasure  │  Placement       │  shoal-erasure / shoal-placement
│ Coding   │  (hash ring)     │
├──────────┴──────────────────┤
│   LogTree (mutation DAG)    │  shoal-logtree
├─────────────────────────────┤
│   Metadata Store (Fjall)    │  shoal-meta
├─────────────────────────────┤
│   Content Addressing (CDC)  │  shoal-cas
├─────────────────────────────┤
│   Shard Store (trait)       │  shoal-store
├─────────────────────────────┤
│   Network (iroh + gossip)   │  shoal-net
└─────────────────────────────┘
```

## Technology Stack

All dependencies are pure Rust:

| Layer            | Crate                  | Purpose                                              |
| ---------------- | ---------------------- | ---------------------------------------------------- |
| Hashing          | `blake3`               | Content addressing, integrity verification           |
| Metadata         | `fjall` v3             | LSM-tree embedded KV store                           |
| Erasure coding   | `reed-solomon-simd` v3 | Reed-Solomon with runtime SIMD detection             |
| Networking       | `iroh` 0.96            | QUIC connections, hole punching, relay fallback      |
| Gossip/broadcast | `iroh-gossip` 0.96     | HyParView + PlumTree epidemic broadcast              |
| Membership       | `foca`                 | SWIM protocol for failure detection                  |
| CDC              | `fastcdc` v3           | Content-defined chunking for deduplication           |
| Compression      | `zstd` 0.13            | Per-chunk zstd compression (level 3)                 |
| Signing          | `ed25519-dalek` v2     | Cryptographic signatures for LogTree entries         |
| Serialization    | `postcard` + `serde`   | Compact binary serialization                         |
| HTTP             | `axum` + `hyper`       | S3-compatible API                                    |
| Async            | `tokio`                | Runtime                                              |
| Observability    | `tracing` + `metrics`  | Logging and metrics                                  |
| Benchmarking     | `criterion` 0.5        | Performance benchmarks                               |

### Why these choices?

- **Fjall over redb**: LSM-tree is write-optimized — critical during rebalancing. Fjall is a local cache/index, not source of truth.
- **foca over custom SWIM**: Production-grade SWIM+Inf.+Susp., `no_std` compatible, pluggable transport over iroh.
- **iroh-gossip over foca for broadcast**: foca = membership/failure detection; iroh-gossip = event dissemination.
- **Custom shard transfer over iroh-blobs**: Simple protocol on iroh QUIC streams.
- **Hand-written consistent hash ring**: ~150 lines. No crate handles ring diff computation for rebalancing.

## Key Data Types

```rust
// === Identifiers (all 32 bytes, all BLAKE3) ===
pub struct ShardId([u8; 32]);      // blake3(shard_data)
pub struct ChunkId([u8; 32]);      // blake3(chunk_data)
pub struct ObjectId([u8; 32]);     // blake3(manifest_serialized)
pub struct NodeId([u8; 32]);       // derived from iroh endpoint key

// === Core structures ===
pub struct Manifest {
    pub version: u8,
    pub object_id: ObjectId,
    pub total_size: u64,
    pub chunk_size: u32,
    pub chunks: Vec<ChunkMeta>,
    pub created_at: u64,
    pub metadata: BTreeMap<String, String>,
}

pub struct ChunkMeta {
    pub chunk_id: ChunkId,
    pub offset: u64,
    pub raw_length: u32,
    pub stored_length: u32,
    pub compression: Compression,   // None | Zstd
    pub shards: Vec<ShardMeta>,
}

pub struct ShardMeta {
    pub shard_id: ShardId,
    pub index: u8,           // position in RS coding (0..k+m)
    pub size: u32,
}

pub struct Member {
    pub node_id: NodeId,
    pub addr: NodeAddr,
    pub capacity: u64,
    pub state: MemberState,   // Alive | Suspect | Dead
    pub generation: u64,
}

pub struct NodeConfig {
    pub storage_backend: StorageBackend,   // Memory | File | Direct
    pub chunk_size: u32,                    // 128KB..4MB
    pub erasure_k: u8,                      // data shards (2..16)
    pub erasure_m: u8,                      // parity shards (1..8)
    pub repair_max_bandwidth: u64,
    pub repair_concurrent_transfers: u16,   // 1 (RPi) .. 64 (datacenter)
    pub gossip_interval_ms: u32,
    pub repair_circuit_breaker: RepairCircuitBreaker,
}

pub struct RepairCircuitBreaker {
    pub max_down_fraction: f64,             // default 0.5
    pub queue_pressure_threshold: usize,    // default 10000
    pub rebalance_cooldown_secs: u64,       // default 60
}
```

## Detailed Behavior

### Write Path

1. Client PUT via S3 API
2. CDC chunking (FastCDC v2020, min=16KB, avg=64KB, max=256KB)
3. zstd compress each chunk (level 3). Skip if compressed >= raw size
4. Reed-Solomon encode → k data + m parity shards
5. BLAKE3 hash each shard → ShardId
6. Distribute shards to ring owners via QUIC
7. Build manifest → persist in Fjall + broadcast via gossip/LogTree
8. LogTree entry (ed25519 signed) records mutation
9. ObjectId = blake3(serialized manifest), stored in Fjall
10. Every object goes through this same pipeline — no inline shortcut

### Read Path

1. Client GET via S3 API
2. Look up manifest in Fjall
3. For each chunk: fetch shards from ring owners (local first, remote via QUIC)
4. EC decode → decompress → verify raw_length
5. Concatenate → return to client

### Node Join

1. New node connects to seed nodes via iroh
2. foca SWIM propagates membership change
3. All nodes recompute placement ring
4. Ring diff → rebalancing transfers shards gradually (throttled)
5. Node fully operational once rebalancing completes

### Node Failure

1. foca detects (ping → indirect ping → suspect → dead)
2. Membership change propagated via gossip, ring recomputed
3. Repair scheduler prioritizes shards with fewest remaining copies
4. Repair executor fetches/reconstructs shards, writes to new owners

### Anti-Entropy (Background)

- Periodic verification: do I hold all shards the ring assigns me?
- Periodic integrity: re-hash and compare to ShardId
- Corrupted/missing shards → repair queue

---

# Completed Milestones

All milestones are complete. Summary for reference:

| #  | Milestone                    | Crate(s)                     | Status |
| -- | ---------------------------- | ---------------------------- | ------ |
| 0  | Workspace Setup              | (all)                        | ✅      |
| 1  | Shared Types                 | shoal-types                  | ✅      |
| 2  | Shard Store                  | shoal-store                  | ✅      |
| 3  | Content Addressing           | shoal-cas                    | ✅      |
| 4  | Erasure Coding               | shoal-erasure                | ✅      |
| 5  | Placement Ring               | shoal-placement              | ✅      |
| 6  | Metadata Store               | shoal-meta                   | ✅      |
| 7  | Local Pipeline Integration   | tests/integration            | ✅      |
| 8  | Network Protocol             | shoal-net                    | ✅      |
| 9  | Cluster Membership           | shoal-cluster                | ✅      |
| 10 | Auto-Repair                  | shoal-repair                 | ✅      |
| 11 | Engine Orchestrator          | shoal-engine                 | ✅      |
| 12 | S3 HTTP API                  | shoal-s3                     | ✅      |
| 13 | Daemon Binary                | shoald                       | ✅      |
| 14 | End-to-End Integration Tests | tests/integration            | ✅      |
| 15 | Chaos Tests                  | tests/chaos                  | ✅      |

---

## Notes for Implementation

### Error Handling

- `thiserror` for library error types, `anyhow` only in `shoald`
- Every error type should be descriptive with context

### Logging

- `tracing` throughout with structured fields
- Important: shard stored/transferred, node joined/died, repair started/completed
- Debug: individual message sends/receives
- Trace: ring computation details

### Performance Considerations

- Writes: pipeline erasure encoding (encode chunk N+1 while distributing chunk N)
- Reads: fetch shards in parallel across nodes
- Repair: always respect rate limiter, never starve client traffic
- All objects same pipeline (no inline shortcut)

### Production Hardening

- **Content-addressing everywhere**: BLAKE3 hash at every layer
- **Verify-on-read**: FileStore re-hashes every shard, corruption treated as missing
- **End-to-end integrity**: Network transfers verify blake3 at receiver on every hop
- **Manifest versioning**: `version: u8` field, reject unknown versions
- **Rebalancing circuit breaker**: Stop repair if >=50% nodes down, cooldown after restart, queue pressure throttling
- **Majority-vote scrub**: Peer consensus for corruption detection, not primary-wins
- **m=1 warnings**: `suggest_config` warns when parity is insufficient
- **Inode monitoring**: `FileStore::capacity()` warns at >80% inode usage

### Things NOT to do yet

- No encryption at rest
- No bucket-level ACLs
- No object versioning (LogTree provides mutation history but not S3-style versioning)
- No lifecycle policies
- No `io_uring` / `O_DIRECT` backend
- No `ZoneReplicator` (cross-zone replication)
- No small object packing (future optimization)
