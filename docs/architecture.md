# Architecture

Shoal is a distributed, self-healing object storage engine. This document
describes how the system is structured and how data flows through it.

## Layer Stack

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

Each layer is a separate Rust crate. Dependencies flow strictly downward.

## Crate Responsibilities

| Crate | Role |
|-------|------|
| `shoal-types` | Shared types: `ShardId`, `ChunkId`, `ObjectId`, `NodeId`, `Manifest`, `Member`, configs |
| `shoal-store` | `ShardStore` trait + two backends: `MemoryStore` (testing/ephemeral) and `FileStore` (production, atomic writes, 2-level fan-out) |
| `shoal-cas` | Content addressing: fixed-size chunker, streaming chunker, manifest builder. `ObjectId = blake3(serialized manifest)` |
| `shoal-meta` | Metadata persistence via Fjall (LSM-tree). Five keyspaces: objects, manifests, shardmap, membership, repair_queue. All keyspaces are caches reconstructible from the cluster |
| `shoal-erasure` | Reed-Solomon wrapper around `reed-solomon-simd`. Encode/decode with automatic padding, fast-path when all data shards present |
| `shoal-placement` | Hand-written consistent hash ring (~150 lines). Weighted vnodes, topology-aware placement, ring diff for rebalancing |
| `shoal-cluster` | Membership via foca (SWIM protocol) over iroh transport. Gossip via iroh-gossip (HyParView + PlumTree). `ClusterState` recomputes the ring on membership changes |
| `shoal-repair` | `RepairDetector` (subscribes to cluster events), `RepairScheduler` (priority queue), `RepairExecutor` (fetch + RS reconstruct + redistribute), `Throttle` (token bucket), circuit breaker |
| `shoal-net` | Protocol on iroh QUIC. Connection pooling, shard push/pull with blake3 integrity verification, ALPN-based cluster isolation |
| `shoal-engine` | `ShoalNode` orchestrator: `put_object`, `get_object`, `delete_object`. Ties all layers together. Background tasks for repair |
| `shoal-s3` | S3-compatible HTTP API: PUT/GET/DELETE/HEAD objects, ListObjectsV2, multipart upload, Bearer auth |
| `shoald` | Binary entrypoint. TOML config, auto-detection, CLI commands (`start`, `status`, `repair status`, `benchmark`) |

## Data Model

All identifiers are 32-byte BLAKE3 hashes:

```
ObjectId  = blake3(postcard_serialize(manifest_content))
ChunkId   = blake3(chunk_data)
ShardId   = blake3(shard_data)
NodeId    = iroh Ed25519 public key (32 bytes)
```

An object is decomposed as:

```
Object (arbitrary size)
  └── Chunks (fixed size, e.g. 256 KB)
        └── Shards (k data + m parity per chunk)
              └── Stored on nodes via consistent hash ring
```

The `Manifest` records the full mapping from object to chunks to shards,
including offsets and sizes for each piece.

## Write Path

```
Client PUT ──► S3 API ──► Engine.put_object()
                              │
                              ├── 1. Chunk data (fixed-size splitter)
                              ├── 2. For each chunk:
                              │      ├── Reed-Solomon encode → k data + m parity shards
                              │      ├── blake3 hash each shard → ShardId
                              │      └── Ring.owners(shard_id) → target nodes
                              ├── 3. Push shards to target nodes via transport
                              ├── 4. Build Manifest (ObjectId = blake3 of serialized content)
                              ├── 5. Store manifest in local Fjall
                              ├── 6. Gossip manifest to all cluster nodes
                              └── 7. Return ObjectId to client
```

Every object, regardless of size, goes through this same pipeline.

## Read Path

```
Client GET ──► S3 API ──► Engine.get_object()
                              │
                              ├── 1. Lookup ObjectId in local Fjall (bucket/key → ObjectId → Manifest)
                              ├── 2. For each chunk in manifest:
                              │      ├── Try local store first
                              │      ├── Pull from remote nodes if needed
                              │      └── Collect k shards, RS decode
                              ├── 3. Concatenate decoded chunks
                              └── 4. Stream to client
```

The fast path skips RS decode when all k data shards are available (simple
concatenation).

## Failure Handling

### Node Failure

1. foca SWIM detects the node is unresponsive (ping → indirect ping → suspect → dead)
2. Membership change propagated via gossip to all nodes
3. All nodes recompute the placement ring
4. `RepairDetector` identifies under-replicated shards
5. `RepairScheduler` prioritizes by fewest surviving copies
6. `RepairExecutor` fetches surviving copies (or RS-reconstructs) and pushes to new owners

### Circuit Breaker

The repair system has built-in safety:

- **Mass failure**: if >= 50% of nodes are down, all repair is suspended
- **Cooldown**: if a node was down < 60s, don't rebalance its shards (probably rebooting)
- **Queue pressure**: throttle aggressively when repair queue exceeds threshold
- **Bandwidth cap**: token-bucket rate limiter, configurable per hardware profile

### Integrity

- All data is content-addressed (blake3). Corruption is detectable at every layer
- `FileStore` re-hashes on read; corrupt data is treated as missing
- Network transfers verify blake3 at the receiver
- Background anti-entropy: majority-vote scrub (local hash vs. peer consensus)

## Metadata Store (Fjall)

Fjall is a local cache/index, never the source of truth. Everything is
reconstructible from the cluster:

| Keyspace | Key → Value | Source of truth |
|----------|-------------|-----------------|
| `objects` | `bucket/key` → `ObjectId` | Manifest gossip |
| `manifests` | `ObjectId` → serialized `Manifest` | Manifest gossip |
| `shardmap` | `ShardId` → `Vec<NodeId>` | Placement ring computation |
| `membership` | `NodeId` → `Member` | foca SWIM protocol |
| `repair_queue` | `ShardId` → priority | Local, rebuilt by anti-entropy scan |

If Fjall is lost (disk failure), the node can reconstruct its entire index
by scanning manifest shards from the cluster.

## Networking

Built on iroh (QUIC + hole punching + relay fallback):

- Each node has an Ed25519 identity (iroh `SecretKey` / `PublicKey`)
- Cluster isolation via ALPN: `shoal/0/<blake3(secret)[:16]>`
- Connection pooling: reuse QUIC connections to the same node
- Protocol: postcard-serialized `ShoalMessage` enum over bidirectional streams
- Gossip: iroh-gossip (HyParView + PlumTree) for cluster-wide event dissemination

See [security.md](security.md) for the trust model and authentication design.
