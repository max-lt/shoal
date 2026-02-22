# Shoal

Distributed, self-healing object storage engine written in Rust.

Shoal is a lightweight, peer-to-peer alternative to S3-compatible storage that can run anywhere — from a Raspberry Pi with an SD card to a 128-core datacenter server with NVMe arrays. Nodes discover each other automatically, form a cluster, distribute data using erasure coding, and self-repair when nodes join or leave.

The name comes from a **shoal of fish** — lightweight individual units that move together, self-organize, and reform when members disappear.

## Design Principles

- **No C dependencies** — Zero C/C++ bindings. Cross-compilation is trivial.
- **Adaptive** — Same binary runs on a Raspberry Pi 4 and a datacenter monster. Config adapts to available resources.
- **Self-healing** — Nodes join and leave. The cluster detects failures, rebalances, and repairs automatically.
- **Content-addressed** — All data is identified by its BLAKE3 hash. Integrity is verifiable at every layer.
- **Deterministic placement** — Every node can independently compute where any shard belongs. No central coordinator.

## Architecture

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

## How It Works

**Write path**: Object data is chunked into fixed-size pieces, each chunk is Reed-Solomon encoded into `k` data shards + `m` parity shards, shards are placed on nodes via a consistent hash ring, and a manifest is built and stored.

**Read path**: Look up the manifest, fetch `k` shards per chunk from the ring, erasure decode, concatenate, and stream back to the client.

**Self-healing**: When a node fails, the cluster detects it via the SWIM protocol, identifies under-replicated shards, and the repair scheduler fetches surviving copies (or reconstructs from parity) and places them on new owners.

## Workspace Layout

```
crates/
  shoal-types/       Shared types and identifiers (ShardId, ChunkId, ObjectId, NodeId)
  shoal-store/       ShardStore trait + MemoryStore and FileStore backends
  shoal-cas/         Content addressing, chunking, and manifests
  shoal-meta/        Metadata persistence (Fjall LSM-tree)
  shoal-erasure/     Reed-Solomon erasure coding (reed-solomon-simd)
  shoal-placement/   Consistent hashing ring with topology-aware placement
  shoal-cluster/     Membership (foca SWIM) + gossip (iroh-gossip)
  shoal-repair/      Auto-repair, rebalancing, throttling
  shoal-net/         Network protocol on iroh QUIC
  shoal-engine/      Node orchestrator, write/read pipelines
  shoal-s3/          S3-compatible HTTP API (axum)
  shoal-cli/         Binary entrypoint
tests/
  integration/       Full pipeline integration tests
```

## Technology Stack

No C/C++ bindings in the dependency tree.

| Purpose          | Crate               |
| ---------------- | -------------------- |
| Hashing          | `blake3`             |
| Metadata DB      | `fjall` v3           |
| Erasure coding   | `reed-solomon-simd`  |
| Networking       | `iroh` 0.35          |
| Gossip           | `iroh-gossip`        |
| Membership       | `foca`               |
| Serialization    | `postcard` + `serde` |
| HTTP             | `axum`               |
| Async            | `tokio`              |

## Build & Test

```bash
cargo build                          # build everything
cargo test                           # run all 139 tests
cargo test -p shoal-types            # single crate
cargo test --test local_pipeline     # integration test
cargo clippy -- -D warnings          # lint (zero warnings)
cargo fmt --check                    # format check
```

## Current Status

Milestones 0–7 are complete. The core data pipeline works end-to-end in local mode:

| Milestone | Crate            | Status | Tests |
| --------- | ---------------- | ------ | ----- |
| 0         | Workspace setup  | Done   | —     |
| 1         | `shoal-types`    | Done   | 31    |
| 2         | `shoal-store`    | Done   | 24    |
| 3         | `shoal-cas`      | Done   | 13    |
| 4         | `shoal-erasure`  | Done   | 21    |
| 5         | `shoal-placement`| Done   | 19    |
| 6         | `shoal-meta`     | Done   | 20    |
| 7         | Integration test | Done   | 11    |
| 8         | `shoal-net`      | Next   | —     |
| 9–15      | Cluster, repair, engine, S3, CLI | Planned | — |

See [ARCHITECTURE.md](ARCHITECTURE.md) for a detailed architecture reference and [plan.md](plan.md) for the implementation roadmap.

## License

This project is not yet licensed. All rights reserved.
