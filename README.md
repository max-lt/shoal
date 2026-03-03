# Shoal

Self-healing distributed object storage with S3 API and erasure
coding.

Shoal is a lightweight alternative to MinIO and Ceph for self-hosted
S3-compatible storage. A cluster of 3 to ~50 nodes distributes data
using Reed-Solomon erasure coding, detects failures via QUIC
health checks, and automatically repairs under-replicated data. It targets
deployments from small teams to mid-size infrastructure — up to 100
million objects across a handful of machines.

The name comes from a **shoal of fish**: individual units that move
together, self-organize, and reform when members disappear.

## Why Shoal

**3-node minimum.** MinIO distributed mode requires at least 4 nodes.
Shoal forms a functional erasure-coded cluster with 3.

**Content-addressed dedup.** Every shard is identified by its BLAKE3
hash. Identical data is stored once, regardless of how many objects
reference it. Re-uploading a slightly modified file only stores the
changed chunks.

**Automatic repair.** When a node dies, the cluster detects it within
seconds, identifies which shards lost a copy, and reconstructs them
from parity on surviving nodes. No operator intervention required.

**No central coordinator.** Every node independently computes shard
placement using a consistent hash ring. There is no metadata leader,
no quorum elections, no split-brain risk from coordinator failure.

**Flexible erasure coding.** Choose `k` data shards and `m` parity
shards per deployment. Want maximum storage efficiency? Use k=8 m=2.
Want maximum resilience on 3 nodes? Use k=2 m=1.

## Quick Start

```bash
cargo build --release
./target/release/shoald start
```

Then use any S3 client:

```bash
aws --endpoint-url http://localhost:4821 \
    s3 cp myfile.txt s3://mybucket/myfile.txt
aws --endpoint-url http://localhost:4821 \
    s3 cp s3://mybucket/myfile.txt -
```

See [Getting Started](docs/getting-started.md) for multi-node setup.

## How It Works

**Write path.** Object data is split into content-defined chunks. Each
chunk is compressed with zstd and Reed-Solomon encoded into `k` data
shards + `m` parity shards. Shards are placed on nodes via the
consistent hash ring. A manifest mapping chunks to shards is built
and gossiped to the cluster.

**Read path.** Look up the manifest, fetch any `k` shards per chunk
from the ring, erasure-decode, concatenate, and stream back to the
client.

**Self-healing.** When a node fails, QUIC health checks detect it
and the repair scheduler kicks in. It identifies under-replicated
shards, fetches surviving copies (or reconstructs from parity), and
places them on new owners. A circuit breaker stops repair if too
many nodes are down simultaneously to prevent cascade failures.

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
│   LogTree (audit DAG)       │  shoal-logtree
├─────────────────────────────┤
│   Metadata Store (Fjall)    │  shoal-meta
├─────────────────────────────┤
│   Content Addressing        │  shoal-cas
├─────────────────────────────┤
│   Shard Store (FileStore)   │  shoal-store
├─────────────────────────────┤
│   Network (iroh QUIC)       │  shoal-net
└─────────────────────────────┘
```

## Crates

| Crate             | Description                          |
| ----------------- | ------------------------------------ |
| `shoal-types`     | Shared types and identifiers         |
| `shoal-store`     | Shard storage trait + backends       |
| `shoal-cas`       | Content addressing and chunking      |
| `shoal-meta`      | Metadata persistence (Fjall)         |
| `shoal-erasure`   | Reed-Solomon erasure coding          |
| `shoal-placement` | Consistent hash ring                 |
| `shoal-net`       | QUIC transport (iroh)                |
| `shoal-cluster`   | Membership (QUIC ping) + gossip      |
| `shoal-logtree`   | Signed append-only mutation DAG      |
| `shoal-repair`    | Auto-repair and circuit breaker      |
| `shoal-engine`    | Node orchestrator, write/read paths  |
| `shoal-s3`        | S3-compatible HTTP API (axum)        |
| `shoald`          | Daemon binary and CLI                |

## Technology Stack

| Purpose        | Crate               |
| -------------- | -------------------- |
| Hashing        | `blake3`             |
| Metadata DB    | `fjall` v3           |
| Erasure coding | `reed-solomon-simd`  |
| Networking     | `iroh`               |
| Gossip         | `iroh-gossip`        |
| Serialization  | `postcard` + `serde` |
| HTTP           | `axum`               |
| Async          | `tokio`              |

## Documentation

| Document                                   | Description                                   |
| ------------------------------------------ | --------------------------------------------- |
| [Getting Started](docs/getting-started.md) | Single-node and multi-node setup              |
| [Architecture](docs/architecture.md)       | System design, data flow, failure handling     |
| [Design](DESIGN.md)                        | Internal design decisions and trade-offs       |
| [Configuration](docs/configuration.md)     | Full TOML config reference, hardware profiles  |
| [Security](docs/security.md)               | Trust model, CA-based authentication design    |

## License

This project is not yet licensed. All rights reserved.
