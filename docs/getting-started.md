# Getting Started

## Prerequisites

- Rust toolchain (1.80+)
- Linux, macOS, or Windows

No C compiler or system libraries required.

## Build

```bash
git clone https://github.com/your-org/shoal
cd shoal
cargo build --release
```

The binary is at `target/release/shoald`.

## Single-Node Quick Start

Start a node with auto-detected settings:

```bash
./shoald start
```

This will:
- Generate an Ed25519 identity for the node
- Create `~/.shoal/` for metadata and shard storage
- Start the S3 API on `http://0.0.0.0:4821`
- Print the node's ID and a randomly generated cluster secret

You can now use any S3 client:

```bash
# Store an object
aws --endpoint-url http://localhost:4821 s3 cp myfile.txt s3://mybucket/myfile.txt

# Retrieve it
aws --endpoint-url http://localhost:4821 s3 cp s3://mybucket/myfile.txt -

# List objects
aws --endpoint-url http://localhost:4821 s3 ls s3://mybucket/
```

## Multi-Node Cluster

### Node 1 (seed)

```bash
./shoald start --config node1.toml
```

```toml
# node1.toml
[node]
data_dir = "/data/shoal-1"
listen_addr = "0.0.0.0:4820"
s3_listen_addr = "0.0.0.0:4821"

[cluster]
secret = "my-cluster-secret"

[erasure]
k = 4
m = 2
```

Note the NodeId printed at startup.

### Node 2

```bash
./shoald start --config node2.toml
```

```toml
# node2.toml
[node]
data_dir = "/data/shoal-2"
listen_addr = "0.0.0.0:4820"
s3_listen_addr = "0.0.0.0:4821"

[cluster]
secret = "my-cluster-secret"
peers = ["<node1-id>@192.168.1.10:4820"]
```

Node 2 will:
1. Connect to node 1 via iroh QUIC
2. Join the cluster via foca SWIM protocol
3. Receive existing manifests via gossip
4. Start serving reads and accepting writes

### Node 3+

Same pattern. Only one seed peer is needed; nodes discover each other
transitively via the SWIM protocol.

## Cluster Status

```bash
./shoald status
```

Shows cluster members, their state (Alive/Suspect/Dead), shard counts, and
storage capacity.

```bash
./shoald repair status
```

Shows the repair queue length and current repair activity.

## Configuration

See [configuration.md](configuration.md) for the full config reference.

## Running Tests

```bash
cargo test                           # all 441 tests
cargo test -p shoal-types            # single crate
cargo test --test torture            # torture/stress tests
cargo test --test local_pipeline     # local pipeline integration
cargo clippy -- -D warnings          # lint
cargo fmt --check                    # format check
```

## What Happens Under the Hood

When you `PUT` an object:

1. The object is split into fixed-size **chunks** (default 256 KB)
2. Each chunk is Reed-Solomon encoded into **k data + m parity shards**
3. Each shard is placed on nodes via the **consistent hash ring**
4. A **manifest** is built (maps object to chunks to shards) and gossiped
5. The S3 API returns success

When you `GET` an object:

1. The manifest is looked up locally (received via gossip)
2. For each chunk, **k shards** are fetched (local first, then remote)
3. Shards are Reed-Solomon decoded back into the original chunk
4. Chunks are concatenated and streamed to the client

If a node fails, the cluster self-repairs: surviving shards are fetched (or
reconstructed from parity), and new copies are placed on healthy nodes.
