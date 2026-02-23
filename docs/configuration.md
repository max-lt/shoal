# Configuration Reference

Shoal is configured via a TOML file. If no config file is provided, the node
auto-detects system resources and picks sensible defaults.

## Config File Location

```bash
shoal start                      # auto-detect, no config file
shoal start --config shoal.toml  # explicit config file
```

## Full Example

```toml
[node]
data_dir = "/var/lib/shoal"
listen_addr = "0.0.0.0:4820"
s3_listen_addr = "0.0.0.0:4821"

[cluster]
secret = "my-cluster-secret"
peers = ["abc123@192.168.1.10:4820"]

[storage]
backend = "file"
chunk_size = 262144

[erasure]
k = 4
m = 2
shard_replication = 1

[repair]
max_bandwidth = "100MB/s"
concurrent_transfers = 8

[s3]
access_key = "shoal"
secret_key = "shoalsecret"

[log]
level = "info"
```

## Sections

### `[node]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `data_dir` | path | `~/.shoal` | Directory for metadata DB and shard files |
| `listen_addr` | string | `0.0.0.0:4820` | QUIC listen address for inter-node traffic |
| `s3_listen_addr` | string | `0.0.0.0:4821` | HTTP listen address for S3 API |

### `[cluster]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `secret` | string | *(random)* | Shared cluster secret. If empty, a random secret is generated and printed at startup |
| `peers` | list of strings | `[]` | Seed peers to contact on startup. Format: `"node-id"` or `"node-id@host:port"` |

### `[storage]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `backend` | string | `"file"` | Storage backend: `"file"` (production) or `"memory"` (testing/ephemeral) |
| `chunk_size` | integer | *(auto)* | Chunk size in bytes. Auto-detected based on RAM (see below) |

### `[erasure]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `k` | integer | `4` | Number of data shards per chunk |
| `m` | integer | `2` | Number of parity shards per chunk |
| `shard_replication` | integer | `1` | How many nodes store each individual shard. With erasure coding, 1 is usually sufficient |

The total number of shards per chunk is `k + m`. To read, any `k` of the
`k + m` shards suffice. The system tolerates `m` simultaneous shard losses
per chunk.

### `[repair]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `max_bandwidth` | string | *(auto)* | Maximum repair bandwidth, e.g. `"100MB/s"`, `"1GB/s"` |
| `concurrent_transfers` | integer | *(auto)* | Number of concurrent shard transfers during repair |

### `[s3]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `access_key` | string | *(none)* | S3 access key ID. If unset, authentication is disabled |
| `secret_key` | string | *(none)* | S3 secret key (used as Bearer token secret) |

### `[log]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `level` | string | `"info"` | Log level: `"trace"`, `"debug"`, `"info"`, `"warn"`, `"error"` |

## Hardware Profiles (Auto-Detection)

When `chunk_size`, `max_bandwidth`, or `concurrent_transfers` are not
specified, Shoal probes system RAM and picks a profile:

| Profile | RAM | Chunk Size | Concurrent Transfers | Max Bandwidth |
|---------|-----|------------|---------------------|---------------|
| RPi | < 1 GB | 128 KB | 1 | 1 MB/s |
| Default | 1 -- 16 GB | 256 KB | 8 | 100 MB/s |
| Datacenter | > 16 GB | 4 MB | 64 | 1 GB/s |

You can override any individual value regardless of profile.

## Erasure Coding Recommendations

The default `k=4, m=2` is a good general-purpose choice (1.5x storage
overhead, tolerates 2 failures per chunk). For different cluster sizes,
`suggest_config` recommends:

| Cluster Size | k | m | Overhead | Tolerated Failures |
|-------------|---|---|----------|--------------------|
| 1 node | 1 | 0 | 1.0x | 0 |
| 2 nodes | 1 | 1 | 2.0x | 1 |
| 3 nodes | 2 | 1 | 1.5x | 1 |
| 4 nodes | 2 | 2 | 2.0x | 2 |
| 5 nodes | 3 | 2 | 1.67x | 2 |
| 6--11 nodes | 4 | 2 | 1.5x | 2 |
| 12+ nodes | 8 | 4 | 1.5x | 4 |

With `shard_replication > 1`, each shard is additionally copied to multiple
nodes. This is belt-and-suspenders on top of erasure coding and is usually
not needed unless you want faster local reads.

## Environment Variables

Currently, Shoal does not read environment variables. All configuration is
via the TOML file or auto-detection.
