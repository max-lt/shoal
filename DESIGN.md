# Design

How Shoal works internally, and why it works that way.

## Data Model

An S3 object goes through several transformations before it
lands on disk:

```
Object (arbitrary bytes)
  → Chunks (content-defined, ~64 KB average via FastCDC)
    → Shards (Reed-Solomon: k data + m parity per chunk)
      → Files on disk (one file per shard, content-addressed)
```

Each shard is identified by `ShardId = blake3(payload)`. Two
shards with identical content always produce the same ID and
are stored once.

A **manifest** ties it all together: it lists which chunks
make up the object, and which shards make up each chunk. The
manifest is identified by `ObjectId = blake3(manifest content)`.
S3 object keys like `bucket/path` are mapped to ObjectIds in
the metadata store.

## Storage Layers

Each node runs two storage backends, each suited to a different
access pattern:

**FileStore** stores shards as individual files on disk with a
two-level fan-out directory structure:
`shards/{hex[0..2]}/{hex[2..4]}/{hex}`. Each file carries an
8-byte header (refcount + payload size) followed by the raw
payload. Writes are atomic (temp file → fsync → rename). Reads
re-hash the payload and compare to the ShardId; a mismatch
is reported as corruption.

**MetaStore** (Fjall LSM-tree) stores everything else: object
key mappings, manifests, shard placement map, membership state,
repair queue, peer addresses, API keys, bucket metadata, tags,
and lifecycle rules. Fjall batches writes through a WAL and
caches reads in memory, which makes it well-suited for small,
frequently accessed metadata.

This split is deliberate. Manifests are small (a few hundred
bytes) and read on every GET. Fjall serves them from memory
most of the time. Shards are large and read sequentially from
disk. Each backend handles the access pattern it was designed
for.

## Write Path

1. The S3 layer receives a PUT and passes the body to the
   engine.

2. The engine splits the body into chunks using FastCDC
   (content-defined chunking, ~64 KB average, 256 KB max).
   Each chunk is compressed with zstd (level 3). If compression
   would increase the size, the raw data is kept. The chunk is
   then Reed-Solomon encoded into `k` data shards + `m` parity
   shards.

3. Every shard is written to the local FileStore first. The
   consistent hash ring determines which node owns each shard,
   and the engine pushes shards to their remote owners over
   QUIC. Once a remote push succeeds, the local copy is deleted
   if the writer is not a ring owner for that shard.

4. A manifest is built and written to MetaStore, along with
   the object key mapping (`bucket/key → ObjectId`).

5. If LogTree is enabled, a signed log entry is appended and
   gossiped to the cluster so other nodes learn about the new
   object.

Read-after-write on the writing node is guaranteed because the
manifest is in MetaStore before the PUT returns. Other nodes
see the object once gossip delivers the log entry.

## Read Path

1. Look up `bucket/key` in the local stores (LogTree when
   enabled, MetaStore otherwise) to get the ObjectId, then
   fetch the manifest.

2. For each chunk, the engine needs any `k` of the `k + m`
   shards. It checks the local FileStore first, then fetches
   missing shards from peers over QUIC.

3. Each chunk is Reed-Solomon decoded, decompressed if needed,
   and the original data is reconstructed.

4. Chunks are concatenated and streamed back to the client.

Because any `k` shards suffice, reads tolerate up to `m`
missing or slow nodes per chunk.

## Content-Defined Chunking

Shoal uses FastCDC rather than fixed-size chunking. CDC
computes chunk boundaries based on content using a rolling
hash, so inserting a byte at the beginning of a file only
affects the first chunk — all subsequent boundaries shift by
at most a few bytes and most chunks remain identical.

This matters for content-addressed storage: when a file is
re-uploaded with small modifications, unchanged chunks
produce the same ShardIds and are already stored. Only the
modified chunks create new shards. The dedup is a natural
consequence of content addressing combined with CDC; there
is no separate dedup index or overhead.

In practice, for typical S3 workloads (write-once, read-many,
unique objects), CDC gives marginal dedup benefit over
fixed-size. It becomes valuable for versioned objects,
incremental backups, and datasets with small edits.

## Cluster Membership

Nodes discover and monitor each other using QUIC-based health
checks. The PeerManager sends periodic ping messages to every
known peer. If a peer stops responding, it transitions through
Alive → Suspect → Dead based on configurable timeouts
(default: suspect after 5 s, dead after 15 s).

Every node independently maintains a membership list. New
nodes join by sending a JoinRequest to a seed peer, which
replies with the full member list.

State changes (new objects, deletions) propagate via
iroh-gossip over the same QUIC connections used for shard
transfers. No separate gossip network.

## Placement

Shard placement uses a consistent hash ring. Each node gets
virtual nodes on the ring, computed as
`blake3(node_id || vnode_index)`. To place a shard, walk the
ring clockwise from the shard's position (first 8 bytes of
its ShardId, which is already a blake3 hash) until hitting a
vnode.

The ring is deterministic: given the same set of nodes, every
node computes the same ring independently. No coordinator.

When nodes join or leave, `Ring::diff()` computes exactly
which shards need to move. Only shards in the affected ring
segments are rebalanced.

## Repair

Four components:

**Detector** watches cluster events. When a node is declared
dead, it scans local shards to find those that were assigned
to the dead node and enqueues them for repair. Priority is
based on surviving copy count: fewer copies = more urgent.

**Scheduler** maintains a priority queue and deduplicates
entries. Rate limits prevent overwhelming the cluster during
large-scale failures.

**Executor** repairs individual shards using two strategies:
first, direct copy from a surviving peer. If no peer holds the
shard, it falls back to RS reconstruction — fetch enough
sibling shards from the same chunk, decode the original data,
re-encode, and produce the missing shard.

**Circuit breaker** suspends repair when too many nodes are
down (default threshold: 50%). Without this, a network
partition could cause the surviving side to aggressively
replicate data, wasting bandwidth and storage. The breaker
resets when enough nodes return.

## Deep Scrub

The deep scrubber verifies shard integrity using
majority vote:

1. Compute the local blake3 hash.
2. Ask peers who also hold the shard for their hash.
3. If local matches the majority → healthy.
4. If local differs → local is corrupt, enqueue for repair.
5. If no majority → flag for manual inspection.

This catches silent corruption (bit rot, firmware bugs) that
verify-on-read alone would miss if the shard is rarely
accessed.

## LogTree

The LogTree is a signed, hash-chained DAG recording every
mutation (put, delete, API key management, bucket operations,
tagging, lifecycle configuration) across the cluster. Each
entry references its parents by hash, forming a Merkle DAG.
Entries are signed with ed25519.

It serves two purposes:

**Sync.** Nodes exchange LogTree tips on gossip. Missing
entries are fetched with their ancestors, then replayed. This
is how new or recovering nodes catch up without scanning all
data.

**Conflict resolution.** Concurrent writes to the same key
are resolved by Last-Writer-Wins using the entry's hybrid
logical clock timestamp (HLC). Equal HLCs are broken by
highest NodeId.

## Trade-offs

**Read-through consistency.** A GET on any node will find an
object even before gossip has delivered the log entry: if the
manifest is not found locally, `lookup_manifest` actively
queries all peers by key and caches the result. This makes
point reads strongly consistent across the cluster.
Listings (`list_objects`) are eventually consistent — they
only return objects known locally via gossip. Read-after-write
on the writing node is always guaranteed.

**Converged architecture.** Metadata and data live on the same
nodes, unlike hyperscaler designs that separate them into
independent fleets. This is simpler to deploy but means the
metadata index must fit on each node. At 100 million objects
the index is roughly 30 GB — fine for any server with an SSD.
At a billion objects it would be 300 GB, which is where this
architecture would need to change. Shoal targets small-to-mid
deployments, not hyperscale.

**Erasure coding only.** Some systems offer both replication
(3 full copies) and erasure coding. Shoal uses only erasure
coding. Replication is faster for writes but costs 3x storage.
With k=2 m=1, Shoal provides the same fault tolerance at 1.5x
storage.

## Cluster Resizing

Shoal supports adding and removing nodes from a running cluster.
The ring is recomputed on every membership change and new writes
immediately use the updated topology. However, existing shards
are not proactively rebalanced — they stay on their current
nodes until repair moves them.

### Scale-up

When new nodes join, the ring includes them and new writes are
distributed across all nodes. Old data remains on the original
nodes. Over time, new writes naturally balance the cluster. For
workloads that are mostly write-once, the imbalance is
negligible after a moderate volume of new writes.

`Ring::diff()` can compute exactly which shards should move to
restore perfect balance, but no background rebalancer is wired
up yet. This is planned future work.

### Scale-down

Nodes must be removed one at a time. After each removal, wait
for the repair queue to drain before removing the next node.
This ensures every shard has enough surviving copies before
another node disappears.

Removing multiple nodes simultaneously is dangerous. With
`replication_factor=2`, any shard whose two copies both lived
on removed nodes is permanently lost. The circuit breaker
(default 50%) suspends repair when too many nodes are down at
once, which prevents cascade failures but also means
under-replicated shards cannot be fixed until nodes return.

Safe procedure for removing N nodes from a cluster:

1. Remove one node (graceful leave or shutdown).
2. Monitor the repair queue until it is empty.
3. Repeat for the next node.

This guarantees at most one node is missing at any time, which
is within the fault tolerance of any configuration where m >= 1.
