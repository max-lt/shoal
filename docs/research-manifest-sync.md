# Research: Manifest Synchronization — Hypercore, DAG Logs, and Alternatives

## Context

Shoal currently synchronizes manifests via fire-and-forget broadcast over QUIC
(uni-directional streams). Each `put_object` broadcasts a `ManifestPut` message
to all known peers. There is no ordering, no causal tracking, no delivery
guarantee, and no audit trail.

This document evaluates whether **Hypercore** (or its concepts) could improve
manifest synchronization, and explores a **Git-like DAG log** design that was
discussed as an alternative.

---

## 1. Hypercore Analysis

### What Hypercore Is

Hypercore is an **append-only, signed, Merkle-tree-backed log**:

- Each entry (block) is addressed by sequence number
- A flat-tree Merkle structure (PPSP RFC 7574) enables sparse replication
  and cryptographic proofs for individual entries
- An Ed25519 keypair is generated per feed; the public key is the feed ID
- Only the private key holder can append — **single-writer by design**
- Uses BLAKE2b for hashing

### JavaScript (holepunchto/hypercore)

- Version 10 (LTS), actively maintained, 1,955 commits
- V10 introduced truncation support and multi-signer quorum (but not
  multi-writer — quorum is for signing, not writing)

### Rust Port (datrs/hypercore)

- Version 0.16.0 (February 2026), actively maintained by Blake Griffith
- `#![forbid(unsafe_code)]` — 100% safe Rust
- Supports in-memory and disk storage, batch appends, proof generation
- **Missing**: no Autobase, no multi-writer, no manifest wire format (v11)
- Related: `hypercore-protocol-rs` 0.7.0 (Feb 2026) — wire protocol, also
  actively maintained. Uses Noise handshake via `snow`

### Multi-Writer: Autobase (JS only)

Autobase (JS, 140 stars, 1,281 commits) adds multi-writer on top of Hypercore:

- Each writer has their own Hypercore feed
- Writers reference previous entries (any writer), forming a **causal DAG**
- A deterministic linearization produces a total order
- "Indexers" (trusted quorum) provide ordering finality
- A derived "view" (typically Hyperbee B-tree) represents current state

**There is no Rust port of Autobase.** No issues or discussions about it in the
datrs ecosystem.

### Assessment for Shoal

Hypercore is a poor fit for manifest synchronization:

| Concern | Detail |
|---------|--------|
| Single-writer | Any node in Shoal can PUT — fundamental mismatch |
| Multi-writer gap | Would need N feeds (one per node) + custom merge layer |
| No Autobase in Rust | Would require porting 1,281 commits of linearization logic |
| BLAKE2b vs BLAKE3 | Shoal uses BLAKE3 everywhere; mixing hash algorithms adds complexity |
| Overkill | Sparse replication and Merkle proofs are not needed — manifests are small and every node needs all of them |

**Conclusion: Hypercore is not suitable for Shoal's manifest sync.**

However, the *concepts* are valuable — append-only logs, Merkle-chained entries,
and signed histories.

---

## 2. Current Shoal Manifest Sync: Gaps

The current architecture (fire-and-forget broadcast) has these limitations:

1. **No ordering** — concurrent writes to the same key have no defined winner
2. **No causal tracking** — no way to know which update supersedes another
3. **No delivery guarantee** — failed broadcasts are silently dropped
4. **No audit trail** — no record of who wrote what, when
5. **No deletion broadcast** — `delete_object` doesn't propagate to peers
6. **No catch-up beyond bulk sync** — a rejoining node downloads ALL manifests;
   no delta sync
7. **No timestamps** — `created_at` is wall-clock time, not a monotonic/logical
   clock

---

## 3. Proposed Design: Git-like DAG Log

### Core Idea

Each mutation (PUT, DELETE) produces a **signed, Merkle-chained log entry**.
Entries form a **DAG** (directed acyclic graph) — like Git commits, not a
linear chain. Concurrent writes from different nodes create branches; any node
can produce a **merge entry** that references multiple tips.

### Data Structure

```rust
/// A single entry in the mutation log.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry {
    /// Hybrid Logical Clock timestamp (Lamport + wall-clock)
    pub hlc: u64,

    /// Node that authored this entry
    pub node_id: NodeId,

    /// The mutation
    pub action: Action,

    /// Parent entry hashes (1 for normal writes, N for merges)
    pub parents: Vec<[u8; 32]>,

    /// blake3(hlc || node_id || action || parents)
    pub hash: [u8; 32],

    /// Ed25519 signature by node_id's key
    pub signature: [u8; 64],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Action {
    Put {
        bucket: String,
        key: String,
        object_id: ObjectId,
    },
    Delete {
        bucket: String,
        key: String,
    },
}
```

### How It Works

**Normal write** — one parent (the node's own previous entry):
```
Node A:  A1 ← A2 ← A3
Node B:  B1 ← B2
```

**Merge** — multiple parents (tips from diverged branches):
```
Node A:  A1 ← A2 ← A3
                       \
Node B:  B1 ← B2 ←──── MERGE ← ...
```

**Conflict resolution**: Last-Writer-Wins using HLC. The merge entry documents
the resolution — which manifest won and why.

**Convergence**: Natural, like `git pull`. When node C receives chains from A
and B, it merges. When A receives C's merge, A sees its tip is already included
— nothing to do.

### Sync Protocol

Like `git fetch`:

1. Rejoining node sends its **tip hashes** (one per known branch)
2. Peer responds with all entries **since** those tips
3. Merkle chain guarantees only the delta is transferred
4. Receiver verifies each entry: check hash, check signature, check parents exist
5. Receiver creates a merge entry if branches have diverged

### What This Enables

- **Audit trail** — every mutation is signed and traceable
- **Causal ordering** — the DAG encodes happens-before relationships
- **Efficient rejoin** — delta sync instead of bulk download
- **Tamper detection** — signed entries with hash chains; tampering breaks the chain
- **Replay** — reconstruct any node's state by replaying the log
- **CDC** — stream mutations to external systems (analytics, compliance)
- **Conflict visibility** — merges document concurrent writes explicitly

---

## 4. Alternative Approaches from the Rust Ecosystem

### Merkle Search Tree (`merkle-search-tree` crate)

- Purpose-built for **anti-entropy in distributed systems**
- Based on Auvolat & Taiani 2019 paper
- O(1) root hash comparison, efficient diff of divergent key ranges
- Works well with CRDTs for concurrent modification
- **Good for**: efficient set reconciliation of manifest keys
- **Limitation**: no ordering or audit trail; it's a key-value structure

### Range-Based Set Reconciliation (Recon protocol)

- Used by Ceramic Network, implemented in Rust (`ceramic-one`)
- Based on Aljoscha Meyer's paper (arXiv:2212.13567)
- Recursive set partitioning with fingerprint comparison
- No per-connection state, DoS-resistant
- **Good for**: efficient discovery of missing manifests
- **Limitation**: MIT-licensed but embedded in Ceramic codebase; extraction effort unclear

### Prolly Tree (`prollytree` crate)

- Hybrid B-tree + Merkle tree, content-addressed and deterministic
- Supports diff, merge, ordered scans
- **Good for**: deterministic state snapshots with efficient diffing
- **Less mature** than merkle-search-tree

### iroh-docs / iroh-willow

- Already in Shoal's iroh ecosystem
- iroh-docs: multi-dimensional key-value documents with efficient sync
- iroh-willow: Willow protocol implementation (v0.0.1, pre-release)
- **Risk**: iroh-willow is experimental; iroh-docs ties to iroh's data model

---

## 5. Recommendation

### Phase 1: HLC + DAG Log (Milestone 16 candidate)

Implement the Git-like DAG log described in section 3:

1. **Add HLC** to `shoal-types` — Hybrid Logical Clock for causal timestamps
2. **Add `LogEntry` and `Action`** to `shoal-types`
3. **Add `oplog` keyspace** to `shoal-meta` — persist the DAG entries
4. **Modify write path** in `shoal-engine` — append LogEntry on every PUT/DELETE
5. **Modify manifest broadcast** — include LogEntry with ManifestPut messages
6. **Add delta sync** — replace bulk `pull_all_manifests` with tip-based protocol
7. **Add merge logic** — detect diverged branches, create merge entries

### Phase 2: Anti-Entropy (Milestone 17 candidate)

Add periodic background reconciliation using Merkle-based diffing:

1. **Merkle tree over oplog** — periodic root hash comparison between peers
2. **Efficient diff** — exchange only divergent ranges (merkle-search-tree or
   custom implementation)
3. **Consistency verification** — detect and repair manifest gaps without
   full resync

### What NOT to Do

- Don't add Hypercore as a dependency — it doesn't fit the multi-writer model
- Don't implement full Autobase linearization — LWW with HLC is sufficient
- Don't add iroh-willow yet — too immature (v0.0.1)
- Don't over-engineer the merge logic — for object storage, LWW is the right
  conflict resolution strategy

---

## References

- [holepunchto/hypercore](https://github.com/holepunchto/hypercore) — Original JS implementation
- [datrs/hypercore](https://github.com/datrs/hypercore) — Rust port, v0.16.0
- [holepunchto/autobase](https://github.com/holepunchto/autobase) — Multi-writer layer (JS only)
- [merkle-search-tree](https://crates.io/crates/merkle-search-tree) — Anti-entropy for distributed KV
- [Range-Based Set Reconciliation](https://arxiv.org/abs/2212.13567) — Aljoscha Meyer's paper
- [Ceramic Recon Protocol (CIP-124)](https://cips.ceramic.network/CIPs/cip-124) — Production use of range-based reconciliation
- [iroh-willow](https://docs.rs/iroh-willow/latest/iroh_willow/) — Willow protocol for iroh (pre-release)
- [prollytree](https://lib.rs/crates/prollytree) — Content-addressed Prolly Tree
