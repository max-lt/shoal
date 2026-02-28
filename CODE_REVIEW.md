# Shoal Deep Code Review

**Reviewer**: Senior Systems Engineer (Rust, distributed systems, storage infrastructure)
**Codebase**: ~35,500 lines across 13 crates + integration/chaos tests
**Method**: Full source code analysis only. No documentation consulted.

---

## Dimension Ratings (1-5)

| Dimension | Rating | Summary |
|-----------|--------|---------|
| 1. Distributed Systems Correctness | 3.5/5 | LWW-based DAG is sound; no formal split-brain protection; gossip-only metadata propagation has convergence gaps |
| 2. Storage Engine Integrity | 2.5/5 | **Missing fsync** in FileStore is a critical data loss path; verify-on-read mitigates but doesn't prevent loss |
| 3. Concurrency & Safety | 4/5 | Zero unsafe in most crates; one justified libc call; `expect("lock poisoned")` is the main concern |
| 4. Error Handling & Resilience | 3.5/5 | Proper `thiserror` throughout; `unwrap_or_default()` on serialization silently drops errors; partial failure handling is good |
| 5. Security | 3/5 | SigV4 with constant-time comparison is good; admin endpoints are **unauthenticated**; multipart uploads are unbounded in memory |
| 6. Performance | 3.5/5 | Hot path allocations in CDC chunking; connection pool holds mutex during QUIC connect; shard fetch is sequential per chunk |
| 7. API Correctness | 3.5/5 | Core S3 ops work; no Range request support; no continuation token for ListObjectsV2; multipart ETag format is wrong |
| 8. Code Quality | 4.5/5 | Idiomatic Rust; clean separation of concerns; comprehensive test suite; strong type system usage |

---

## Findings

### 1. Distributed Systems Correctness

#### Finding 1.1 — LogTree LWW conflict resolution is correct but has a determinism gap
- **File:Line** — `crates/shoal-logtree/src/tree.rs:738`
- **Severity** — 🟡 Minor
- **Description** — Conflict resolution uses `(HLC desc, NodeId desc)` for Last-Writer-Wins. This is deterministic and converges correctly. The `HybridClock` implementation (`shoal-types/src/lib.rs:464-478`) uses CAS loops with `SeqCst` ordering and `witness()` correctly advances the local clock. However, the HLC only provides causal ordering within a single node's chain — concurrent writes on two partitioned nodes with drifted wall clocks can produce unexpected ordering when the partition heals.
- **Recommendation** — Document the wall-clock dependency explicitly. Consider adding a clock-skew warning when `witness()` observes a remote HLC more than N seconds ahead.

#### Finding 1.2 — No split-brain protection beyond circuit breaker
- **File:Line** — `crates/shoal-repair/src/circuit_breaker.rs:52-89`
- **Severity** — 🟠 Major
- **Description** — The circuit breaker suspends repair when >=50% of nodes are down (`max_down_fraction`). However, there is no quorum mechanism preventing both halves of a network partition from accepting writes. Two partitioned sub-clusters will each maintain their own LogTree DAG tips, diverging independently. When the partition heals, the LWW merge may silently discard writes from the "losing" side. This is acceptable for an eventually-consistent system, but the lack of any notification that writes were lost is concerning.
- **Recommendation** — Emit a `ClusterEvent::ConflictDetected` when merge discovers concurrent writes to the same key from different nodes with close HLC timestamps. Log the overwritten ObjectId.

#### Finding 1.3 — Gossip broadcast is fire-and-forget
- **File:Line** — `crates/shoal-engine/src/node.rs:554-559`
- **Severity** — 🟡 Minor
- **Description** — All metadata broadcasts (log entries, manifests) use `gossip.broadcast_payload()` which is fire-and-forget. If the broadcast fails, a `warn!` is logged but the write still succeeds. The periodic `sync_log_from_peers()` provides eventual consistency, but there's no bounded convergence guarantee — a node that missed a gossip message won't see the update until the next sync cycle.
- **Recommendation** — This is acceptable for the current architecture. Ensure `sync_log_from_peers()` runs on a bounded interval (it does in the daemon).

#### Finding 1.4 — `compute_delta` BFS stops at peer tips but peer may have pruned ancestors
- **File:Line** — `crates/shoal-logtree/src/tree.rs:387-471`
- **Severity** — 🟡 Minor
- **Description** — The delta computation uses the peer's tips as a termination condition for BFS traversal. If the peer has pruned entries before a snapshot (via `prune_before`), the delta may include entries whose parents are in the pruned region. The `apply_sync_entries` method at line 604-621 handles this correctly by only validating in-delta parents, treating out-of-delta parent references as boundary entries. This is a correct design.
- **Recommendation** — No change needed. The current relaxed parent validation for sync entries is the right approach for a prunable DAG.

#### Finding 1.5 — Membership uses direct ping, not indirect ping
- **File:Line** — `crates/shoal-cluster/src/membership.rs:119-150`
- **Severity** — 🟠 Major
- **Description** — The `PeerManager` pings peers directly via QUIC `request_response`. Unlike SWIM protocol (which the plan references via `foca`), there is no indirect ping through a third node to distinguish "peer is dead" from "network path to peer is broken." This means asymmetric network failures (A cannot reach B, but C can reach both) will cause A to mark B as dead, triggering unnecessary shard repairs.
- **Recommendation** — Implement indirect ping: when direct ping fails N times, ask a random healthy peer to ping the suspect node on your behalf before declaring it dead.

### 2. Storage Engine Integrity

#### Finding 2.1 — FileStore: No fsync before rename (DATA LOSS)
- **File:Line** — `crates/shoal-store/src/file_store.rs:69-75`
- **Severity** — 🔴 Critical
- **Description** — The write path uses `tokio::fs::write()` then `tokio::fs::rename()` without calling `fsync()` on the file or the parent directory. On power loss, the kernel may have committed the rename to the journal but not the file contents, resulting in a valid filename pointing to zero-filled or partial data. The verify-on-read mechanism (`file_store.rs:84-95`) catches this as `CorruptShard`, but the data is irrecoverably lost — it has already been acknowledged to the writer.
- **Recommendation** —
```rust
// After write, before rename:
let file = tokio::fs::File::open(&tmp_path).await?;
file.sync_all().await?;
tokio::fs::rename(&tmp_path, &path).await?;
// Fsync parent directory:
let parent = tokio::fs::File::open(path.parent().unwrap()).await?;
parent.sync_all().await?;
```

#### Finding 2.2 — Manifest ObjectId not verified on deserialization
- **File:Line** — `crates/shoal-cas/src/manifest.rs:87-97`
- **Severity** — 🟠 Major
- **Description** — `deserialize_manifest()` accepts any manifest without verifying that the embedded `object_id` matches `blake3(ManifestContent)`. A corrupted or tampered manifest in Fjall or received from a peer would be silently accepted. The system claims "content-addressing everywhere" and "verify-on-read" but manifests are not covered.
- **Recommendation** — Add `verify_manifest()` that recomputes `ObjectId` from `ManifestContent` and compares. Call it in `deserialize_manifest()` or at the engine layer on every manifest load.

#### Finding 2.3 — Orphaned temp files accumulate on write failures
- **File:Line** — `crates/shoal-store/src/file_store.rs:67-75`
- **Severity** — 🟡 Minor
- **Description** — If `tokio::fs::write()` succeeds but `tokio::fs::rename()` fails, the `.tmp.{N}` file persists on disk indefinitely. Over time, failed writes leak disk space.
- **Recommendation** — Use a Drop guard or `finally` block to clean up the temp file on error.

#### Finding 2.4 — Shard data not garbage-collected on object deletion
- **File:Line** — `crates/shoal-engine/src/node.rs:774-837`
- **Severity** — 🟡 Minor
- **Description** — `delete_object()` removes the key mapping and manifest reference but explicitly does NOT delete the underlying shard data. Comment at line 777: "a background GC pass would clean up orphaned shards (post-milestone optimization)." This means deleted objects continue consuming storage until manually cleaned.
- **Recommendation** — Implement background shard GC that scans for shards not referenced by any manifest.

### 3. Concurrency & Safety

#### Finding 3.1 — Connection pool holds mutex during QUIC handshake
- **File:Line** — `crates/shoal-net/src/transport.rs:127-148`
- **Severity** — 🟠 Major
- **Description** — `get_connection()` holds the connection cache mutex for the entire duration of `self.endpoint.connect()`, which performs a full QUIC handshake (potentially including relay negotiation). This blocks ALL concurrent `get_connection()` calls to ANY peer, not just the target peer. If a connect takes 5 seconds (relay fallback, slow peer), all other shard pushes/pulls are stalled.
- **Recommendation** — Use per-peer locking or a concurrent map. Alternatively, release the lock after cache miss, connect without the lock, then re-acquire and check before inserting (double-check pattern). The comment at lines 33-37 explains why a global mutex was chosen (to prevent duplicate connections), but the blast radius is too wide.

#### Finding 3.2 — `expect("lock poisoned")` throughout codebase
- **File:Line** — `crates/shoal-store/src/memory_store.rs:45,71,76,86,91,107` and `crates/shoal-engine/src/node.rs:265,322,336,522`
- **Severity** — 🟡 Minor
- **Description** — At least 10 call sites use `expect("lock poisoned")` on `std::sync::Mutex` and `RwLock`. If any task panics while holding a lock, ALL subsequent operations on that store will panic, cascading to process-wide crash.
- **Recommendation** — Either use `parking_lot::Mutex` (does not poison) or handle the `PoisonError` by unwrapping the inner value.

#### Finding 3.3 — Zero `unsafe` in all library crates (except justified libc call)
- **File:Line** — `crates/shoal-store/src/file_store.rs:211-256`
- **Severity** — 🔵 Suggestion (positive finding)
- **Description** — The entire codebase has exactly one `unsafe` block: the `libc::statvfs` call for filesystem capacity reporting. It is well-documented with a safety comment, correctly uses `CString`, and checks the return value. All other crates are safe Rust.

### 4. Error Handling & Resilience

#### Finding 4.1 — `unwrap_or_default()` on serialization silently drops data
- **File:Line** — `crates/shoal-engine/src/node.rs:552,802,905,939,970,994,1033`
- **Severity** — 🟠 Major
- **Description** — Multiple sites serialize `LogEntry` with `postcard::to_allocvec(&log_entry).unwrap_or_default()`. If serialization fails, an empty `Vec<u8>` is broadcast via gossip. Peers receiving this will fail to deserialize (empty bytes), silently dropping the mutation. The entry was already applied locally but never propagated — a silent consistency divergence.
- **Recommendation** — Propagate the error. If `to_allocvec` fails (which is extremely unlikely for a well-formed `LogEntry`), the write should fail rather than silently lose the replication.

#### Finding 4.2 — Erasure decoder fast path silently accepts duplicate indices
- **File:Line** — `crates/shoal-erasure/src/decoder.rs:60-68`
- **Severity** — 🟠 Major
- **Description** — When `originals.len() == k`, the fast path bypasses Reed-Solomon decoding and concatenates the shard data by index. However, it does not check for duplicate shard indices. If two shards with the same index are provided (e.g., from a buggy repair), the output is silently garbage — wrong data returned to the user with no error.
- **Recommendation** — Add a uniqueness check: `let indices: HashSet<_> = originals.iter().map(|(i, _)| *i).collect(); if indices.len() != k { /* fall through to RS decode */ }`.

#### Finding 4.3 — Partial write failure leaves local shards without remote copies
- **File:Line** — `crates/shoal-engine/src/node.rs:486-523`
- **Severity** — 🟡 Minor
- **Description** — When remote shard pushes fail, they are queued in `pending_pushes` for retry. This is a correct design. The retry mechanism at `node.rs:258-332` is well-implemented: it re-checks ring ownership (in case the ring changed), re-resolves addresses, and re-queues on failure. The writer keeps a local copy as a fallback.

#### Finding 4.4 — `ErasureEncoder::new(0, m)` causes panic, not error
- **File:Line** — `crates/shoal-erasure/src/encoder.rs:38-39`
- **Severity** — 🟡 Minor
- **Description** — The encoder constructor accepts `k=0` without validation. This causes a division-by-zero panic at `encode()` time (`chunk.len().div_ceil(self.k)`).
- **Recommendation** — Add validation: `assert!(k >= 1, "k must be at least 1")` or return `Result`.

### 5. Security

#### Finding 5.1 — Admin endpoints are unauthenticated
- **File:Line** — `crates/shoal-s3/src/lib.rs:174-185`
- **Severity** — 🔴 Critical
- **Description** — `POST /admin/keys`, `GET /admin/keys`, `DELETE /admin/keys/{id}`, and `POST /admin/buckets/{name}` have NO authentication middleware applied. Any network-reachable client can create API keys, list all key IDs, delete keys, and create buckets. The code even has a TODO comment at line 175: "gate behind admin_secret once we have a proper admin UI / bootstrap flow."
- **Recommendation** — Gate admin routes behind `Authorization: Bearer <admin_secret>` immediately. The admin secret should be configured via environment variable or config file, not hardcoded.

#### Finding 5.2 — Multipart upload parts stored entirely in memory
- **File:Line** — `crates/shoal-s3/src/handlers.rs:545` and `crates/shoal-s3/src/lib.rs:64`
- **Severity** — 🟠 Major
- **Description** — `upload.parts.insert(part_number, body.to_vec())` stores each uploaded part as a `Vec<u8>` in a `HashMap` held in an `RwLock`. There is no limit on the number of concurrent multipart uploads or total memory consumed. An attacker can initiate thousands of multipart uploads and upload large parts to exhaust server memory. The 5 GiB body limit (`DefaultBodyLimit::max(5 * 1024 * 1024 * 1024)`) applies per-request, but accumulation across uploads is unbounded.
- **Recommendation** — Add limits: max concurrent uploads per bucket, max total memory for in-flight parts, and an upload expiration timer.

#### Finding 5.3 — API key generation uses modular bias
- **File:Line** — `crates/shoal-s3/src/handlers.rs:50`
- **Severity** — 🟡 Minor
- **Description** — `(rng.next_u32() as usize) % ALPHA_NUMERIC.len()` introduces modular bias since `2^32 % 36 != 0`. With 36 characters and u32, the bias is ~0.00000084% per character — negligible in practice but not cryptographically ideal.
- **Recommendation** — Use `rng.random_range(0..ALPHA_NUMERIC.len())` from the `rand` crate for unbiased sampling. This is not urgent — the access key ID is not a secret.

#### Finding 5.4 — SigV4 implementation is solid
- **File:Line** — `crates/shoal-s3/src/auth.rs:221-298`
- **Severity** — 🔵 Suggestion (positive finding)
- **Description** — The AWS Signature V4 verification uses `subtle::ConstantTimeEq` for signature comparison (line 289-292), correctly preventing timing attacks. The HMAC chain derivation matches the AWS spec. `UNSIGNED-PAYLOAD` is accepted as the default payload hash, which is standard for S3-compatible servers.

#### Finding 5.5 — No bucket/key name validation
- **File:Line** — `crates/shoal-s3/src/handlers.rs:430-512`
- **Severity** — 🟡 Minor
- **Description** — Bucket names and object keys are passed directly to `MetaStore` and `FileStore` without validation. While `FileStore` uses hex-encoded `ShardId` filenames (preventing path traversal for shard storage), the metadata store keys use raw bucket/key strings. Bucket names like `../../etc` or keys with null bytes could cause issues with Fjall or downstream systems.
- **Recommendation** — Validate bucket names against S3 naming rules (3-63 chars, lowercase, no consecutive dots) and reject keys containing null bytes.

#### Finding 5.6 — API key secrets sent over plaintext QUIC
- **File:Line** — `crates/shoal-net/src/transport.rs:568-601`
- **Severity** — 🟠 Major
- **Description** — `pull_api_keys()` transmits `(access_key_id, secret_access_key)` pairs over QUIC streams. While QUIC is encrypted (TLS 1.3), the API key secrets are serialized in plaintext within the application-layer message. Any node in the cluster can request any API key secret from any other node — there is no authorization check on who can pull secrets.
- **Recommendation** — Restrict API key pull to the `ShoalMessage::ApiKeyRequest` handler on the receiving side. Consider encrypting secrets at the application layer or restricting which nodes can request keys (e.g., only during join).

### 6. Performance

#### Finding 6.1 — CDC chunking copies all chunk data
- **File:Line** — `crates/shoal-cas/src/cdc_chunker.rs:103`
- **Severity** — 🟡 Minor
- **Description** — `Bytes::copy_from_slice(chunk_data)` copies each chunk from the input buffer. Since FastCDC provides `(offset, length)` pairs into the original slice, a zero-copy approach using `Bytes::from(data_bytes).slice(offset..offset+length)` would avoid ~1x total data copying.
- **Recommendation** — Accept `Bytes` input and use `Bytes::slice()` for zero-copy chunking in the hot path.

#### Finding 6.2 — Shard fetching is sequential within each chunk
- **File:Line** — `crates/shoal-engine/src/node.rs:630-718`
- **Severity** — 🟡 Minor
- **Description** — The read path fetches shards one-at-a-time within each chunk: for each shard, it tries local store, then cache, then sequentially iterates through remote owners. With `k+m=6` shards and network latency, this serializes 6 potential network round-trips per chunk. S3-compatible clients expect sub-second GET latency for small objects.
- **Recommendation** — Fetch all shards for a chunk concurrently using `JoinSet`, keeping only the first `k` successful responses.

#### Finding 6.3 — `get_object` loads entire object into memory
- **File:Line** — `crates/shoal-engine/src/node.rs:625`
- **Severity** — 🟡 Minor
- **Description** — `Vec::with_capacity(manifest.total_size as usize)` pre-allocates the entire object in memory. For a 1 GB object, this is a 1 GB allocation. There is no streaming/chunked response path.
- **Recommendation** — For large objects, consider a streaming approach that yields chunks as they are decoded, feeding directly into the HTTP response body.

#### Finding 6.4 — Vnode position computation heap-allocates
- **File:Line** — `crates/shoal-placement/src/ring.rs:391`
- **Severity** — 🔵 Suggestion
- **Description** — `Vec::with_capacity(34)` heap-allocates on every `vnode_position` call. Since this is called `weight` times per `add_node`, a stack-allocated `[u8; 34]` array would eliminate these allocations.

### 7. API Correctness

#### Finding 7.1 — No Range request support (GET partial content)
- **File:Line** — `crates/shoal-s3/src/handlers.rs:603-649`
- **Severity** — 🟠 Major
- **Description** — The `get_object_handler` does not parse the `Range` header or return `206 Partial Content`. All GETs return the full object body. This breaks S3 clients that rely on range requests for resumable downloads, multipart downloads, and partial reads (e.g., Parquet/ORC column access patterns).
- **Recommendation** — Parse `Range: bytes=start-end`, slice the assembled data, and return `206 Partial Content` with `Content-Range` header.

#### Finding 7.2 — ListObjectsV2 has no continuation token
- **File:Line** — `crates/shoal-s3/src/handlers.rs:379-401`
- **Severity** — 🟡 Minor
- **Description** — The `max-keys` parameter is parsed and used to truncate results, and `IsTruncated` is set correctly. However, there is no `continuation-token` support. When `IsTruncated=true`, S3 clients need to send a follow-up request with the continuation token to get the next page. Currently, the client has no way to paginate.
- **Recommendation** — Use the last returned key as the continuation token and filter `list_objects` results starting after that key.

#### Finding 7.3 — Multipart upload ETag format is incorrect
- **File:Line** — `crates/shoal-s3/src/handlers.rs:1019`
- **Severity** — 🟡 Minor
- **Description** — The CompleteMultipartUpload response returns `etag: format!("{object_id}")` without the `-N` suffix. S3 specifies that multipart ETags should be `"hash-partcount"` (e.g., `"abc123-3"`). Some S3 clients check this format to determine if an object was uploaded via multipart.
- **Recommendation** — Return `format!("\"{object_id}-{part_count}\"")`.

#### Finding 7.4 — `PutObject` reads entire body into memory before processing
- **File:Line** — `crates/shoal-s3/src/handlers.rs:435`
- **Severity** — 🟡 Minor
- **Description** — The `body: bytes::Bytes` parameter in the handler signature means axum buffers the entire request body in memory before the handler runs. Combined with the 5 GiB body limit, a single PUT could consume 5 GiB of RAM.
- **Recommendation** — For production, consider streaming the body through CDC chunking without full buffering. The FastCDC algorithm can work on a streaming basis with `StreamCDC`.

#### Finding 7.5 — Upload ID is predictable (sequential counter + blake3)
- **File:Line** — `crates/shoal-s3/src/handlers.rs:201-205`
- **Severity** — 🟡 Minor
- **Description** — `generate_upload_id()` uses `UPLOAD_COUNTER.fetch_add(1)` hashed with blake3. Since blake3 is not a secret, and the counter starts at 1, all upload IDs are deterministic from process start. An attacker could predict future upload IDs and attempt to access or corrupt them.
- **Recommendation** — Include a per-process random seed in the hash input, or use a CSPRNG directly.

### 8. Code Quality

#### Finding 8.1 — Consistent, idiomatic Rust throughout
- **Severity** — 🔵 Suggestion (positive finding)
- **Description** — The codebase consistently uses `thiserror` for errors, `tracing` for logging, `async_trait` for async traits, and derives the correct trait sets for ID types. The `define_id!` macro at `shoal-types/src/lib.rs:22-67` elegantly generates four ID newtypes with all needed traits. Error types are descriptive and contextual.

#### Finding 8.2 — Test coverage is thorough
- **Severity** — 🔵 Suggestion (positive finding)
- **Description** — ~3,100 lines of integration tests cover local pipelines, multi-node write/read, node failure/recovery, rebalancing, stress testing, network partitions, and random kill scenarios. Unit tests exist in every crate. The `shoal-placement` crate alone has 35 unit tests covering every edge case.

#### Finding 8.3 — Repetitive gossip broadcast pattern
- **File:Line** — `crates/shoal-engine/src/node.rs:548-564, 800-813, 903-916, 937-950, 969-982, 993-1005, 1031-1044`
- **Severity** — 🔵 Suggestion
- **Description** — Seven near-identical blocks follow the pattern: serialize log entry → if gossip exists, broadcast, else unicast. This is a candidate for extraction into a helper method.
- **Recommendation** — Extract a `broadcast_log_entry(&self, entry: &LogEntry) -> Result<(), EngineError>` helper.

#### Finding 8.4 — NodeId used as ed25519 public key
- **File:Line** — `crates/shoal-logtree/src/entry.rs:150`
- **Severity** — 🔵 Suggestion
- **Description** — `VerifyingKey::from_bytes(self.node_id.as_bytes())` reconstructs the ed25519 public key from the NodeId bytes. This works because NodeId is derived from the iroh endpoint key, which is also ed25519. This is a strong design choice — the NodeId IS the public key, so there's no separate key management.

---

## Summary

### Top 5 Critical Issues (Ranked by Blast Radius)

1. **🔴 FileStore missing fsync** (`file_store.rs:69-75`) — Silent data loss on power failure. Every acknowledged write is at risk. Blast radius: ALL stored data.

2. **🔴 Admin endpoints unauthenticated** (`shoal-s3/src/lib.rs:174-185`) — Any network-reachable client can create/delete API keys and create buckets. Blast radius: complete security bypass.

3. **🟠 Connection pool mutex blocks all peers during QUIC connect** (`transport.rs:127-148`) — A slow or unreachable peer stalls all concurrent network operations. Blast radius: cluster-wide write/read latency spike.

4. **🟠 Multipart uploads unbounded in memory** (`handlers.rs:545`) — Resource exhaustion DoS vector. Blast radius: OOM crash of the node.

5. **🟠 `unwrap_or_default()` on LogEntry serialization** (`node.rs:552`) — Failed serialization silently drops metadata replication. Blast radius: silent consistency divergence across cluster.

### Distributed Systems Correctness Assessment

The LogTree DAG with HLC timestamps and LWW conflict resolution is a **sound foundation** for eventual consistency. The topological sort (Kahn's algorithm) for sync delta computation is correctly implemented. Signature verification on entry receive prevents forgery. The auto-merge mechanism for multiple tips ensures convergence.

**Key gap**: No quorum writes, no read-repair, no indirect ping for failure detection. The system is designed for AP (availability + partition tolerance) with eventual consistency, which is appropriate for an S3-compatible store. However, the lack of indirect ping means asymmetric network failures cause unnecessary repair storms.

### Data Loss / Corruption Risk Assessment

**Risk: MODERATE-HIGH**

- The missing `fsync` in `FileStore` is the single largest data loss vector. Any power failure can corrupt acknowledged shards.
- Verify-on-read catches corruption but cannot recover the data.
- Manifest ObjectId is not verified on deserialization, allowing corrupted manifests to propagate.
- Erasure coding provides redundancy (k+m shards), but with `shard_replication=1` (default), each shard exists on exactly one node. Power loss on that node loses the shard permanently.

**Mitigating factors**:
- Content-addressing (BLAKE3) at every layer catches most corruption.
- Network transfers verify integrity on receive (`transport.rs:311-323`).
- The repair scheduler prioritizes shards with fewest remaining copies.

### Technical Debt Level: **MEDIUM**

**Priorities (ordered)**:
1. Add fsync to FileStore (Critical — data integrity)
2. Authenticate admin endpoints (Critical — security)
3. Add shard GC on delete (Medium — storage leak)
4. Fix connection pool contention (Medium — latency under load)
5. Add Range request support (Medium — S3 compatibility)
6. Validate manifest ObjectId on load (Medium — integrity)
7. Fix erasure decoder fast-path duplicate index bug (Medium — correctness)
8. Extract gossip broadcast helper (Low — code quality)

### "If I Fix a Single Thing" — The ONE Issue Before Deployment

**Add `fsync` to `FileStore::put()`.**

Without fsync, every shard write is a promise written in pencil. A single power event — and they happen — silently replaces stored data with zeros. The verify-on-read mechanism transforms this from "silent corruption" to "data loss with a good error message," but the data is still gone. In a distributed storage system, acknowledged durability is the bedrock contract. Everything else — erasure coding, repair, replication — assumes the local store actually stores. Fix this first.
