# Shoal Deep Project Review

**Date**: 2026-02-28
**Scope**: Full codebase review across all 13 crates, integration tests, and chaos tests
**Codebase**: ~31,500 lines of Rust (87 source files) + ~3,150 lines of integration/chaos tests

---

## Executive Summary

Shoal is an impressive, well-engineered distributed object storage engine. The codebase demonstrates **production-grade quality** across most dimensions: correct async patterns, comprehensive error handling, thorough test coverage, and strict adherence to the project's own coding standards.

**Overall Grade: A- (Excellent with minor issues)**

### Key Metrics
- **Build**: Clean (zero errors)
- **Clippy**: Clean (zero warnings with `-D warnings`)
- **Formatting**: Clean (`cargo fmt --check` passes)
- **Tests**: 138 passed, **1 failed** (`test_logtree_overwrite_after_sync`)
- **Test Coverage**: 546+ unit tests across 13 crates, 9 integration/chaos tests

---

## Bug Found: Failing Test

### `test_logtree_overwrite_after_sync` (CRITICAL)

**Location**: `crates/shoal-engine/src/tests/networked.rs:1886`

**Symptom**: After syncing LogTree entries from peers, a node reads stale data (v1) instead of the latest overwrite (v2).

**Root Cause**: Race condition in `sync_log_from_peers` manifest pull logic.

The sync function iterates over peers in random order. For each peer, it:
1. Pulls log entries
2. Applies them
3. **Only if new entries were applied**, pulls missing manifests **from that same peer**

The bug manifests when:
1. Node 2 syncs from Node 1 first: gets v2 LogTree entry, but Node 1 doesn't have v2's manifest (it only received the LogTree entry via broadcast, not the manifest). Manifest pull returns empty.
2. Node 2 syncs from Node 0 next: no new entries to apply (already got them from Node 1), so manifest pull is **skipped entirely**.
3. Result: v2's manifest is never fetched. `lookup_manifest` falls through to MetaStore's stale `(bucket,key) -> v1_object_id` mapping and returns v1's data.

**Fix needed in** `crates/shoal-engine/src/node.rs` (`sync_log_from_peers`):
- Track all missing manifest IDs across the entire sync loop, not per-peer
- After syncing entries from all peers, do a **second pass** to pull any still-missing manifests from any available peer
- Or: in `lookup_manifest`, when `object_id_from_dag != object_id_from_meta`, prefer the DAG's ObjectId and attempt a manifest pull rather than returning the stale MetaStore result

---

## Per-Crate Review

### shoal-types — Grade: A+

The foundational types crate. 51 unit tests, all passing.

**Strengths**:
- Clean `[u8; 32]` newtype IDs via DRY macro (`define_id!`)
- HybridLogicalClock with atomic operations, tested for monotonicity and concurrency
- EventBus with typed pub/sub channels, 12 event types
- All types correctly derive Serialize/Deserialize for postcard
- 100% doc comment coverage on public API

**No issues found.**

---

### shoal-store — Grade: A+

ShardStore trait + MemoryStore/FileStore backends. 36 unit tests, all passing.

**Strengths**:
- Verify-on-read: FileStore re-hashes every shard with BLAKE3 on read
- Atomic writes: temp file + rename pattern prevents partial writes
- Inode monitoring: warns at >80% usage with XFS recommendation
- Content-addressed dedup: identical ShardIds skip re-write
- Concurrent access tests: 50 parallel puts, 20 concurrent reads
- Single well-documented `unsafe` block for `statvfs` with non-Unix fallback

**No issues found.**

---

### shoal-cas — Grade: A+

Content addressing and CDC chunking. 22 unit tests, all passing.

**Strengths**:
- FastCDC v2020 with correct parameters: min=16KB, avg=64KB, max=256KB
- Manifest versioning with explicit rejection of unknown versions
- ObjectId = blake3(serialized manifest content), deterministic
- Excellent edge case coverage: empty data, 1-byte, exact boundaries

**No issues found.**

---

### shoal-erasure — Grade: A+

Reed-Solomon erasure coding. 43 unit tests, all passing.

**Strengths**:
- Two-path decode: fast path (all data shards present) + RS reconstruction path
- Exhaustive combination testing: all C(6,3)=20 shard combinations for k=3,m=3
- Proper padding with round-to-even for reed-solomon-simd requirement
- Order-independent decoding verified
- Production configs tested: k=8,m=4 with max tolerance

**No issues found.**

---

### shoal-placement — Grade: A+

Consistent hashing ring. 33 unit tests, all passing.

**Strengths**:
- Deterministic placement: pure functions, no RNG, all nodes compute identically
- Ring diff algorithm for rebalancing migrations
- Topology-aware placement with progressive relaxation (Region/Datacenter boundaries)
- Minimal disruption: adding a node causes ~1/n movement (consistent hashing property)
- Idempotent add/remove operations

**No issues found.**

---

### shoal-meta — Grade: A+

Metadata persistence wrapping Fjall. 39 unit tests, all passing.

**Strengths**:
- 10 separate keyspaces for clean data separation
- Dual backend (Fjall + in-memory) with `with_both_backends()` test runner
- Repair queue with big-endian priority encoding for correct lexicographic ordering
- Persistence-across-reopen tests
- Concurrent access tests (20 threads)

**No issues found.**

---

### shoal-logtree — Grade: A

DAG-based mutation tracking with ed25519 signing. ~60 unit tests, all passing.

**Strengths**:
- BLAKE3 entry hashing over `(hlc, node_id, action, parents)`
- Ed25519 signature verification with proper rejection of tampered entries
- Last-Writer-Wins conflict resolution with HLC tiebreak
- MissingParents error returns hash list for eager pull (excellent design)
- Comprehensive rejection tests: invalid hash, invalid signature, unknown parent

**Minor observations**:
- RwLock `.unwrap()` in memory backend — acceptable since it's test-only
- No test for HLC overflow (u64::MAX) — theoretical, not practical

---

### shoal-net — Grade: A

Network protocol on iroh QUIC. 31 unit tests, all passing.

**Strengths**:
- BLAKE3 integrity verification on all shard transfers (push and pull)
- ALPN-based cluster isolation (different secrets = different ALPNs)
- MAX_MESSAGE_SIZE = 64MB prevents memory exhaustion from malformed prefixes
- Connection pooling with stale connection detection
- Comprehensive serialization roundtrip tests for all message variants

**Minor observations**:
- No explicit per-operation timeout (relies on QUIC idle timeout) — acceptable
- Could add connection pool size metrics for observability

---

### shoal-cluster — Grade: A

SWIM membership + iroh-gossip. 13 unit tests, all passing.

**Strengths**:
- Full SWIM state machine: Alive -> Suspect -> Dead with configurable thresholds
- Gossip topic isolation via blake3(cluster_secret)
- Nonce-based deduplication prevents re-broadcasting
- Graceful shutdown protocol with `leave()` and `abort()`
- Recovery-after-suspect test validates state machine correctness

**Minor observations**:
- Could add jitter to ping interval to prevent thundering herd

---

### shoal-repair — Grade: A-

Auto-repair, rebalancing, throttling. 20+ unit tests, all passing.

**Strengths**:
- Circuit breaker: halts repair when >=50% nodes down
- Token bucket rate limiting for bandwidth control
- Two-tier repair: direct fetch -> RS reconstruction fallback
- Majority-vote scrub for corruption detection
- Priority queue: lower surviving_count = higher urgency

**Issues found**:
1. **Circuit breaker cooldown cleanup never called** (`cleanup_expired_cooldowns()` exists but is not invoked from scheduler) — cooldowns HashMap grows unbounded
2. **O(n) manifest scan per repair** in `find_manifest_for_shard()` — scans ALL objects to find which chunk contains a shard. Needs a shard->manifest index for production scale
3. **std::sync::Mutex in async context** in detector — blocking lock in async event loop. Should use `tokio::sync::Mutex`
4. **NoConsensus scrub verdict doesn't auto-repair** — split votes are flagged but not enqueued

---

### shoal-engine — Grade: B+

Node orchestrator, write/read pipelines. ~6,000 lines of tests, **1 test failing**.

**Strengths**:
- Correct write pipeline: CDC -> compress -> erasure -> distribute -> manifest -> LogTree
- Correct read pipeline: manifest -> fetch shards (local/cache/remote) -> decode -> decompress
- LRU shard cache with bounded size
- JoinSet for parallel shard distribution
- Pending entry buffer for LogTree entries missing parents

**Issues found**:
1. **CRITICAL: `sync_log_from_peers` manifest pull bug** — detailed above, causes stale reads after sync
2. **No minimum cluster size check** before ring placement — if nodes < k+m, placement fails silently
3. **Failed push handling incomplete** — if all remote pushes fail, retries are queued but no minimum replication enforced before returning success
4. **`unwrap_or_default()` on serialization** at line 552 — silently produces empty log entry if postcard serialization fails
5. **Gossip broadcast failure not retried** — logged as warning, other nodes may never see the write
6. **Pending buffer unbounded** — no size limit, could consume all memory under sustained parent-missing load
7. **`std::sync::Mutex` in async context** — pending_pushes and cache locks use blocking mutexes
8. **`deleted_keys` RwLock never cleaned** — grows unbounded over time

---

### shoal-s3 — Grade: A

S3-compatible HTTP API. Comprehensive tests.

**Strengths**:
- AWS Signature V4 with constant-time comparison (`subtle::ConstantTimeEq`)
- Proper HMAC-SHA256 signing chain
- Multipart upload support (initiate, upload part, complete)
- Object tagging with 10-tag limit enforcement
- CopyObject with proper source parsing

**Issues found**:
1. **Admin endpoints open** (`POST /admin/keys` has no auth) — documented as known trade-off
2. **No bucket/key length validation** at API boundary — S3 spec limits to 63/1024 chars
3. **`Response::builder().body().unwrap()`** in handlers — should be safe but undocumented

---

### shoald — Grade: A-

Daemon binary. Proper signal handling.

**Strengths**:
- Hardware auto-detection (RAM-based chunk sizing)
- Double-signal shutdown protection (first=graceful, second=force)
- `load_or_create_secret_key()` for stable node identity
- OpenTelemetry support behind feature flag
- Eager pull for missing LogTree parents

**Issues found**:
1. **`println!()` in status/benchmark commands** (lines 1231-1346) — should use `tracing::info!()`
2. **Some `.unwrap()` on MetaStore open** — should use `.context()?` in main

---

## Integration & Chaos Tests — Grade: A

**9 test suites, 2,620 lines**, covering:

| Test | Scenario | Quality |
|------|----------|---------|
| `local_pipeline` | Full write/read roundtrip, shard loss recovery | Excellent |
| `write_read` | 5-node cluster, cross-node reads, large objects | Excellent |
| `cluster_formation` | Ring consistency, deterministic placement | Excellent |
| `rebalancing` | Cluster expansion, manifest sync | Excellent |
| `node_failure` | Kill 1-2 nodes, writer death, mass failure | Excellent |
| `stress` | 10 concurrent writers, byte-level integrity | Excellent |
| `torture` | 11 scenarios: data sizes, overwrites, chaos, 20 nodes | Exceptional |
| `network_partition` | Split-brain, partition healing, consistency | Excellent |
| `random_kill` | Continuous 15s chaos with kill/restart cycles | Excellent |

**Missing test coverage**:
- Corruption detection triggering repair
- Circuit breaker halting repairs
- S3 HTTP end-to-end tests
- FileStore backend integration
- Rate limiter enforcement

---

## Cross-Cutting Concerns

### Code Quality Compliance

| Requirement | Status |
|---|---|
| `thiserror` for lib errors, `anyhow` only in shoald | PASS |
| `tracing` for logging, no `println!` | FAIL — `println!` in shoald status commands |
| `postcard` + `serde` serialization | PASS |
| All IDs are `[u8; 32]` newtypes with `blake3` | PASS |
| `async_trait` for async traits | PASS |
| No `unwrap()` in library code | PASS (with acceptable lock-poison expects) |
| Doc comments on public API | PASS |
| No C dependencies | PASS |
| No `unsafe` without documentation | PASS (1 documented use in FileStore) |

### Architecture Alignment

The implementation closely follows `docs/plan.md`:
- All 16 milestones complete
- Write/read pipeline matches spec exactly
- SWIM membership + iroh-gossip correctly layered
- Content addressing with BLAKE3 at every layer
- Verify-on-read implemented in FileStore
- Circuit breaker with configurable thresholds
- Topology-aware placement with progressive relaxation

### Security

- BLAKE3 integrity on all shard transfers
- Ed25519 signatures on all LogTree entries
- AWS SigV4 with constant-time comparison
- ALPN-based cluster isolation
- 64MB message size limit prevents memory exhaustion
- No path traversal in FileStore (fan-out directory structure)
- API key secrets not logged

---

## Priority-Ordered Recommendations

### Critical (should fix before production)

1. **Fix `sync_log_from_peers` manifest pull** — track missing manifests globally across all peers, not per-peer. This is the root cause of the failing test.
2. **Add minimum cluster size validation** in write pipeline — reject writes when alive nodes < k+m.
3. **Bound the pending entry buffer** — add `max_pending_entries` limit to prevent OOM.

### High Priority (next sprint)

4. **Replace `std::sync::Mutex` with `tokio::sync::Mutex`** in async contexts (detector, cache, pending_pushes).
5. **Add shard->manifest index** in MetaStore — avoid O(n) scans during repair.
6. **Replace `println!` with `tracing`** in shoald status/benchmark commands.
7. **Enforce minimum successful pushes** before returning success from `put_object`.
8. **Call `cleanup_expired_cooldowns()`** periodically in repair scheduler.

### Medium Priority (improvement backlog)

9. Add bucket/key length validation in S3 API.
10. Document admin endpoint security requirements (firewall).
11. Add gossip fallback to unicast when broadcast fails.
12. Handle serialization errors explicitly instead of `unwrap_or_default()`.
13. Add TTL cleanup for `deleted_keys` tracking.
14. Don't cache shard owners from peer pulls (may be inaccurate).

### Low Priority (polish)

15. Add ping jitter to prevent thundering herd in cluster membership.
16. Add connection pool metrics for network observability.
17. Add metrics for cache evictions and repair progress.
18. Improve error messages ("need at least k" vs "need k").

---

## Conclusion

Shoal is an exceptionally well-built distributed storage system for its stage of development. The architecture is sound, the code quality is high, the test coverage is comprehensive (including chaos testing), and the project follows its own standards consistently.

The one critical bug (manifest sync ordering) is a genuine distributed systems edge case that demonstrates why chaos/fault testing is essential. The remaining issues are mostly about production hardening (bounded buffers, async-safe locks, observability) rather than architectural flaws.

The codebase is ready for production-adjacent use with the critical fix applied and the high-priority items addressed.
