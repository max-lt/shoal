# Real QUIC Transport Tests

## Goal

Reproduce the bugs reported during S3 torture testing that the mock transport tests cannot catch:

- **Large object reads fail**: streaming large objects (1MB+) over QUIC breaks due to shard fetch timeouts or stream ordering issues that don't exist with in-memory mocks.
- **Concurrent writes corrupt state**: multiple simultaneous `put_object` calls on the same node cause QUIC connection pool contention, dropped streams, or interleaved shard pushes — none of which surface with the mock transport's instant, infallible delivery.
- **Cross-node manifest visibility**: after a PUT on node A, GET on node B fails because the ManifestPut broadcast over QUIC didn't arrive or was silently dropped — the mock transport delivers broadcasts synchronously and never loses them.

These are **real QUIC transport layer bugs**. The existing 115 engine tests all use `FailableMockTransport` — an in-memory mock that routes shards directly between `MemoryStore` instances without real networking. The 4 chaos tests add latency/drops/partitions on top, but the mock still bypasses QUIC entirely. To reproduce these bugs, we need tests that use `ShoalTransport` with real iroh QUIC endpoints.

**Constraint**: Do NOT modify any existing tests. Add new test file(s) only.

## Files to Create / Modify

| File | Change |
|------|--------|
| `crates/shoal-engine/src/tests/real_transport.rs` | **New**: `TestProtocolHandler` + `QuicTestCluster` + tests |
| `crates/shoal-engine/src/tests/mod.rs` | Add `mod real_transport;` |
| `crates/shoal-engine/Cargo.toml` | Add `iroh` dev-dep with needed features (if not already sufficient) |

---

## Step 1: `TestProtocolHandler` — Minimal Protocol Handler

The real `ShoalProtocol` lives in the `shoald` binary crate and cannot be imported from `shoal-engine` tests. We need a minimal version that handles the same message types.

Implement `iroh::protocol::ProtocolHandler` for a struct that holds:
- `store: Arc<dyn ShardStore>` — the node's shard store
- `meta: Arc<MetaStore>` — the node's metadata store

Handler logic (mirrors `shoald/src/handler.rs:148-367`):

**Uni streams** (`handle_connection`):
- `ShardPush { shard_id, data }` → `store.put(shard_id, data)`
- `ManifestPut { bucket, key, manifest_bytes }` → deserialize manifest, `meta.put_manifest()`, `meta.put_object_key()`

**Bi streams** (`handle_bi_streams`):
- `ShardRequest { shard_id }` → `store.get(shard_id)` → respond with `ShardResponse`
- `ManifestRequest { bucket, key }` → look up in meta → respond with `ManifestResponse`
- `ManifestSyncRequest` → `meta.list_all_object_entries()` → respond with `ManifestSyncResponse`

Skip: SwimData, LogEntryBroadcast, LogSyncRequest (not needed for these tests).

Uses `ShoalTransport::handle_connection()` and `ShoalTransport::handle_bi_streams()` static methods — same as production code.

---

## Step 2: `QuicTestCluster` — Real Endpoint Setup

```rust
struct QuicTestCluster {
    nodes: Vec<ShoalNode>,
    node_ids: Vec<NodeId>,
    transports: Vec<Arc<ShoalTransport>>,
    _routers: Vec<iroh::protocol::Router>,     // keep alive
    cluster: Arc<ClusterState>,
}
```

Construction (`QuicTestCluster::new(n, chunk_size, k, m)`):

1. For each node, generate a `SecretKey` from deterministic seed
2. Create `iroh::Endpoint` with `RelayMode::Disabled`, bind locally
3. Create `ShoalTransport::from_endpoint_with_alpn(endpoint.clone(), SHOAL_ALPN.to_vec())`
4. Create `MemoryStore`, `MetaStore::open_temporary()`
5. Create `TestProtocolHandler` with store + meta references
6. Create `iroh::protocol::Router::builder(endpoint).accept(SHOAL_ALPN, protocol).spawn()`
7. Wait briefly for endpoints to bind, then collect `EndpointAddr` from each
8. Build `ClusterState` with all nodes, build address book with real addresses
9. Create `ShoalNode` with real `ShoalTransport` as `Arc<dyn Transport>`

**Key detail**: After `Router::spawn()`, use `endpoint.bound_sockets()` to get the local socket addresses. Construct `EndpointAddr` with `EndpointAddr::new(endpoint.id()).with_ip_addr(bound_socket)` so peers have real direct addresses.

**Address book**: Each node's address book maps every *other* node's NodeId → EndpointAddr (with direct socket addresses). This is critical because `RelayMode::Disabled` means there's no relay to discover peers through.

---

## Step 3: Test Scenarios

### 3a. `test_quic_put_get_roundtrip`

Baseline smoke test: 3 nodes, k=2 m=1.
1. PUT 5000-byte object on node 0
2. GET from node 0 → matches
3. GET from node 1 → matches (pulls shards via real QUIC)

Ensures the basic shard push/pull path works over real QUIC before testing the failure-prone scenarios.

### 3b. `test_quic_large_object`

**Reproduces**: large object read failures over QUIC (shard fetch timeouts, stream ordering issues).

4 nodes, k=2 m=2.
1. PUT 1MB object on node 0
2. GET from all 4 nodes → all match
3. Validates: many shards distributed and fetched end-to-end over QUIC without timeouts or stream corruption

### 3c. `test_quic_cross_node_manifest_sync`

**Reproduces**: cross-node manifest visibility failures (PUT on A, GET on B returns 404).

3 nodes, k=2 m=1.
1. PUT on node 0 (broadcasts ManifestPut to all peers via real QUIC uni streams)
2. Short sleep to let broadcasts arrive
3. GET from node 1 and node 2 → both succeed
4. Validates: ManifestPut broadcast actually arrives over QUIC + shard pull works

### 3d. `test_quic_concurrent_writes`

**Reproduces**: concurrent write corruption (connection pool contention, dropped/interleaved QUIC streams).

3 nodes, k=2 m=1.
1. Spawn 20 concurrent put_object calls on node 0 (different keys)
2. All must succeed
3. Read all 20 keys from node 1 → all match
4. Validates: connection pooling + concurrent QUIC streams don't corrupt or lose data

### 3e. `test_quic_manifest_sync_from_peers`

**Reproduces**: manifest sync failures (ManifestSyncRequest/Response over bi-directional QUIC streams).

4 nodes, k=2 m=2.
1. PUT 5 objects on node 0
2. On node 3, call `sync_manifests_from_peers()`
3. GET all 5 from node 3 → all match
4. Validates: bulk manifest sync over real QUIC bi-streams works correctly

---

## Critical Files Reference

- **Transport trait**: `crates/shoal-net/src/lib.rs:27-71`
- **ShoalTransport**: `crates/shoal-net/src/transport.rs` — `handle_connection()`, `handle_bi_streams()`, `send_on_stream()`, `recv_message()`, `from_endpoint_with_alpn()`
- **Production handler**: `crates/shoald/src/handler.rs:148-367` — reference for TestProtocolHandler
- **ShoalNode**: `crates/shoal-engine/src/node.rs` — `new()`, `with_transport()`, `with_address_book()`, `put_object()`, `get_object()`, `sync_manifests_from_peers()`
- **Router setup**: `crates/shoald/src/main.rs:477-480` — `Router::builder(endpoint).accept(alpn, protocol).spawn()`
- **Existing TestCluster**: `crates/shoal-engine/src/tests/networked.rs:218-423` — reference for cluster setup pattern
- **SHOAL_ALPN**: `crates/shoal-net/src/lib.rs:74` — `b"shoal/0"`

## Verification

```bash
# Run only real transport tests
cargo test -p shoal-engine real_transport

# Full suite (existing tests must still pass)
cargo test -p shoal-engine
cargo test

# Lint
cargo clippy -- -D warnings
cargo fmt
```
