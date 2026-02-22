# Shoal — Distributed Object Storage Engine

100% pure Rust. Zero C bindings. See @plan.md for full architecture and implementation plan.

## Build & Test

```bash
cargo build                          # build everything
cargo test                           # all tests
cargo test -p shoal-types            # single crate
cargo test --test local_pipeline     # single integration test
cargo clippy -- -D warnings          # lint (must pass, zero warnings)
cargo fmt --check                    # format check
```

IMPORTANT: Run `cargo clippy -- -D warnings` and `cargo fmt --check` before considering any milestone complete.

## Workspace Layout

Rust monorepo. All crates live in `crates/`. Integration tests in `tests/`. Benches in `benches/`.

```
crates/
  shoal-types/       Shared types, IDs (ShardId, ChunkId, ObjectId, NodeId)
  shoal-store/       ShardStore trait + MemoryStore, FileStore backends
  shoal-cas/         Content addressing, chunking, manifests
  shoal-meta/        Metadata persistence (wraps Fjall)
  shoal-erasure/     Reed-Solomon erasure coding (wraps reed-solomon-simd)
  shoal-placement/   Consistent hashing ring (hand-written, ~150 lines)
  shoal-cluster/     Membership (foca SWIM) + gossip (iroh-gossip)
  shoal-repair/      Auto-repair, rebalancing, throttling
  shoal-net/         Network protocol on iroh QUIC
  shoal-engine/      Node orchestrator, write/read pipelines
  shoal-s3/          S3-compatible HTTP API (axum)
  shoal-cli/         Binary entrypoint
```

## Code Style

- `thiserror` for error types in library crates, `anyhow` only in `shoal-cli`
- `tracing` for all logging, with structured fields. No `println!`
- Serialization: `postcard` + `serde` for wire format and persistence
- All IDs are `[u8; 32]` newtypes. Use `blake3` for hashing
- Async: `tokio` runtime. Traits use `async_trait` or `impl Future` where possible
- Keep functions small. Prefer composition over deep nesting
- Every public type and function gets a doc comment

## Dependencies (all pure Rust)

| Purpose        | Crate                                             |
| -------------- | ------------------------------------------------- |
| Hashing        | `blake3`                                          |
| Metadata DB    | `fjall` v3 (LSM-tree)                             |
| Erasure coding | `reed-solomon-simd` v3                            |
| Networking     | `iroh` 0.35 (stable)                              |
| Gossip         | `iroh-gossip` (version compatible with iroh 0.35) |
| Membership     | `foca` (SWIM protocol)                            |
| Serialization  | `postcard`, `serde`                               |
| HTTP           | `axum`                                            |
| Async          | `tokio`                                           |

IMPORTANT: Never add a dependency that requires C/C++ compilation or `cc` build script. If unsure, check the dep's build.rs before adding.

## Workflow

This project is built milestone by milestone. See @SHOAL.md for the full plan with checkboxes.

1. Implement only the milestone you are asked to work on
2. After completing a milestone, run ALL tests for the affected crates
3. Run `cargo clippy -- -D warnings` — must be clean
4. Run `cargo fmt` to format
5. Do NOT move to the next milestone unless explicitly asked

## Testing Conventions

- Unit tests: in the same file, under `#[cfg(test)] mod tests`
- Integration tests: in `tests/integration/`
- Use `tempfile` crate for tests that need filesystem
- Use `tokio::test` for async tests
- Name tests descriptively: `test_put_get_roundtrip`, `test_decode_with_missing_shards`
- Every milestone has explicit test requirements in SHOAL.md — implement ALL of them

## Common Patterns

### ID types

```rust
#[derive(Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct ShardId([u8; 32]);

impl ShardId {
    pub fn from_data(data: &[u8]) -> Self {
        Self(blake3::hash(data).into())
    }
}
```

### Error types per crate

```rust
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("shard not found: {0}")]
    NotFound(ShardId),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
```

### ShardStore trait pattern

```rust
#[async_trait::async_trait]
pub trait ShardStore: Send + Sync {
    async fn put(&self, id: ShardId, data: &[u8]) -> Result<(), StoreError>;
    async fn get(&self, id: ShardId) -> Result<Option<Vec<u8>>, StoreError>;
    // ...
}
```

## Things to Avoid

- No `unwrap()` in library code. Use proper error propagation
- No `println!`. Use `tracing::{info, debug, warn, error}`
- No `unsafe` unless absolutely necessary and documented why
- No C dependencies. Ever
- No premature optimization. Correctness first, benchmark later
- Do not implement milestones out of order
