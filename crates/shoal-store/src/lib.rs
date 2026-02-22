//! Shard storage trait and backend implementations.
//!
//! This crate defines the [`ShardStore`] trait for persisting erasure-coded
//! shards, along with two concrete backends:
//!
//! - [`MemoryStore`] — in-memory storage backed by a `RwLock<HashMap>`.
//! - [`FileStore`] — file-based storage with a 2-level fan-out directory layout.

mod error;
mod file_store;
mod memory_store;
mod traits;

pub use error::StoreError;
pub use file_store::FileStore;
pub use memory_store::MemoryStore;
pub use traits::{ShardStore, StorageCapacity};
