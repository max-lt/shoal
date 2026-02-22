//! Core trait and types for shard storage.

use bytes::Bytes;
use shoal_types::ShardId;

use crate::error::StoreError;

/// Capacity information for a storage backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageCapacity {
    /// Total capacity in bytes.
    pub total_bytes: u64,
    /// Bytes currently used.
    pub used_bytes: u64,
    /// Bytes available for new data.
    pub available_bytes: u64,
}

/// Trait for storing and retrieving erasure-coded shards.
///
/// All implementations must be `Send + Sync` for use across async tasks.
/// Data is passed as [`Bytes`] to enable zero-copy transfers through the pipeline.
#[async_trait::async_trait]
pub trait ShardStore: Send + Sync {
    /// Store a shard with the given ID.
    async fn put(&self, id: ShardId, data: Bytes) -> Result<(), StoreError>;

    /// Retrieve a shard by ID. Returns `None` if not found.
    async fn get(&self, id: ShardId) -> Result<Option<Bytes>, StoreError>;

    /// Delete a shard by ID.
    async fn delete(&self, id: ShardId) -> Result<(), StoreError>;

    /// Check whether a shard exists.
    async fn contains(&self, id: ShardId) -> Result<bool, StoreError>;

    /// List all stored shard IDs.
    async fn list(&self) -> Result<Vec<ShardId>, StoreError>;

    /// Report current storage capacity.
    async fn capacity(&self) -> Result<StorageCapacity, StoreError>;

    /// Verify shard integrity by re-hashing and comparing to the ID.
    async fn verify(&self, id: ShardId) -> Result<bool, StoreError>;
}
