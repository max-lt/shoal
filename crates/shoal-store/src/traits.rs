//! Core trait and types for shard storage.

use bytes::Bytes;
use shoal_types::ShardId;

use crate::error::StoreError;

/// Size of the shard header in bytes:
/// refcount (u32 LE) + payload size (u32 LE) + shard type (u32 LE).
pub const SHARD_HEADER_SIZE: usize = 12;

/// Content-addressed data shard (integrity-verified on read).
pub const SHARD_TYPE_DATA: u32 = 0;

/// Manifest shard keyed by ObjectId (not content-addressed, skip verify).
pub const SHARD_TYPE_MANIFEST: u32 = 1;

/// Capacity information for a storage backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageCapacity {
    /// Total capacity in bytes.
    pub total_bytes: u64,
    /// Bytes currently used.
    pub used_bytes: u64,
    /// Bytes available for new data.
    pub available_bytes: u64,
    /// Total inodes on the filesystem (0 if unavailable).
    pub inodes_total: u64,
    /// Free inodes on the filesystem (0 if unavailable).
    pub inodes_free: u64,
}

/// Trait for storing and retrieving erasure-coded shards.
///
/// All implementations must be `Send + Sync` for use across async tasks.
/// Data is passed as [`Bytes`] to enable zero-copy transfers through the pipeline.
#[async_trait::async_trait]
pub trait ShardStore: Send + Sync {
    /// Store a data shard (type = [`SHARD_TYPE_DATA`]).
    async fn put(&self, id: ShardId, data: Bytes) -> Result<(), StoreError>;

    /// Store a shard with an explicit type.
    ///
    /// Use [`SHARD_TYPE_MANIFEST`] for manifest shards keyed by ObjectId.
    /// The default implementation ignores the type and delegates to [`put`](Self::put).
    async fn put_typed(&self, id: ShardId, data: Bytes, shard_type: u32) -> Result<(), StoreError> {
        let _ = shard_type;
        self.put(id, data).await
    }

    /// Retrieve a shard by ID. Returns `None` if not found.
    ///
    /// For data shards, implementations verify integrity on read.
    /// For manifest shards (type = 1), verification is skipped.
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
    ///
    /// Only meaningful for data shards. Manifest shards will always
    /// return `false` since their key is not `blake3(payload)`.
    async fn verify(&self, id: ShardId) -> Result<bool, StoreError>;

    /// Read the shard type from the header without reading the full payload.
    ///
    /// Returns `None` if the shard does not exist.
    async fn shard_type(&self, id: ShardId) -> Result<Option<u32>, StoreError>;

    /// Increment the reference count for a shard. Returns the new count.
    async fn increment_refcount(&self, id: ShardId) -> Result<u32, StoreError>;

    /// Decrement the reference count for a shard (saturating at 0).
    /// Returns the new count. Caller should delete the shard when it reaches 0.
    async fn decrement_refcount(&self, id: ShardId) -> Result<u32, StoreError>;
}
