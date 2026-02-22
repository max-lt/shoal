//! Error types for repair operations.

use shoal_types::ShardId;

/// Errors that can occur during repair operations.
#[derive(Debug, thiserror::Error)]
pub enum RepairError {
    /// Failed to access the metadata store.
    #[error("metadata error: {0}")]
    Meta(#[from] shoal_meta::MetaError),

    /// Failed to access the shard store.
    #[error("store error: {0}")]
    Store(#[from] shoal_store::StoreError),

    /// Failed to perform erasure decoding.
    #[error("erasure error: {0}")]
    Erasure(#[from] shoal_erasure::ErasureError),

    /// Failed to transfer a shard over the network.
    #[error("network error: {0}")]
    Net(#[from] shoal_net::NetError),

    /// Not enough shards available for repair (fewer than k surviving copies).
    #[error("not enough shards for repair of {shard_id}: need {needed}, found {found}")]
    InsufficientShards {
        /// The shard that needs repair.
        shard_id: ShardId,
        /// How many shards are needed (k).
        needed: usize,
        /// How many were found.
        found: usize,
    },

    /// Repair was blocked by the circuit breaker.
    #[error("repair suspended by circuit breaker: {reason}")]
    CircuitBreakerOpen {
        /// Why the circuit breaker tripped.
        reason: String,
    },

    /// No manifest found for the chunk containing this shard.
    #[error("no manifest found for shard {0}")]
    ManifestNotFound(ShardId),
}
