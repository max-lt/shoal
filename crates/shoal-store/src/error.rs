//! Error types for shard storage operations.

use shoal_types::ShardId;

/// Errors that can occur during shard storage operations.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The requested shard was not found.
    #[error("shard not found: {0}")]
    NotFound(ShardId),

    /// An I/O error occurred.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// The store has reached its capacity limit.
    #[error("store capacity exceeded: need {needed} bytes, only {available} available")]
    CapacityExceeded {
        /// Bytes needed for the operation.
        needed: u64,
        /// Bytes currently available.
        available: u64,
    },
}
