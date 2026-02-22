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

    /// Shard data on disk does not match its content-addressed ID.
    ///
    /// The shard is treated as missing (not returned to the caller) and
    /// should be enqueued for repair.
    #[error("shard corruption detected: expected {expected}, actual hash {actual}")]
    CorruptShard {
        /// The ID that was requested.
        expected: ShardId,
        /// The ID computed from the data actually on disk.
        actual: ShardId,
    },
}
