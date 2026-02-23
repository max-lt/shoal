//! Error types for the engine.

/// Errors that can occur during engine operations.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// Failed to access the metadata store.
    #[error("metadata error: {0}")]
    Meta(#[from] shoal_meta::MetaError),

    /// Failed to access the shard store.
    #[error("store error: {0}")]
    Store(#[from] shoal_store::StoreError),

    /// Content addressing / chunking error.
    #[error("cas error: {0}")]
    Cas(#[from] shoal_cas::CasError),

    /// Erasure coding error.
    #[error("erasure error: {0}")]
    Erasure(#[from] shoal_erasure::ErasureError),

    /// Network transport error.
    #[error("network error: {0}")]
    Net(#[from] shoal_net::NetError),

    /// Repair subsystem error.
    #[error("repair error: {0}")]
    Repair(#[from] shoal_repair::RepairError),

    /// LogTree error.
    #[error("logtree error: {0}")]
    LogTree(#[from] shoal_logtree::LogTreeError),

    /// No LogTree configured.
    #[error("no log tree configured")]
    NoLogTree,

    /// No transport configured.
    #[error("no transport configured")]
    NoTransport,

    /// Object not found.
    #[error("object not found: bucket={bucket}, key={key}")]
    ObjectNotFound {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
    },

    /// Not enough nodes in the cluster for the configured erasure coding.
    #[error("not enough nodes: need {needed} (k+m), have {available}")]
    NotEnoughNodes {
        /// Minimum nodes required (k + m).
        needed: usize,
        /// Nodes currently available.
        available: usize,
    },

    /// Failed to reconstruct object data — not enough shards.
    #[error("read failed: not enough shards for chunk {chunk_index}: need {needed}, found {found}")]
    ReadFailed {
        /// Which chunk couldn't be read.
        chunk_index: usize,
        /// How many shards were needed.
        needed: usize,
        /// How many were found.
        found: usize,
    },
}
