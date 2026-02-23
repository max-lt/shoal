//! Error types for the LogTree crate.

/// Errors that can occur during LogTree operations.
#[derive(Debug, thiserror::Error)]
pub enum LogTreeError {
    /// Entry hash verification failed.
    #[error("invalid entry hash")]
    InvalidHash,

    /// Entry signature verification failed.
    #[error("invalid entry signature")]
    InvalidSignature,

    /// Entry references unknown parent hashes.
    #[error("missing parent entries: {0:?}")]
    MissingParents(Vec<[u8; 32]>),

    /// Storage backend error.
    #[error("storage error: {0}")]
    Storage(String),

    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Snapshot not found.
    #[error("snapshot not found: {0}")]
    SnapshotNotFound(String),
}

impl From<postcard::Error> for LogTreeError {
    fn from(e: postcard::Error) -> Self {
        Self::Serialization(e.to_string())
    }
}
