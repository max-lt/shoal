//! Error types for the metadata store.

/// Errors returned by [`MetaStore`](crate::MetaStore) operations.
#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    /// Fjall database error.
    #[error("fjall error: {0}")]
    Fjall(#[from] fjall::Error),

    /// I/O error (e.g. from Fjall guard operations).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization/deserialization error.
    #[error("serialization error: {0}")]
    Serde(#[from] postcard::Error),
}
