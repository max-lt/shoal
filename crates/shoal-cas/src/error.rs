//! Error types for content addressing operations.

/// Errors that can occur during CAS operations.
#[derive(Debug, thiserror::Error)]
pub enum CasError {
    /// Serialization or deserialization failed.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// An I/O error occurred during streaming.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Manifest has an unsupported version.
    #[error("unsupported manifest version {found}, this node supports version {supported}")]
    UnsupportedVersion {
        /// Version found in the manifest.
        found: u8,
        /// Version this node supports.
        supported: u8,
    },
}
