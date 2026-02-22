//! Error types for network operations.

use shoal_types::ShardId;

/// Errors that can occur during network operations.
#[derive(Debug, thiserror::Error)]
pub enum NetError {
    /// Failed to connect to a remote endpoint.
    #[error("connection error: {0}")]
    Connect(String),

    /// A QUIC connection error.
    #[error("connection error: {0}")]
    Connection(#[from] iroh::endpoint::ConnectionError),

    /// Failed to open a stream.
    #[error("stream open error: {0}")]
    StreamOpen(String),

    /// Error writing to a stream.
    #[error("write error: {0}")]
    Write(#[from] iroh::endpoint::WriteError),

    /// Stream was already closed when trying to finish.
    #[error("stream closed: {0}")]
    ClosedStream(#[from] iroh::endpoint::ClosedStream),

    /// Error reading from a stream.
    #[error("read error: {0}")]
    ReadToEnd(#[from] iroh::endpoint::ReadToEndError),

    /// Error reading exact bytes.
    #[error("read exact error: {0}")]
    ReadExact(#[from] iroh::endpoint::ReadExactError),

    /// Serialization or deserialization failed.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Shard data integrity check failed: blake3 hash mismatch.
    #[error("integrity check failed for shard {expected}: actual hash {actual}")]
    IntegrityFailure {
        /// The expected shard ID.
        expected: ShardId,
        /// The actual hash of the received data.
        actual: ShardId,
    },

    /// The remote endpoint was not found or unreachable.
    #[error("endpoint error: {0}")]
    Endpoint(String),

    /// The stream was closed unexpectedly.
    #[error("stream closed")]
    StreamClosed,
}
