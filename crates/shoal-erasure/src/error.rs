//! Error types for erasure coding operations.

/// Errors that can occur during erasure encoding or decoding.
#[derive(Debug, thiserror::Error)]
pub enum ErasureError {
    /// The Reed-Solomon library returned an error.
    #[error("reed-solomon error: {0}")]
    ReedSolomon(#[from] reed_solomon_simd::Error),

    /// Not enough shards were provided for decoding.
    #[error("not enough shards: need {needed}, got {got}")]
    NotEnoughShards {
        /// Minimum shards required (k).
        needed: usize,
        /// Shards actually provided.
        got: usize,
    },

    /// The input chunk was empty.
    #[error("cannot encode empty chunk")]
    EmptyChunk,
}
