//! Reed-Solomon erasure coding wrapper.
//!
//! This crate provides:
//! - [`ErasureEncoder`] — splits a chunk into `k` data shards and `m` parity shards.
//! - [`decode`] — reconstructs the original chunk from any `k` of the `k + m` shards.
//! - [`suggest_config`] — recommends `(k, m)` parameters based on cluster size.
//!
//! All shards are content-addressed via BLAKE3 hashing. Padding is handled
//! automatically to satisfy `reed-solomon-simd`'s even-size requirement.

mod config;
mod decoder;
mod encoder;
mod error;

pub use config::suggest_config;
pub use decoder::decode;
pub use encoder::{ErasureEncoder, Shard};
pub use error::ErasureError;
