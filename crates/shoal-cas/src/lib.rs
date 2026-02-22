//! Content addressing, chunking, and manifest building.
//!
//! This crate provides:
//! - [`Chunker`] — splits data into fixed-size chunks, each identified by its BLAKE3 hash.
//! - [`build_manifest`] — constructs a [`Manifest`] from chunk metadata.
//!
//! The manifest itself is treated as a regular object: serialized with postcard,
//! then chunked, erasure-coded, and distributed like any other data.

mod chunker;
mod error;
mod manifest;

pub use chunker::{Chunk, Chunker};
pub use error::CasError;
pub use manifest::{
    build_manifest, build_manifest_with_timestamp, deserialize_manifest, serialize_manifest,
};
