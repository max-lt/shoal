//! [`ShoalEngine`] — the data-plane trait for protocol adapters.
//!
//! Protocol adapters (S3, SFTP, Admin API, etc.) depend on this trait
//! instead of the concrete [`ShoalNode`](crate::ShoalNode) struct, making
//! them interchangeable.

use std::collections::BTreeMap;

use shoal_types::{Manifest, ObjectId};

use crate::error::EngineError;

/// The data-plane interface exposed to protocol adapters.
///
/// Covers the five operations any object storage protocol needs:
///
/// - **put** — store an object
/// - **get** — retrieve an object (data + manifest)
/// - **head** — retrieve metadata only
/// - **delete** — remove an object
/// - **list** — enumerate objects by prefix
#[async_trait::async_trait]
pub trait ShoalEngine: Send + Sync {
    /// Store an object.
    ///
    /// Returns the content-addressed [`ObjectId`] of the stored object.
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        metadata: BTreeMap<String, String>,
    ) -> Result<ObjectId, EngineError>;

    /// Retrieve an object's data and manifest.
    async fn get_object(&self, bucket: &str, key: &str)
    -> Result<(Vec<u8>, Manifest), EngineError>;

    /// Retrieve object metadata (manifest) without fetching data.
    async fn head_object(&self, bucket: &str, key: &str) -> Result<Manifest, EngineError>;

    /// Delete an object.
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), EngineError>;

    /// List objects in a bucket with an optional prefix filter.
    async fn list_objects(&self, bucket: &str, prefix: &str) -> Result<Vec<String>, EngineError>;
}
