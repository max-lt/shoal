//! [`ShoalEngine`] — the data-plane trait for protocol adapters.
//!
//! Protocol adapters (S3, SFTP, Admin API, etc.) depend on this trait
//! instead of the concrete [`ShoalNode`](crate::ShoalNode) struct, making
//! them interchangeable.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use shoal_meta::MetaStore;
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

    /// Create an API key, persist it, and replicate via LogTree+gossip.
    async fn create_api_key(&self, key_id: &str, secret: &str) -> Result<(), EngineError>;

    /// Delete an API key, persist the deletion, and replicate via LogTree+gossip.
    async fn delete_api_key(&self, key_id: &str) -> Result<(), EngineError>;

    /// Look up an API key secret, falling back to QUIC peer pull if not found locally.
    ///
    /// Returns `Some(secret)` if found, `None` if no peer has it.
    async fn lookup_api_key(&self, access_key_id: &str) -> Result<Option<String>, EngineError>;

    /// Copy an object from one location to another (key mapping only, no data copy).
    async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
    ) -> Result<ObjectId, EngineError>;

    /// Create a bucket (register the name).
    async fn create_bucket(&self, bucket: &str) -> Result<(), EngineError>;

    /// Delete a bucket. Fails if the bucket contains objects.
    async fn delete_bucket(&self, bucket: &str) -> Result<(), EngineError>;

    /// List all known bucket names.
    async fn list_buckets(&self) -> Result<BTreeSet<String>, EngineError>;

    /// Check if a bucket exists.
    async fn bucket_exists(&self, bucket: &str) -> Result<bool, EngineError>;

    /// Set tags on an object, persisting and replicating via LogTree+gossip.
    async fn put_object_tags(
        &self,
        bucket: &str,
        key: &str,
        tags: BTreeMap<String, String>,
    ) -> Result<(), EngineError>;

    /// Retrieve tags for an object.
    async fn get_object_tags(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<BTreeMap<String, String>, EngineError>;

    /// Delete all tags from an object, replicating via LogTree+gossip.
    async fn delete_object_tags(&self, bucket: &str, key: &str) -> Result<(), EngineError>;

    /// Return a reference to the metadata store.
    ///
    /// Protocol adapters use this for admin operations (API key management,
    /// etc.) that don't go through the data-plane methods above.
    fn meta(&self) -> &Arc<MetaStore>;
}
