//! Manifest storage helpers backed by [`ShardStore`].
//!
//! Manifests are serialized with postcard and stored as typed shards
//! (`SHARD_TYPE_MANIFEST`), keyed by `ShardId::from(manifest.object_id)`.

use bytes::Bytes;
use shoal_store::{SHARD_TYPE_MANIFEST, ShardStore};
use shoal_types::{Manifest, ObjectId, ShardId};

use crate::error::EngineError;

/// Serialize and store a manifest in the shard store.
pub async fn put_manifest(store: &dyn ShardStore, manifest: &Manifest) -> Result<(), EngineError> {
    let sid = ShardId::from(manifest.object_id);
    let bytes = postcard::to_allocvec(manifest)
        .map_err(|e| shoal_store::StoreError::Io(std::io::Error::other(e.to_string())))?;
    store
        .put_typed(sid, Bytes::from(bytes), SHARD_TYPE_MANIFEST)
        .await?;
    Ok(())
}

/// Retrieve and deserialize a manifest from the shard store.
pub async fn get_manifest(
    store: &dyn ShardStore,
    id: &ObjectId,
) -> Result<Option<Manifest>, EngineError> {
    let sid = ShardId::from(*id);

    match store.get(sid).await? {
        Some(bytes) => {
            let manifest: Manifest = postcard::from_bytes(&bytes)
                .map_err(|e| shoal_store::StoreError::Io(std::io::Error::other(e.to_string())))?;
            Ok(Some(manifest))
        }
        None => Ok(None),
    }
}
