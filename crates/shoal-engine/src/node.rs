//! [`ShoalNode`] — the node orchestrator that ties all components together.
//!
//! A `ShoalNode` owns the local shard store, metadata store, cluster state,
//! and network transport, and exposes the write/read/delete pipeline for
//! objects.

use std::collections::BTreeMap;
use std::sync::Arc;

use shoal_cas::{Chunker, build_manifest};
use shoal_cluster::ClusterState;
use shoal_erasure::ErasureEncoder;
use shoal_meta::MetaStore;
use shoal_store::ShardStore;
use shoal_types::*;
use tracing::{debug, info};

use crate::error::EngineError;

/// Configuration for creating a [`ShoalNode`].
pub struct ShoalNodeConfig {
    /// Node identifier.
    pub node_id: NodeId,
    /// Chunk size for content addressing.
    pub chunk_size: u32,
    /// Number of data shards for erasure coding.
    pub erasure_k: usize,
    /// Number of parity shards for erasure coding.
    pub erasure_m: usize,
    /// Virtual nodes per physical node in the placement ring.
    pub vnodes_per_node: u16,
}

impl Default for ShoalNodeConfig {
    fn default() -> Self {
        Self {
            node_id: NodeId::from([0u8; 32]),
            chunk_size: 262_144,
            erasure_k: 4,
            erasure_m: 2,
            vnodes_per_node: 128,
        }
    }
}

/// The node orchestrator that ties all Shoal components together.
///
/// Owns the local stores, cluster state, and provides the full
/// write/read/delete pipeline for objects.
pub struct ShoalNode {
    /// This node's identifier.
    node_id: NodeId,
    /// Local shard storage.
    store: Arc<dyn ShardStore>,
    /// Metadata persistence (manifests, object keys, shard map, membership).
    meta: Arc<MetaStore>,
    /// Cluster membership and placement ring.
    cluster: Arc<ClusterState>,
    /// Content-addressing chunker.
    chunker: Chunker,
    /// Chunk size (kept separately since Chunker doesn't expose it).
    chunk_size: u32,
    /// Erasure encoder.
    encoder: ErasureEncoder,
    /// Erasure coding parameters.
    erasure_k: usize,
    erasure_m: usize,
    /// Replication factor (k + m).
    replication_factor: usize,
}

impl ShoalNode {
    /// Create a new node with the given configuration and components.
    pub fn new(
        config: ShoalNodeConfig,
        store: Arc<dyn ShardStore>,
        meta: Arc<MetaStore>,
        cluster: Arc<ClusterState>,
    ) -> Self {
        let replication_factor = config.erasure_k + config.erasure_m;
        Self {
            node_id: config.node_id,
            store,
            meta,
            cluster,
            chunker: Chunker::new(config.chunk_size),
            chunk_size: config.chunk_size,
            encoder: ErasureEncoder::new(config.erasure_k, config.erasure_m),
            erasure_k: config.erasure_k,
            erasure_m: config.erasure_m,
            replication_factor,
        }
    }

    /// Return this node's ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Return a reference to the cluster state.
    pub fn cluster(&self) -> &Arc<ClusterState> {
        &self.cluster
    }

    /// Return a reference to the metadata store.
    pub fn meta(&self) -> &Arc<MetaStore> {
        &self.meta
    }

    /// Return a reference to the shard store.
    pub fn store(&self) -> &Arc<dyn ShardStore> {
        &self.store
    }

    // ------------------------------------------------------------------
    // Write path
    // ------------------------------------------------------------------

    /// Store an object: chunk → erasure-encode → distribute → build manifest.
    ///
    /// Returns the [`ObjectId`] of the stored object.
    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        metadata: BTreeMap<String, String>,
    ) -> Result<ObjectId, EngineError> {
        let total_size = data.len() as u64;

        info!(bucket, key, total_size, "put_object: starting write");

        // Step 1: chunk the data.
        let chunks = self.chunker.chunk(data);
        debug!(num_chunks = chunks.len(), "chunked object");

        // Step 2: erasure-encode each chunk and distribute shards.
        let ring = self.cluster.ring().await;

        let mut chunk_metas = Vec::with_capacity(chunks.len());

        for chunk in &chunks {
            let (shards, _original_size) = self.encoder.encode(&chunk.data)?;

            let mut shard_metas = Vec::with_capacity(shards.len());

            for shard in &shards {
                // Determine owners via placement ring.
                let owners = ring.owners(&shard.id, self.replication_factor);

                // Store on local node if we are an owner, otherwise store anyway
                // (in a single-node setup the local node is always the owner).
                let mut stored = false;
                for owner in &owners {
                    if *owner == self.node_id {
                        self.store.put(shard.id, shard.data.clone()).await?;
                        stored = true;
                        break;
                    }
                }

                // In single-node or if we're not an owner, store locally anyway
                // so the data is available for reads from this node.
                if !stored {
                    self.store.put(shard.id, shard.data.clone()).await?;
                }

                // Record shard owners in metadata.
                self.meta.put_shard_owners(&shard.id, &owners)?;

                shard_metas.push(ShardMeta {
                    shard_id: shard.id,
                    index: shard.index,
                    size: shard.data.len() as u32,
                });
            }

            chunk_metas.push(ChunkMeta {
                chunk_id: chunk.id,
                offset: chunk.offset,
                size: chunk.data.len() as u32,
                shards: shard_metas,
            });
        }

        // Step 3: build manifest.
        let manifest = build_manifest(&chunk_metas, total_size, self.chunk_size, metadata)?;
        let object_id = manifest.object_id;

        // Step 4: persist manifest and key mapping.
        self.meta.put_manifest(&manifest)?;
        self.meta.put_object_key(bucket, key, &object_id)?;

        info!(
            bucket, key, %object_id,
            chunks = chunk_metas.len(),
            "put_object: write complete"
        );

        Ok(object_id)
    }

    // ------------------------------------------------------------------
    // Read path
    // ------------------------------------------------------------------

    /// Retrieve an object by bucket/key.
    ///
    /// Looks up the manifest, fetches shards, erasure-decodes each chunk,
    /// and returns the reconstructed data.
    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(Vec<u8>, Manifest), EngineError> {
        // Step 1: look up ObjectId.
        let object_id =
            self.meta
                .get_object_key(bucket, key)?
                .ok_or_else(|| EngineError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                })?;

        // Step 2: fetch manifest.
        let manifest =
            self.meta
                .get_manifest(&object_id)?
                .ok_or_else(|| EngineError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                })?;

        debug!(
            %object_id,
            num_chunks = manifest.chunks.len(),
            total_size = manifest.total_size,
            "get_object: reading"
        );

        // Step 3: for each chunk, fetch k shards and decode.
        let mut result = Vec::with_capacity(manifest.total_size as usize);

        for (ci, chunk_meta) in manifest.chunks.iter().enumerate() {
            let mut collected: Vec<(u8, Vec<u8>)> = Vec::new();

            for shard_meta in &chunk_meta.shards {
                if collected.len() >= self.erasure_k {
                    break;
                }
                if let Some(data) = self.store.get(shard_meta.shard_id).await? {
                    collected.push((shard_meta.index, data.to_vec()));
                } else {
                    debug!(
                        shard_id = %shard_meta.shard_id,
                        index = shard_meta.index,
                        "shard not found locally"
                    );
                }
            }

            if collected.len() < self.erasure_k {
                return Err(EngineError::ReadFailed {
                    chunk_index: ci,
                    needed: self.erasure_k,
                    found: collected.len(),
                });
            }

            let chunk_data = shoal_erasure::decode(
                self.erasure_k,
                self.erasure_m,
                &collected,
                chunk_meta.size as usize,
            )?;

            result.extend_from_slice(&chunk_data);
        }

        info!(
            bucket, key, %object_id,
            size = result.len(),
            "get_object: read complete"
        );

        Ok((result, manifest))
    }

    // ------------------------------------------------------------------
    // Delete path
    // ------------------------------------------------------------------

    /// Delete an object by removing its key mapping and manifest.
    ///
    /// Shard data is left in place for now; a background GC pass would
    /// clean up orphaned shards (post-milestone optimization).
    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), EngineError> {
        // Look up to verify it exists.
        let object_id =
            self.meta
                .get_object_key(bucket, key)?
                .ok_or_else(|| EngineError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                })?;

        // Remove key mapping.
        self.meta.delete_object_key(bucket, key)?;

        info!(bucket, key, %object_id, "delete_object: key mapping removed");

        Ok(())
    }

    /// Check if an object exists.
    pub fn has_object(&self, bucket: &str, key: &str) -> Result<bool, EngineError> {
        Ok(self.meta.get_object_key(bucket, key)?.is_some())
    }

    /// List objects in a bucket with an optional prefix.
    pub fn list_objects(&self, bucket: &str, prefix: &str) -> Result<Vec<String>, EngineError> {
        Ok(self.meta.list_objects(bucket, prefix)?)
    }

    /// Retrieve object metadata (manifest) without fetching data.
    pub fn head_object(&self, bucket: &str, key: &str) -> Result<Manifest, EngineError> {
        let object_id =
            self.meta
                .get_object_key(bucket, key)?
                .ok_or_else(|| EngineError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                })?;

        self.meta
            .get_manifest(&object_id)?
            .ok_or_else(|| EngineError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })
    }
}
