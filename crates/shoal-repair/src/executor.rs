//! Repair executor: fetches missing shards from surviving nodes and pushes
//! them to their new owners.
//!
//! The executor tries two strategies:
//! 1. **Direct copy**: fetch the exact shard from a surviving node that holds it.
//! 2. **RS reconstruction**: if no direct copy is available, fetch sibling shards
//!    from the same chunk, RS-decode to recover the missing shard data, then
//!    re-encode to produce the correct shard.

use std::sync::Arc;

use bytes::Bytes;
use shoal_cluster::ClusterState;
use shoal_meta::MetaStore;
use shoal_store::ShardStore;
use shoal_types::events::{EventBus, RepairCompleted, RepairStarted, ShardSource, ShardStored};
use shoal_types::{Manifest, NodeId, ShardId, ShardMeta};
use tracing::{debug, warn};

use crate::error::RepairError;
use crate::throttle::Throttle;

/// Abstracts shard transfer so that tests can mock the network.
#[async_trait::async_trait]
pub trait ShardTransfer: Send + Sync {
    /// Pull a shard from a remote node.
    async fn pull_shard(
        &self,
        node_id: NodeId,
        shard_id: ShardId,
    ) -> Result<Option<Bytes>, RepairError>;

    /// Push a shard to a remote node.
    async fn push_shard(
        &self,
        node_id: NodeId,
        shard_id: ShardId,
        data: Bytes,
    ) -> Result<(), RepairError>;
}

/// Repairs individual shards by fetching from surviving nodes.
pub struct RepairExecutor {
    cluster: Arc<ClusterState>,
    meta: Arc<MetaStore>,
    local_store: Arc<dyn ShardStore>,
    transfer: Arc<dyn ShardTransfer>,
    local_node_id: NodeId,
    replication_factor: usize,
    erasure_k: usize,
    erasure_m: usize,
    event_bus: EventBus,
}

impl RepairExecutor {
    /// Create a new executor.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cluster: Arc<ClusterState>,
        meta: Arc<MetaStore>,
        local_store: Arc<dyn ShardStore>,
        transfer: Arc<dyn ShardTransfer>,
        local_node_id: NodeId,
        replication_factor: usize,
        erasure_k: usize,
        erasure_m: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            cluster,
            meta,
            local_store,
            transfer,
            local_node_id,
            replication_factor,
            erasure_k,
            erasure_m,
            event_bus: EventBus::new(),
        })
    }

    /// Set a shared event bus for repair event emissions.
    pub fn with_event_bus(self: Arc<Self>, bus: EventBus) -> Arc<Self> {
        Arc::new(Self {
            cluster: self.cluster.clone(),
            meta: self.meta.clone(),
            local_store: self.local_store.clone(),
            transfer: self.transfer.clone(),
            local_node_id: self.local_node_id,
            replication_factor: self.replication_factor,
            erasure_k: self.erasure_k,
            erasure_m: self.erasure_m,
            event_bus: bus,
        })
    }

    /// Repair a single shard.
    ///
    /// 1. Determine who should own the shard (from the current ring).
    /// 2. Try to fetch a direct copy from a surviving node.
    /// 3. If direct copy fails, attempt RS reconstruction from sibling shards.
    /// 4. Store the shard locally if we are an owner, or push to the correct node.
    /// 5. Update the shard map.
    #[tracing::instrument(skip(self, throttle), fields(method = tracing::field::Empty))]
    pub async fn repair_shard(
        &self,
        shard_id: ShardId,
        throttle: &Throttle,
    ) -> Result<(), RepairError> {
        self.event_bus.emit(RepairStarted { shard_id });

        let ring = self.cluster.ring().await;
        let target_owners = ring.owners(&shard_id, self.replication_factor);

        debug!(%shard_id, owners = ?target_owners, "repairing shard");

        // Strategy 1: try direct fetch from any node that might have it.
        let data = self.try_direct_fetch(shard_id).await;

        let shard_data = match data {
            Some(d) => {
                tracing::Span::current().record("method", "direct_copy");
                d
            }
            None => {
                // Strategy 2: RS reconstruction from sibling shards.
                tracing::Span::current().record("method", "rs_reconstruct");
                debug!(%shard_id, "direct fetch failed — attempting RS reconstruction");
                self.try_rs_reconstruct(shard_id).await?
            }
        };

        let shard_size = shard_data.len() as u64;

        // Throttle the transfer.
        throttle.acquire(shard_size).await;

        // Store/push to each target owner.
        for owner in &target_owners {
            if *owner == self.local_node_id {
                // Store locally.
                if !self.local_store.contains(shard_id).await? {
                    self.local_store.put(shard_id, shard_data.clone()).await?;
                    debug!(%shard_id, "repaired shard stored locally");
                }
            } else {
                // Push to remote owner.
                if let Err(e) = self
                    .transfer
                    .push_shard(*owner, shard_id, shard_data.clone())
                    .await
                {
                    warn!(
                        %shard_id,
                        %owner,
                        error = %e,
                        "failed to push repaired shard to owner — will retry later"
                    );
                }
            }
        }

        // Update shard map.
        self.meta.put_shard_owners(&shard_id, &target_owners)?;

        self.event_bus.emit(ShardStored {
            shard_id,
            source: ShardSource::Repair,
        });
        self.event_bus.emit(RepairCompleted { shard_id });

        debug!(%shard_id, size = shard_size, "shard repair complete");
        Ok(())
    }

    /// Try to fetch the shard directly from any node that might hold it.
    async fn try_direct_fetch(&self, shard_id: ShardId) -> Option<Bytes> {
        // Check local store first.
        if let Ok(Some(data)) = self.local_store.get(shard_id).await {
            return Some(data);
        }

        // Check shard map for known owners.
        let known_owners = self
            .meta
            .get_shard_owners(&shard_id)
            .ok()
            .flatten()
            .unwrap_or_default();

        for owner in known_owners {
            if owner == self.local_node_id {
                continue;
            }
            match self.transfer.pull_shard(owner, shard_id).await {
                Ok(Some(data)) => {
                    debug!(%shard_id, %owner, "fetched shard via direct copy");
                    return Some(data);
                }
                Ok(None) => {
                    debug!(%shard_id, %owner, "owner does not have shard");
                }
                Err(e) => {
                    debug!(%shard_id, %owner, error = %e, "failed to pull from owner");
                }
            }
        }

        None
    }

    /// Try to reconstruct the shard via RS decoding from sibling shards.
    ///
    /// Finds the chunk this shard belongs to (via manifest scan), fetches
    /// enough sibling shards, decodes the original chunk, then re-encodes
    /// to produce the missing shard.
    async fn try_rs_reconstruct(&self, shard_id: ShardId) -> Result<Bytes, RepairError> {
        // Find the manifest that contains this shard.
        let (manifest, chunk_idx, shard_idx) = self
            .find_manifest_for_shard(shard_id)?
            .ok_or(RepairError::ManifestNotFound(shard_id))?;

        let chunk_meta = &manifest.chunks[chunk_idx];
        let k = self.erasure_k;
        let m = self.erasure_m;

        // Fetch as many sibling shards as we can.
        let mut collected: Vec<(u8, Vec<u8>)> = Vec::new();
        for shard_meta in &chunk_meta.shards {
            if collected.len() >= k {
                break;
            }
            if shard_meta.shard_id == shard_id {
                // This is the one we're trying to reconstruct — skip.
                continue;
            }
            if let Some(data) = self.try_direct_fetch(shard_meta.shard_id).await {
                collected.push((shard_meta.index, data.to_vec()));
            }
        }

        if collected.len() < k {
            return Err(RepairError::InsufficientShards {
                shard_id,
                needed: k,
                found: collected.len(),
            });
        }

        // RS decode to recover the stored (possibly compressed) chunk data.
        let original_size = chunk_meta.stored_length as usize;
        let chunk_data = shoal_erasure::decode(k, m, &collected, original_size)?;

        // Re-encode to get all shards including the missing one.
        let encoder = shoal_erasure::ErasureEncoder::new(k, m);
        let (shards, _) = encoder.encode(&chunk_data)?;

        // Find the shard we need.
        let target_shard = shards.into_iter().find(|s| s.index == shard_idx).ok_or(
            RepairError::InsufficientShards {
                shard_id,
                needed: k,
                found: 0,
            },
        )?;

        debug!(
            %shard_id,
            chunk_idx,
            "reconstructed shard via RS decode"
        );

        Ok(target_shard.data)
    }

    /// Scan manifests to find which one contains the given shard.
    ///
    /// Returns `(manifest, chunk_index, shard_index)`.
    fn find_manifest_for_shard(
        &self,
        shard_id: ShardId,
    ) -> Result<Option<(Manifest, usize, u8)>, RepairError> {
        // Iterate over all known manifests to find the one containing this shard.
        // In a production system this would be indexed; for now we scan.
        // The MetaStore doesn't expose a manifest iterator, so we look up
        // via the shard map → chunk → manifest chain.
        //
        // For now, we rely on the caller to have stored manifests and use a
        // linear scan approach through the objects keyspace.
        let objects = self.meta.list_objects("__all__", "")?;
        for obj in &objects {
            if let Ok(Some(manifest)) = self.meta.get_manifest(&obj.object_id) {
                for (ci, chunk) in manifest.chunks.iter().enumerate() {
                    for shard_meta in &chunk.shards {
                        if shard_meta.shard_id == shard_id {
                            let index = shard_meta.index;
                            return Ok(Some((manifest, ci, index)));
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    /// Repair a shard when we know the manifest and chunk context.
    ///
    /// This is a more efficient path used when the caller already has
    /// the manifest information (e.g., during a full-object repair).
    pub async fn repair_shard_with_context(
        &self,
        shard_id: ShardId,
        shard_meta: &ShardMeta,
        sibling_shards: &[ShardMeta],
        original_chunk_size: u32,
        throttle: &Throttle,
    ) -> Result<Bytes, RepairError> {
        // Try direct fetch first.
        if let Some(data) = self.try_direct_fetch(shard_id).await {
            return Ok(data);
        }

        // Collect sibling shards for RS reconstruction.
        let k = self.erasure_k;
        let m = self.erasure_m;
        let mut collected: Vec<(u8, Vec<u8>)> = Vec::new();

        for sibling in sibling_shards {
            if collected.len() >= k {
                break;
            }
            if sibling.shard_id == shard_id {
                continue;
            }
            if let Some(data) = self.try_direct_fetch(sibling.shard_id).await {
                collected.push((sibling.index, data.to_vec()));
            }
        }

        if collected.len() < k {
            return Err(RepairError::InsufficientShards {
                shard_id,
                needed: k,
                found: collected.len(),
            });
        }

        let chunk_data = shoal_erasure::decode(k, m, &collected, original_chunk_size as usize)?;
        let encoder = shoal_erasure::ErasureEncoder::new(k, m);
        let (shards, _) = encoder.encode(&chunk_data)?;

        let target = shards
            .into_iter()
            .find(|s| s.index == shard_meta.index)
            .ok_or(RepairError::InsufficientShards {
                shard_id,
                needed: k,
                found: 0,
            })?;

        throttle.acquire(target.data.len() as u64).await;
        Ok(target.data)
    }
}
