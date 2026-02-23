//! [`ShoalNode`] — the node orchestrator that ties all components together.
//!
//! A `ShoalNode` owns the local shard store, metadata store, cluster state,
//! and network transport, and exposes the write/read/delete pipeline for
//! objects.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use iroh::EndpointAddr;
use shoal_cas::{Chunker, build_manifest};
use shoal_cluster::ClusterState;
use shoal_erasure::ErasureEncoder;
use shoal_meta::MetaStore;
use shoal_net::{ShoalMessage, Transport};
use shoal_store::ShardStore;
use shoal_types::*;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

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
    /// Shard replication factor — how many nodes store each individual shard.
    ///
    /// Defaults to 1: each shard is placed on exactly one node, and the
    /// erasure coding (k data + m parity shards distributed across different
    /// nodes) provides the redundancy. Set to 2+ for belt-and-suspenders
    /// replication on top of erasure coding.
    pub shard_replication: usize,
}

impl Default for ShoalNodeConfig {
    fn default() -> Self {
        Self {
            node_id: NodeId::from([0u8; 32]),
            chunk_size: 262_144,
            erasure_k: 4,
            erasure_m: 2,
            vnodes_per_node: 128,
            shard_replication: 1,
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
    /// Per-shard replication factor (how many ring owners per shard).
    shard_replication: usize,
    /// Network transport (None in tests / single-node mode).
    transport: Option<Arc<dyn Transport>>,
    /// NodeId → EndpointAddr mapping for remote nodes.
    address_book: Arc<RwLock<HashMap<NodeId, EndpointAddr>>>,
}

impl ShoalNode {
    /// Create a new node with the given configuration and components.
    pub fn new(
        config: ShoalNodeConfig,
        store: Arc<dyn ShardStore>,
        meta: Arc<MetaStore>,
        cluster: Arc<ClusterState>,
    ) -> Self {
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
            shard_replication: config.shard_replication.max(1),
            transport: None,
            address_book: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Set the network transport for distributed operations.
    pub fn with_transport(mut self, transport: Arc<dyn Transport>) -> Self {
        self.transport = Some(transport);
        self
    }

    /// Set the address book for resolving NodeId to EndpointAddr.
    pub fn with_address_book(mut self, book: Arc<RwLock<HashMap<NodeId, EndpointAddr>>>) -> Self {
        self.address_book = book;
        self
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
                let owners = ring.owners(&shard.id, self.shard_replication);

                // Store locally only if this node is an owner (or if there
                // is no transport — single-node / test mode).
                let local_is_owner = owners.contains(&self.node_id);
                if local_is_owner || self.transport.is_none() {
                    self.store.put(shard.id, shard.data.clone()).await?;
                }

                // Push to remote owners.
                if let Some(transport) = &self.transport {
                    for owner in &owners {
                        if *owner == self.node_id {
                            continue;
                        }
                        if let Some(addr) = self.resolve_addr(owner).await
                            && let Err(e) = transport
                                .push_shard(addr, shard.id, shard.data.clone())
                                .await
                        {
                            warn!(
                                target_node = %owner,
                                shard_id = %shard.id,
                                %e,
                                "failed to push shard to remote owner"
                            );
                        }
                    }
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

        // Step 4: persist manifest and key mapping locally.
        self.meta.put_manifest(&manifest)?;
        self.meta.put_object_key(bucket, key, &object_id)?;

        // Step 5: broadcast manifest to all known peers.
        if let Some(transport) = &self.transport {
            let manifest_bytes = postcard::to_allocvec(&manifest).unwrap_or_default();
            let msg = ShoalMessage::ManifestPut {
                bucket: bucket.to_string(),
                key: key.to_string(),
                manifest_bytes,
            };
            let peers = self.cluster.members().await;
            for peer in &peers {
                if peer.node_id == self.node_id {
                    continue;
                }
                if let Some(addr) = self.resolve_addr(&peer.node_id).await
                    && let Err(e) = transport.send_to(addr, &msg).await
                {
                    warn!(
                        peer = %peer.node_id,
                        %e,
                        "failed to broadcast manifest"
                    );
                }
            }
        }

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
    /// Looks up the manifest, fetches shards (locally first, then from
    /// remote peers), erasure-decodes each chunk, and returns the
    /// reconstructed data.
    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(Vec<u8>, Manifest), EngineError> {
        // Step 1: look up manifest — try local first, then ask peers.
        let manifest = match self.lookup_manifest(bucket, key).await? {
            Some(m) => m,
            None => {
                return Err(EngineError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                });
            }
        };
        let object_id = manifest.object_id;

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
                // Try local first.
                if let Some(data) = self.store.get(shard_meta.shard_id).await? {
                    collected.push((shard_meta.index, data.to_vec()));
                    continue;
                }
                // Try remote if transport available.
                if let Some(transport) = &self.transport {
                    // Build candidate list: start with known/computed owners,
                    // then fall back to all cluster members. This handles
                    // ring changes after node failures — the current ring
                    // may point to nodes that don't hold the shard because
                    // placement changed since write time.
                    let owners = match self.meta.get_shard_owners(&shard_meta.shard_id)? {
                        Some(owners) => owners,
                        None => {
                            let ring = self.cluster.ring().await;
                            ring.owners(&shard_meta.shard_id, self.shard_replication)
                        }
                    };

                    // Collect all members as fallback candidates (excluding
                    // the owners we'll try first and ourselves).
                    let members = self.cluster.members().await;
                    let owner_set: std::collections::HashSet<NodeId> =
                        owners.iter().copied().collect();
                    let fallback: Vec<NodeId> = members
                        .iter()
                        .map(|m| m.node_id)
                        .filter(|nid| *nid != self.node_id && !owner_set.contains(nid))
                        .collect();

                    let mut found = false;
                    for owner in owners.iter().chain(fallback.iter()) {
                        if *owner == self.node_id {
                            continue;
                        }
                        if let Some(addr) = self.resolve_addr(owner).await {
                            match transport.pull_shard(addr, shard_meta.shard_id).await {
                                Ok(Some(data)) => {
                                    debug!(
                                        shard_id = %shard_meta.shard_id,
                                        from = %owner,
                                        "pulled shard from remote"
                                    );
                                    // Cache locally for future reads.
                                    let _ = self.store.put(shard_meta.shard_id, data.clone()).await;
                                    collected.push((shard_meta.index, data.to_vec()));
                                    found = true;
                                    break;
                                }
                                Ok(None) => {
                                    debug!(
                                        shard_id = %shard_meta.shard_id,
                                        from = %owner,
                                        "remote node does not have shard"
                                    );
                                }
                                Err(e) => {
                                    debug!(
                                        shard_id = %shard_meta.shard_id,
                                        from = %owner,
                                        %e,
                                        "failed to pull shard from remote"
                                    );
                                }
                            }
                        }
                    }
                    if !found {
                        debug!(
                            shard_id = %shard_meta.shard_id,
                            index = shard_meta.index,
                            "shard not found on any node"
                        );
                    }
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

    // ------------------------------------------------------------------
    // Manifest sync
    // ------------------------------------------------------------------

    /// Sync manifests from cluster peers.
    ///
    /// When a node joins (or restarts), it may have missed manifest
    /// broadcasts for objects stored before it was part of the cluster.
    /// This method pulls all manifests from each peer and stores them
    /// locally, ensuring `list_objects` returns a complete view.
    ///
    /// Returns the total number of new manifests synced.
    pub async fn sync_manifests_from_peers(&self) -> Result<usize, EngineError> {
        let Some(transport) = &self.transport else {
            return Ok(0);
        };

        let peers = self.cluster.members().await;
        let mut total_synced = 0usize;

        for peer in &peers {
            if peer.node_id == self.node_id {
                continue;
            }
            let Some(addr) = self.resolve_addr(&peer.node_id).await else {
                continue;
            };

            match transport.pull_all_manifests(addr).await {
                Ok(entries) => {
                    for entry in entries {
                        match postcard::from_bytes::<Manifest>(&entry.manifest_bytes) {
                            Ok(manifest) => {
                                // Only store if we don't already have this key mapping.
                                if self
                                    .meta
                                    .get_object_key(&entry.bucket, &entry.key)?
                                    .is_none()
                                {
                                    self.meta.put_manifest(&manifest)?;
                                    self.meta.put_object_key(
                                        &entry.bucket,
                                        &entry.key,
                                        &manifest.object_id,
                                    )?;
                                    total_synced += 1;
                                    debug!(
                                        bucket = %entry.bucket,
                                        key = %entry.key,
                                        object_id = %manifest.object_id,
                                        from = %peer.node_id,
                                        "synced manifest from peer"
                                    );
                                }
                            }
                            Err(e) => {
                                warn!(
                                    from = %peer.node_id,
                                    %e,
                                    "failed to deserialize synced manifest"
                                );
                            }
                        }
                    }
                    // One successful peer is enough — manifests are the same everywhere.
                    if total_synced > 0 {
                        info!(
                            total_synced,
                            from = %peer.node_id,
                            "manifest sync complete"
                        );
                    }
                    break;
                }
                Err(e) => {
                    warn!(from = %peer.node_id, %e, "failed to sync manifests from peer");
                }
            }
        }

        Ok(total_synced)
    }

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    /// Look up a manifest locally, falling back to asking peers.
    ///
    /// If found remotely, the manifest and key mapping are cached locally.
    async fn lookup_manifest(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<Manifest>, EngineError> {
        // Try local first.
        if let Some(object_id) = self.meta.get_object_key(bucket, key)?
            && let Some(manifest) = self.meta.get_manifest(&object_id)?
        {
            return Ok(Some(manifest));
        }

        // Ask peers if transport is available.
        let Some(transport) = &self.transport else {
            return Ok(None);
        };

        let peers = self.cluster.members().await;
        for peer in &peers {
            if peer.node_id == self.node_id {
                continue;
            }
            let Some(addr) = self.resolve_addr(&peer.node_id).await else {
                continue;
            };
            match transport.pull_manifest(addr, bucket, key).await {
                Ok(Some(manifest_bytes)) => {
                    match postcard::from_bytes::<Manifest>(&manifest_bytes) {
                        Ok(manifest) => {
                            debug!(
                                %bucket, %key,
                                object_id = %manifest.object_id,
                                from = %peer.node_id,
                                "fetched manifest from peer"
                            );
                            // Cache locally.
                            let _ = self.meta.put_manifest(&manifest);
                            let _ = self.meta.put_object_key(bucket, key, &manifest.object_id);
                            // Also cache shard owners from the manifest.
                            for chunk in &manifest.chunks {
                                for shard_meta in &chunk.shards {
                                    // The shard owners aren't in the manifest, but
                                    // we know the writer's node stored them all.
                                    // Just record the peer as an owner for now.
                                    let _ = self
                                        .meta
                                        .put_shard_owners(&shard_meta.shard_id, &[peer.node_id]);
                                }
                            }
                            return Ok(Some(manifest));
                        }
                        Err(e) => {
                            warn!(from = %peer.node_id, %e, "bad manifest from peer");
                        }
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    debug!(from = %peer.node_id, %e, "manifest pull failed");
                }
            }
        }

        Ok(None)
    }

    /// Resolve a NodeId to an EndpointAddr using the address book,
    /// falling back to constructing one from the NodeId bytes (relay-only).
    async fn resolve_addr(&self, node_id: &NodeId) -> Option<EndpointAddr> {
        // Check address book first.
        if let Some(addr) = self.address_book.read().await.get(node_id) {
            return Some(addr.clone());
        }
        // Fallback: construct from public key bytes (iroh relay discovery).
        match iroh::EndpointId::from_bytes(node_id.as_bytes()) {
            Ok(eid) => Some(EndpointAddr::new(eid)),
            Err(_) => {
                warn!(%node_id, "cannot resolve address");
                None
            }
        }
    }
}
