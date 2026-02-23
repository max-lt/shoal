//! [`ShoalNode`] — the node orchestrator that ties all components together.
//!
//! A `ShoalNode` owns the local shard store, metadata store, cluster state,
//! and network transport, and exposes the write/read/delete pipeline for
//! objects.

use std::collections::{BTreeMap, HashMap, HashSet};
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

use crate::cache::ShardCache;
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
    /// Maximum bytes for the read-through shard cache (non-owned shards).
    ///
    /// When a node pulls a shard from a remote peer during a read, the
    /// shard is cached in a bounded LRU instead of the main store. This
    /// prevents unbounded storage growth. Set to 0 to disable caching.
    /// Default: 100 MB.
    pub cache_max_bytes: u64,
}

/// Default cache size: 100 MB.
const DEFAULT_CACHE_MAX_BYTES: u64 = 100 * 1024 * 1024;

impl Default for ShoalNodeConfig {
    fn default() -> Self {
        Self {
            node_id: NodeId::from([0u8; 32]),
            chunk_size: 262_144,
            erasure_k: 4,
            erasure_m: 2,
            vnodes_per_node: 128,
            shard_replication: 1,
            cache_max_bytes: DEFAULT_CACHE_MAX_BYTES,
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
    /// LRU cache for non-owned shards pulled during reads.
    shard_cache: ShardCache,
    /// Keys explicitly deleted on this node.
    ///
    /// Prevents `lookup_manifest` from re-fetching deleted objects from
    /// peers when `pull_manifest` is available.
    deleted_keys: RwLock<HashSet<(String, String)>>,
    /// Hybrid Logical Clock for ordering concurrent writes.
    hlc: HybridClock,
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
            shard_cache: ShardCache::new(config.cache_max_bytes),
            deleted_keys: RwLock::new(HashSet::new()),
            hlc: HybridClock::new(),
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

    /// Return a reference to the read-through shard cache.
    pub fn shard_cache(&self) -> &ShardCache {
        &self.shard_cache
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

        // Remote pushes are fired in parallel to avoid blocking on slow or
        // dead nodes. Local stores and metadata writes stay sequential
        // (fast, no network).
        let mut push_tasks = tokio::task::JoinSet::new();

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

                // Push to remote owners (spawned in parallel).
                if let Some(transport) = &self.transport {
                    for owner in &owners {
                        if *owner == self.node_id {
                            continue;
                        }
                        if let Some(addr) = self.resolve_addr(owner).await {
                            let transport = transport.clone();
                            let owner_id = *owner;
                            let shard_id = shard.id;
                            let data = shard.data.clone();
                            push_tasks.spawn(async move {
                                if let Err(e) = transport.push_shard(addr, shard_id, data).await {
                                    warn!(
                                        target_node = %owner_id,
                                        shard_id = %shard_id,
                                        %e,
                                        "failed to push shard to remote owner"
                                    );
                                }
                            });
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

        // Wait for all remote shard pushes to complete.
        while let Some(res) = push_tasks.join_next().await {
            if let Err(e) = res {
                warn!(%e, "shard push task panicked");
            }
        }

        // Step 3: build manifest with HLC timestamp.
        let mut manifest = build_manifest(&chunk_metas, total_size, self.chunk_size, metadata)?;
        manifest.hlc = self.hlc.tick();
        manifest.writer_node = self.node_id;
        let object_id = manifest.object_id;

        // Step 4: persist manifest, key mapping, and version locally.
        self.meta.put_manifest(&manifest)?;
        self.meta.put_object_key(bucket, key, &object_id)?;
        self.meta
            .put_version(bucket, key, manifest.hlc, &object_id)?;

        // Step 5: broadcast manifest to all known peers in parallel.
        if let Some(transport) = &self.transport {
            let manifest_bytes = postcard::to_allocvec(&manifest).unwrap_or_default();
            let peers = self.cluster.members().await;
            let mut broadcast_tasks = tokio::task::JoinSet::new();

            for peer in &peers {
                if peer.node_id == self.node_id {
                    continue;
                }
                if let Some(addr) = self.resolve_addr(&peer.node_id).await {
                    let transport = transport.clone();
                    let peer_id = peer.node_id;
                    let msg = ShoalMessage::ManifestPut {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        manifest_bytes: manifest_bytes.clone(),
                    };
                    broadcast_tasks.spawn(async move {
                        if let Err(e) = transport.send_to(addr, &msg).await {
                            warn!(
                                peer = %peer_id,
                                %e,
                                "failed to broadcast manifest"
                            );
                        }
                    });
                }
            }

            while let Some(res) = broadcast_tasks.join_next().await {
                if let Err(e) = res {
                    warn!(%e, "manifest broadcast task panicked");
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

        let data = self.reconstruct_from_manifest(&manifest).await?;

        info!(
            bucket, key, object_id = %manifest.object_id,
            size = data.len(),
            "get_object: read complete"
        );

        Ok((data, manifest))
    }

    /// Reconstruct object data from a manifest by fetching and decoding shards.
    async fn reconstruct_from_manifest(&self, manifest: &Manifest) -> Result<Vec<u8>, EngineError> {
        debug!(
            object_id = %manifest.object_id,
            num_chunks = manifest.chunks.len(),
            total_size = manifest.total_size,
            "reconstruct_from_manifest: reading"
        );

        let mut result = Vec::with_capacity(manifest.total_size as usize);

        for (ci, chunk_meta) in manifest.chunks.iter().enumerate() {
            let mut collected: Vec<(u8, Vec<u8>)> = Vec::new();

            for shard_meta in &chunk_meta.shards {
                if collected.len() >= self.erasure_k {
                    break;
                }
                // Try local store first (owned shards).
                if let Some(data) = self.store.get(shard_meta.shard_id).await? {
                    collected.push((shard_meta.index, data.to_vec()));
                    continue;
                }
                // Try read-through cache (non-owned, previously pulled shards).
                if let Some(data) = self.shard_cache.get(&shard_meta.shard_id) {
                    collected.push((shard_meta.index, data.to_vec()));
                    continue;
                }
                // Try remote if transport available.
                if let Some(transport) = &self.transport {
                    let owners = match self.meta.get_shard_owners(&shard_meta.shard_id)? {
                        Some(owners) => owners,
                        None => {
                            let ring = self.cluster.ring().await;
                            ring.owners(&shard_meta.shard_id, self.shard_replication)
                        }
                    };

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
                                    self.shard_cache.put(shard_meta.shard_id, data.clone());
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

        Ok(result)
    }

    // ------------------------------------------------------------------
    // Delete path
    // ------------------------------------------------------------------

    /// Delete an object by adding a delete marker.
    ///
    /// Instead of physically removing the manifest, this creates a special
    /// delete-marker manifest with `is_delete_marker = true` and an HLC
    /// timestamp. This preserves version history for S3-compatible versioning.
    /// Previous versions remain accessible via `?versionId=`.
    ///
    /// Shard data from previous versions is left in place; a background GC
    /// pass would clean up orphaned shards (post-milestone optimization).
    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), EngineError> {
        // Look up to verify it exists.
        if self.meta.get_object_key(bucket, key)?.is_none() {
            return Err(EngineError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }

        // Build a delete marker manifest.
        let hlc = self.hlc.tick();
        let delete_marker = Manifest {
            version: MANIFEST_VERSION,
            object_id: ObjectId::from_data(
                &postcard::to_allocvec(&(bucket, key, hlc)).unwrap_or_default(),
            ),
            total_size: 0,
            chunk_size: 0,
            chunks: vec![],
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            metadata: BTreeMap::new(),
            hlc,
            writer_node: self.node_id,
            is_delete_marker: true,
        };

        // Store the delete marker as a version.
        self.meta.put_manifest(&delete_marker)?;
        self.meta
            .put_version(bucket, key, hlc, &delete_marker.object_id)?;

        // Remove the "current" key mapping so normal GET returns 404.
        self.meta.delete_object_key(bucket, key)?;

        // Track deletion so lookup_manifest won't re-fetch from peers.
        self.deleted_keys
            .write()
            .await
            .insert((bucket.to_string(), key.to_string()));

        info!(bucket, key, hlc, "delete_object: delete marker added");

        Ok(())
    }

    /// Delete a specific version of an object.
    ///
    /// This physically removes the version entry. If the deleted version was
    /// the latest, the `objects` keyspace is updated to point to the previous
    /// version (or removed if no versions remain).
    pub async fn delete_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_hlc: u64,
    ) -> Result<(), EngineError> {
        // Verify the version exists.
        if self.meta.get_version(bucket, key, version_hlc)?.is_none() {
            return Err(EngineError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }

        // Remove the version.
        self.meta.delete_version(bucket, key, version_hlc)?;

        // Update the "current" pointer: find the latest non-delete-marker version.
        let remaining = self.meta.list_versions(bucket, key)?;
        let latest_non_delete = remaining.iter().rev().find(|(_, oid)| {
            self.meta
                .get_manifest(oid)
                .ok()
                .flatten()
                .is_some_and(|m| !m.is_delete_marker)
        });

        match latest_non_delete {
            Some((_, oid)) => {
                self.meta.put_object_key(bucket, key, oid)?;
            }
            None => {
                // No non-delete versions remain — remove the key mapping.
                let _ = self.meta.delete_object_key(bucket, key);
            }
        }

        info!(
            bucket,
            key, version_hlc, "delete_object_version: version removed"
        );
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

    /// Get a specific version of an object by HLC timestamp.
    pub async fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_hlc: u64,
    ) -> Result<(Vec<u8>, Manifest), EngineError> {
        let object_id = self
            .meta
            .get_version(bucket, key, version_hlc)?
            .ok_or_else(|| EngineError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })?;

        let manifest =
            self.meta
                .get_manifest(&object_id)?
                .ok_or_else(|| EngineError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                })?;

        if manifest.is_delete_marker {
            return Err(EngineError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }

        let data = self.reconstruct_from_manifest(&manifest).await?;
        Ok((data, manifest))
    }

    /// List all versions of an object, returning `(hlc, ObjectId, is_delete_marker)`.
    pub fn list_object_versions(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Vec<(u64, ObjectId, bool)>, EngineError> {
        let versions = self.meta.list_versions(bucket, key)?;
        let mut result = Vec::with_capacity(versions.len());
        for (hlc, oid) in versions {
            let is_delete = self
                .meta
                .get_manifest(&oid)?
                .is_some_and(|m| m.is_delete_marker);
            result.push((hlc, oid, is_delete));
        }
        Ok(result)
    }

    /// Return this node's HLC (for external use, e.g. gossip reception).
    pub fn hlc(&self) -> &HybridClock {
        &self.hlc
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
                }
                Err(e) => {
                    warn!(from = %peer.node_id, %e, "failed to sync manifests from peer");
                }
            }
        }

        if total_synced > 0 {
            info!(total_synced, "manifest sync complete");
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

        // Don't re-fetch from peers if this key was explicitly deleted.
        if self
            .deleted_keys
            .read()
            .await
            .contains(&(bucket.to_string(), key.to_string()))
        {
            return Ok(None);
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
