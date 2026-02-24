//! [`ShoalNode`] — the node orchestrator that ties all components together.
//!
//! A `ShoalNode` owns the local shard store, metadata store, cluster state,
//! and network transport, and exposes the write/read/delete pipeline for
//! objects.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use iroh::EndpointAddr;
use shoal_cas::{Chunker, build_manifest};
use shoal_cluster::{ClusterState, GossipHandle};
use shoal_erasure::ErasureEncoder;
use shoal_logtree::LogTree;
use shoal_meta::MetaStore;
use shoal_net::{ShoalMessage, Transport};
use shoal_store::ShardStore;
use shoal_types::*;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::cache::ShardCache;
use crate::error::EngineError;

/// A shard that failed to push to its ring owner and needs retry.
struct PendingPush {
    shard_id: ShardId,
    target_node: NodeId,
}

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
    /// Optional LogTree for DAG-based mutation tracking.
    ///
    /// When present, replaces MetaStore for object metadata (resolve, list,
    /// manifest cache) and replaces ManifestPut broadcasts with LogEntry
    /// broadcasts.
    log_tree: Option<Arc<LogTree>>,
    /// Gossip handle for epidemic broadcast (manifests, log entries).
    ///
    /// When present, `put_object` broadcasts via gossip instead of unicast.
    gossip: Option<GossipHandle>,
    /// Shards that failed to push to ring owners and need background retry.
    ///
    /// Uses `std::sync::Mutex` (not tokio) — the critical section is pure
    /// in-memory Vec ops with no I/O.
    pending_pushes: Mutex<Vec<PendingPush>>,
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
            log_tree: None,
            gossip: None,
            pending_pushes: Mutex::new(Vec::new()),
        }
    }

    /// Set the network transport for distributed operations.
    pub fn with_transport(mut self, transport: Arc<dyn Transport>) -> Self {
        self.transport = Some(transport);
        self
    }

    /// Set the LogTree for DAG-based mutation tracking.
    pub fn with_log_tree(mut self, log_tree: Arc<LogTree>) -> Self {
        self.log_tree = Some(log_tree);
        self
    }

    /// Set the address book for resolving NodeId to EndpointAddr.
    pub fn with_address_book(mut self, book: Arc<RwLock<HashMap<NodeId, EndpointAddr>>>) -> Self {
        self.address_book = book;
        self
    }

    /// Set the gossip handle for epidemic broadcast.
    ///
    /// When set, manifest and log entry broadcasts go through gossip
    /// instead of unicast QUIC streams.
    pub fn with_gossip(mut self, handle: GossipHandle) -> Self {
        self.gossip = Some(handle);
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
    // Pending push retry
    // ------------------------------------------------------------------

    /// Retry pushing shards that failed during previous writes.
    ///
    /// Takes all pending pushes, attempts to send each to the correct ring
    /// owner, and deletes the local copy on success. Failed retries are
    /// re-queued for the next cycle.
    ///
    /// Returns the number of successfully retried pushes.
    pub async fn retry_pending_pushes(&self) -> usize {
        let transport = match &self.transport {
            Some(t) => t.clone(),
            None => return 0,
        };

        let pushes: Vec<PendingPush> =
            { std::mem::take(&mut *self.pending_pushes.lock().expect("lock poisoned")) };

        if pushes.is_empty() {
            return 0;
        }

        let ring = self.cluster.ring().await;
        let mut still_pending = Vec::new();
        let mut succeeded = 0usize;

        for push in pushes {
            // Shard cleaned up or no longer locally stored? Skip.
            if !self.store.contains(push.shard_id).await.unwrap_or(false) {
                continue;
            }

            let owners = ring.owners(&push.shard_id, self.shard_replication);

            // We became the owner (ring changed)? No push needed.
            if owners.contains(&self.node_id) {
                succeeded += 1;
                continue;
            }

            // Re-target if the ring changed and original target is no longer an owner.
            let target = if owners.contains(&push.target_node) {
                push.target_node
            } else {
                owners[0]
            };

            let data = match self.store.get(push.shard_id).await.ok().flatten() {
                Some(d) => d,
                None => continue,
            };

            if let Some(addr) = self.resolve_addr(&target).await {
                match transport.push_shard(addr, push.shard_id, data).await {
                    Ok(()) => {
                        let _ = self.store.delete(push.shard_id).await;
                        succeeded += 1;
                    }
                    Err(_) => still_pending.push(PendingPush {
                        shard_id: push.shard_id,
                        target_node: target,
                    }),
                }
            } else {
                still_pending.push(PendingPush {
                    shard_id: push.shard_id,
                    target_node: target,
                });
            }
        }

        if !still_pending.is_empty() {
            self.pending_pushes
                .lock()
                .expect("lock poisoned")
                .extend(still_pending);
        }

        if succeeded > 0 {
            info!(succeeded, "completed pending shard pushes");
        }

        succeeded
    }

    /// Return the number of shards awaiting retry.
    pub fn pending_push_count(&self) -> usize {
        self.pending_pushes.lock().expect("lock poisoned").len()
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
        let mut local_new: u32 = 0;
        let mut local_existing: u32 = 0;
        let mut remote_pushed: u32 = 0;

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

                // Always store every shard locally. The writer is the only
                // node guaranteed to have the data; remote pushes can fail
                // (transient network issues, slow peer startup) and with
                // shard_replication=1 a failed push means the shard is lost.
                // Keeping a full local copy ensures reads can always pull
                // from the writer as a fallback. Non-owned shards will be
                // cleaned up by background rebalancing once pushes succeed.
                let already_exists = self.store.contains(shard.id).await?;

                self.store.put(shard.id, shard.data.clone()).await?;

                if already_exists {
                    local_existing += 1;
                    debug!(
                        shard = %shard.id,
                        index = shard.index,
                        "shard already exists locally, skipped"
                    );
                } else {
                    local_new += 1;
                    debug!(
                        shard = %shard.id,
                        index = shard.index,
                        size = shard.data.len(),
                        "new shard stored locally"
                    );
                }

                // Push to remote owners (spawned in parallel).
                if let Some(transport) = &self.transport {
                    for owner in &owners {
                        if *owner == self.node_id {
                            continue;
                        }

                        if let Some(addr) = self.resolve_addr(owner).await {
                            remote_pushed += 1;
                            let transport = transport.clone();
                            let owner_id = *owner;
                            let shard_id = shard.id;
                            let data = shard.data.clone();
                            push_tasks.spawn(async move {
                                let success =
                                    transport.push_shard(addr, shard_id, data).await.is_ok();

                                if !success {
                                    warn!(
                                        target_node = %owner_id,
                                        %shard_id,
                                        "failed to push shard to remote owner"
                                    );
                                }
                                (shard_id, owner_id, success)
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

        // Wait for all remote shard pushes to complete, tracking results.
        let mut failed_pushes: Vec<PendingPush> = Vec::new();

        while let Some(join_result) = push_tasks.join_next().await {
            match join_result {
                Ok((shard_id, target_node, true)) => {
                    // ACK'd — delete local copy if writer is not a ring owner.
                    let owners = ring.owners(&shard_id, self.shard_replication);

                    if !owners.contains(&self.node_id) {
                        self.store.delete(shard_id).await?;
                        debug!(
                            shard = %shard_id,
                            target = %target_node,
                            "deleted local copy after successful push"
                        );
                    }
                }
                Ok((shard_id, target_node, false)) => {
                    // Push failed — keep local copy, queue for retry.
                    failed_pushes.push(PendingPush {
                        shard_id,
                        target_node,
                    });
                }
                Err(e) => warn!(%e, "shard push task panicked"),
            }
        }

        if !failed_pushes.is_empty() {
            info!(
                count = failed_pushes.len(),
                "queued failed pushes for background retry"
            );
            self.pending_pushes
                .lock()
                .expect("lock poisoned")
                .extend(failed_pushes);
        }

        info!(
            bucket,
            key, local_new, local_existing, remote_pushed, "shard distribution complete"
        );

        // Step 3: build manifest.
        let manifest = build_manifest(&chunk_metas, total_size, self.chunk_size, metadata)?;
        let object_id = manifest.object_id;

        // Step 4: persist manifest and key mapping, then broadcast.
        if let Some(log_tree) = &self.log_tree {
            // LogTree mode: append log entry + broadcast LogEntryBroadcast.
            let log_entry = log_tree.append_put(bucket, key, object_id, &manifest)?;

            let entry_bytes = postcard::to_allocvec(&log_entry).unwrap_or_default();
            let manifest_bytes = postcard::to_allocvec(&manifest).unwrap_or_default();

            if let Some(gossip) = &self.gossip {
                // Gossip broadcast — epidemic dissemination to all peers.
                let payload = GossipPayload::LogEntry {
                    entry_bytes,
                    manifest_bytes: Some(manifest_bytes),
                };

                if let Err(e) = gossip.broadcast_payload(&payload).await {
                    warn!(%e, "failed to broadcast log entry via gossip");
                }
            } else {
                // Unicast fallback (tests without gossip).
                self.unicast_to_peers(&ShoalMessage::LogEntryBroadcast {
                    entry_bytes,
                    manifest_bytes: Some(manifest_bytes),
                })
                .await;
            }
        } else {
            // MetaStore mode: persist locally + broadcast ManifestPut.
            self.meta.put_manifest(&manifest)?;
            self.meta.put_object_key(bucket, key, &object_id)?;

            let manifest_bytes = postcard::to_allocvec(&manifest).unwrap_or_default();

            if let Some(gossip) = &self.gossip {
                // Gossip broadcast — epidemic dissemination to all peers.
                let payload = GossipPayload::ManifestPut {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    manifest_bytes,
                };

                if let Err(e) = gossip.broadcast_payload(&payload).await {
                    warn!(%e, "failed to broadcast manifest via gossip");
                }
            } else {
                // Unicast fallback (tests without gossip).
                self.unicast_to_peers(&ShoalMessage::ManifestPut {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    manifest_bytes,
                })
                .await;
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
                                    // Cache in bounded LRU (not the main store).
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
        if let Some(log_tree) = &self.log_tree {
            // LogTree mode: append delete entry + broadcast.
            // Verify existence via LogTree resolve.
            if log_tree.resolve(bucket, key)?.is_none() {
                return Err(EngineError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                });
            }

            let log_entry = log_tree.append_delete(bucket, key)?;

            let entry_bytes = postcard::to_allocvec(&log_entry).unwrap_or_default();

            if let Some(gossip) = &self.gossip {
                let payload = GossipPayload::LogEntry {
                    entry_bytes,
                    manifest_bytes: None,
                };

                if let Err(e) = gossip.broadcast_payload(&payload).await {
                    warn!(%e, "failed to broadcast delete log entry via gossip");
                }
            } else {
                self.unicast_to_peers(&ShoalMessage::LogEntryBroadcast {
                    entry_bytes,
                    manifest_bytes: None,
                })
                .await;
            }

            info!(bucket, key, "delete_object: delete entry appended");
        } else {
            // Fallback: old behavior (MetaStore).
            let object_id = self.meta.get_object_key(bucket, key)?.ok_or_else(|| {
                EngineError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                }
            })?;

            self.meta.delete_object_key(bucket, key)?;

            // Track deletion so lookup_manifest won't re-fetch from peers.
            self.deleted_keys
                .write()
                .await
                .insert((bucket.to_string(), key.to_string()));

            info!(bucket, key, %object_id, "delete_object: key mapping removed");
        }

        Ok(())
    }

    /// Check if an object exists.
    pub fn has_object(&self, bucket: &str, key: &str) -> Result<bool, EngineError> {
        if let Some(log_tree) = &self.log_tree {
            Ok(log_tree.resolve(bucket, key)?.is_some())
        } else {
            Ok(self.meta.get_object_key(bucket, key)?.is_some())
        }
    }

    /// List objects in a bucket with an optional prefix.
    pub fn list_objects(&self, bucket: &str, prefix: &str) -> Result<Vec<String>, EngineError> {
        if let Some(log_tree) = &self.log_tree {
            Ok(log_tree.list_keys(bucket, prefix)?)
        } else {
            Ok(self.meta.list_objects(bucket, prefix)?)
        }
    }

    /// Retrieve object metadata (manifest) without fetching data.
    pub fn head_object(&self, bucket: &str, key: &str) -> Result<Manifest, EngineError> {
        if let Some(log_tree) = &self.log_tree {
            let object_id =
                log_tree
                    .resolve(bucket, key)?
                    .ok_or_else(|| EngineError::ObjectNotFound {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                    })?;

            log_tree
                .get_manifest(&object_id)?
                .ok_or_else(|| EngineError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                })
        } else {
            let object_id = self.meta.get_object_key(bucket, key)?.ok_or_else(|| {
                EngineError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                }
            })?;

            self.meta
                .get_manifest(&object_id)?
                .ok_or_else(|| EngineError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                })
        }
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
                                // Store if we don't have this key, or if the
                                // peer has a different (newer) ObjectId for it.
                                let local_oid =
                                    self.meta.get_object_key(&entry.bucket, &entry.key)?;
                                let dominated = match local_oid {
                                    None => true,
                                    Some(oid) => oid != manifest.object_id,
                                };

                                if dominated {
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
    // LogTree sync
    // ------------------------------------------------------------------

    /// Sync log entries from cluster peers using the LogTree.
    ///
    /// When a node joins (or restarts), it pulls missing log entries from
    /// peers using its current DAG tips, allowing efficient delta sync.
    ///
    /// Returns the total number of new entries applied.
    pub async fn sync_log_from_peers(&self) -> Result<usize, EngineError> {
        let log_tree = self.log_tree.as_ref().ok_or(EngineError::NoLogTree)?;
        let transport = self.transport.as_ref().ok_or(EngineError::NoTransport)?;

        let my_tips = log_tree.tips()?;
        let tip_refs: Vec<[u8; 32]> = my_tips.clone();

        let peers = self.cluster.members().await;
        let mut total_applied = 0usize;

        for peer in &peers {
            if peer.node_id == self.node_id {
                continue;
            }

            let Some(addr) = self.resolve_addr(&peer.node_id).await else {
                continue;
            };

            match transport.pull_log_entries(addr, &tip_refs).await {
                Ok((entry_bytes_list, manifest_pairs)) => {
                    // Deserialize entries.
                    let mut entries = Vec::new();
                    for eb in &entry_bytes_list {
                        match postcard::from_bytes::<shoal_logtree::LogEntry>(eb) {
                            Ok(entry) => entries.push(entry),
                            Err(e) => {
                                warn!(
                                    from = %peer.node_id,
                                    %e,
                                    "failed to deserialize log entry from peer"
                                );
                            }
                        }
                    }

                    // Deserialize manifests.
                    let mut manifests = Vec::new();
                    for (oid, mb) in &manifest_pairs {
                        match postcard::from_bytes::<Manifest>(mb) {
                            Ok(manifest) => manifests.push((*oid, manifest)),
                            Err(e) => {
                                warn!(
                                    from = %peer.node_id,
                                    %e,
                                    "failed to deserialize manifest from peer"
                                );
                            }
                        }
                    }

                    match log_tree.apply_sync_entries(&entries, &manifests) {
                        Ok(applied) => {
                            if applied > 0 {
                                debug!(
                                    from = %peer.node_id,
                                    applied,
                                    "applied log entries from peer"
                                );
                            }
                            total_applied += applied;
                        }
                        Err(e) => {
                            warn!(
                                from = %peer.node_id,
                                %e,
                                "failed to apply log entries from peer"
                            );
                        }
                    }
                }
                Err(e) => {
                    warn!(from = %peer.node_id, %e, "failed to pull log entries from peer");
                }
            }
        }

        if total_applied > 0 {
            info!(total_applied, "log sync complete");
        }

        Ok(total_applied)
    }

    /// Return a reference to the LogTree, if configured.
    pub fn log_tree(&self) -> Option<&Arc<LogTree>> {
        self.log_tree.as_ref()
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
        // LogTree mode: resolve from the DAG's materialized state,
        // falling back to MetaStore cache if the DAG is incomplete
        // (e.g. entries received via gossip before parents arrived).
        if let Some(log_tree) = &self.log_tree
            && let Some(object_id) = log_tree.resolve(bucket, key)?
            && let Some(m) = log_tree.get_manifest(&object_id)?
        {
            return Ok(Some(m));
        }

        // Fallback: check MetaStore cache (populated by gossip receiver
        // even when LogTree entries can't be applied yet).

        // Fallback: MetaStore + peer pull.

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

    /// Send a message to all peers via unicast QUIC streams.
    ///
    /// Used as a fallback when gossip is not available (tests, single-node).
    async fn unicast_to_peers(&self, msg: &ShoalMessage) {
        let Some(transport) = &self.transport else {
            return;
        };

        let peers = self.cluster.members().await;
        let mut tasks = tokio::task::JoinSet::new();

        for peer in &peers {
            if peer.node_id == self.node_id {
                continue;
            }

            if let Some(addr) = self.resolve_addr(&peer.node_id).await {
                let transport = transport.clone();
                let peer_id = peer.node_id;
                let msg = msg.clone();
                tasks.spawn(async move {
                    if let Err(e) = transport.send_to(addr, &msg).await {
                        warn!(peer = %peer_id, %e, "failed to send unicast message");
                    }
                });
            }
        }

        while let Some(res) = tasks.join_next().await {
            if let Err(e) = res {
                warn!(%e, "unicast broadcast task panicked");
            }
        }
    }
}
