//! Incoming protocol handler for the Shoal daemon.
//!
//! Implements iroh's [`ProtocolHandler`] trait to handle incoming QUIC
//! connections dispatched by the iroh [`Router`].
//!
//! [`ProtocolHandler`]: iroh::protocol::ProtocolHandler
//! [`Router`]: iroh::protocol::Router

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use iroh::endpoint::Connection;
use iroh::protocol::AcceptError;
use shoal_cluster::membership::MembershipHandle;
use shoal_engine::pending::{self, PendingBuffer, PendingEntry};
use shoal_logtree::{LogEntry, LogTree, LogTreeError};
use shoal_meta::MetaStore;
use shoal_net::{ManifestSyncEntry, ShoalMessage, ShoalTransport, Transport};
use shoal_store::ShardStore;
use shoal_types::events::{EventBus, ShardSource, ShardStored};
use shoal_types::{Manifest, NodeId, ObjectId};
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Handles incoming Shoal protocol connections.
///
/// Registered with an iroh [`Router`](iroh::protocol::Router) to process
/// incoming QUIC connections from other Shoal nodes. Dispatches messages
/// to the appropriate subsystems: shard store, metadata store, and
/// membership service.
pub struct ShoalProtocol {
    store: Arc<dyn ShardStore>,
    meta: Arc<MetaStore>,
    membership: Arc<MembershipHandle>,
    address_book: Arc<RwLock<HashMap<NodeId, iroh::EndpointAddr>>>,
    log_tree: Option<Arc<LogTree>>,
    /// Buffer for log entries that arrived before their parents.
    pending_entries: PendingBuffer,
    /// Transport for outgoing targeted pulls (eager pull on MissingParents).
    transport: Option<Arc<dyn Transport>>,
    /// Typed event bus for emitting events on incoming operations.
    event_bus: Option<EventBus>,
}

impl fmt::Debug for ShoalProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShoalProtocol").finish_non_exhaustive()
    }
}

impl ShoalProtocol {
    /// Create a new protocol handler.
    pub fn new(
        store: Arc<dyn ShardStore>,
        meta: Arc<MetaStore>,
        membership: Arc<MembershipHandle>,
        address_book: Arc<RwLock<HashMap<NodeId, iroh::EndpointAddr>>>,
    ) -> Self {
        Self {
            store,
            meta,
            membership,
            address_book,
            log_tree: None,
            pending_entries: Arc::new(std::sync::Mutex::new(Vec::new())),
            transport: None,
            event_bus: None,
        }
    }

    /// Set the LogTree for DAG-based mutation tracking.
    pub fn with_log_tree(mut self, log_tree: Arc<LogTree>) -> Self {
        self.log_tree = Some(log_tree);
        self
    }

    /// Set the transport for outgoing targeted pulls.
    pub fn with_transport(mut self, transport: Arc<dyn Transport>) -> Self {
        self.transport = Some(transport);
        self
    }

    /// Set the typed event bus for emitting events.
    pub fn with_event_bus(mut self, bus: EventBus) -> Self {
        self.event_bus = Some(bus);
        self
    }

    /// Return a clone of the pending entry buffer for external use.
    pub fn pending_buffer(&self) -> PendingBuffer {
        self.pending_entries.clone()
    }
}

/// Process a `ProvideLogEntries` message received via unicast.
///
/// Applies entries to the LogTree and caches manifests in MetaStore.
/// Then drains the pending buffer in case newly arrived entries unblock
/// previously buffered ones.
///
/// On `MissingParents`, buffers the entry and (if transport is available)
/// spawns a background targeted pull from the entry's author.
fn handle_provide_log_entries(
    entry_bytes_list: Vec<Vec<u8>>,
    manifest_pairs: Vec<(ObjectId, Vec<u8>)>,
    log_tree: Option<&Arc<LogTree>>,
    meta: &MetaStore,
    pending_buf: &PendingBuffer,
    transport: Option<&Arc<dyn Transport>>,
    address_book: &Arc<RwLock<HashMap<NodeId, iroh::EndpointAddr>>>,
) {
    let Some(log_tree) = log_tree else {
        return;
    };

    // Build a manifest lookup table.
    let manifests: HashMap<ObjectId, Manifest> = manifest_pairs
        .iter()
        .filter_map(|(oid, mb)| postcard::from_bytes::<Manifest>(mb).ok().map(|m| (*oid, m)))
        .collect();

    let mut applied = 0usize;
    let mut missing_authors: Vec<(NodeId, Vec<[u8; 32]>)> = Vec::new();

    for eb in &entry_bytes_list {
        let entry = match postcard::from_bytes::<LogEntry>(eb) {
            Ok(e) => e,
            Err(e) => {
                warn!(%e, "failed to deserialize provided log entry");
                continue;
            }
        };

        // Find the associated manifest (if any) for this entry.
        let manifest = match &entry.action {
            shoal_logtree::Action::Put { manifest_id, .. } => manifests.get(manifest_id),
            _ => None,
        };

        // Cache manifest in MetaStore regardless of DAG state.
        if let Some(m) = manifest {
            let _ = meta.put_manifest(m);

            if let shoal_logtree::Action::Put { bucket, key, .. } = &entry.action {
                let _ = meta.put_object_key(bucket, key, &m.object_id);
            }
        }

        match log_tree.receive_entry(&entry, manifest) {
            Ok(true) => applied += 1,
            Ok(false) => {} // already known
            Err(LogTreeError::MissingParents(parents)) => {
                // Buffer for later.
                let mb = manifest.and_then(|m| postcard::to_allocvec(m).ok());
                let author = entry.node_id;
                pending_buf
                    .lock()
                    .expect("pending lock poisoned")
                    .push(PendingEntry::new(entry, mb));

                // Track for eager pull.
                missing_authors.push((author, parents));
            }
            Err(e) => {
                warn!(%e, "failed to apply provided log entry");
            }
        }
    }

    if applied > 0 {
        debug!(applied, "applied provided log entries");
        pending::drain_pending(log_tree, pending_buf);
    }

    // Eager pull: for entries with missing parents, spawn background
    // targeted pulls from each author.
    if !missing_authors.is_empty()
        && let Some(transport) = transport
    {
        let transport = transport.clone();
        let log_tree = log_tree.clone();
        let pending_buf = pending_buf.clone();
        let address_book = address_book.clone();

        tokio::spawn(async move {
            // Deduplicate by author.
            let mut seen_authors = std::collections::HashSet::new();
            for (author, missing_hashes) in &missing_authors {
                if !seen_authors.insert(*author) {
                    continue;
                }

                let addr = {
                    let book = address_book.read().await;
                    book.get(author).cloned()
                };

                let Some(addr) = addr else {
                    continue;
                };

                let tips = match log_tree.tips() {
                    Ok(t) => t,
                    Err(_) => continue,
                };

                match transport.pull_log_sync(addr, missing_hashes, &tips).await {
                    Ok((entry_bytes, manifest_pairs)) => {
                        let mut entries = Vec::new();
                        for eb in &entry_bytes {
                            if let Ok(e) = postcard::from_bytes::<LogEntry>(eb) {
                                entries.push(e);
                            }
                        }
                        let mut mans = Vec::new();
                        for (oid, mb) in &manifest_pairs {
                            if let Ok(m) = postcard::from_bytes::<Manifest>(mb) {
                                mans.push((*oid, m));
                            }
                        }
                        let _ = log_tree.apply_sync_entries(&entries, &mans);
                        pending::drain_pending(&log_tree, &pending_buf);
                    }
                    Err(e) => {
                        debug!(from = %author, %e, "eager pull for missing parents failed");
                    }
                }
            }
        });
    }
}

impl iroh::protocol::ProtocolHandler for ShoalProtocol {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        // Learn the remote peer's address for future routing.
        let remote_id = conn.remote_id();
        let remote_node_id = NodeId::from(*remote_id.as_bytes());
        let remote_addr = iroh::EndpointAddr::new(remote_id);
        self.address_book
            .write()
            .await
            .entry(remote_node_id)
            .or_insert(remote_addr);

        // Spawn a handler for uni-directional streams (SWIM data + log entry provides).
        let conn_uni = conn.clone();
        let membership = self.membership.clone();
        let log_tree_uni = self.log_tree.clone();
        let meta_uni = self.meta.clone();
        let pending_uni = self.pending_entries.clone();
        let transport_uni = self.transport.clone();
        let address_book_uni = self.address_book.clone();
        tokio::spawn(async move {
            ShoalTransport::handle_connection(conn_uni, move |msg, _conn| {
                let membership = membership.clone();
                let log_tree = log_tree_uni.clone();
                let meta = meta_uni.clone();
                let pending = pending_uni.clone();
                let transport = transport_uni.clone();
                let address_book = address_book_uni.clone();
                async move {
                    match msg {
                        ShoalMessage::SwimData(data) => {
                            if let Err(e) = membership.feed_data(data) {
                                warn!(%e, "failed to feed SWIM data");
                            }
                        }
                        ShoalMessage::ProvideLogEntries { entries, manifests } => {
                            handle_provide_log_entries(
                                entries,
                                manifests,
                                log_tree.as_ref(),
                                &meta,
                                &pending,
                                transport.as_ref(),
                                &address_book,
                            );
                        }
                        other => {
                            debug!("unhandled uni-stream message: {other:?}");
                        }
                    }
                }
            })
            .await;
        });

        // Handle bi-directional streams (shard pull, manifest pull, log sync).
        let store = self.store.clone();
        let meta = self.meta.clone();
        let log_tree_bi = self.log_tree.clone();
        let event_bus_bi = self.event_bus.clone();
        tokio::spawn(async move {
            ShoalTransport::handle_bi_streams(conn, move |msg| {
                let store = store.clone();
                let meta = meta.clone();
                let log_tree = log_tree_bi.clone();
                let event_bus = event_bus_bi.clone();
                async move {
                    match msg {
                        ShoalMessage::ShardPush { shard_id, data } => {
                            debug!(%shard_id, len = data.len(), "received shard push (bi-stream)");
                            let ok = store.put(shard_id, bytes::Bytes::from(data)).await.is_ok();
                            if !ok {
                                warn!(%shard_id, "failed to store pushed shard");
                            } else if let Some(bus) = &event_bus {
                                bus.emit(ShardStored {
                                    shard_id,
                                    source: ShardSource::PeerPush,
                                });
                            }
                            Some(ShoalMessage::ShardPushAck { shard_id, ok })
                        }
                        ShoalMessage::ShardRequest { shard_id } => {
                            let data = store.get(shard_id).await.ok().flatten();
                            Some(ShoalMessage::ShardResponse {
                                shard_id,
                                data: data.map(|b| b.to_vec()),
                            })
                        }
                        ShoalMessage::ManifestRequest { bucket, key } => {
                            // LOCAL-ONLY: no recursive peer pull.
                            let manifest_bytes = meta
                                .get_object_key(&bucket, &key)
                                .ok()
                                .flatten()
                                .and_then(|oid| meta.get_manifest(&oid).ok().flatten())
                                .or_else(|| {
                                    let lt = log_tree.as_ref()?;
                                    let oid = lt.resolve(&bucket, &key).ok()??;
                                    lt.get_manifest(&oid).ok()?
                                })
                                .and_then(|m| postcard::to_allocvec(&m).ok());
                            Some(ShoalMessage::ManifestResponse {
                                bucket,
                                key,
                                manifest_bytes,
                            })
                        }
                        ShoalMessage::ManifestSyncRequest => {
                            let entries = meta
                                .list_all_object_entries()
                                .unwrap_or_default()
                                .into_iter()
                                .filter_map(|(bucket, key, oid)| {
                                    let manifest = meta.get_manifest(&oid).ok().flatten()?;
                                    let bytes = postcard::to_allocvec(&manifest).ok()?;
                                    Some(ManifestSyncEntry {
                                        bucket,
                                        key,
                                        manifest_bytes: bytes,
                                    })
                                })
                                .collect();
                            Some(ShoalMessage::ManifestSyncResponse { entries })
                        }
                        ShoalMessage::LogSyncRequest { tips } => {
                            // LOCAL-ONLY: compute delta from local store.
                            if let Some(log_tree) = &log_tree {
                                let delta = log_tree.compute_delta(&tips).unwrap_or_default();
                                let entries: Vec<Vec<u8>> = delta
                                    .iter()
                                    .filter_map(|e| postcard::to_allocvec(e).ok())
                                    .collect();
                                let manifests: Vec<(ObjectId, Vec<u8>)> = delta
                                    .iter()
                                    .filter_map(|e| match &e.action {
                                        shoal_logtree::Action::Put { manifest_id, .. } => {
                                            let m = log_tree.get_manifest(manifest_id).ok()??;
                                            Some((*manifest_id, postcard::to_allocvec(&m).ok()?))
                                        }
                                        _ => None,
                                    })
                                    .collect();
                                Some(ShoalMessage::LogSyncResponse { entries, manifests })
                            } else {
                                Some(ShoalMessage::LogSyncResponse {
                                    entries: vec![],
                                    manifests: vec![],
                                })
                            }
                        }
                        ShoalMessage::LogSyncPull {
                            entry_hashes,
                            requester_tips,
                        } => {
                            // LOCAL-ONLY: BFS from entry_hashes backward,
                            // stopping at requester_tips. No recursive peer pull.
                            if let Some(log_tree) = &log_tree {
                                let delta = log_tree
                                    .compute_pull_delta(&entry_hashes, &requester_tips)
                                    .unwrap_or_default();
                                let entries: Vec<Vec<u8>> = delta
                                    .iter()
                                    .filter_map(|e| postcard::to_allocvec(e).ok())
                                    .collect();
                                let manifests: Vec<(ObjectId, Vec<u8>)> = delta
                                    .iter()
                                    .filter_map(|e| match &e.action {
                                        shoal_logtree::Action::Put { manifest_id, .. } => {
                                            let m = log_tree.get_manifest(manifest_id).ok()??;
                                            Some((*manifest_id, postcard::to_allocvec(&m).ok()?))
                                        }
                                        _ => None,
                                    })
                                    .collect();
                                Some(ShoalMessage::LogSyncPullResponse { entries, manifests })
                            } else {
                                Some(ShoalMessage::LogSyncPullResponse {
                                    entries: vec![],
                                    manifests: vec![],
                                })
                            }
                        }
                        _ => None,
                    }
                }
            })
            .await;
        });

        Ok(())
    }
}
