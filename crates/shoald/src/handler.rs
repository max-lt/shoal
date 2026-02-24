//! Incoming protocol handler for the Shoal daemon.
//!
//! Implements iroh's [`ProtocolHandler`] trait to handle incoming QUIC
//! connections dispatched by the iroh [`Router`].
//!
//! [`ProtocolHandler`]: iroh::protocol::ProtocolHandler
//! [`Router`]: iroh::protocol::Router

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use iroh::endpoint::Connection;
use iroh::protocol::AcceptError;
use shoal_cluster::membership::MembershipHandle;
use shoal_logtree::{LogEntry, LogTree, LogTreeError};
use shoal_meta::MetaStore;
use shoal_net::{ManifestSyncEntry, ShoalMessage, ShoalTransport};
use shoal_store::ShardStore;
use shoal_types::{Manifest, NodeId, ObjectId};
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Maximum number of entries in the pending buffer before we start dropping.
const PENDING_BUFFER_CAP: usize = 1000;

/// An entry waiting for its parents to arrive.
pub(crate) struct PendingEntry {
    entry: LogEntry,
    manifest_bytes: Option<Vec<u8>>,
}

/// Type alias for the shared pending entry buffer.
pub type PendingBuffer = Arc<Mutex<Vec<PendingEntry>>>;

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
    pending_entries: Arc<Mutex<Vec<PendingEntry>>>,
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
            pending_entries: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Set the LogTree for DAG-based mutation tracking.
    pub fn with_log_tree(mut self, log_tree: Arc<LogTree>) -> Self {
        self.log_tree = Some(log_tree);
        self
    }

    /// Return a clone of the pending entry buffer for external drain calls.
    pub fn pending_buffer(&self) -> PendingBuffer {
        self.pending_entries.clone()
    }
}

/// Drain pending log entries from an external context (e.g. after sync).
///
/// Call this after `sync_log_from_peers()` to flush entries that arrived
/// before their parents were synced.
pub fn drain_pending_log_entries(log_tree: &LogTree, pending: &Mutex<Vec<PendingEntry>>) -> usize {
    drain_pending(log_tree, pending)
}

/// Try to apply buffered pending entries whose parents may now be available.
///
/// Returns the number of entries successfully applied.
fn drain_pending(log_tree: &LogTree, pending: &Mutex<Vec<PendingEntry>>) -> usize {
    let mut buf = pending.lock().expect("pending lock poisoned");

    if buf.is_empty() {
        return 0;
    }

    let mut applied = 0;
    let mut i = 0;

    while i < buf.len() {
        let manifest = buf[i]
            .manifest_bytes
            .as_ref()
            .and_then(|b| postcard::from_bytes::<Manifest>(b).ok());

        match log_tree.receive_entry(&buf[i].entry, manifest.as_ref()) {
            Ok(true) => {
                debug!("applied pending log entry");
                buf.swap_remove(i);
                applied += 1;
                // Don't increment i — swap_remove moved the last element here.
            }
            Ok(false) => {
                // Already known — drop from buffer.
                buf.swap_remove(i);
            }
            Err(LogTreeError::MissingParents(_)) => {
                // Still missing parents — keep in buffer.
                i += 1;
            }
            Err(e) => {
                warn!(%e, "dropping invalid pending log entry");
                buf.swap_remove(i);
            }
        }
    }

    if applied > 0 {
        debug!(
            applied,
            remaining = buf.len(),
            "drained pending log entries"
        );
    }

    applied
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
            .insert(remote_node_id, remote_addr);

        // Spawn a handler for uni-directional streams (SWIM data, shard push, manifest, log entries).
        let conn_uni = conn.clone();
        let membership = self.membership.clone();
        let store_uni = self.store.clone();
        let meta_uni = self.meta.clone();
        let log_tree_uni = self.log_tree.clone();
        let pending_uni = self.pending_entries.clone();
        tokio::spawn(async move {
            ShoalTransport::handle_connection(conn_uni, move |msg, _conn| {
                let membership = membership.clone();
                let store = store_uni.clone();
                let meta = meta_uni.clone();
                let log_tree = log_tree_uni.clone();
                let pending = pending_uni.clone();
                async move {
                    match msg {
                        ShoalMessage::SwimData(data) => {
                            if let Err(e) = membership.feed_data(data) {
                                warn!(%e, "failed to feed SWIM data");
                            }
                        }
                        ShoalMessage::ShardPush { shard_id, data } => {
                            debug!(%shard_id, len = data.len(), "received shard push");
                            if let Err(e) = store.put(shard_id, bytes::Bytes::from(data)).await {
                                warn!(%shard_id, %e, "failed to store pushed shard");
                            }
                        }
                        ShoalMessage::LogEntryBroadcast {
                            entry_bytes,
                            manifest_bytes,
                        } => {
                            if let Some(log_tree) = &log_tree {
                                match postcard::from_bytes::<LogEntry>(&entry_bytes) {
                                    Ok(entry) => {
                                        let manifest = manifest_bytes
                                            .as_ref()
                                            .and_then(|b| postcard::from_bytes::<Manifest>(b).ok());

                                        match log_tree.receive_entry(&entry, manifest.as_ref()) {
                                            Ok(true) => {
                                                debug!("stored log entry");
                                                // Try to drain buffered entries now that
                                                // a new entry (potential parent) is available.
                                                drain_pending(log_tree, &pending);
                                            }
                                            Ok(false) => {} // already known
                                            Err(LogTreeError::MissingParents(_)) => {
                                                // Buffer for retry — parents may arrive later.
                                                let mut buf =
                                                    pending.lock().expect("pending lock poisoned");

                                                if buf.len() >= PENDING_BUFFER_CAP {
                                                    warn!(
                                                        cap = PENDING_BUFFER_CAP,
                                                        "pending entry buffer full, dropping oldest"
                                                    );
                                                    buf.remove(0);
                                                }

                                                buf.push(PendingEntry {
                                                    entry,
                                                    manifest_bytes,
                                                });
                                                debug!(
                                                    pending = buf.len(),
                                                    "buffered log entry with missing parents"
                                                );
                                            }
                                            Err(e) => {
                                                warn!(
                                                    %e,
                                                    "failed to process log entry"
                                                );
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        warn!(
                                            %e,
                                            "failed to deserialize log entry broadcast"
                                        );
                                    }
                                }
                            } else {
                                debug!("received log entry broadcast but no LogTree configured");
                            }
                        }
                        ShoalMessage::ManifestPut {
                            bucket,
                            key,
                            manifest_bytes,
                        } => match postcard::from_bytes::<Manifest>(&manifest_bytes) {
                            Ok(manifest) => {
                                debug!(
                                    %bucket, %key,
                                    object_id = %manifest.object_id,
                                    "received manifest broadcast"
                                );
                                if let Err(e) = meta.put_manifest(&manifest) {
                                    warn!(%e, "failed to store broadcast manifest");
                                }
                                if let Err(e) =
                                    meta.put_object_key(&bucket, &key, &manifest.object_id)
                                {
                                    warn!(
                                        %e,
                                        "failed to store broadcast object key"
                                    );
                                }
                            }
                            Err(e) => {
                                warn!(
                                    %e,
                                    "failed to deserialize broadcast manifest"
                                );
                            }
                        },
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
        tokio::spawn(async move {
            ShoalTransport::handle_bi_streams(conn, move |msg| {
                let store = store.clone();
                let meta = meta.clone();
                let log_tree = log_tree_bi.clone();
                async move {
                    match msg {
                        ShoalMessage::ShardRequest { shard_id } => {
                            let data = store.get(shard_id).await.ok().flatten();
                            Some(ShoalMessage::ShardResponse {
                                shard_id,
                                data: data.map(|b| b.to_vec()),
                            })
                        }
                        ShoalMessage::ManifestRequest { bucket, key } => {
                            let manifest_bytes = meta
                                .get_object_key(&bucket, &key)
                                .ok()
                                .flatten()
                                .and_then(|oid| meta.get_manifest(&oid).ok().flatten())
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
                        _ => None,
                    }
                }
            })
            .await;
        });

        Ok(())
    }
}
