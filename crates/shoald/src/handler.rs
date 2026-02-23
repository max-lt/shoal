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
use shoal_logtree::LogTree;
use shoal_meta::MetaStore;
use shoal_net::{ManifestSyncEntry, ShoalMessage, ShoalTransport};
use shoal_store::ShardStore;
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
        }
    }

    /// Set the LogTree for DAG-based mutation tracking.
    pub fn with_log_tree(mut self, log_tree: Arc<LogTree>) -> Self {
        self.log_tree = Some(log_tree);
        self
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
            .insert(remote_node_id, remote_addr);

        // Spawn a handler for uni-directional streams (SWIM data, shard push, manifest, log entries).
        let conn_uni = conn.clone();
        let membership = self.membership.clone();
        let store_uni = self.store.clone();
        let meta_uni = self.meta.clone();
        let log_tree_uni = self.log_tree.clone();
        tokio::spawn(async move {
            ShoalTransport::handle_connection(conn_uni, move |msg, _conn| {
                let membership = membership.clone();
                let store = store_uni.clone();
                let meta = meta_uni.clone();
                let log_tree = log_tree_uni.clone();
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
                                match postcard::from_bytes::<shoal_logtree::LogEntry>(&entry_bytes)
                                {
                                    Ok(entry) => {
                                        let manifest = manifest_bytes
                                            .as_ref()
                                            .and_then(|b| postcard::from_bytes::<Manifest>(b).ok());
                                        match log_tree.receive_entry(&entry, manifest.as_ref()) {
                                            Ok(true) => {
                                                debug!("stored log entry");
                                            }
                                            Ok(false) => {} // already known
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
