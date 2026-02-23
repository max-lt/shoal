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
use shoal_meta::MetaStore;
use shoal_net::{ManifestSyncEntry, ShoalMessage, ShoalTransport};
use shoal_store::ShardStore;
use shoal_types::{HybridClock, NodeId};
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
    /// Hybrid Logical Clock for causal ordering of manifest updates.
    hlc: Arc<HybridClock>,
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
        hlc: Arc<HybridClock>,
    ) -> Self {
        Self {
            store,
            meta,
            membership,
            address_book,
            hlc,
        }
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

        // Spawn a handler for uni-directional streams (SWIM data, shard push, manifest).
        let conn_uni = conn.clone();
        let membership = self.membership.clone();
        let store_uni = self.store.clone();
        let meta_uni = self.meta.clone();
        let hlc_uni = self.hlc.clone();
        tokio::spawn(async move {
            ShoalTransport::handle_connection(conn_uni, move |msg, _conn| {
                let membership = membership.clone();
                let store = store_uni.clone();
                let meta = meta_uni.clone();
                let hlc = hlc_uni.clone();
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
                        ShoalMessage::ManifestPut {
                            bucket,
                            key,
                            manifest_bytes,
                        } => match postcard::from_bytes::<shoal_types::Manifest>(&manifest_bytes) {
                            Ok(manifest) => {
                                // Update local HLC to maintain causal ordering.
                                hlc.update(manifest.hlc);

                                // Check if we should accept this manifest based on HLC ordering.
                                let should_accept =
                                    match meta.should_accept_manifest(&bucket, &key, &manifest) {
                                        Ok(accept) => accept,
                                        Err(e) => {
                                            warn!(%e, "failed to check manifest acceptance");
                                            true // Accept on error to avoid data loss.
                                        }
                                    };

                                if should_accept {
                                    debug!(
                                        %bucket, %key,
                                        object_id = %manifest.object_id,
                                        hlc = manifest.hlc,
                                        "accepted manifest broadcast (HLC wins)"
                                    );
                                    if let Err(e) = meta.put_manifest(&manifest) {
                                        warn!(%e, "failed to store broadcast manifest");
                                    }
                                    // Store the version entry.
                                    if let Err(e) = meta.put_version(
                                        &bucket,
                                        &key,
                                        manifest.hlc,
                                        &manifest.object_id,
                                    ) {
                                        warn!(%e, "failed to store broadcast version");
                                    }
                                    // Update the "current" pointer only if not a delete marker.
                                    if manifest.is_delete_marker {
                                        if let Err(e) = meta.delete_object_key(&bucket, &key) {
                                            warn!(
                                                %e,
                                                "failed to remove key for delete marker"
                                            );
                                        }
                                    } else if let Err(e) =
                                        meta.put_object_key(&bucket, &key, &manifest.object_id)
                                    {
                                        warn!(
                                            %e,
                                            "failed to store broadcast object key"
                                        );
                                    }
                                } else {
                                    debug!(
                                        %bucket, %key,
                                        object_id = %manifest.object_id,
                                        hlc = manifest.hlc,
                                        "ignored manifest broadcast (HLC loses)"
                                    );
                                    // Still store the manifest and version for history,
                                    // but don't update the "current" pointer.
                                    if let Err(e) = meta.put_manifest(&manifest) {
                                        warn!(%e, "failed to store non-winning manifest");
                                    }
                                    if let Err(e) = meta.put_version(
                                        &bucket,
                                        &key,
                                        manifest.hlc,
                                        &manifest.object_id,
                                    ) {
                                        warn!(%e, "failed to store non-winning version");
                                    }
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

        // Handle bi-directional streams (shard pull, manifest pull).
        let store = self.store.clone();
        let meta = self.meta.clone();
        tokio::spawn(async move {
            ShoalTransport::handle_bi_streams(conn, move |msg| {
                let store = store.clone();
                let meta = meta.clone();
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
                        _ => None,
                    }
                }
            })
            .await;
        });

        Ok(())
    }
}
