//! Gossip-based event broadcast using iroh-gossip.
//!
//! The [`GossipService`] wraps an iroh-gossip topic to broadcast and receive
//! [`ClusterEvent`]s across all nodes in the cluster. This is used for
//! high-level events (shard stored, repair needed) while foca handles
//! low-level membership detection.
//!
//! **Note**: This service requires a running iroh [`Endpoint`] and
//! [`Router`](iroh::protocol::Router). In environments where iroh cannot
//! bind (e.g. sandboxed CI), the membership service still functions
//! without gossip — foca provides direct node-to-node communication.

use std::sync::Arc;

use bytes::Bytes;
use futures_lite::StreamExt;
use iroh::Endpoint;
use iroh::protocol::Router;
use iroh_gossip::Gossip;
use iroh_gossip::api::{Event, GossipReceiver, GossipSender};
use iroh_gossip::net::GOSSIP_ALPN;
use iroh_gossip::proto::TopicId;
use shoal_types::ClusterEvent;
use tracing::{debug, error, info, warn};

use crate::error::ClusterError;
use crate::state::ClusterState;

/// Gossip-based event broadcast service.
///
/// Joins a cluster-wide gossip topic and:
/// - Broadcasts local [`ClusterEvent`]s to all peers.
/// - Receives remote events and applies them to [`ClusterState`].
pub struct GossipService {
    gossip: Gossip,
    topic_id: TopicId,
    state: Arc<ClusterState>,
    _router: Option<Router>,
}

impl GossipService {
    /// Create the gossip service with an externally-managed Router.
    ///
    /// The caller must register the returned [`Gossip`] (via [`gossip()`](Self::gossip))
    /// with their own iroh [`Router`] using [`GOSSIP_ALPN`].
    ///
    /// Use this when the node already has a Router for other protocols.
    pub fn new(gossip: Gossip, cluster_secret: &[u8], state: Arc<ClusterState>) -> Self {
        let topic_id = TopicId::from_bytes(*blake3::hash(cluster_secret).as_bytes());
        info!(?topic_id, "gossip service created (external router)");
        Self {
            gossip,
            topic_id,
            state,
            _router: None,
        }
    }

    /// Create and start the gossip service with its own Router.
    ///
    /// The `cluster_secret` is hashed to derive a unique topic ID.
    /// Suitable for standalone use or tests.
    pub async fn start(
        endpoint: Endpoint,
        cluster_secret: &[u8],
        state: Arc<ClusterState>,
    ) -> Result<Self, ClusterError> {
        let topic_id = TopicId::from_bytes(*blake3::hash(cluster_secret).as_bytes());

        let gossip = Gossip::builder()
            .max_message_size(8192)
            .spawn(endpoint.clone());

        let router = Router::builder(endpoint)
            .accept(GOSSIP_ALPN, gossip.clone())
            .spawn();

        info!(?topic_id, "gossip service started");

        Ok(Self {
            gossip,
            topic_id,
            state,
            _router: Some(router),
        })
    }

    /// Return a reference to the underlying [`Gossip`] instance.
    ///
    /// Used by the caller to register with their iroh Router:
    /// ```ignore
    /// Router::builder(endpoint)
    ///     .accept(GOSSIP_ALPN, gossip_service.gossip().clone())
    ///     .spawn();
    /// ```
    pub fn gossip(&self) -> &Gossip {
        &self.gossip
    }

    /// Return the ALPN protocol identifier for gossip.
    ///
    /// Convenience accessor so callers don't need to depend on `iroh-gossip`
    /// directly.
    pub fn alpn() -> &'static [u8] {
        GOSSIP_ALPN
    }

    /// Join the gossip topic with the given bootstrap peers.
    ///
    /// Spawns a background task to receive and apply incoming events to
    /// [`ClusterState`]. Returns a [`GossipHandle`] for broadcasting events.
    pub async fn join(
        &self,
        bootstrap_peers: Vec<iroh::EndpointId>,
    ) -> Result<GossipHandle, ClusterError> {
        let topic = self
            .gossip
            .subscribe_and_join(self.topic_id, bootstrap_peers)
            .await
            .map_err(|e| ClusterError::Gossip(e.to_string()))?;

        let (sender, receiver) = topic.split();

        // Spawn the receiver loop in the background.
        let state = self.state.clone();
        tokio::spawn(run_receiver_loop(receiver, state));

        Ok(GossipHandle { sender })
    }

    /// Return the topic ID used by this gossip instance.
    pub fn topic_id(&self) -> TopicId {
        self.topic_id
    }

    /// Return a reference to the shared cluster state.
    pub fn state(&self) -> &Arc<ClusterState> {
        &self.state
    }

    /// Shut down the gossip service.
    pub async fn shutdown(self) -> Result<(), ClusterError> {
        self.gossip
            .shutdown()
            .await
            .map_err(|e| ClusterError::Gossip(e.to_string()))?;
        if let Some(router) = self._router {
            router
                .shutdown()
                .await
                .map_err(|e| ClusterError::Gossip(e.to_string()))?;
        }
        info!("gossip service shut down");
        Ok(())
    }
}

/// Handle for broadcasting gossip messages on a topic.
///
/// The receive loop is spawned automatically when [`GossipService::join`] is called.
#[derive(Clone)]
pub struct GossipHandle {
    sender: GossipSender,
}

impl GossipHandle {
    /// Broadcast a cluster event to all peers on the gossip topic.
    pub async fn broadcast(&self, event: &ClusterEvent) -> Result<(), ClusterError> {
        let data =
            postcard::to_allocvec(event).map_err(|e| ClusterError::Serialization(e.to_string()))?;
        self.sender
            .broadcast(Bytes::from(data))
            .await
            .map_err(|e: iroh_gossip::api::ApiError| ClusterError::Gossip(e.to_string()))?;
        debug!(?event, "broadcast cluster event");
        Ok(())
    }
}

/// Background receiver loop that applies incoming gossip events to cluster state.
async fn run_receiver_loop(mut receiver: GossipReceiver, state: Arc<ClusterState>) {
    info!("gossip receiver loop started");
    while let Some(event) = receiver.next().await {
        match event {
            Ok(Event::Received(msg)) => match postcard::from_bytes::<ClusterEvent>(&msg.content) {
                Ok(cluster_event) => {
                    debug!(?cluster_event, "received gossip event");
                    state.emit_event(cluster_event);
                }
                Err(e) => {
                    warn!("failed to decode gossip message: {e}");
                }
            },
            Ok(Event::NeighborUp(id)) => {
                debug!(%id, "gossip neighbor up");
            }
            Ok(Event::NeighborDown(id)) => {
                debug!(%id, "gossip neighbor down");
            }
            Ok(Event::Lagged) => {
                warn!("gossip receiver lagged — some events may be lost");
            }
            Err(e) => {
                error!("gossip receiver error: {e}");
                break;
            }
        }
    }

    info!("gossip receiver loop exited");
}
