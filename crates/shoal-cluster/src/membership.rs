//! Peer management with QUIC-based health checking.
//!
//! The [`PeerManager`] provides cluster membership via a
//! simple design built directly on iroh QUIC:
//!
//! - **Ping/pong**: periodic `Ping` messages on bi-streams detect failures.
//! - **Join/leave**: `JoinRequest`/`JoinResponse` messages bootstrap new nodes.
//! - **ClusterState**: the shared membership view is updated on state transitions.
//!
//! The [`PeerHandle`] is the public API for interacting with a running
//! `PeerManager` from the daemon handler and other components.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use shoal_meta::MetaStore;
use shoal_net::{ShoalMessage, Transport};
use shoal_types::{Member, MemberState, NodeId, NodeTopology};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::error::ClusterError;
use crate::state::ClusterState;

/// Shared address book type: maps NodeId → EndpointAddr.
pub type AddressBook = Arc<RwLock<HashMap<NodeId, iroh::EndpointAddr>>>;

/// Configuration for the [`PeerManager`].
#[derive(Debug, Clone)]
pub struct PeerManagerConfig {
    /// Interval between ping rounds.
    pub ping_interval: Duration,
    /// Duration after last pong before marking a peer suspect.
    pub suspect_timeout: Duration,
    /// Duration after last pong before marking a peer dead.
    pub dead_timeout: Duration,
    /// Number of consecutive ping failures before marking suspect.
    pub max_failures_before_suspect: u32,
}

impl PeerManagerConfig {
    /// Create a config suitable for fast test execution.
    pub fn test_config() -> Self {
        Self {
            ping_interval: Duration::from_millis(100),
            suspect_timeout: Duration::from_millis(500),
            dead_timeout: Duration::from_secs(2),
            max_failures_before_suspect: 3,
        }
    }

    /// Create a default config for production use.
    pub fn default_config() -> Self {
        Self {
            ping_interval: Duration::from_secs(1),
            suspect_timeout: Duration::from_secs(5),
            dead_timeout: Duration::from_secs(15),
            max_failures_before_suspect: 3,
        }
    }
}

/// Internal state for a single tracked peer.
struct PeerState {
    addr: iroh::EndpointAddr,
    last_pong: Instant,
    consecutive_failures: u32,
    state: MemberState,
    generation: u64,
    capacity: u64,
    topology: NodeTopology,
}

/// Manages cluster peers with QUIC-based health checking.
///
/// Runs a background ping loop that sends `Ping` messages to all known
/// peers and transitions them through Alive → Suspect → Dead based on
/// response patterns.
struct PeerManager {
    config: PeerManagerConfig,
    #[allow(dead_code)]
    node_id: NodeId,
    peers: Arc<RwLock<HashMap<NodeId, PeerState>>>,
    cluster: Arc<ClusterState>,
    transport: Arc<dyn Transport>,
    meta: Option<Arc<MetaStore>>,
    #[allow(dead_code)]
    address_book: AddressBook,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
}

impl PeerManager {
    /// Run the ping loop until shutdown.
    async fn run(&self) {
        info!("peer manager started");

        let mut interval = tokio::time::interval(self.config.ping_interval);
        let mut shutdown_rx = self.shutdown_rx.clone();

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.ping_all_peers().await;
                    self.check_timeouts().await;
                }
                _ = shutdown_rx.changed() => {
                    info!("peer manager shutting down");
                    break;
                }
            }
        }

        info!("peer manager stopped");
    }

    /// Send a ping to every known peer.
    async fn ping_all_peers(&self) {
        let peers: Vec<(NodeId, iroh::EndpointAddr)> = {
            let peers = self.peers.read().await;
            peers
                .iter()
                .filter(|(_, ps)| ps.state != MemberState::Dead)
                .map(|(id, ps)| (*id, ps.addr.clone()))
                .collect()
        };

        for (peer_id, addr) in peers {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            let ping = ShoalMessage::Ping { timestamp };
            match self.transport.request_response(addr, &ping).await {
                Ok(ShoalMessage::Pong { .. }) => {
                    self.handle_pong_internal(peer_id).await;
                }
                Ok(other) => {
                    debug!(%peer_id, "unexpected ping response: {other:?}");
                    self.record_failure(peer_id).await;
                }
                Err(e) => {
                    debug!(%peer_id, %e, "ping failed");
                    self.record_failure(peer_id).await;
                }
            }
        }
    }

    /// Record a successful pong from a peer.
    async fn handle_pong_internal(&self, node_id: NodeId) {
        let mut peers = self.peers.write().await;
        if let Some(ps) = peers.get_mut(&node_id) {
            let was_suspect = ps.state == MemberState::Suspect;
            ps.last_pong = Instant::now();
            ps.consecutive_failures = 0;

            if was_suspect {
                ps.state = MemberState::Alive;
                let member = Member {
                    node_id,
                    capacity: ps.capacity,
                    state: MemberState::Alive,
                    generation: ps.generation,
                    topology: ps.topology.clone(),
                };
                info!(%node_id, "peer recovered from suspect to alive");
                drop(peers);
                self.cluster.add_member(member).await;
            }
        }
    }

    /// Record a ping failure for a peer.
    async fn record_failure(&self, node_id: NodeId) {
        let mut peers = self.peers.write().await;
        if let Some(ps) = peers.get_mut(&node_id) {
            ps.consecutive_failures += 1;
        }
    }

    /// Check all peers for suspect/dead timeouts.
    async fn check_timeouts(&self) {
        let now = Instant::now();
        let mut newly_dead = Vec::new();

        {
            let mut peers = self.peers.write().await;
            for (node_id, ps) in peers.iter_mut() {
                match ps.state {
                    MemberState::Alive => {
                        if ps.consecutive_failures >= self.config.max_failures_before_suspect
                            || now.duration_since(ps.last_pong) >= self.config.suspect_timeout
                        {
                            ps.state = MemberState::Suspect;
                            info!(%node_id, failures = ps.consecutive_failures, "peer is now suspect");
                        }
                    }
                    MemberState::Suspect => {
                        if now.duration_since(ps.last_pong) >= self.config.dead_timeout {
                            ps.state = MemberState::Dead;
                            info!(%node_id, "peer declared dead");
                            newly_dead.push(*node_id);
                        }
                    }
                    MemberState::Dead => {}
                }
            }
        }

        for node_id in newly_dead {
            self.cluster.mark_dead(&node_id).await;

            if let Some(meta) = &self.meta
                && let Some(member) = self.cluster.get_member(&node_id).await
                && let Err(e) = meta.put_member(&member)
            {
                error!(%e, "failed to persist dead member state");
            }
        }
    }
}

/// Handle to a running [`PeerManager`].
///
/// Provides methods for the daemon to interact with the peer manager:
/// joining seeds, handling incoming join requests and pongs, and
/// graceful shutdown.
pub struct PeerHandle {
    peers: Arc<RwLock<HashMap<NodeId, PeerState>>>,
    cluster: Arc<ClusterState>,
    transport: Arc<dyn Transport>,
    meta: Option<Arc<MetaStore>>,
    address_book: AddressBook,
    node_id: NodeId,
    #[allow(dead_code)]
    config: PeerManagerConfig,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl PeerHandle {
    /// Return a reference to the shared cluster state.
    pub fn state(&self) -> &Arc<ClusterState> {
        &self.cluster
    }

    /// Add a known peer (e.g. from persisted state or CLI).
    ///
    /// The peer is added to the peer map and cluster state, making it
    /// visible for pinging and shard placement.
    pub async fn add_peer(
        &self,
        node_id: NodeId,
        addr: iroh::EndpointAddr,
        generation: u64,
        capacity: u64,
        topology: NodeTopology,
    ) {
        let member = Member {
            node_id,
            capacity,
            state: MemberState::Alive,
            generation,
            topology: topology.clone(),
        };

        // Update address book.
        self.address_book
            .write()
            .await
            .insert(node_id, addr.clone());

        // Add to peer tracking.
        self.peers.write().await.insert(
            node_id,
            PeerState {
                addr,
                last_pong: Instant::now(),
                consecutive_failures: 0,
                state: MemberState::Alive,
                generation,
                capacity,
                topology,
            },
        );

        // Persist member.
        if let Some(meta) = &self.meta
            && let Err(e) = meta.put_member(&member)
        {
            error!(%e, "failed to persist member");
        }

        // Add to cluster state (updates ring).
        self.cluster.add_member(member).await;
    }

    /// Join the cluster via a seed node.
    ///
    /// Sends a `JoinRequest` to the seed and processes the `JoinResponse`
    /// to populate the cluster state and address book.
    pub async fn join_via_seed(&self, seed_addr: iroh::EndpointAddr) -> Result<(), ClusterError> {
        let request = ShoalMessage::JoinRequest {
            node_id: self.node_id,
            generation: 1,
            capacity: u64::MAX,
            topology: NodeTopology::default(),
        };

        info!("sending join request to seed");
        let response = self.transport.request_response(seed_addr, &request).await?;

        match response {
            ShoalMessage::JoinResponse { members } => {
                info!(count = members.len(), "received join response with members");
                for member in members {
                    if member.node_id == self.node_id {
                        continue;
                    }

                    let addr = {
                        let book = self.address_book.read().await;
                        book.get(&member.node_id).cloned()
                    }
                    .unwrap_or_else(|| {
                        let eid = iroh::EndpointId::from_bytes(member.node_id.as_bytes())
                            .unwrap_or_else(|_| {
                                warn!(node_id = %member.node_id, "invalid endpoint ID bytes");
                                // This shouldn't happen but fall back gracefully.
                                iroh::EndpointId::from_bytes(&[0u8; 32]).expect("zero ID")
                            });
                        iroh::EndpointAddr::new(eid)
                    });

                    self.add_peer(
                        member.node_id,
                        addr,
                        member.generation,
                        member.capacity,
                        member.topology,
                    )
                    .await;
                }
                Ok(())
            }
            other => Err(ClusterError::Net(shoal_net::NetError::Serialization(
                format!("expected JoinResponse, got: {other:?}"),
            ))),
        }
    }

    /// Handle an incoming join request from a new node.
    ///
    /// Adds the joining node to the cluster and returns a `JoinResponse`
    /// with the current member list.
    pub async fn handle_join_request(
        &self,
        node_id: NodeId,
        generation: u64,
        capacity: u64,
        topology: NodeTopology,
        remote_addr: iroh::EndpointAddr,
    ) -> ShoalMessage {
        self.add_peer(node_id, remote_addr, generation, capacity, topology)
            .await;

        // Collect all members (including ourselves) for the response.
        let members = self.cluster.members().await;
        let mut all_members = vec![Member {
            node_id: self.node_id,
            capacity: u64::MAX,
            state: MemberState::Alive,
            generation: 1,
            topology: NodeTopology::default(),
        }];
        all_members.extend(members);

        ShoalMessage::JoinResponse {
            members: all_members,
        }
    }

    /// Handle an incoming pong from a peer (out-of-band).
    pub async fn handle_pong(&self, node_id: NodeId) {
        let mut peers = self.peers.write().await;
        if let Some(ps) = peers.get_mut(&node_id) {
            let was_suspect = ps.state == MemberState::Suspect;
            ps.last_pong = Instant::now();
            ps.consecutive_failures = 0;

            if was_suspect {
                ps.state = MemberState::Alive;
                let member = Member {
                    node_id,
                    capacity: ps.capacity,
                    state: MemberState::Alive,
                    generation: ps.generation,
                    topology: ps.topology.clone(),
                };
                info!(%node_id, "peer recovered from suspect to alive (external pong)");
                drop(peers);
                self.cluster.add_member(member).await;
            }
        }
    }

    /// Gracefully leave the cluster.
    pub fn leave(&self) {
        info!("leaving cluster");
        let _ = self.shutdown_tx.send(true);
    }

    /// Abort the background task.
    pub fn abort(&self) {
        self.task.abort();
    }

    /// Check whether the background task is still running.
    pub fn is_running(&self) -> bool {
        !self.task.is_finished()
    }
}

/// Start the peer manager and return a handle.
///
/// Spawns a background task that periodically pings all known peers
/// and transitions them through Alive → Suspect → Dead based on
/// response patterns.
pub fn start(
    node_id: NodeId,
    config: PeerManagerConfig,
    cluster: Arc<ClusterState>,
    transport: Arc<dyn Transport>,
    meta: Option<Arc<MetaStore>>,
    address_book: AddressBook,
) -> PeerHandle {
    let peers: Arc<RwLock<HashMap<NodeId, PeerState>>> = Arc::new(RwLock::new(HashMap::new()));
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let manager = PeerManager {
        config: config.clone(),
        node_id,
        peers: peers.clone(),
        cluster: cluster.clone(),
        transport: transport.clone(),
        meta: meta.clone(),
        address_book: address_book.clone(),
        shutdown_rx,
    };

    let task = tokio::spawn(async move {
        manager.run().await;
    });

    PeerHandle {
        peers,
        cluster,
        transport,
        meta,
        address_book,
        node_id,
        config,
        shutdown_tx,
        task,
    }
}
