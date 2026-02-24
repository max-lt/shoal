//! Cluster state: live membership view and placement ring.
//!
//! [`ClusterState`] is the shared, read-mostly data structure that other
//! components (engine, repair, etc.) use to find out who is in the cluster
//! and where shards belong.

use std::collections::HashMap;
use std::sync::Arc;

use shoal_placement::Ring;
use shoal_types::{ClusterEvent, Member, MemberState, NodeId};
use tokio::sync::{RwLock, broadcast};
use tracing::info;

/// Shared cluster state maintained by the membership service.
///
/// Holds the current set of members, the placement ring, and a broadcast
/// channel through which other components can subscribe to cluster events.
pub struct ClusterState {
    /// Current cluster members, keyed by node ID.
    members: RwLock<HashMap<NodeId, Member>>,
    /// Consistent hashing ring for shard placement.
    ring: RwLock<Ring>,
    /// This node's identifier.
    local_node_id: NodeId,
    /// Broadcast channel for cluster events.
    ///
    /// Subscribers: repair detector, engine, etc.
    event_tx: broadcast::Sender<ClusterEvent>,
    /// Base vnodes per node for the ring.
    vnodes_per_node: u16,
}

impl ClusterState {
    /// Create a new cluster state for the given local node.
    pub fn new(local_node_id: NodeId, vnodes_per_node: u16) -> Arc<Self> {
        let (event_tx, _) = broadcast::channel(256);
        Arc::new(Self {
            members: RwLock::new(HashMap::new()),
            ring: RwLock::new(Ring::new(vnodes_per_node)),
            local_node_id,
            event_tx,
            vnodes_per_node,
        })
    }

    /// Subscribe to ALL cluster events (local + remote).
    ///
    /// Use this for components that need to react to any membership change
    /// (repair detector, etc.).
    pub fn subscribe(&self) -> broadcast::Receiver<ClusterEvent> {
        self.event_tx.subscribe()
    }

    /// Return this node's ID.
    pub fn local_node_id(&self) -> NodeId {
        self.local_node_id
    }

    /// Return the base vnodes-per-node count for the ring.
    pub fn vnodes_per_node(&self) -> u16 {
        self.vnodes_per_node
    }

    /// Add or update a member in the cluster.
    ///
    /// Adds the node to the placement ring and broadcasts a
    /// [`ClusterEvent::NodeJoined`] event.
    pub async fn add_member(&self, member: Member) {
        let node_id = member.node_id;
        let capacity = member.capacity;
        let topology = member.topology.clone();

        {
            let mut members = self.members.write().await;
            members.insert(node_id, member.clone());
        }
        {
            let mut ring = self.ring.write().await;
            ring.add_node(node_id, capacity, topology);
        }

        info!(%node_id, "member joined cluster");
        let _ = self.event_tx.send(ClusterEvent::NodeJoined(member));
    }

    /// Remove a member from the cluster (graceful departure).
    ///
    /// Removes the node from the placement ring and broadcasts a
    /// [`ClusterEvent::NodeLeft`] event.
    pub async fn remove_member(&self, node_id: &NodeId) {
        {
            let mut members = self.members.write().await;
            members.remove(node_id);
        }
        {
            let mut ring = self.ring.write().await;
            ring.remove_node(node_id);
        }

        info!(%node_id, "member left cluster");
        let _ = self.event_tx.send(ClusterEvent::NodeLeft(*node_id));
    }

    /// Mark a member as dead (failure detected).
    ///
    /// Updates the member's state, removes from the placement ring,
    /// and broadcasts a [`ClusterEvent::NodeDead`] event.
    pub async fn mark_dead(&self, node_id: &NodeId) {
        {
            let mut members = self.members.write().await;
            if let Some(member) = members.get_mut(node_id) {
                member.state = MemberState::Dead;
            }
        }
        {
            let mut ring = self.ring.write().await;
            ring.remove_node(node_id);
        }

        info!(%node_id, "member declared dead");
        let _ = self.event_tx.send(ClusterEvent::NodeDead(*node_id));
    }

    /// Return a snapshot of all current members.
    pub async fn members(&self) -> Vec<Member> {
        self.members.read().await.values().cloned().collect()
    }

    /// Return a specific member by node ID.
    pub async fn get_member(&self, node_id: &NodeId) -> Option<Member> {
        self.members.read().await.get(node_id).cloned()
    }

    /// Return a clone of the current placement ring.
    pub async fn ring(&self) -> Ring {
        self.ring.read().await.clone()
    }

    /// Return the number of members in the cluster.
    pub async fn member_count(&self) -> usize {
        self.members.read().await.len()
    }

    /// Return the number of alive members in the cluster.
    pub async fn alive_count(&self) -> usize {
        self.members
            .read()
            .await
            .values()
            .filter(|m| m.state == MemberState::Alive)
            .count()
    }

    /// Return a snapshot of all node IDs.
    pub async fn node_ids(&self) -> Vec<NodeId> {
        self.members.read().await.keys().copied().collect()
    }
}

impl std::fmt::Debug for ClusterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterState")
            .field("local_node_id", &self.local_node_id)
            .field("vnodes_per_node", &self.vnodes_per_node)
            .finish_non_exhaustive()
    }
}
