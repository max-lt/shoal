//! Cluster state: live membership view and placement ring.
//!
//! [`ClusterState`] is the shared, read-mostly data structure that other
//! components (engine, repair, etc.) use to find out who is in the cluster
//! and where shards belong.

use std::collections::HashMap;
use std::sync::Arc;

use shoal_placement::Ring;
use shoal_types::events::{EventBus, EventOrigin, MembershipDead, MembershipLeft, MembershipReady};
use shoal_types::{Member, MemberState, NodeId};
use tokio::sync::RwLock;
use tracing::info;

/// Shared cluster state maintained by the membership service.
///
/// Holds the current set of members, the placement ring, and a typed
/// [`EventBus`] through which other components receive cluster events.
pub struct ClusterState {
    /// Current cluster members, keyed by node ID.
    members: RwLock<HashMap<NodeId, Member>>,
    /// Consistent hashing ring for shard placement.
    ring: RwLock<Ring>,
    /// This node's identifier.
    local_node_id: NodeId,
    /// Type-safe event bus for intra-node pub/sub.
    ///
    /// Emits typed events (e.g. [`MembershipReady`], [`MembershipDead`]).
    event_bus: EventBus,
    /// Base vnodes per node for the ring.
    vnodes_per_node: u16,
}

impl ClusterState {
    /// Create a new cluster state for the given local node.
    pub fn new(local_node_id: NodeId, vnodes_per_node: u16) -> Arc<Self> {
        Arc::new(Self {
            members: RwLock::new(HashMap::new()),
            ring: RwLock::new(Ring::new(vnodes_per_node)),
            local_node_id,
            event_bus: EventBus::new(),
            vnodes_per_node,
        })
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
    /// Adds the node to the placement ring and emits a
    /// [`MembershipReady`] event.
    pub async fn add_member(&self, member: Member) {
        let node_id = member.node_id;
        let capacity = member.capacity;
        let topology = member.topology.clone();

        {
            let mut members = self.members.write().await;
            members.insert(node_id, member);
        }
        {
            let mut ring = self.ring.write().await;
            ring.add_node(node_id, capacity, topology);
        }

        info!(%node_id, "member joined cluster");
        self.event_bus.emit(MembershipReady {
            node_id,
            origin: EventOrigin::Local,
        });
    }

    /// Remove a member from the cluster (graceful departure).
    ///
    /// Removes the node from the placement ring and emits a
    /// [`MembershipLeft`] event.
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
        self.event_bus.emit(MembershipLeft {
            node_id: *node_id,
            origin: EventOrigin::Local,
        });
    }

    /// Mark a member as dead (failure detected).
    ///
    /// Updates the member's state, removes from the placement ring,
    /// and emits a [`MembershipDead`] event.
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
        self.event_bus.emit(MembershipDead {
            node_id: *node_id,
            origin: EventOrigin::Local,
        });
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

    /// Return a reference to the typed event bus.
    pub fn event_bus(&self) -> &EventBus {
        &self.event_bus
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
