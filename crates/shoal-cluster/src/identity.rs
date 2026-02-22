//! Cluster member identity for the foca SWIM protocol.
//!
//! [`ClusterIdentity`] wraps node metadata needed for membership detection
//! and implements foca's [`Identity`](foca::Identity) trait. It is kept
//! separate from [`Member`](shoal_types::Member) because foca manages the
//! alive/suspect/down state internally.

use serde::{Deserialize, Serialize};
use shoal_types::{Member, MemberState, NodeId, NodeTopology};

/// Identity exchanged between nodes via the foca SWIM protocol.
///
/// Contains everything needed to address a node and resolve identity
/// conflicts. The `generation` field is bumped on each node restart so
/// that the cluster can distinguish a restarted node from a zombie.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterIdentity {
    /// Unique node identifier (derived from the iroh endpoint key).
    pub node_id: NodeId,
    /// Restart counter — incremented every time the node process starts.
    pub generation: u64,
    /// Advertised storage capacity in bytes.
    pub capacity: u64,
    /// Physical location in the infrastructure hierarchy.
    pub topology: NodeTopology,
}

impl ClusterIdentity {
    /// Create a new identity for the given node.
    pub fn new(node_id: NodeId, generation: u64, capacity: u64, topology: NodeTopology) -> Self {
        Self {
            node_id,
            generation,
            capacity,
            topology,
        }
    }
}

impl foca::Identity for ClusterIdentity {
    /// The address type used to identify a node on the network.
    /// Two identities with the same `Addr` are considered the same physical node.
    type Addr = NodeId;

    /// Create a renewed identity after this node has been declared down.
    /// The new identity has a bumped generation so it wins the conflict.
    fn renew(&self) -> Option<Self> {
        Some(Self {
            generation: self.generation + 1,
            ..self.clone()
        })
    }

    /// Return this node's unique address.
    fn addr(&self) -> NodeId {
        self.node_id
    }

    /// Resolve a conflict when two identities share the same address.
    /// The identity with the higher generation wins.
    fn win_addr_conflict(&self, adversary: &Self) -> bool {
        self.generation > adversary.generation
    }
}

// ---------------------------------------------------------------------------
// Conversions between ClusterIdentity and shoal_types::Member
// ---------------------------------------------------------------------------

impl From<ClusterIdentity> for Member {
    fn from(id: ClusterIdentity) -> Self {
        Member {
            node_id: id.node_id,
            capacity: id.capacity,
            state: MemberState::Alive,
            generation: id.generation,
            topology: id.topology,
        }
    }
}

impl From<&ClusterIdentity> for Member {
    fn from(id: &ClusterIdentity) -> Self {
        Member {
            node_id: id.node_id,
            capacity: id.capacity,
            state: MemberState::Alive,
            generation: id.generation,
            topology: id.topology.clone(),
        }
    }
}

impl From<Member> for ClusterIdentity {
    fn from(m: Member) -> Self {
        ClusterIdentity {
            node_id: m.node_id,
            generation: m.generation,
            capacity: m.capacity,
            topology: m.topology,
        }
    }
}
