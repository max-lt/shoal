//! Error types for the cluster crate.

use shoal_types::NodeId;

/// Errors produced by the cluster membership and gossip subsystems.
#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    /// An error originating from the foca SWIM protocol.
    #[error("foca error: {0}")]
    Foca(String),

    /// An error from the gossip broadcast layer.
    #[error("gossip error: {0}")]
    Gossip(String),

    /// Serialization or deserialization failure.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// The requested node was not found in the cluster state.
    #[error("node not found: {0}")]
    NodeNotFound(NodeId),

    /// The membership service has stopped.
    #[error("service stopped")]
    ServiceStopped,

    /// An error from the metadata store.
    #[error("meta store error: {0}")]
    Meta(#[from] shoal_meta::MetaError),
}
