//! Error types for the cluster crate.

use shoal_types::NodeId;

/// Errors produced by the cluster membership and gossip subsystems.
#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    /// A ping to a peer timed out or failed.
    #[error("ping failed for node {0}: {1}")]
    PingFailed(NodeId, String),

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

    /// A network-level error.
    #[error("network error: {0}")]
    Net(#[from] shoal_net::NetError),

    /// An error from the metadata store.
    #[error("meta store error: {0}")]
    Meta(#[from] shoal_meta::MetaError),
}
