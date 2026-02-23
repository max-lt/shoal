//! Cluster membership (foca SWIM) and gossip (iroh-gossip).
//!
//! This crate provides:
//!
//! - [`ClusterIdentity`] — node identity implementing foca's [`Identity`](foca::Identity) trait.
//! - [`ClusterState`] — shared cluster state with membership and placement ring.
//! - [`membership`] — SWIM-based membership service powered by foca.
//! - [`gossip`] — event broadcast layer using iroh-gossip.

mod error;
pub mod gossip;
mod identity;
pub mod membership;
mod state;

#[cfg(test)]
mod tests;

pub use error::ClusterError;
pub use gossip::{GossipHandle, GossipService};
pub use identity::ClusterIdentity;
pub use state::ClusterState;
