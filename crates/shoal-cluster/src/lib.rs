//! Cluster membership (QUIC ping/pong) and gossip (iroh-gossip).
//!
//! This crate provides:
//!
//! - [`ClusterState`] — shared cluster state with membership and placement ring.
//! - [`membership`] — peer management with QUIC-based health checking.
//! - [`gossip`] — event broadcast layer using iroh-gossip.

mod error;
pub mod gossip;
pub mod membership;
mod state;

#[cfg(test)]
mod tests;

pub use error::ClusterError;
pub use gossip::{GossipHandle, GossipService};
pub use membership::{AddressBook, PeerHandle, PeerManagerConfig};
pub use state::ClusterState;
