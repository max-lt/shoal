//! Consistent hashing ring for deterministic shard placement.
//!
//! This crate implements a consistent hash ring that maps shard IDs to node IDs
//! for deterministic placement. Nodes can have different weights (proportional
//! to their storage capacity), and the ring supports efficient computation of
//! which shards must migrate when membership changes.
//!
//! The ring uses virtual nodes (vnodes): each physical node gets multiple
//! positions on the ring, determined by `blake3(node_id ++ vnode_index)`.
//! More vnodes per node = more uniform distribution.

mod ring;

pub use ring::{Migration, NodeInfo, Ring};
