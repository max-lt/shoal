//! Shared test utilities for shoal-engine tests.

use std::sync::Arc;

use shoal_cluster::ClusterState;
use shoal_meta::MetaStore;
use shoal_store::MemoryStore;
use shoal_types::{Member, MemberState, NodeId, NodeTopology};

use crate::node::{ShoalNode, ShoalNodeConfig};

pub const TEST_MAX_BYTES: u64 = 1_000_000_000;

/// Generate deterministic, non-repeating test data.
pub fn test_data(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let mut state: u32 = 0xDEAD_BEEF;
    for _ in 0..size {
        state = state.wrapping_mul(1103515245).wrapping_add(12345);
        data.push((state >> 16) as u8);
    }
    data
}

/// Create a single-node ShoalNode for testing.
pub async fn single_node(chunk_size: u32, k: usize, m: usize) -> ShoalNode {
    let node_id = NodeId::from([1u8; 32]);
    let store = Arc::new(MemoryStore::new(TEST_MAX_BYTES));
    let meta = Arc::new(MetaStore::open_temporary().unwrap());
    let cluster = ClusterState::new(node_id, 128);

    // Register the local node in the cluster ring so shards can be placed.
    cluster
        .add_member(Member {
            node_id,
            capacity: TEST_MAX_BYTES,
            state: MemberState::Alive,
            generation: 1,
            topology: NodeTopology::default(),
        })
        .await;

    ShoalNode::new(
        ShoalNodeConfig {
            node_id,
            chunk_size,
            erasure_k: k,
            erasure_m: m,
            vnodes_per_node: 128,
            shard_replication: 1,
            cache_max_bytes: u64::MAX,
        },
        store,
        meta,
        cluster,
    )
}

/// Create a 3-node setup: 3 ShoalNodes sharing a ClusterState but each
/// with their own store. In a real cluster they would be separate processes;
/// here we simulate multi-node by sharing the cluster state and using
/// separate stores and meta stores.
pub async fn three_node_cluster(chunk_size: u32, k: usize, m: usize) -> Vec<ShoalNode> {
    let node_ids: Vec<NodeId> = (1..=3u8).map(|i| NodeId::from([i; 32])).collect();
    // All nodes share the same cluster state so they see the same ring.
    let cluster = ClusterState::new(node_ids[0], 128);

    for &nid in &node_ids {
        cluster
            .add_member(Member {
                node_id: nid,
                capacity: TEST_MAX_BYTES,
                state: MemberState::Alive,
                generation: 1,
                topology: NodeTopology::default(),
            })
            .await;
    }

    node_ids
        .iter()
        .map(|&nid| {
            let store = Arc::new(MemoryStore::new(TEST_MAX_BYTES));
            let meta = Arc::new(MetaStore::open_temporary().unwrap());
            ShoalNode::new(
                ShoalNodeConfig {
                    node_id: nid,
                    chunk_size,
                    erasure_k: k,
                    erasure_m: m,
                    vnodes_per_node: 128,
                    shard_replication: 1,
                    cache_max_bytes: u64::MAX,
                },
                store,
                meta,
                cluster.clone(),
            )
        })
        .collect()
}
