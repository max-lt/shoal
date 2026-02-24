//! Tests for shard distribution and ring placement with shard_replication.

use std::collections::BTreeMap;

use shoal_cluster::ClusterState;
use shoal_types::{Member, MemberState, NodeId, NodeTopology};

use super::helpers::{TEST_MAX_BYTES, test_data, three_node_cluster};

/// Reproduce the bug: if the writer node is NOT in the ring, all shards
/// get placed on the only ring member (the peer). This simulates what
/// happens in cmd_start when the local node forgets to add itself.
#[tokio::test]
#[ntest::timeout(10000)]
async fn test_bug_writer_not_in_ring_all_shards_go_to_peer() {
    let writer_id = NodeId::from([1u8; 32]);
    let peer_id = NodeId::from([2u8; 32]);

    // Create cluster state but only add the PEER, not the writer.
    let cluster = ClusterState::new(writer_id, 128);
    cluster
        .add_member(Member {
            node_id: peer_id,
            capacity: TEST_MAX_BYTES,
            state: MemberState::Alive,
            generation: 1,
            topology: NodeTopology::default(),
        })
        .await;

    let ring = cluster.ring().await;

    // Generate some shard IDs and check owners.
    let mut peer_count = 0;
    let mut writer_count = 0;
    let total = 100;
    for i in 0..total {
        let shard_id = shoal_types::ShardId::from_data(&[i as u8; 32]);
        let owners = ring.owners(&shard_id, 1);
        assert_eq!(owners.len(), 1, "shard_replication=1 should give 1 owner");
        if owners[0] == peer_id {
            peer_count += 1;
        } else if owners[0] == writer_id {
            writer_count += 1;
        }
    }

    // BUG: writer is not in ring, so ALL shards go to peer.
    assert_eq!(peer_count, total, "all shards should go to peer (bug)");
    assert_eq!(writer_count, 0, "writer has no shards (not in ring)");
}

/// After fix: when both nodes are in the ring, shards should be
/// distributed roughly 50/50 between the two nodes.
#[tokio::test]
#[ntest::timeout(10000)]
async fn test_fix_both_nodes_in_ring_shards_distributed() {
    let node_a = NodeId::from([1u8; 32]);
    let node_b = NodeId::from([2u8; 32]);

    let cluster = ClusterState::new(node_a, 128);
    cluster
        .add_member(Member {
            node_id: node_a,
            capacity: TEST_MAX_BYTES,
            state: MemberState::Alive,
            generation: 1,
            topology: NodeTopology::default(),
        })
        .await;
    cluster
        .add_member(Member {
            node_id: node_b,
            capacity: TEST_MAX_BYTES,
            state: MemberState::Alive,
            generation: 1,
            topology: NodeTopology::default(),
        })
        .await;

    let ring = cluster.ring().await;

    let mut a_count = 0usize;
    let mut b_count = 0usize;
    let total = 1000;
    for i in 0u32..total {
        let shard_id = shoal_types::ShardId::from_data(&i.to_le_bytes());
        let owners = ring.owners(&shard_id, 1);
        assert_eq!(owners.len(), 1);
        if owners[0] == node_a {
            a_count += 1;
        } else {
            b_count += 1;
        }
    }

    // With equal weight, distribution should be roughly 50/50.
    // Allow 35/65 to account for hash distribution variance.
    assert!(
        a_count > 350 && a_count < 650,
        "node_a got {a_count}/{total} shards -- expected ~500"
    );
    assert!(
        b_count > 350 && b_count < 650,
        "node_b got {b_count}/{total} shards -- expected ~500"
    );
}

/// Verify that with shard_replication=1 and 3 nodes, each of the 6 shards
/// (k=4, m=2) for a single chunk goes to exactly 1 node, and at least 2
/// distinct nodes are used (not all on the same node).
#[tokio::test]
#[ntest::timeout(10000)]
async fn test_shard_replication_1_distributes_across_nodes() {
    // Use the three_node_cluster helper but check shard placement via ring.
    let node_ids: Vec<NodeId> = (1..=3u8).map(|i| NodeId::from([i; 32])).collect();
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

    let ring = cluster.ring().await;

    // Simulate encoding: generate 6 shard IDs for one chunk.
    let encoder = shoal_erasure::ErasureEncoder::new(4, 2);
    let chunk_data = vec![42u8; 1024];
    let (shards, _) = encoder.encode(&chunk_data).unwrap();
    assert_eq!(shards.len(), 6);

    let mut owner_set = std::collections::HashSet::new();
    for shard in &shards {
        let owners = ring.owners(&shard.id, 1);
        assert_eq!(owners.len(), 1, "shard_replication=1 -> exactly 1 owner");
        owner_set.insert(owners[0]);
    }

    // With 3 nodes and 6 shards, we expect at least 2 distinct nodes
    // to be used (statistically almost certainly all 3).
    assert!(
        owner_set.len() >= 2,
        "expected shards on at least 2 nodes, got {}",
        owner_set.len()
    );
}

/// Verify that the writer node stores ALL shards locally even when
/// transport is None (current behavior in test mode / single-node).
/// This test documents the current behavior that should change:
/// in distributed mode, the writer should only store shards it owns.
#[tokio::test]
#[ntest::timeout(10000)]
async fn test_writer_stores_all_shards_locally_without_transport() {
    let nodes = three_node_cluster(1024, 2, 1).await;
    let data = test_data(1024);

    // Write from node 0 (no transport -> stores everything locally).
    nodes[0]
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    let manifest = nodes[0].head_object("b", "k").await.unwrap();

    // Without transport, the writer stores ALL shards (k+m = 3) locally.
    let mut local_count = 0;
    for chunk in &manifest.chunks {
        for shard in &chunk.shards {
            if nodes[0]
                .store()
                .get(shard.shard_id)
                .await
                .unwrap()
                .is_some()
            {
                local_count += 1;
            }
        }
    }

    // Current behavior: writer stores all 3 shards (k=2 + m=1).
    assert_eq!(
        local_count, 3,
        "without transport, writer stores all shards locally"
    );
}
