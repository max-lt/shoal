//! Integration test: cluster formation.
//!
//! Verifies that N nodes form a consistent cluster with uniform ring state.

use std::collections::BTreeMap;

use shoal_integration_tests::{IntegrationCluster, test_data};

/// Start 5 nodes in-process, verify all 5 discover each other.
/// All nodes share the same ClusterState, so membership is immediate.
/// The ring should contain all 5 nodes and be consistent across them.
#[tokio::test]
async fn test_5_node_cluster_formation() {
    let c = IntegrationCluster::new(5, 1024, 4, 2).await;

    // All nodes should be in the ring.
    let ring = c.node(0).cluster().ring().await;
    assert_eq!(ring.node_count(), 5, "ring should contain 5 nodes");

    // Every node should see the same ring.
    for i in 0..5 {
        let r = c.node(i).cluster().ring().await;
        assert_eq!(r.node_count(), 5, "node {i} should see 5 nodes in ring");
    }
}

/// Verify consistent ring: all nodes place the same shard on the same owner.
#[tokio::test]
async fn test_5_node_consistent_ring() {
    let c = IntegrationCluster::new(5, 1024, 4, 2).await;

    // Write an object from node 0.
    let data = test_data(5000);
    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k").await;

    // All 5 nodes should use the same ring for shard placement.
    // Verify by reading from each node.
    for i in 0..5 {
        let (got, _) = c.node(i).get_object("b", "k").await.unwrap();
        assert_eq!(got, data, "node {i} should read the object correctly");
    }
}

/// Ring should be well-distributed: shards spread across multiple nodes.
#[tokio::test]
async fn test_5_node_shard_distribution() {
    let c = IntegrationCluster::new(5, 1024, 4, 2).await;

    // Write 20 objects to get good distribution.
    for i in 0..20 {
        let data = test_data(2000 + i * 100);
        let key = format!("obj-{i}");
        c.node(i % 5)
            .put_object("b", &key, &data, BTreeMap::new())
            .await
            .unwrap();
    }

    // At least 3 of 5 nodes should have shards.
    let mut nodes_with_shards = 0;
    for i in 0..5 {
        if c.local_shard_count(i).await > 0 {
            nodes_with_shards += 1;
        }
    }
    assert!(
        nodes_with_shards >= 3,
        "shards should be distributed across at least 3 of 5 nodes, got {nodes_with_shards}"
    );
}

/// Cluster formation with 10 nodes.
#[tokio::test]
async fn test_10_node_cluster() {
    let c = IntegrationCluster::new(10, 2048, 4, 2).await;

    let ring = c.node(0).cluster().ring().await;
    assert_eq!(ring.node_count(), 10);

    let data = test_data(50_000);
    c.node(3)
        .put_object("b", "big", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(3, "b", "big").await;

    // Read from various nodes.
    for &i in &[0, 4, 7, 9] {
        let (got, _) = c.node(i).get_object("b", "big").await.unwrap();
        assert_eq!(
            got, data,
            "node {i} should read correctly in 10-node cluster"
        );
    }
}
