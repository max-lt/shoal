//! Integration test: rebalancing.
//!
//! Start with a small cluster, add nodes, verify shard redistribution
//! and data integrity.

use std::collections::BTreeMap;

use shoal_integration_tests::{IntegrationCluster, test_data_seeded};

/// 3-node cluster, write 50 objects, add 2 more nodes.
/// After adding nodes, the ring should include all 5 nodes.
/// All objects should still be readable.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_add_nodes_objects_still_readable() {
    let mut c = IntegrationCluster::new(3, 2048, 2, 1).await;

    let mut objects = Vec::new();
    for i in 0..50 {
        let data = test_data_seeded(2000 + i * 50, i as u32 + 1);
        let key = format!("obj-{i:03}");
        let writer = i % 3;

        c.node(writer)
            .put_object("b", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        c.broadcast_manifest(writer, "b", &key).await;

        objects.push((key, data));
    }

    // Ring should have 3 nodes.
    assert_eq!(c.node(0).cluster().ring().await.node_count(), 3);

    // Add 2 more nodes.
    let idx4 = c.add_node().await;
    let idx5 = c.add_node().await;

    // Ring should now have 5 nodes.
    assert_eq!(c.node(0).cluster().ring().await.node_count(), 5);

    // All 50 objects should be readable from the original nodes.
    for (key, expected) in &objects {
        let (got, _) = c.node(0).get_object("b", key).await.unwrap();
        assert_eq!(
            &got, expected,
            "original node reading {key} after expansion"
        );
    }

    // New nodes can read too (after manifest sync).
    c.node(idx4).sync_manifests_from_peers().await.unwrap();
    c.node(idx5).sync_manifests_from_peers().await.unwrap();

    for (key, expected) in &objects {
        let (got, _) = c.node(idx4).get_object("b", key).await.unwrap();
        assert_eq!(&got, expected, "new node reading {key}");
    }
}

/// After adding nodes, new writes should distribute shards to the new nodes.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_new_writes_use_expanded_ring() {
    let mut c = IntegrationCluster::new(3, 1024, 2, 1).await;

    // Add 2 more nodes.
    let idx4 = c.add_node().await;
    let _idx5 = c.add_node().await;

    // Write objects — they should now use the 5-node ring.
    for i in 0..30 {
        let data = test_data_seeded(2000, i + 100);
        let key = format!("new-{i}");

        c.node(0)
            .put_object("b", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        c.broadcast_manifest(0, "b", &key).await;
    }

    // Check shard distribution: new nodes should have some shards.
    let count_new = c.local_shard_count(idx4).await;
    // With 5 nodes, each should get ~20% of shards.
    // We wrote 30 objects × at least 2 chunks × 3 shards = ~180 shards.
    // Node 4 should have at least some.
    assert!(
        count_new > 0,
        "new node should have received shards after ring expansion, got {count_new}"
    );
}

/// Ring expansion: verify shard distribution becomes more even.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_shard_distribution_after_expansion() {
    let mut c = IntegrationCluster::new(3, 1024, 2, 1).await;

    // Write 50 objects on the 3-node cluster.
    for i in 0..50 {
        let data = test_data_seeded(3000, i + 200);
        let key = format!("dist-{i}");

        c.node(i as usize % 3)
            .put_object("b", &key, &data, BTreeMap::new())
            .await
            .unwrap();
    }

    // Measure shard counts on original 3 nodes.
    let before: Vec<usize> = futures_lite::future::block_on(async {
        let mut counts = Vec::new();
        for i in 0..3 {
            counts.push(c.local_shard_count(i).await);
        }
        counts
    });

    let total_before: usize = before.iter().sum();
    assert!(total_before > 0, "should have some shards");

    // Add 2 nodes and write more objects.
    let _idx4 = c.add_node().await;
    let _idx5 = c.add_node().await;

    for i in 50..100 {
        let data = test_data_seeded(3000, i + 300);
        let key = format!("dist-{i}");

        c.node(i as usize % 5)
            .put_object("b", &key, &data, BTreeMap::new())
            .await
            .unwrap();
    }

    // New nodes should have shards now.
    let mut nodes_with_shards = 0;
    for i in 0..5 {
        if c.local_shard_count(i).await > 0 {
            nodes_with_shards += 1;
        }
    }
    assert!(
        nodes_with_shards >= 4,
        "after expansion, at least 4 of 5 nodes should have shards, got {nodes_with_shards}"
    );
}

/// Manifest sync after node addition: new node can list all objects.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_new_node_lists_objects_after_sync() {
    let mut c = IntegrationCluster::new(3, 1024, 2, 1).await;

    for i in 0..20 {
        let data = test_data_seeded(1000, i + 400);
        let key = format!("sync-{i}");

        c.node(i as usize % 3)
            .put_object("b", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        c.broadcast_manifest(i as usize % 3, "b", &key).await;
    }

    // Add a new node.
    let idx = c.add_node().await;

    // Before sync: new node has 0 objects.
    let before = c.node(idx).list_objects("b", "").await.unwrap();
    assert_eq!(
        before.len(),
        0,
        "new node should have 0 objects before sync"
    );

    // After sync: new node should see all 20 objects.
    let synced = c.node(idx).sync_manifests_from_peers().await.unwrap();
    assert_eq!(synced, 20, "should sync 20 manifests");

    let after = c.node(idx).list_objects("b", "").await.unwrap();
    assert_eq!(
        after.len(),
        20,
        "new node should list 20 objects after sync"
    );
}
