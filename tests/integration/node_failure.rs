//! Integration test: node failure.
//!
//! 5-node cluster with replication. Kill nodes, verify data survives.

use std::collections::BTreeMap;

use shoal_integration_tests::{IntegrationCluster, test_data_seeded};

/// 5-node cluster, write 50 objects, kill 1 node, read all 50 → succeed.
#[tokio::test]
async fn test_kill_one_node_all_objects_readable() {
    // k=4, m=2, replication=2: each shard on 2 nodes.
    let c = IntegrationCluster::with_replication(5, 2048, 4, 2, 2).await;

    let mut objects = Vec::new();
    for i in 0..50 {
        let data = test_data_seeded(2000 + i * 50, i as u32 + 1);
        let key = format!("obj-{i:03}");
        let writer = i % 5;

        c.node(writer)
            .put_object("b", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        c.broadcast_manifest(writer, "b", &key);

        objects.push((key, data));
    }

    // Kill node 2.
    c.kill_node(2).await;

    // All 50 objects should still be readable from surviving nodes.
    for (key, expected) in &objects {
        // Try reading from a surviving node.
        let mut read_ok = false;
        for reader in [0, 1, 3, 4] {
            match c.node(reader).get_object("b", key).await {
                Ok((got, _)) => {
                    assert_eq!(&got, expected, "data mismatch for {key}");
                    read_ok = true;
                    break;
                }
                Err(_) => continue,
            }
        }
        assert!(
            read_ok,
            "object {key} should be readable after 1 node failure"
        );
    }
}

/// Kill 2 nodes: with k=4, m=2 and replication=3, should still read.
#[tokio::test]
async fn test_kill_two_nodes_still_readable() {
    let c = IntegrationCluster::with_replication(5, 2048, 4, 2, 3).await;

    let mut objects = Vec::new();
    for i in 0..50 {
        let data = test_data_seeded(3000, i as u32 + 100);
        let key = format!("obj-{i:03}");
        let writer = i % 5;

        c.node(writer)
            .put_object("b", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        c.broadcast_manifest(writer, "b", &key);

        objects.push((key, data));
    }

    // Kill 2 nodes.
    c.kill_node(3).await;
    c.kill_node(4).await;

    // All objects should still be readable from surviving nodes.
    for (key, expected) in &objects {
        let mut read_ok = false;
        for reader in [0, 1, 2] {
            match c.node(reader).get_object("b", key).await {
                Ok((got, _)) => {
                    assert_eq!(&got, expected, "data mismatch for {key}");
                    read_ok = true;
                    break;
                }
                Err(_) => continue,
            }
        }
        assert!(
            read_ok,
            "object {key} should be readable after 2 node failures"
        );
    }
}

/// Kill the writer node: non-writer nodes should still read.
#[tokio::test]
async fn test_kill_writer_node() {
    let c = IntegrationCluster::with_replication(5, 2048, 4, 2, 2).await;

    let data = test_data_seeded(20_000, 42);
    c.node(0)
        .put_object("b", "important.dat", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "important.dat");

    // Kill the writer.
    c.kill_node(0).await;

    // Other nodes should still read.
    for reader in 1..5 {
        let (got, _) = c
            .node(reader)
            .get_object("b", "important.dat")
            .await
            .unwrap();
        assert_eq!(got, data, "node {reader} should read after writer died");
    }
}

/// Kill a node, revive it, verify reads work before and after.
#[tokio::test]
async fn test_node_kill_and_revive() {
    let c = IntegrationCluster::with_replication(5, 2048, 4, 2, 2).await;

    let mut objects = Vec::new();
    for i in 0..20 {
        let data = test_data_seeded(5000, i as u32 + 200);
        let key = format!("obj-{i}");

        c.node(i % 5)
            .put_object("b", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        c.broadcast_manifest(i % 5, "b", &key);

        objects.push((key, data));
    }

    // Kill node 1.
    c.kill_node(1).await;

    // Reads from surviving nodes should work.
    for (key, expected) in &objects {
        let (got, _) = c.node(0).get_object("b", key).await.unwrap();
        assert_eq!(&got, expected, "{key} after kill");
    }

    // Revive node 1.
    c.revive_node(1).await;

    // Node 1 should read again (still has manifests from broadcast).
    for (key, expected) in &objects {
        let (got, _) = c.node(1).get_object("b", key).await.unwrap();
        assert_eq!(&got, expected, "{key} after revive");
    }
}

/// Mass failure and recovery: kill all except writer, revive all.
#[tokio::test]
async fn test_mass_failure_and_recovery() {
    // Full replication so writer has all shards locally.
    let c = IntegrationCluster::with_replication(5, 2048, 4, 2, 5).await;

    let data = test_data_seeded(30_000, 99);
    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k");

    // Kill everyone except node 0.
    for i in 1..5 {
        c.kill_node(i).await;
    }

    // Writer still reads.
    let (got, _) = c.node(0).get_object("b", "k").await.unwrap();
    assert_eq!(got, data, "writer reads during mass failure");

    // Revive all.
    for i in 1..5 {
        c.revive_node(i).await;
    }

    // Everyone reads.
    for i in 0..5 {
        let (got, _) = c.node(i).get_object("b", "k").await.unwrap();
        assert_eq!(got, data, "node {i} after mass recovery");
    }
}

/// 4-node cluster: write on node 0, kill node 2, write on node 1,
/// respawn node 2. Verify node 2 catches up on the missed write
/// transparently via manifest pull from peers.
///
/// This test does NOT use `broadcast_manifest` — it relies on
/// `put_object`'s built-in manifest broadcast (via `send_to`) and
/// the `pull_manifest` fallback for the respawned node.
#[tokio::test]
async fn test_4_nodes_kill_write_respawn_catches_up() {
    // k=2, m=1, shard_replication=2: each shard on 2 nodes.
    let c = IntegrationCluster::with_replication(4, 1024, 2, 1, 2).await;

    // Step 1: write on node 0. Manifest propagates to all 4 nodes
    // via put_object's built-in send_to broadcast.
    let data1 = test_data_seeded(5000, 1);
    c.node(0)
        .put_object("b", "key1", &data1, BTreeMap::new())
        .await
        .unwrap();

    // All 4 nodes can read key1.
    for i in 0..4 {
        let (got, _) = c.node(i).get_object("b", "key1").await.unwrap();
        assert_eq!(got, data1, "node {i} should read key1 before kill");
    }

    // Step 2: kill node 2.
    c.kill_node(2).await;

    // Step 3: write on node 1 while node 2 is dead.
    // send_to for node 2 will fail (down), so node 2 misses this manifest.
    let data2 = test_data_seeded(3000, 2);
    c.node(1)
        .put_object("b", "key2", &data2, BTreeMap::new())
        .await
        .unwrap();

    // Surviving nodes can read key2.
    for reader in [0, 1, 3] {
        let (got, _) = c.node(reader).get_object("b", "key2").await.unwrap();
        assert_eq!(
            got, data2,
            "node {reader} should read key2 while node 2 down"
        );
    }

    // Step 4: respawn node 2.
    c.revive_node(2).await;

    // Node 2 still reads key1 (had it before death).
    let (got, _) = c.node(2).get_object("b", "key1").await.unwrap();
    assert_eq!(got, data1, "node 2 reads key1 after revive");

    // Node 2 reads key2 — it missed the manifest broadcast, but
    // lookup_manifest falls back to pull_manifest from living peers.
    let (got, _) = c.node(2).get_object("b", "key2").await.unwrap();
    assert_eq!(
        got, data2,
        "node 2 reads key2 after revive via peer fallback"
    );
}

/// Delete object, verify it's gone.
#[tokio::test]
async fn test_delete_object_after_node_failure() {
    let c = IntegrationCluster::with_replication(5, 2048, 4, 2, 2).await;

    let data = test_data_seeded(5000, 77);
    c.node(0)
        .put_object("b", "deleteme", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "deleteme");

    // Verify it's readable.
    let (got, _) = c.node(1).get_object("b", "deleteme").await.unwrap();
    assert_eq!(got, data);

    // Kill a node, then delete.
    c.kill_node(4).await;
    c.node(1).delete_object("b", "deleteme").await.unwrap();

    // Should be gone from node 1.
    assert!(c.node(1).get_object("b", "deleteme").await.is_err());
}
