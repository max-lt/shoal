//! Chaos test: network partition.
//!
//! 6-node cluster. Simulate a partition (3 vs 3). Each side continues
//! operating. Heal the partition. Verify data consistency.

use std::collections::BTreeMap;

use shoal_integration_tests::{IntegrationCluster, test_data_seeded};

/// 6-node cluster. Write objects. Simulate partition (nodes 0-2 vs 3-5).
/// Each partition side can read its own data. After healing, verify all
/// data is consistent.
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_network_partition_and_heal() {
    // replication=3 ensures shards are spread across nodes.
    let c = IntegrationCluster::with_replication(6, 2048, 2, 1, 3).await;

    // --- Phase 1: Write objects to the healthy cluster ---
    let mut pre_partition_objects = Vec::new();
    for i in 0..30 {
        let data = test_data_seeded(3000, i + 1);
        let key = format!("pre-{i:03}");
        let writer = i as usize % 6;

        c.node(writer)
            .put_object("part", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        c.broadcast_manifest(writer, "part", &key).await;

        pre_partition_objects.push((key, data));
    }

    // Verify all objects are readable from every node.
    for (key, expected) in &pre_partition_objects {
        for node in 0..6 {
            let (got, _) = c.node(node).get_object("part", key).await.unwrap();
            assert_eq!(&got, expected, "pre-partition: {key} on node {node}");
        }
    }

    // --- Phase 2: Simulate partition (kill nodes 3-5 from perspective of 0-2) ---
    // Partition A: nodes 0, 1, 2 (alive)
    // Partition B: nodes 3, 4, 5 (killed from A's perspective)
    for i in 3..6 {
        c.kill_node(i).await;
    }

    // --- Phase 3: Partition A can still read pre-partition objects ---
    // (objects written with replication=3 should have copies on partition A)
    let mut partition_a_readable = 0;
    for (key, expected) in &pre_partition_objects {
        for reader in 0..3 {
            if let Ok((got, _)) = c.node(reader).get_object("part", key).await {
                assert_eq!(&got, expected, "partition A: {key} on node {reader}");
                partition_a_readable += 1;
                break;
            }
        }
    }

    // With replication=3 and 6 nodes, most objects should be readable.
    assert!(
        partition_a_readable >= 20,
        "partition A should read most pre-partition objects: {partition_a_readable}/30"
    );

    // --- Phase 4: Write new objects during partition (on partition A only) ---
    let mut partition_a_objects = Vec::new();
    for i in 0..10 {
        let data = test_data_seeded(2000, i + 100);
        let key = format!("during-a-{i}");

        c.node(i as usize % 3)
            .put_object("part", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        // Only broadcast to partition A nodes.
        let manifest = c
            .node(i as usize % 3)
            .head_object("part", &key)
            .await
            .unwrap();
        for target in 0..3 {
            if target != i as usize % 3 {
                c.node(target).meta().put_manifest(&manifest).unwrap();
                c.node(target)
                    .meta()
                    .put_object_key("part", &key, &manifest.object_id)
                    .unwrap();
            }
        }

        partition_a_objects.push((key, data));
    }

    // Partition A reads its own objects.
    for (key, expected) in &partition_a_objects {
        let (got, _) = c.node(0).get_object("part", key).await.unwrap();
        assert_eq!(&got, expected, "partition A during-partition: {key}");
    }

    // --- Phase 5: Heal partition ---
    for i in 3..6 {
        c.revive_node(i).await;
    }

    // --- Phase 6: After healing, all pre-partition objects readable from all nodes ---
    for (key, expected) in &pre_partition_objects {
        // Try from both sides of the former partition.
        for &reader in &[0, 3] {
            let (got, _) = c.node(reader).get_object("part", key).await.unwrap();
            assert_eq!(&got, expected, "post-heal: {key} on node {reader}");
        }
    }

    // Partition A's new objects should be readable from A.
    for (key, expected) in &partition_a_objects {
        let (got, _) = c.node(0).get_object("part", key).await.unwrap();
        assert_eq!(&got, expected, "post-heal from A: {key}");
    }

    // After sync, partition B nodes should also see partition A's objects.
    for i in 3..6 {
        c.node(i).sync_manifests_from_peers().await.unwrap();
    }

    for (key, expected) in &partition_a_objects {
        let (got, _) = c.node(3).get_object("part", key).await.unwrap();
        assert_eq!(&got, expected, "post-heal from B after sync: {key}");
    }
}

/// Partition then simultaneous writes from both sides.
/// After healing, all data from both sides should be readable.
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_split_brain_writes_both_sides() {
    let c = IntegrationCluster::with_replication(6, 2048, 2, 1, 3).await;

    // Partition: kill 3-5 (from A's perspective), kill 0-2 (from B's perspective).
    // In our mock, killing means "transport to those nodes fails".
    // But since we share a single ClusterState, we can't have truly independent
    // partitions. Instead, we simulate by writing to each side independently.

    // Write objects from "side A" (nodes 0-2) — don't broadcast to 3-5.
    let mut side_a = Vec::new();
    for i in 0..10 {
        let data = test_data_seeded(1500, i + 1000);
        let key = format!("side-a-{i}");

        c.node(i as usize % 3)
            .put_object("split", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        // Broadcast only to side A.
        let manifest = c
            .node(i as usize % 3)
            .head_object("split", &key)
            .await
            .unwrap();
        for target in 0..3 {
            if target != i as usize % 3 {
                c.node(target).meta().put_manifest(&manifest).unwrap();
                c.node(target)
                    .meta()
                    .put_object_key("split", &key, &manifest.object_id)
                    .unwrap();
            }
        }
        side_a.push((key, data));
    }

    // Write objects from "side B" (nodes 3-5) — don't broadcast to 0-2.
    let mut side_b = Vec::new();
    for i in 0..10 {
        let data = test_data_seeded(1500, i + 2000);
        let key = format!("side-b-{i}");

        c.node(3 + i as usize % 3)
            .put_object("split", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        let manifest = c
            .node(3 + i as usize % 3)
            .head_object("split", &key)
            .await
            .unwrap();
        for target in 3..6 {
            if target != 3 + i as usize % 3 {
                c.node(target).meta().put_manifest(&manifest).unwrap();
                c.node(target)
                    .meta()
                    .put_object_key("split", &key, &manifest.object_id)
                    .unwrap();
            }
        }
        side_b.push((key, data));
    }

    // Side A can read side A's objects.
    for (key, expected) in &side_a {
        let (got, _) = c.node(0).get_object("split", key).await.unwrap();
        assert_eq!(&got, expected, "side A reads {key}");
    }

    // Side B can read side B's objects.
    for (key, expected) in &side_b {
        let (got, _) = c.node(3).get_object("split", key).await.unwrap();
        assert_eq!(&got, expected, "side B reads {key}");
    }

    // "Heal": sync manifests across all nodes.
    for i in 0..6 {
        c.node(i).sync_manifests_from_peers().await.unwrap();
    }

    // After healing, ALL objects should be readable from ANY node.
    for (key, expected) in side_a.iter().chain(side_b.iter()) {
        let (got, _) = c.node(0).get_object("split", key).await.unwrap();
        assert_eq!(&got, expected, "post-heal node 0: {key}");

        let (got, _) = c.node(4).get_object("split", key).await.unwrap();
        assert_eq!(&got, expected, "post-heal node 4: {key}");
    }
}
