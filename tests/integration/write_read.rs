//! Integration test: write/read.
//!
//! 5-node cluster. Write objects of varying sizes, read from random nodes,
//! verify data integrity.

use std::collections::BTreeMap;

use shoal_integration_tests::{IntegrationCluster, test_data_seeded};

/// Write 100 objects of varying sizes (1 KB to ~100 KB), read each from a
/// random node, verify data matches.
#[tokio::test]
async fn test_100_objects_varying_sizes() {
    let c = IntegrationCluster::new(5, 4096, 4, 2).await;

    let mut objects = Vec::new();
    for i in 0..100 {
        let size = 1024 + i * 1000; // 1 KB to ~100 KB
        let data = test_data_seeded(size, i as u32 + 1);
        let key = format!("obj-{i:03}");
        let writer = i % 5;

        c.node(writer)
            .put_object("test", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        c.broadcast_manifest(writer, "test", &key).await;

        objects.push((key, data));
    }

    // Read each object from a different node (round-robin).
    for (i, (key, expected)) in objects.iter().enumerate() {
        let reader = (i + 2) % 5; // different from writer
        let (got, _) = c.node(reader).get_object("test", key).await.unwrap();
        assert_eq!(
            got, *expected,
            "object {key} mismatch when read from node {reader}"
        );
    }
}

/// Write and immediately read from the SAME node — should always succeed.
#[tokio::test]
async fn test_write_then_immediate_read_same_node() {
    let c = IntegrationCluster::new(5, 2048, 4, 2).await;

    for i in 0..50 {
        let data = test_data_seeded(5000, i + 100);
        let key = format!("immediate-{i}");
        let node = i as usize % 5;

        c.node(node)
            .put_object("b", &key, &data, BTreeMap::new())
            .await
            .unwrap();

        // Read immediately from the SAME node (no broadcast needed).
        let (got, _) = c.node(node).get_object("b", &key).await.unwrap();
        assert_eq!(got, data, "immediate read failed for {key} on node {node}");
    }
}

/// Write and immediately read from a DIFFERENT node — requires manifest broadcast.
#[tokio::test]
async fn test_write_then_read_different_node() {
    let c = IntegrationCluster::new(5, 2048, 4, 2).await;

    for i in 0..50 {
        let data = test_data_seeded(5000, i + 200);
        let key = format!("cross-{i}");
        let writer = i as usize % 5;
        let reader = (writer + 1) % 5;

        c.node(writer)
            .put_object("b", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        c.broadcast_manifest(writer, "b", &key).await;

        let (got, _) = c.node(reader).get_object("b", &key).await.unwrap();
        assert_eq!(got, data, "cross-node read failed for {key}");
    }
}

/// Large objects: write 10 objects of 1 MB each, read back.
#[tokio::test]
async fn test_large_objects_1mb() {
    let c = IntegrationCluster::new(5, 65536, 4, 2).await;

    for i in 0..10 {
        let data = test_data_seeded(1_048_576, i + 300);
        let key = format!("large-{i}");

        c.node(i as usize % 5)
            .put_object("big", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        c.broadcast_manifest(i as usize % 5, "big", &key).await;

        let reader = (i as usize + 3) % 5;
        let (got, _) = c.node(reader).get_object("big", &key).await.unwrap();
        assert_eq!(got.len(), data.len(), "1MB object {key}: size mismatch");
        assert_eq!(got, data, "1MB object {key}: data mismatch");
    }
}

/// All nodes list the same objects.
#[tokio::test]
async fn test_all_nodes_list_same_objects() {
    let c = IntegrationCluster::new(5, 2048, 4, 2).await;

    for i in 0..30 {
        let data = test_data_seeded(1000 + i * 100, i as u32 + 400);
        let key = format!("listed-{i:03}");
        let writer = i % 5;

        c.node(writer)
            .put_object("catalog", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        c.broadcast_manifest(writer, "catalog", &key).await;
    }

    // Every node should list 30 objects.
    for i in 0..5 {
        let keys = c.node(i).list_objects("catalog", "").await.unwrap();
        assert_eq!(
            keys.len(),
            30,
            "node {i} should list 30 objects, got {}",
            keys.len()
        );
    }
}

/// Prefix filtering works across all nodes.
#[tokio::test]
async fn test_list_objects_with_prefix() {
    let c = IntegrationCluster::new(5, 2048, 4, 2).await;

    // Write objects with different prefixes.
    for i in 0..10 {
        let data = test_data_seeded(500, i + 500);

        c.node(0)
            .put_object(
                "b",
                &format!("images/photo-{i}.jpg"),
                &data,
                BTreeMap::new(),
            )
            .await
            .unwrap();
        c.node(0)
            .put_object("b", &format!("docs/readme-{i}.md"), &data, BTreeMap::new())
            .await
            .unwrap();

        c.broadcast_manifest(0, "b", &format!("images/photo-{i}.jpg"))
            .await;
        c.broadcast_manifest(0, "b", &format!("docs/readme-{i}.md"))
            .await;
    }

    for i in 0..5 {
        let images = c.node(i).list_objects("b", "images/").await.unwrap();
        let docs = c.node(i).list_objects("b", "docs/").await.unwrap();
        let all = c.node(i).list_objects("b", "").await.unwrap();

        assert_eq!(images.len(), 10, "node {i}: images prefix");
        assert_eq!(docs.len(), 10, "node {i}: docs prefix");
        assert_eq!(all.len(), 20, "node {i}: all objects");
    }
}
