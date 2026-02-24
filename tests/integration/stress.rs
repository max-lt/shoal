//! Integration test: stress.
//!
//! 5-node cluster with concurrent writers and readers.
//! Verifies no data corruption under load.

use std::collections::BTreeMap;
use std::sync::Arc;

use shoal_integration_tests::{IntegrationCluster, test_data_seeded};
use tokio::sync::Barrier;

/// 10 concurrent writers, each writing 100 objects = 1000 total.
/// Then verify all 1000 objects are readable from every node.
#[tokio::test]
async fn test_concurrent_writers_1000_objects() {
    let c = Arc::new(IntegrationCluster::new(5, 2048, 4, 2).await);

    let barrier = Arc::new(Barrier::new(10));
    let mut handles = Vec::new();

    for writer_id in 0..10 {
        let cluster = c.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await; // Start all writers simultaneously.
            let node = writer_id % 5;
            for j in 0..100 {
                let seed = (writer_id * 1000 + j) as u32;
                let data = test_data_seeded(1000 + j * 10, seed);
                let key = format!("w{writer_id}-{j:03}");

                cluster
                    .node(node)
                    .put_object("stress", &key, &data, BTreeMap::new())
                    .await
                    .unwrap();
                cluster.broadcast_manifest(node, "stress", &key).await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Verify all 1000 objects are readable from every node.
    for writer_id in 0..10 {
        for j in 0..100 {
            let seed = (writer_id * 1000 + j) as u32;
            let expected = test_data_seeded(1000 + j * 10, seed);
            let key = format!("w{writer_id}-{j:03}");

            let reader = (writer_id + j + 1) % 5;
            let (got, _) = c.node(reader).get_object("stress", &key).await.unwrap();
            assert_eq!(
                got, expected,
                "data corruption detected for {key} on reader {reader}"
            );
        }
    }
}

/// Concurrent readers and writers: writers write while readers read.
#[tokio::test]
async fn test_concurrent_read_write() {
    let c = Arc::new(IntegrationCluster::new(5, 2048, 4, 2).await);

    // Pre-populate some objects.
    for i in 0..50 {
        let data = test_data_seeded(3000, i + 500);
        let key = format!("pre-{i:03}");
        c.node(i as usize % 5)
            .put_object("rw", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        c.broadcast_manifest(i as usize % 5, "rw", &key).await;
    }

    // Spawn 5 writer tasks writing new objects.
    let barrier = Arc::new(Barrier::new(10));
    let mut handles = Vec::new();

    for writer_id in 0..5 {
        let cluster = c.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            for j in 0..50 {
                let seed = (writer_id * 100 + j + 10000) as u32;
                let data = test_data_seeded(2000, seed);
                let key = format!("new-w{writer_id}-{j:03}");

                cluster
                    .node(writer_id)
                    .put_object("rw", &key, &data, BTreeMap::new())
                    .await
                    .unwrap();
                cluster.broadcast_manifest(writer_id, "rw", &key).await;
            }
        }));
    }

    // Spawn 5 reader tasks reading pre-populated objects.
    for reader_id in 0..5 {
        let cluster = c.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            for i in 0..50 {
                let expected = test_data_seeded(3000, i + 500);
                let key = format!("pre-{i:03}");

                let (got, _) = cluster
                    .node(reader_id)
                    .get_object("rw", &key)
                    .await
                    .unwrap();
                assert_eq!(
                    got, expected,
                    "concurrent read corruption for {key} on reader {reader_id}"
                );
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Final check: all new objects should be readable.
    for writer_id in 0..5 {
        for j in 0..50 {
            let seed = (writer_id * 100 + j + 10000) as u32;
            let expected = test_data_seeded(2000, seed);
            let key = format!("new-w{writer_id}-{j:03}");

            let (got, _) = c.node(0).get_object("rw", &key).await.unwrap();
            assert_eq!(got, expected, "post-stress read for {key}");
        }
    }
}

/// Verify no data corruption: write with known data, read back,
/// compare byte-by-byte.
#[tokio::test]
async fn test_no_data_corruption() {
    let c = IntegrationCluster::new(5, 1024, 4, 2).await;

    // Write objects with carefully controlled data.
    for i in 0..100 {
        let size = 100 + i * 100; // 100 bytes to 10 KB
        let data = test_data_seeded(size, i as u32 + 9999);
        let key = format!("integrity-{i:03}");

        c.node(i % 5)
            .put_object("verify", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        c.broadcast_manifest(i % 5, "verify", &key).await;
    }

    // Read back from different nodes, verify every byte.
    for i in 0..100 {
        let size = 100 + i * 100;
        let expected = test_data_seeded(size, i as u32 + 9999);
        let key = format!("integrity-{i:03}");
        let reader = (i + 2) % 5;

        let (got, _) = c.node(reader).get_object("verify", &key).await.unwrap();
        assert_eq!(
            got.len(),
            expected.len(),
            "size mismatch for {key}: got {} expected {}",
            got.len(),
            expected.len()
        );
        for (byte_idx, (a, b)) in got.iter().zip(expected.iter()).enumerate() {
            assert_eq!(
                a, b,
                "byte mismatch at position {byte_idx} in {key}: got {a:#x} expected {b:#x}"
            );
        }
    }
}
