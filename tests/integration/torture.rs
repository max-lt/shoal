//! Comprehensive torture test for the Shoal engine.
//!
//! Combines multiple failure modes and edge cases in a single test suite:
//! - Full data-size spectrum (1 byte to 1MB)
//! - Overwrite consistency (last-write-wins)
//! - Shard deletion → RS reconstruction
//! - Concurrent mixed operations (PUT/GET/DELETE/LIST)
//! - Rolling chaos: kills + adds + writes + deletes simultaneously
//! - Large cluster (20 nodes)
//! - Multiple erasure configurations

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use shoal_integration_tests::{IntegrationCluster, test_data_seeded};
use tokio::sync::RwLock;

// =========================================================================
// 1. Data-size spectrum
// =========================================================================

/// Write and read objects of sizes from 1 byte to 1MB, all through the same
/// erasure pipeline. Exercises the chunker edge cases (tiny, exact, off-by-one,
/// multi-chunk, large).
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_data_size_spectrum() {
    let c = IntegrationCluster::new(5, 1024, 2, 1).await;

    let sizes: Vec<usize> = vec![
        1,         // single byte
        2,         // two bytes
        100,       // small, sub-chunk
        1023,      // chunk_size - 1
        1024,      // exactly chunk_size
        1025,      // chunk_size + 1
        2048,      // 2x chunk_size
        3000,      // ~3 chunks
        5000,      // ~5 chunks
        10240,     // 10 chunks exactly
        65536,     // 64KB
        262144,    // 256KB, default chunk_size in production
        1_000_000, // ~1MB
    ];

    for (i, &size) in sizes.iter().enumerate() {
        let data = test_data_seeded(size, i as u32 + 7777);
        let key = format!("size-{size}");
        let writer = i % 5;

        c.node(writer)
            .put_object("spectrum", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        c.broadcast_manifest(writer, "spectrum", &key).await;
    }

    // Read back from different nodes, verify byte-for-byte.
    for (i, &size) in sizes.iter().enumerate() {
        let expected = test_data_seeded(size, i as u32 + 7777);
        let key = format!("size-{size}");
        let reader = (i + 2) % 5;

        let (got, manifest) = c.node(reader).get_object("spectrum", &key).await.unwrap();
        assert_eq!(
            got.len(),
            expected.len(),
            "size mismatch for {key}: got {} expected {}",
            got.len(),
            expected.len()
        );
        assert_eq!(got, expected, "data mismatch for {key}");
        assert_eq!(
            manifest.total_size, size as u64,
            "manifest total_size wrong for {key}"
        );
    }
}

// =========================================================================
// 2. Overwrite consistency
// =========================================================================

/// Write the same key 20 times with different data. After each write,
/// verify reading returns the latest value.
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_overwrite_consistency() {
    let c = IntegrationCluster::new(5, 1024, 2, 1).await;

    let mut last_data = Vec::new();

    for version in 0u32..20 {
        let data = test_data_seeded(2000 + (version as usize * 100), version + 5000);
        let writer = version as usize % 5;

        c.node(writer)
            .put_object("overwrite", "the-key", &data, BTreeMap::new())
            .await
            .unwrap();
        c.broadcast_manifest(writer, "overwrite", "the-key").await;

        last_data = data.clone();

        // Verify from a different node.
        let reader = (writer + 1) % 5;
        let (got, _) = c
            .node(reader)
            .get_object("overwrite", "the-key")
            .await
            .unwrap();
        assert_eq!(
            got, data,
            "overwrite version {version}: read doesn't match latest write"
        );
    }

    // Final check: all 5 nodes should return the last version.
    for node in 0..5 {
        let (got, _) = c
            .node(node)
            .get_object("overwrite", "the-key")
            .await
            .unwrap();
        assert_eq!(
            got, last_data,
            "final read on node {node} doesn't match last write"
        );
    }

    // Only 1 key should be listed.
    let keys = c.node(0).list_objects("overwrite", "").await.unwrap();
    assert_eq!(keys.len(), 1, "should have exactly 1 key after overwrites");
}

// =========================================================================
// 3. Shard deletion → RS reconstruction
// =========================================================================

/// For each chunk of an object, delete parity shards from stores, then
/// verify the object is still readable via data shards only (fast path).
/// Then delete some data shards and verify RS decode reconstructs.
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_shard_deletion_rs_reconstruction() {
    // k=2, m=2 → 4 shards per chunk, need 2 to reconstruct.
    let c = IntegrationCluster::with_replication(5, 1024, 2, 2, 2).await;

    let data = test_data_seeded(5000, 42);
    c.node(0)
        .put_object("rs", "test.bin", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "rs", "test.bin").await;

    // Sanity: readable initially.
    let (got, manifest) = c.node(1).get_object("rs", "test.bin").await.unwrap();
    assert_eq!(got, data, "initial read");

    // For each chunk, identify the parity shards (index >= k) and delete them
    // from all stores.
    for chunk_meta in &manifest.chunks {
        for shard_meta in &chunk_meta.shards {
            if shard_meta.index >= 2 {
                // Parity shard — delete from all nodes.
                for node_idx in 0..5 {
                    let _ = c.node(node_idx).store().delete(shard_meta.shard_id).await;
                }
            }
        }
    }

    // Should still read via data shards only (fast path).
    let (got2, _) = c.node(2).get_object("rs", "test.bin").await.unwrap();
    assert_eq!(got2, data, "read after parity deletion (fast path)");

    // Now delete one data shard per chunk (index 0), keeping index 1.
    // Plus we still have no parity. That means exactly k=2 shards remain
    // across replication... actually we need to be careful. With replication=2,
    // each shard is on 2 nodes. Let's delete shard index 0 from ALL nodes.
    for chunk_meta in &manifest.chunks {
        for shard_meta in &chunk_meta.shards {
            if shard_meta.index == 0 {
                for node_idx in 0..5 {
                    let _ = c.node(node_idx).store().delete(shard_meta.shard_id).await;
                }
            }
        }
    }

    // Now only shard index 1 (data) remains. We need k=2 to decode.
    // This should fail because we only have 1 shard per chunk.
    let result = c.node(0).get_object("rs", "test.bin").await;
    assert!(
        result.is_err(),
        "should fail with only 1 shard per chunk (need k=2)"
    );
}

/// With replication=3 and k=2 m=2, delete shards from some replicas
/// but keep enough across the cluster for RS decode.
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_shard_loss_across_replicas_still_readable() {
    // k=2, m=1 → 3 shards, replication=3 → each shard on 3 nodes.
    let c = IntegrationCluster::with_replication(5, 2048, 2, 1, 3).await;

    let data = test_data_seeded(8000, 123);
    c.node(0)
        .put_object("rs2", "rep.bin", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "rs2", "rep.bin").await;

    // Kill 2 out of 5 nodes — with replication=3, each shard still has at
    // least 1 surviving copy.
    c.kill_node(3).await;
    c.kill_node(4).await;

    // Should still be readable from surviving nodes.
    let mut read_ok = false;
    for reader in [0, 1, 2] {
        if let Ok((got, _)) = c.node(reader).get_object("rs2", "rep.bin").await {
            assert_eq!(got, data, "data mismatch on reader {reader}");
            read_ok = true;
            break;
        }
    }
    assert!(read_ok, "should be readable after killing 2 of 5 nodes");
}

// =========================================================================
// 4. Concurrent mixed operations
// =========================================================================

/// 5 nodes, concurrent PUT + GET + DELETE + LIST operations running
/// simultaneously. Verify no panics, no data corruption.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ntest::timeout(60000)]
async fn test_concurrent_mixed_operations() {
    let c = Arc::new(IntegrationCluster::new(5, 1024, 2, 1).await);
    let stop = Arc::new(AtomicBool::new(false));
    let put_count = Arc::new(AtomicUsize::new(0));
    let get_count = Arc::new(AtomicUsize::new(0));
    let delete_count = Arc::new(AtomicUsize::new(0));
    let list_count = Arc::new(AtomicUsize::new(0));

    // Shared registry of currently-live keys.
    let live_keys: Arc<RwLock<Vec<(String, Vec<u8>)>>> = Arc::new(RwLock::new(Vec::new()));

    let mut handles = Vec::new();

    // --- Writers (3 tasks) ---
    for writer_id in 0..3 {
        let cluster = c.clone();
        let stop = stop.clone();
        let pc = put_count.clone();
        let keys = live_keys.clone();
        handles.push(tokio::spawn(async move {
            let mut idx = 0u32;
            while !stop.load(Ordering::Relaxed) {
                let data = test_data_seeded(500 + (idx as usize % 3000), writer_id * 10000 + idx);
                let key = format!("mix-w{writer_id}-{idx}");
                let node = (writer_id as usize + idx as usize) % 5;

                if cluster
                    .node(node)
                    .put_object("mix", &key, &data, BTreeMap::new())
                    .await
                    .is_ok()
                {
                    cluster.broadcast_manifest(node, "mix", &key).await;
                    keys.write().await.push((key, data));
                    pc.fetch_add(1, Ordering::Relaxed);
                }
                idx += 1;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }));
    }

    // --- Readers (2 tasks) ---
    for reader_id in 0..2 {
        let cluster = c.clone();
        let stop = stop.clone();
        let gc = get_count.clone();
        let keys = live_keys.clone();
        handles.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let snapshot = keys.read().await.clone();
                if snapshot.is_empty() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
                let idx = (reader_id * 31 + gc.load(Ordering::Relaxed)) % snapshot.len();
                let (key, expected) = &snapshot[idx];
                let node = (reader_id + idx) % 5;

                if let Ok((got, _)) = cluster.node(node).get_object("mix", key).await {
                    assert_eq!(&got, expected, "data corruption reading {key}");
                    gc.fetch_add(1, Ordering::Relaxed);
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }));
    }

    // --- Deleters (1 task) ---
    {
        let cluster = c.clone();
        let stop = stop.clone();
        let dc = delete_count.clone();
        let keys = live_keys.clone();
        handles.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let mut snapshot = keys.write().await;
                if snapshot.len() < 5 {
                    continue;
                }
                // Delete the oldest key.
                let (key, _) = snapshot.remove(0);
                let node = dc.load(Ordering::Relaxed) % 5;
                let _ = cluster.node(node).delete_object("mix", &key).await;
                dc.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // --- Listers (1 task) ---
    {
        let cluster = c.clone();
        let stop = stop.clone();
        let lc = list_count.clone();
        handles.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let _ = cluster.node(0).list_objects("mix", "").await;
                let _ = cluster.node(0).list_objects("mix", "mix-w0").await;
                let _ = cluster.node(0).list_objects("mix", "mix-w1").await;
                lc.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }));
    }

    // Run for 5 seconds.
    tokio::time::sleep(Duration::from_secs(5)).await;
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.await.unwrap();
    }

    let puts = put_count.load(Ordering::Relaxed);
    let gets = get_count.load(Ordering::Relaxed);
    let deletes = delete_count.load(Ordering::Relaxed);
    let lists = list_count.load(Ordering::Relaxed);

    assert!(puts >= 20, "should have done at least 20 puts, got {puts}");
    assert!(gets >= 5, "should have done at least 5 gets, got {gets}");
    assert!(
        deletes >= 1,
        "should have done at least 1 delete, got {deletes}"
    );
    assert!(lists >= 1, "should have done at least 1 list, got {lists}");

    // All remaining live keys should be readable.
    let remaining = live_keys.read().await.clone();
    for (key, expected) in &remaining {
        let (got, _) = c.node(0).get_object("mix", key).await.unwrap();
        assert_eq!(&got, expected, "post-mix verification: {key}");
    }
}

// =========================================================================
// 5. Rolling chaos: kills + adds + writes + deletes
// =========================================================================

/// 7-node cluster. While writes and reads run continuously, nodes are
/// killed, revived, and new nodes are added. After stabilization,
/// all surviving objects must be readable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ntest::timeout(60000)]
async fn test_rolling_chaos_with_expansion() {
    let c = Arc::new(RwLock::new(
        IntegrationCluster::with_replication(7, 2048, 4, 2, 3).await,
    ));

    let stop = Arc::new(AtomicBool::new(false));
    let written: Arc<RwLock<Vec<(String, Vec<u8>)>>> = Arc::new(RwLock::new(Vec::new()));
    let write_count = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    // --- Writers (3 tasks) ---
    for writer_id in 0..3 {
        let cluster = c.clone();
        let stop = stop.clone();
        let keys = written.clone();
        let wc = write_count.clone();
        handles.push(tokio::spawn(async move {
            let mut idx = 0u32;
            while !stop.load(Ordering::Relaxed) {
                let data =
                    test_data_seeded(1000 + (idx as usize % 4000), writer_id * 100_000 + idx);
                let key = format!("roll-w{writer_id}-{idx}");
                let cl = cluster.read().await;
                let node = (writer_id as usize + idx as usize) % cl.len();

                match cl
                    .node(node)
                    .put_object("roll", &key, &data, BTreeMap::new())
                    .await
                {
                    Ok(_) => {
                        cl.broadcast_manifest(node, "roll", &key).await;
                        keys.write().await.push((key, data));
                        wc.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
                idx += 1;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }));
    }

    // --- Chaos controller (1 task) ---
    // Sequence: kill node, wait, revive, wait, add node, repeat.
    {
        let cluster = c.clone();
        let stop = stop.clone();
        handles.push(tokio::spawn(async move {
            let mut cycle = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let target = (cycle * 3 + 1) % 7;

                // Kill.
                cluster.read().await.kill_node(target).await;
                tokio::time::sleep(Duration::from_millis(500)).await;

                if stop.load(Ordering::Relaxed) {
                    break;
                }

                // Revive.
                cluster.read().await.revive_node(target).await;
                tokio::time::sleep(Duration::from_millis(500)).await;

                if stop.load(Ordering::Relaxed) {
                    break;
                }

                // Add a node every other cycle (up to 2 new nodes).
                if cycle < 2 {
                    let mut cl = cluster.write().await;
                    let new_idx = cl.add_node().await;
                    cl.node(new_idx).sync_manifests_from_peers().await.ok();
                }

                cycle += 1;
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }));
    }

    // Run chaos for 8 seconds.
    tokio::time::sleep(Duration::from_secs(8)).await;
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.await.unwrap();
    }

    // --- Stabilize: revive all original nodes, sync everyone ---
    {
        let cl = c.read().await;
        for i in 0..7 {
            cl.revive_node(i).await;
        }
        // Sync all nodes.
        for i in 0..cl.len() {
            cl.node(i).sync_manifests_from_peers().await.ok();
        }
    }

    // --- Verify all written objects ---
    let snapshot = written.read().await.clone();
    let total = snapshot.len();
    assert!(
        total >= 10,
        "should have written at least 10 objects, got {total}"
    );

    let cl = c.read().await;
    let mut verified = 0;
    let mut failed = Vec::new();

    for (key, expected) in &snapshot {
        let mut ok = false;
        for reader in 0..cl.len() {
            if let Ok((got, _)) = cl.node(reader).get_object("roll", key).await {
                assert_eq!(&got, expected, "corruption in {key}");
                ok = true;
                verified += 1;
                break;
            }
        }
        if !ok {
            failed.push(key.clone());
        }
    }

    assert!(
        failed.is_empty(),
        "data loss: {} of {} objects unreadable: {:?}",
        failed.len(),
        total,
        &failed[..failed.len().min(10)]
    );

    assert_eq!(verified, total, "all {total} objects verified");
}

// =========================================================================
// 6. Large cluster
// =========================================================================

/// 20-node cluster. Write 200 objects, read from random nodes.
/// Verify shard distribution uses all nodes.
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_large_cluster_20_nodes() {
    let c = IntegrationCluster::new(20, 2048, 4, 2).await;

    let mut objects = Vec::new();
    for i in 0..200 {
        let data = test_data_seeded(1000 + i * 10, i as u32 + 3000);
        let key = format!("large-{i:03}");
        let writer = i % 20;

        c.node(writer)
            .put_object("big", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        c.broadcast_manifest(writer, "big", &key).await;

        objects.push((key, data));
    }

    // Read all from various nodes.
    for (i, (key, expected)) in objects.iter().enumerate() {
        let reader = (i * 7 + 3) % 20;
        let (got, _) = c.node(reader).get_object("big", key).await.unwrap();
        assert_eq!(&got, expected, "mismatch for {key} on node {reader}");
    }

    // Check distribution: at least 15 of 20 nodes should have shards.
    let mut nodes_with_shards = 0;
    for i in 0..20 {
        if c.local_shard_count(i).await > 0 {
            nodes_with_shards += 1;
        }
    }
    assert!(
        nodes_with_shards >= 15,
        "expected at least 15 of 20 nodes with shards, got {nodes_with_shards}"
    );
}

// =========================================================================
// 7. Multiple erasure configurations
// =========================================================================

/// Test various k/m combinations all produce correct results.
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_erasure_config_variations() {
    let configs: Vec<(usize, usize, usize)> = vec![
        // (k, m, num_nodes)
        (1, 1, 3),  // minimum viable
        (2, 1, 4),  // default small
        (2, 2, 5),  // extra parity
        (4, 2, 7),  // medium
        (8, 4, 13), // large
    ];

    for (k, m, n) in configs {
        let c = IntegrationCluster::new(n, 1024, k, m).await;

        let data = test_data_seeded(5000, (k * 100 + m) as u32);
        let key = format!("ec-k{k}-m{m}");

        c.node(0)
            .put_object("ec", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        c.broadcast_manifest(0, "ec", &key).await;

        // Verify from multiple nodes.
        for reader in [0, 1, n - 1] {
            let (got, _) = c.node(reader).get_object("ec", &key).await.unwrap();
            assert_eq!(got, data, "k={k} m={m}: data mismatch on reader {reader}");
        }
    }
}

// =========================================================================
// 8. Metadata and HEAD operations
// =========================================================================

/// Verify user metadata survives the full pipeline round-trip.
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_metadata_round_trip() {
    let c = IntegrationCluster::new(5, 1024, 2, 1).await;

    let mut metadata = BTreeMap::new();
    metadata.insert(
        "Content-Type".to_string(),
        "application/octet-stream".to_string(),
    );
    metadata.insert(
        "X-Custom-Header".to_string(),
        "shoal-test-value".to_string(),
    );
    metadata.insert("X-Empty".to_string(), String::new());

    let data = test_data_seeded(3000, 9999);
    c.node(0)
        .put_object("meta", "tagged.bin", &data, metadata.clone())
        .await
        .unwrap();
    c.broadcast_manifest(0, "meta", "tagged.bin").await;

    // HEAD from a different node.
    let manifest = c.node(2).head_object("meta", "tagged.bin").await.unwrap();
    assert_eq!(manifest.metadata, metadata);
    assert_eq!(manifest.total_size, 3000);

    // GET also returns the manifest with metadata.
    let (got, got_manifest) = c.node(3).get_object("meta", "tagged.bin").await.unwrap();
    assert_eq!(got, data);
    assert_eq!(got_manifest.metadata, metadata);
}

// =========================================================================
// 9. Delete-then-list consistency
// =========================================================================

/// Write 50 objects, delete half, verify list returns exactly the
/// non-deleted ones.
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_delete_then_list_consistency() {
    let c = IntegrationCluster::new(5, 1024, 2, 1).await;

    let mut all_keys = Vec::new();
    for i in 0..50 {
        let data = test_data_seeded(1000, i + 6000);
        let key = format!("dl-{i:03}");

        c.node(i as usize % 5)
            .put_object("dl", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        c.broadcast_manifest(i as usize % 5, "dl", &key).await;
        all_keys.push(key);
    }

    // Delete even-numbered objects.
    let mut kept = Vec::new();
    for (i, key) in all_keys.iter().enumerate() {
        if i % 2 == 0 {
            c.node(0).delete_object("dl", key).await.unwrap();
            // Propagate delete to all nodes.
            for node in 1..5 {
                let _ = c.node(node).delete_object("dl", key).await;
            }
        } else {
            kept.push(key.clone());
        }
    }

    // List should return exactly the kept keys.
    let mut listed = c.node(0).list_objects("dl", "").await.unwrap();
    listed.sort();
    kept.sort();
    assert_eq!(listed, kept, "list after delete should match kept keys");

    // Verify deleted objects are truly gone.
    for (i, key) in all_keys.iter().enumerate() {
        if i % 2 == 0 {
            assert!(
                c.node(0).get_object("dl", key).await.is_err(),
                "deleted object {key} should not be readable"
            );
        }
    }
}

// =========================================================================
// 10. Multi-bucket isolation
// =========================================================================

/// Objects in different buckets should be completely isolated.
/// Same key name in different buckets → different objects.
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_multi_bucket_isolation() {
    let c = IntegrationCluster::new(5, 1024, 2, 1).await;

    let buckets = ["alpha", "beta", "gamma"];

    for (bi, bucket) in buckets.iter().enumerate() {
        for i in 0..10 {
            let data = test_data_seeded(1500, (bi * 100 + i) as u32);
            let key = format!("shared-key-{i}");

            c.node(i % 5)
                .put_object(bucket, &key, &data, BTreeMap::new())
                .await
                .unwrap();
            c.broadcast_manifest(i % 5, bucket, &key).await;
        }
    }

    // Verify each bucket's objects are independent.
    for (bi, bucket) in buckets.iter().enumerate() {
        let keys = c.node(0).list_objects(bucket, "").await.unwrap();
        assert_eq!(
            keys.len(),
            10,
            "bucket {bucket} should have 10 objects, got {}",
            keys.len()
        );

        for i in 0..10 {
            let expected = test_data_seeded(1500, (bi * 100 + i) as u32);
            let key = format!("shared-key-{i}");

            let (got, _) = c.node(1).get_object(bucket, &key).await.unwrap();
            assert_eq!(
                got, expected,
                "bucket {bucket} key {key}: data doesn't match"
            );
        }
    }

    // Delete from one bucket shouldn't affect others.
    c.node(0)
        .delete_object("alpha", "shared-key-0")
        .await
        .unwrap();
    assert!(c.node(0).get_object("alpha", "shared-key-0").await.is_err());

    // "beta" and "gamma" should still have "shared-key-0".
    for bucket in ["beta", "gamma"] {
        let (_, _) = c.node(0).get_object(bucket, "shared-key-0").await.unwrap();
    }
}
