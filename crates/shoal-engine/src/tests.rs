//! Tests for the shoal-engine crate.

use std::collections::BTreeMap;
use std::sync::Arc;

use shoal_cluster::ClusterState;
use shoal_meta::MetaStore;
use shoal_store::MemoryStore;
use shoal_types::{Member, MemberState, NodeId, NodeTopology};

use crate::error::EngineError;
use crate::node::{ShoalNode, ShoalNodeConfig};

const TEST_MAX_BYTES: u64 = 1_000_000_000;

/// Generate deterministic, non-repeating test data.
fn test_data(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let mut state: u32 = 0xDEAD_BEEF;
    for _ in 0..size {
        state = state.wrapping_mul(1103515245).wrapping_add(12345);
        data.push((state >> 16) as u8);
    }
    data
}

/// Create a single-node ShoalNode for testing.
async fn single_node(chunk_size: u32, k: usize, m: usize) -> ShoalNode {
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
async fn three_node_cluster(chunk_size: u32, k: usize, m: usize) -> Vec<ShoalNode> {
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
                },
                store,
                meta,
                cluster.clone(),
            )
        })
        .collect()
}

// -----------------------------------------------------------------------
// Single-node put/get
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_single_node_put_get() {
    let node = single_node(1024, 2, 1).await;
    let data = test_data(5000);

    let oid = node
        .put_object("mybucket", "hello.txt", &data, BTreeMap::new())
        .await
        .unwrap();

    let (got, manifest) = node.get_object("mybucket", "hello.txt").await.unwrap();
    assert_eq!(got, data);
    assert_eq!(manifest.object_id, oid);
    assert_eq!(manifest.total_size, 5000);
}

#[tokio::test]
async fn test_single_node_put_get_exact_chunk() {
    let node = single_node(1024, 2, 1).await;
    // Exactly 2 chunks (2048 bytes, chunk_size=1024).
    let data = test_data(2048);

    node.put_object("b", "exact", &data, BTreeMap::new())
        .await
        .unwrap();

    let (got, _) = node.get_object("b", "exact").await.unwrap();
    assert_eq!(got, data);
}

// -----------------------------------------------------------------------
// Small objects (10 bytes)
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_small_object() {
    let node = single_node(1024, 2, 1).await;
    let data = b"tiny data!".to_vec();

    node.put_object("b", "tiny", &data, BTreeMap::new())
        .await
        .unwrap();

    let (got, manifest) = node.get_object("b", "tiny").await.unwrap();
    assert_eq!(got, data);
    assert_eq!(manifest.total_size, 10);
    assert_eq!(manifest.chunks.len(), 1);
}

#[tokio::test]
async fn test_single_byte_object() {
    let node = single_node(1024, 2, 1).await;
    let data = vec![42u8];

    node.put_object("b", "one", &data, BTreeMap::new())
        .await
        .unwrap();

    let (got, _) = node.get_object("b", "one").await.unwrap();
    assert_eq!(got, data);
}

// -----------------------------------------------------------------------
// Metadata pass-through
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_metadata_preserved() {
    let node = single_node(1024, 2, 1).await;
    let mut meta = BTreeMap::new();
    meta.insert("content-type".to_string(), "text/plain".to_string());
    meta.insert("x-custom".to_string(), "value42".to_string());

    node.put_object("b", "meta", b"hello", meta.clone())
        .await
        .unwrap();

    let (_, manifest) = node.get_object("b", "meta").await.unwrap();
    assert_eq!(manifest.metadata, meta);
}

// -----------------------------------------------------------------------
// Delete
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_delete_object() {
    let node = single_node(1024, 2, 1).await;
    node.put_object("b", "del", b"data", BTreeMap::new())
        .await
        .unwrap();

    assert!(node.has_object("b", "del").unwrap());

    node.delete_object("b", "del").await.unwrap();

    assert!(!node.has_object("b", "del").unwrap());

    // Get after delete returns ObjectNotFound.
    let err = node.get_object("b", "del").await.unwrap_err();
    assert!(matches!(err, EngineError::ObjectNotFound { .. }));
}

#[tokio::test]
async fn test_delete_nonexistent() {
    let node = single_node(1024, 2, 1).await;
    let err = node.delete_object("b", "nope").await.unwrap_err();
    assert!(matches!(err, EngineError::ObjectNotFound { .. }));
}

// -----------------------------------------------------------------------
// has_object / list_objects / head_object
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_has_object() {
    let node = single_node(1024, 2, 1).await;
    assert!(!node.has_object("b", "k").unwrap());

    node.put_object("b", "k", b"val", BTreeMap::new())
        .await
        .unwrap();

    assert!(node.has_object("b", "k").unwrap());
}

#[tokio::test]
async fn test_list_objects() {
    let node = single_node(1024, 2, 1).await;
    node.put_object("b", "photos/a.jpg", b"a", BTreeMap::new())
        .await
        .unwrap();
    node.put_object("b", "photos/b.jpg", b"b", BTreeMap::new())
        .await
        .unwrap();
    node.put_object("b", "docs/c.txt", b"c", BTreeMap::new())
        .await
        .unwrap();

    let photos = node.list_objects("b", "photos/").unwrap();
    assert_eq!(photos.len(), 2);

    let docs = node.list_objects("b", "docs/").unwrap();
    assert_eq!(docs.len(), 1);

    let all = node.list_objects("b", "").unwrap();
    assert_eq!(all.len(), 3);
}

#[tokio::test]
async fn test_head_object() {
    let node = single_node(1024, 2, 1).await;
    let data = test_data(3000);
    let mut meta = BTreeMap::new();
    meta.insert(
        "content-type".to_string(),
        "application/octet-stream".to_string(),
    );

    let oid = node
        .put_object("b", "head", &data, meta.clone())
        .await
        .unwrap();

    let manifest = node.head_object("b", "head").unwrap();
    assert_eq!(manifest.object_id, oid);
    assert_eq!(manifest.total_size, 3000);
    assert_eq!(manifest.metadata, meta);
}

#[tokio::test]
async fn test_head_nonexistent() {
    let node = single_node(1024, 2, 1).await;
    let err = node.head_object("b", "nope").unwrap_err();
    assert!(matches!(err, EngineError::ObjectNotFound { .. }));
}

// -----------------------------------------------------------------------
// Overwrite
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_overwrite_object() {
    let node = single_node(1024, 2, 1).await;

    node.put_object("b", "k", b"version1", BTreeMap::new())
        .await
        .unwrap();
    let (got1, _) = node.get_object("b", "k").await.unwrap();
    assert_eq!(got1, b"version1");

    node.put_object("b", "k", b"version2-longer", BTreeMap::new())
        .await
        .unwrap();
    let (got2, _) = node.get_object("b", "k").await.unwrap();
    assert_eq!(got2, b"version2-longer");
}

// -----------------------------------------------------------------------
// Large object (10KB)
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_large_object() {
    let node = single_node(1024, 2, 1).await;
    let data = test_data(10_000);

    node.put_object("b", "large", &data, BTreeMap::new())
        .await
        .unwrap();

    let (got, manifest) = node.get_object("b", "large").await.unwrap();
    assert_eq!(got, data);
    assert_eq!(manifest.total_size, 10_000);
    // 10000 / 1024 = 9.77 → 10 chunks
    assert_eq!(manifest.chunks.len(), 10);
}

// -----------------------------------------------------------------------
// Object ID determinism
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_object_id_deterministic() {
    let node = single_node(1024, 2, 1).await;
    let data = test_data(2000);

    let oid1 = node
        .put_object("b", "k1", &data, BTreeMap::new())
        .await
        .unwrap();
    let oid2 = node
        .put_object("b", "k2", &data, BTreeMap::new())
        .await
        .unwrap();

    // Same data, same metadata → same ObjectId.
    assert_eq!(oid1, oid2);
}

#[tokio::test]
async fn test_different_data_different_id() {
    let node = single_node(1024, 2, 1).await;

    let oid1 = node
        .put_object("b", "k1", b"hello", BTreeMap::new())
        .await
        .unwrap();
    let oid2 = node
        .put_object("b", "k2", b"world", BTreeMap::new())
        .await
        .unwrap();

    assert_ne!(oid1, oid2);
}

// -----------------------------------------------------------------------
// Multiple buckets
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_multiple_buckets() {
    let node = single_node(1024, 2, 1).await;

    node.put_object("bucket-a", "key", b"data-a", BTreeMap::new())
        .await
        .unwrap();
    node.put_object("bucket-b", "key", b"data-b", BTreeMap::new())
        .await
        .unwrap();

    let (a, _) = node.get_object("bucket-a", "key").await.unwrap();
    let (b, _) = node.get_object("bucket-b", "key").await.unwrap();

    assert_eq!(a, b"data-a");
    assert_eq!(b, b"data-b");
}

// -----------------------------------------------------------------------
// 3-node cluster: put on node 0, get from node 0
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_three_node_put_get_same_node() {
    let nodes = three_node_cluster(1024, 2, 1).await;
    let data = test_data(5000);

    let oid = nodes[0]
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    // In our test setup each node stores shards locally even if not an owner,
    // so reading from the writing node should work.
    let (got, manifest) = nodes[0].get_object("b", "k").await.unwrap();
    assert_eq!(got, data);
    assert_eq!(manifest.object_id, oid);
}

// -----------------------------------------------------------------------
// Shard deletion simulation (k out of k+m shards survive)
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_read_with_missing_parity_shards() {
    let node = single_node(1024, 2, 1).await;
    let data = test_data(1024);

    node.put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    // Get the manifest so we know which shards were created.
    let manifest = node.head_object("b", "k").unwrap();

    // Delete parity shard(s) for each chunk — should still read via k data shards.
    for chunk_meta in &manifest.chunks {
        for shard_meta in &chunk_meta.shards {
            // Parity shards have index >= k
            if shard_meta.index >= 2 {
                node.store().delete(shard_meta.shard_id).await.unwrap();
            }
        }
    }

    let (got, _) = node.get_object("b", "k").await.unwrap();
    assert_eq!(got, data);
}

#[tokio::test]
async fn test_read_with_missing_data_shard() {
    let node = single_node(1024, 2, 1).await;
    let data = test_data(1024);

    node.put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    let manifest = node.head_object("b", "k").unwrap();

    // Delete first data shard (index 0) for each chunk.
    // With k=2, m=1 we still have index 1 (data) + index 2 (parity) = 2 >= k.
    for chunk_meta in &manifest.chunks {
        for shard_meta in &chunk_meta.shards {
            if shard_meta.index == 0 {
                node.store().delete(shard_meta.shard_id).await.unwrap();
            }
        }
    }

    let (got, _) = node.get_object("b", "k").await.unwrap();
    assert_eq!(got, data);
}

#[tokio::test]
async fn test_read_fails_too_many_missing() {
    let node = single_node(1024, 2, 1).await;
    let data = test_data(1024);

    node.put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    let manifest = node.head_object("b", "k").unwrap();

    // Delete 2 out of 3 shards for each chunk → only 1 shard left, need 2 (k=2).
    for chunk_meta in &manifest.chunks {
        let mut deleted = 0;
        for shard_meta in &chunk_meta.shards {
            if deleted < 2 {
                node.store().delete(shard_meta.shard_id).await.unwrap();
                deleted += 1;
            }
        }
    }

    let err = node.get_object("b", "k").await.unwrap_err();
    assert!(matches!(err, EngineError::ReadFailed { .. }));
}

// -----------------------------------------------------------------------
// Erasure config variations
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_erasure_k4_m2() {
    let node = single_node(512, 4, 2).await;
    let data = test_data(3000);

    node.put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    let (got, manifest) = node.get_object("b", "k").await.unwrap();
    assert_eq!(got, data);
    // 3000 / 512 = 5.86 → 6 chunks, each with 6 shards (4+2)
    assert_eq!(manifest.chunks.len(), 6);
    assert_eq!(manifest.chunks[0].shards.len(), 6);
}

#[tokio::test]
async fn test_erasure_k1_m1() {
    // Minimal erasure config: k=1, m=1 (simple mirroring).
    let node = single_node(1024, 1, 1).await;
    let data = test_data(2000);

    node.put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    let (got, _) = node.get_object("b", "k").await.unwrap();
    assert_eq!(got, data);
}

// -----------------------------------------------------------------------
// Empty data edge case
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_empty_object() {
    let node = single_node(1024, 2, 1).await;

    node.put_object("b", "empty", b"", BTreeMap::new())
        .await
        .unwrap();

    let (got, manifest) = node.get_object("b", "empty").await.unwrap();
    assert!(got.is_empty());
    assert_eq!(manifest.total_size, 0);
}

// -----------------------------------------------------------------------
// Concurrent put/get (race conditions, shared state)
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_concurrent_puts_different_keys() {
    let node = Arc::new(single_node(1024, 2, 1).await);

    let mut handles = Vec::new();
    for i in 0..20u32 {
        let n = Arc::clone(&node);
        handles.push(tokio::spawn(async move {
            let data = test_data(500 + i as usize * 100);
            let key = format!("key-{i}");
            n.put_object("b", &key, &data, BTreeMap::new())
                .await
                .unwrap();
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Verify all objects are readable.
    for i in 0..20u32 {
        let key = format!("key-{i}");
        let expected = test_data(500 + i as usize * 100);
        let (got, _) = node.get_object("b", &key).await.unwrap();
        assert_eq!(got, expected, "mismatch for {key}");
    }
}

#[tokio::test]
async fn test_concurrent_reads_same_key() {
    let node = Arc::new(single_node(1024, 2, 1).await);
    let data = test_data(5000);
    node.put_object("b", "shared", &data, BTreeMap::new())
        .await
        .unwrap();

    let mut handles = Vec::new();
    for _ in 0..10 {
        let n = Arc::clone(&node);
        let expected = data.clone();
        handles.push(tokio::spawn(async move {
            let (got, _) = n.get_object("b", "shared").await.unwrap();
            assert_eq!(got, expected);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn test_concurrent_put_and_read() {
    // One task writes objects while another reads previously written ones.
    let node = Arc::new(single_node(1024, 2, 1).await);

    // Pre-populate some data.
    for i in 0..5u32 {
        let data = test_data(1000 + i as usize * 200);
        node.put_object("b", &format!("pre-{i}"), &data, BTreeMap::new())
            .await
            .unwrap();
    }

    let writer = {
        let n = Arc::clone(&node);
        tokio::spawn(async move {
            for i in 5..15u32 {
                let data = test_data(1000 + i as usize * 200);
                n.put_object("b", &format!("new-{i}"), &data, BTreeMap::new())
                    .await
                    .unwrap();
            }
        })
    };

    let reader = {
        let n = Arc::clone(&node);
        tokio::spawn(async move {
            for i in 0..5u32 {
                let expected = test_data(1000 + i as usize * 200);
                let (got, _) = n.get_object("b", &format!("pre-{i}")).await.unwrap();
                assert_eq!(got, expected);
            }
        })
    };

    writer.await.unwrap();
    reader.await.unwrap();
}

// -----------------------------------------------------------------------
// Special characters in bucket/key names
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_special_chars_in_key() {
    let node = single_node(1024, 2, 1).await;
    let data = b"special chars test".to_vec();

    // Keys with various special characters.
    let keys = [
        "hello world",
        "path/to/nested/object",
        "file.with.dots.txt",
        "key-with-dashes",
        "key_with_underscores",
        "UPPERCASE",
        "MiXeD_cAsE-123",
        "key with  multiple   spaces",
    ];

    for key in &keys {
        node.put_object("b", key, &data, BTreeMap::new())
            .await
            .unwrap();
        let (got, _) = node.get_object("b", key).await.unwrap();
        assert_eq!(got, data, "roundtrip failed for key: {key:?}");
    }
}

#[tokio::test]
async fn test_unicode_bucket_and_key() {
    let node = single_node(1024, 2, 1).await;
    let data = b"unicode test".to_vec();

    node.put_object("données", "fichier/à/stocker", &data, BTreeMap::new())
        .await
        .unwrap();
    let (got, _) = node
        .get_object("données", "fichier/à/stocker")
        .await
        .unwrap();
    assert_eq!(got, data);
}

#[tokio::test]
async fn test_empty_key_name() {
    let node = single_node(1024, 2, 1).await;
    let data = b"empty key".to_vec();

    node.put_object("b", "", &data, BTreeMap::new())
        .await
        .unwrap();
    let (got, _) = node.get_object("b", "").await.unwrap();
    assert_eq!(got, data);
}

// -----------------------------------------------------------------------
// Boundary chunk sizes
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_data_exactly_one_chunk() {
    let node = single_node(256, 2, 1).await;
    let data = test_data(256);

    node.put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    let (got, manifest) = node.get_object("b", "k").await.unwrap();
    assert_eq!(got, data);
    assert_eq!(manifest.chunks.len(), 1);
}

#[tokio::test]
async fn test_data_one_byte_over_chunk() {
    let node = single_node(256, 2, 1).await;
    let data = test_data(257);

    node.put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    let (got, manifest) = node.get_object("b", "k").await.unwrap();
    assert_eq!(got, data);
    assert_eq!(manifest.chunks.len(), 2);
    // Second chunk should be 1 byte.
    assert_eq!(manifest.chunks[1].size, 1);
}

#[tokio::test]
async fn test_data_one_byte_under_chunk() {
    let node = single_node(256, 2, 1).await;
    let data = test_data(255);

    node.put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    let (got, manifest) = node.get_object("b", "k").await.unwrap();
    assert_eq!(got, data);
    assert_eq!(manifest.chunks.len(), 1);
}

// -----------------------------------------------------------------------
// Large objects
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_100kb_object() {
    let node = single_node(4096, 4, 2).await;
    let data = test_data(100_000);

    let oid = node
        .put_object("b", "big", &data, BTreeMap::new())
        .await
        .unwrap();
    let (got, manifest) = node.get_object("b", "big").await.unwrap();
    assert_eq!(got, data);
    assert_eq!(manifest.object_id, oid);
    assert_eq!(manifest.total_size, 100_000);
    // 100000 / 4096 = 24.41 → 25 chunks
    assert_eq!(manifest.chunks.len(), 25);
}

#[tokio::test]
async fn test_1mb_object() {
    let node = single_node(8192, 4, 2).await;
    let data = test_data(1_000_000);

    node.put_object("b", "1mb", &data, BTreeMap::new())
        .await
        .unwrap();
    let (got, _) = node.get_object("b", "1mb").await.unwrap();
    assert_eq!(got.len(), 1_000_000);
    assert_eq!(got, data);
}

// -----------------------------------------------------------------------
// Many objects in one bucket
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_many_objects_same_bucket() {
    let node = single_node(1024, 2, 1).await;

    for i in 0..100u32 {
        let data = test_data(200 + i as usize);
        node.put_object("b", &format!("obj-{i:04}"), &data, BTreeMap::new())
            .await
            .unwrap();
    }

    let keys = node.list_objects("b", "").unwrap();
    assert_eq!(keys.len(), 100);

    // Spot check a few.
    for i in [0, 42, 99] {
        let expected = test_data(200 + i as usize);
        let (got, _) = node.get_object("b", &format!("obj-{i:04}")).await.unwrap();
        assert_eq!(got, expected);
    }
}

// -----------------------------------------------------------------------
// Get nonexistent object
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_get_nonexistent_bucket() {
    let node = single_node(1024, 2, 1).await;
    let err = node
        .get_object("no-such-bucket", "no-key")
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::ObjectNotFound { .. }));
}

// -----------------------------------------------------------------------
// Overwrite preserves metadata correctly
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_overwrite_changes_metadata() {
    let node = single_node(1024, 2, 1).await;

    let mut meta1 = BTreeMap::new();
    meta1.insert("content-type".to_string(), "text/plain".to_string());
    node.put_object("b", "k", b"v1", meta1.clone())
        .await
        .unwrap();

    let mut meta2 = BTreeMap::new();
    meta2.insert("content-type".to_string(), "application/json".to_string());
    node.put_object("b", "k", b"v2", meta2.clone())
        .await
        .unwrap();

    let (got, manifest) = node.get_object("b", "k").await.unwrap();
    assert_eq!(got, b"v2");
    assert_eq!(manifest.metadata, meta2);
}

// -----------------------------------------------------------------------
// 3-node cluster: shard loss scenarios
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_three_node_lose_one_shard_per_chunk() {
    // k=2, m=1 across 3 nodes. Lose 1 shard per chunk → still reads.
    let nodes = three_node_cluster(512, 2, 1).await;
    let data = test_data(2000);

    nodes[0]
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    let manifest = nodes[0].head_object("b", "k").unwrap();

    // Delete 1 shard per chunk from the writing node's store.
    for chunk_meta in &manifest.chunks {
        let shard_to_delete = &chunk_meta.shards[0];
        nodes[0]
            .store()
            .delete(shard_to_delete.shard_id)
            .await
            .unwrap();
    }

    let (got, _) = nodes[0].get_object("b", "k").await.unwrap();
    assert_eq!(got, data);
}

#[tokio::test]
async fn test_three_node_k4_m2_lose_two_shards() {
    // k=4, m=2 across 3 nodes. Lose 2 shards per chunk → still reads.
    let nodes = three_node_cluster(1024, 4, 2).await;
    let data = test_data(4000);

    nodes[0]
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    let manifest = nodes[0].head_object("b", "k").unwrap();

    // Delete 2 shards per chunk (parity shards).
    for chunk_meta in &manifest.chunks {
        let mut deleted = 0;
        for shard_meta in chunk_meta.shards.iter().rev() {
            if deleted < 2 {
                nodes[0].store().delete(shard_meta.shard_id).await.unwrap();
                deleted += 1;
            }
        }
    }

    let (got, _) = nodes[0].get_object("b", "k").await.unwrap();
    assert_eq!(got, data);
}

#[tokio::test]
async fn test_three_node_k4_m2_lose_three_shards_fails() {
    // k=4, m=2. Lose 3 shards per chunk → only 3 left < k=4, should fail.
    let nodes = three_node_cluster(1024, 4, 2).await;
    let data = test_data(2000);

    nodes[0]
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    let manifest = nodes[0].head_object("b", "k").unwrap();

    for chunk_meta in &manifest.chunks {
        let mut deleted = 0;
        for shard_meta in &chunk_meta.shards {
            if deleted < 3 {
                nodes[0].store().delete(shard_meta.shard_id).await.unwrap();
                deleted += 1;
            }
        }
    }

    let err = nodes[0].get_object("b", "k").await.unwrap_err();
    assert!(matches!(err, EngineError::ReadFailed { .. }));
}

// -----------------------------------------------------------------------
// Determinism: same data at different chunk sizes produces different manifests
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_different_chunk_sizes_different_manifests() {
    let node_a = single_node(512, 2, 1).await;
    let node_b = single_node(1024, 2, 1).await;
    let data = test_data(2000);

    let oid_a = node_a
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    let oid_b = node_b
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    // Different chunk sizes → different chunking → different manifests.
    assert_ne!(oid_a, oid_b);
}

// -----------------------------------------------------------------------
// Delete then re-put same key
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_delete_then_reput() {
    let node = single_node(1024, 2, 1).await;

    node.put_object("b", "k", b"first", BTreeMap::new())
        .await
        .unwrap();
    node.delete_object("b", "k").await.unwrap();

    node.put_object("b", "k", b"second", BTreeMap::new())
        .await
        .unwrap();
    let (got, _) = node.get_object("b", "k").await.unwrap();
    assert_eq!(got, b"second");
}

// -----------------------------------------------------------------------
// List objects with various prefix patterns
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_list_objects_nested_prefixes() {
    let node = single_node(1024, 2, 1).await;

    let keys = [
        "a/b/c/1.txt",
        "a/b/c/2.txt",
        "a/b/d/3.txt",
        "a/e/4.txt",
        "f/5.txt",
    ];

    for (i, key) in keys.iter().enumerate() {
        let data = test_data(100 + i);
        node.put_object("b", key, &data, BTreeMap::new())
            .await
            .unwrap();
    }

    let abc = node.list_objects("b", "a/b/c/").unwrap();
    assert_eq!(abc.len(), 2);

    let ab = node.list_objects("b", "a/b/").unwrap();
    assert_eq!(ab.len(), 3);

    let a = node.list_objects("b", "a/").unwrap();
    assert_eq!(a.len(), 4);

    let all = node.list_objects("b", "").unwrap();
    assert_eq!(all.len(), 5);

    let f = node.list_objects("b", "f/").unwrap();
    assert_eq!(f.len(), 1);

    let nothing = node.list_objects("b", "z/").unwrap();
    assert!(nothing.is_empty());
}

// -----------------------------------------------------------------------
// Head object returns correct chunk count for various sizes
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_head_object_chunk_count() {
    let node = single_node(256, 2, 1).await;

    let sizes_and_chunks = [
        (0, 0),
        (1, 1),
        (255, 1),
        (256, 1),
        (257, 2),
        (512, 2),
        (513, 3),
        (1024, 4),
    ];

    for (size, expected_chunks) in sizes_and_chunks {
        let data = test_data(size);
        let key = format!("size-{size}");
        node.put_object("b", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        let manifest = node.head_object("b", &key).unwrap();
        assert_eq!(
            manifest.chunks.len(),
            expected_chunks,
            "wrong chunk count for size={size}"
        );
    }
}

// -----------------------------------------------------------------------
// Erasure edge case: k=1, m=0 (no parity at all)
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_erasure_k1_m0_no_redundancy() {
    let node = single_node(1024, 1, 0).await;
    let data = test_data(3000);

    node.put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    let (got, manifest) = node.get_object("b", "k").await.unwrap();
    assert_eq!(got, data);
    // Each chunk should have exactly 1 shard (data only, no parity).
    for chunk in &manifest.chunks {
        assert_eq!(chunk.shards.len(), 1);
    }
}

// -----------------------------------------------------------------------
// Identical data deduplication: shards should have same IDs
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_identical_data_same_shard_ids() {
    let node = single_node(1024, 2, 1).await;
    let data = test_data(2048);

    let oid1 = node
        .put_object("b", "copy1", &data, BTreeMap::new())
        .await
        .unwrap();
    let oid2 = node
        .put_object("b", "copy2", &data, BTreeMap::new())
        .await
        .unwrap();

    // Same data, same metadata → same ObjectId.
    assert_eq!(oid1, oid2);

    // Same shard IDs.
    let m1 = node.head_object("b", "copy1").unwrap();
    let m2 = node.head_object("b", "copy2").unwrap();

    for (c1, c2) in m1.chunks.iter().zip(m2.chunks.iter()) {
        assert_eq!(c1.chunk_id, c2.chunk_id);
        for (s1, s2) in c1.shards.iter().zip(c2.shards.iter()) {
            assert_eq!(s1.shard_id, s2.shard_id);
        }
    }
}

// -----------------------------------------------------------------------
// Manifest version field is correctly set
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_manifest_version_field() {
    let node = single_node(1024, 2, 1).await;
    node.put_object("b", "k", b"data", BTreeMap::new())
        .await
        .unwrap();
    let manifest = node.head_object("b", "k").unwrap();
    assert_eq!(manifest.version, shoal_types::MANIFEST_VERSION);
}

// -----------------------------------------------------------------------
// All shards have correct index values
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_shard_indices_correct() {
    let node = single_node(1024, 4, 2).await;
    let data = test_data(4000);

    node.put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    let manifest = node.head_object("b", "k").unwrap();
    for chunk in &manifest.chunks {
        assert_eq!(chunk.shards.len(), 6); // k=4, m=2
        for (i, shard) in chunk.shards.iter().enumerate() {
            assert_eq!(shard.index, i as u8, "shard index mismatch");
        }
    }
}
