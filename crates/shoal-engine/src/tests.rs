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
