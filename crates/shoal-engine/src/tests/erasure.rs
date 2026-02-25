//! Erasure coding variants, missing shard scenarios, and shard index tests.

use std::collections::BTreeMap;

use crate::error::EngineError;

use super::helpers::{single_node, test_data};

// -----------------------------------------------------------------------
// Erasure config variations
// -----------------------------------------------------------------------

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_erasure_k4_m2() {
    let node = single_node(512, 4, 2).await;
    let data = test_data(3000);

    node.put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    let (got, manifest) = node.get_object("b", "k").await.unwrap();
    assert_eq!(got, data);
    // With CDC, 3000 bytes (< 16KB min) → 1 chunk, each with 6 shards (k=4+m=2).
    assert_eq!(manifest.chunks.len(), 1);
    assert_eq!(manifest.chunks[0].shards.len(), 6);
}

#[tokio::test]
#[ntest::timeout(10000)]
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

#[tokio::test]
#[ntest::timeout(10000)]
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
// All shards have correct index values
// -----------------------------------------------------------------------

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_shard_indices_correct() {
    let node = single_node(1024, 4, 2).await;
    let data = test_data(4000);

    node.put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    let manifest = node.head_object("b", "k").await.unwrap();
    for chunk in &manifest.chunks {
        assert_eq!(chunk.shards.len(), 6); // k=4, m=2
        for (i, shard) in chunk.shards.iter().enumerate() {
            assert_eq!(shard.index, i as u8, "shard index mismatch");
        }
    }
}

// -----------------------------------------------------------------------
// Shard deletion simulation (k out of k+m shards survive)
// -----------------------------------------------------------------------

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_read_with_missing_parity_shards() {
    let node = single_node(1024, 2, 1).await;
    let data = test_data(1024);

    node.put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    // Get the manifest so we know which shards were created.
    let manifest = node.head_object("b", "k").await.unwrap();

    // Delete parity shard(s) for each chunk -- should still read via k data shards.
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
#[ntest::timeout(10000)]
async fn test_read_with_missing_data_shard() {
    let node = single_node(1024, 2, 1).await;
    let data = test_data(1024);

    node.put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    let manifest = node.head_object("b", "k").await.unwrap();

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
#[ntest::timeout(10000)]
async fn test_read_fails_too_many_missing() {
    let node = single_node(1024, 2, 1).await;
    let data = test_data(1024);

    node.put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    let manifest = node.head_object("b", "k").await.unwrap();

    // Delete 2 out of 3 shards for each chunk -> only 1 shard left, need 2 (k=2).
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
// Identical data deduplication: shards should have same IDs
// -----------------------------------------------------------------------

#[tokio::test]
#[ntest::timeout(10000)]
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

    // Same data, same metadata -> same ObjectId.
    assert_eq!(oid1, oid2);

    // Same shard IDs.
    let m1 = node.head_object("b", "copy1").await.unwrap();
    let m2 = node.head_object("b", "copy2").await.unwrap();

    for (c1, c2) in m1.chunks.iter().zip(m2.chunks.iter()) {
        assert_eq!(c1.chunk_id, c2.chunk_id);
        for (s1, s2) in c1.shards.iter().zip(c2.shards.iter()) {
            assert_eq!(s1.shard_id, s2.shard_id);
        }
    }
}

// -----------------------------------------------------------------------
// Different erasure configs produce different manifests
// -----------------------------------------------------------------------

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_different_erasure_configs_different_manifests() {
    // With CDC, chunk_size param doesn't affect chunking (fixed CDC params).
    // Different erasure configs (k, m) produce different shard layouts → different manifests.
    let node_a = single_node(512, 2, 1).await;
    let node_b = single_node(512, 4, 2).await;
    let data = test_data(2000);

    let oid_a = node_a
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    let oid_b = node_b
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    // Different erasure configs -> different shard IDs -> different manifests.
    assert_ne!(oid_a, oid_b);
}
