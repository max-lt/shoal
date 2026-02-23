//! Multi-node cluster tests and shard loss scenarios.

use std::collections::BTreeMap;

use crate::error::EngineError;

use super::helpers::{test_data, three_node_cluster};

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
// 3-node cluster: shard loss scenarios
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_three_node_lose_one_shard_per_chunk() {
    // k=2, m=1 across 3 nodes. Lose 1 shard per chunk -> still reads.
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
    // k=4, m=2 across 3 nodes. Lose 2 shards per chunk -> still reads.
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
    // k=4, m=2. Lose 3 shards per chunk -> only 3 left < k=4, should fail.
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
