//! Full local write/read pipeline integration test.
//!
//! Connects shoal-store + shoal-cas + shoal-erasure + shoal-placement + shoal-meta
//! into a working local pipeline without any networking.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use shoal_cas::{Chunker, build_manifest_with_timestamp};
use shoal_erasure::{ErasureEncoder, decode};
use shoal_meta::MetaStore;
use shoal_placement::Ring;
use shoal_store::{MemoryStore, ShardStore};
use shoal_types::{ChunkMeta, NodeId, NodeTopology, ShardMeta};

/// Create 3 simulated nodes with MemoryStores and a Ring.
fn setup_cluster() -> (HashMap<NodeId, Arc<MemoryStore>>, Ring, Vec<NodeId>) {
    let node_ids: Vec<NodeId> = (1..=3u8).map(|i| NodeId::from([i; 32])).collect();

    let mut stores: HashMap<NodeId, Arc<MemoryStore>> = HashMap::new();
    let mut ring = Ring::new(128);

    for &nid in &node_ids {
        stores.insert(nid, Arc::new(MemoryStore::new(u64::MAX)));
        ring.add_node(nid, 1_000_000_000, NodeTopology::default());
    }

    (stores, ring, node_ids)
}

/// Generate deterministic, non-repeating test data.
fn test_data(size: usize) -> Vec<u8> {
    // Use a simple PRNG to avoid content-addressing deduplication.
    let mut data = Vec::with_capacity(size);
    let mut state: u32 = 0xDEAD_BEEF;
    for _ in 0..size {
        state = state.wrapping_mul(1103515245).wrapping_add(12345);
        data.push((state >> 16) as u8);
    }
    data
}

/// Write path: chunk → erasure encode → distribute shards via ring → build manifest.
async fn write_object(
    data: &[u8],
    bucket: &str,
    key: &str,
    chunk_size: u32,
    k: usize,
    m: usize,
    stores: &HashMap<NodeId, Arc<MemoryStore>>,
    ring: &Ring,
    meta: &MetaStore,
) {
    let chunker = Chunker::new(chunk_size);
    let encoder = ErasureEncoder::new(k, m);

    let chunks = chunker.chunk(data);

    let mut chunk_metas = Vec::new();

    for chunk in &chunks {
        let (shards, original_size) = encoder.encode(&chunk.data).unwrap();

        let mut shard_metas = Vec::new();

        for shard in &shards {
            // Determine owner via ring placement.
            let owners = ring.owners(&shard.id, 1);
            let owner = owners[0];

            // Store the shard in the owner's store.
            let store = stores.get(&owner).unwrap();
            store.put(shard.id, shard.data.clone()).await.unwrap();

            shard_metas.push(ShardMeta {
                shard_id: shard.id,
                index: shard.index,
                size: shard.data.len() as u32,
            });
        }

        chunk_metas.push(ChunkMeta {
            chunk_id: chunk.id,
            offset: chunk.offset,
            raw_length: original_size as u32,
            stored_length: original_size as u32,
            compression: shoal_types::Compression::None,
            shards: shard_metas,
        });
    }

    let manifest = build_manifest_with_timestamp(
        &chunk_metas,
        data.len() as u64,
        chunk_size,
        BTreeMap::new(),
        1700000000,
    )
    .unwrap();

    meta.put_manifest(&manifest).unwrap();
    meta.put_object_key(bucket, key, &manifest.object_id)
        .unwrap();
}

/// Read path: lookup manifest → fetch shards → erasure decode → concatenate.
///
/// Returns `None` if any chunk cannot be decoded (not enough shards).
async fn read_object(
    bucket: &str,
    key: &str,
    k: usize,
    m: usize,
    stores: &HashMap<NodeId, Arc<MemoryStore>>,
    meta: &MetaStore,
) -> Option<Vec<u8>> {
    let object_id = meta.get_object_key(bucket, key).unwrap()?;
    let manifest = meta.get_manifest(&object_id).unwrap()?;

    let mut result = Vec::with_capacity(manifest.total_size as usize);

    for chunk_meta in &manifest.chunks {
        let original_size = chunk_meta.stored_length as usize;

        // Fetch available shards from all stores.
        let mut available_shards: Vec<(u8, Vec<u8>)> = Vec::new();

        for shard_meta in &chunk_meta.shards {
            for store in stores.values() {
                if let Ok(Some(data)) = store.get(shard_meta.shard_id).await {
                    available_shards.push((shard_meta.index, data.to_vec()));
                    break;
                }
            }
        }

        match decode(k, m, &available_shards, original_size) {
            Ok(chunk_data) => result.extend_from_slice(&chunk_data),
            Err(_) => return None,
        }
    }

    Some(result)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_write_read_roundtrip() {
    let (stores, ring, _nodes) = setup_cluster();
    let meta = MetaStore::open_temporary().unwrap();

    // 5000 bytes with chunk_size=1024 → 5 chunks (4×1024 + 1×904).
    let data = test_data(5000);

    write_object(
        &data, "bucket", "test.bin", 1024, 2, 1, &stores, &ring, &meta,
    )
    .await;

    let result = read_object("bucket", "test.bin", 2, 1, &stores, &meta).await;
    assert_eq!(result, Some(data));
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_chunking_produces_expected_count() {
    let (stores, ring, _nodes) = setup_cluster();
    let meta = MetaStore::open_temporary().unwrap();

    let data = test_data(5000);

    write_object(
        &data,
        "bucket",
        "chunks.bin",
        1024,
        2,
        1,
        &stores,
        &ring,
        &meta,
    )
    .await;

    let oid = meta
        .get_object_key("bucket", "chunks.bin")
        .unwrap()
        .unwrap();
    let manifest = meta.get_manifest(&oid).unwrap().unwrap();
    assert_eq!(manifest.chunks.len(), 5);
    assert_eq!(manifest.total_size, 5000);

    // Each chunk should have k+m = 3 shards.
    for cm in &manifest.chunks {
        assert_eq!(cm.shards.len(), 3);
    }
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_shards_distributed_across_nodes() {
    let (stores, ring, nodes) = setup_cluster();
    let meta = MetaStore::open_temporary().unwrap();

    // Use unique data to avoid content-addressing deduplication.
    let data = test_data(5000);
    write_object(
        &data, "bucket", "dist.bin", 1024, 2, 1, &stores, &ring, &meta,
    )
    .await;

    // Count total unique shards across all nodes.
    let mut total = 0usize;
    for &nid in &nodes {
        let store = stores.get(&nid).unwrap();
        let list = store.list().await.unwrap();
        total += list.len();
    }

    // With unique data: 5 chunks × 3 shards = 15 unique shards.
    assert_eq!(total, 15, "expected 5 chunks × 3 shards = 15 unique shards");

    // Each node should have at least 1 shard.
    for &nid in &nodes {
        let store = stores.get(&nid).unwrap();
        let count = store.list().await.unwrap().len();
        assert!(count > 0, "node {} has 0 shards", nid);
    }
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_single_shard_loss_per_chunk_read_succeeds() {
    // Delete 1 shard per chunk (simulating a node loss).
    // With k=2, m=1, losing 1 of 3 shards leaves k=2 → decode succeeds.
    let (stores, ring, _nodes) = setup_cluster();
    let meta = MetaStore::open_temporary().unwrap();

    let data = test_data(5000);
    write_object(
        &data,
        "bucket",
        "resilient.bin",
        1024,
        2,
        1,
        &stores,
        &ring,
        &meta,
    )
    .await;

    // Delete the first shard (index 0) of each chunk.
    let oid = meta
        .get_object_key("bucket", "resilient.bin")
        .unwrap()
        .unwrap();
    let manifest = meta.get_manifest(&oid).unwrap().unwrap();

    for chunk_meta in &manifest.chunks {
        let shard_to_delete = &chunk_meta.shards[0];
        for store in stores.values() {
            if store.contains(shard_to_delete.shard_id).await.unwrap() {
                store.delete(shard_to_delete.shard_id).await.unwrap();
                break;
            }
        }
    }

    // Read should still succeed with 2 remaining shards per chunk.
    let result = read_object("bucket", "resilient.bin", 2, 1, &stores, &meta).await;
    assert_eq!(
        result,
        Some(data),
        "read should succeed after losing 1 shard per chunk"
    );
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_two_shard_loss_per_chunk_read_fails() {
    // Delete 2 shards per chunk (total failure scenario).
    // With k=2, m=1, losing 2 of 3 shards leaves only 1 → decode fails.
    let (stores, ring, _nodes) = setup_cluster();
    let meta = MetaStore::open_temporary().unwrap();

    let data = test_data(5000);
    write_object(
        &data,
        "bucket",
        "fragile.bin",
        1024,
        2,
        1,
        &stores,
        &ring,
        &meta,
    )
    .await;

    // Delete shards at indices 0 and 1 of each chunk.
    let oid = meta
        .get_object_key("bucket", "fragile.bin")
        .unwrap()
        .unwrap();
    let manifest = meta.get_manifest(&oid).unwrap().unwrap();

    for chunk_meta in &manifest.chunks {
        for shard_meta in chunk_meta.shards.iter().take(2) {
            for store in stores.values() {
                if store.contains(shard_meta.shard_id).await.unwrap() {
                    store.delete(shard_meta.shard_id).await.unwrap();
                    break;
                }
            }
        }
    }

    // Read should fail — only 1 shard remains per chunk, need k=2.
    let result = read_object("bucket", "fragile.bin", 2, 1, &stores, &meta).await;
    assert!(
        result.is_none(),
        "read should fail when 2 of 3 shards per chunk are lost"
    );
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_small_object_roundtrip() {
    let (stores, ring, _nodes) = setup_cluster();
    let meta = MetaStore::open_temporary().unwrap();

    let data = b"hello";
    write_object(
        data, "bucket", "tiny.txt", 1024, 2, 1, &stores, &ring, &meta,
    )
    .await;

    let result = read_object("bucket", "tiny.txt", 2, 1, &stores, &meta).await;
    assert_eq!(result, Some(data.to_vec()));
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_exact_chunk_size_object() {
    let (stores, ring, _nodes) = setup_cluster();
    let meta = MetaStore::open_temporary().unwrap();

    let data = test_data(1024);
    write_object(
        &data,
        "bucket",
        "exact.bin",
        1024,
        2,
        1,
        &stores,
        &ring,
        &meta,
    )
    .await;

    let result = read_object("bucket", "exact.bin", 2, 1, &stores, &meta).await;
    assert_eq!(result, Some(data));
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_multiple_objects_independent() {
    let (stores, ring, _nodes) = setup_cluster();
    let meta = MetaStore::open_temporary().unwrap();

    let data_a = test_data(3000);
    let data_b = test_data(7000); // different seed effect via size

    // Use different seeds by prepending a marker byte.
    let mut data_b_unique = vec![0xBBu8];
    data_b_unique.extend(&data_b[1..]);

    write_object(
        &data_a, "bucket", "a.bin", 1024, 2, 1, &stores, &ring, &meta,
    )
    .await;
    write_object(
        &data_b_unique,
        "bucket",
        "b.bin",
        1024,
        2,
        1,
        &stores,
        &ring,
        &meta,
    )
    .await;

    let result_a = read_object("bucket", "a.bin", 2, 1, &stores, &meta).await;
    let result_b = read_object("bucket", "b.bin", 2, 1, &stores, &meta).await;

    assert_eq!(result_a, Some(data_a));
    assert_eq!(result_b, Some(data_b_unique));
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_nonexistent_object_returns_none() {
    let (stores, _ring, _nodes) = setup_cluster();
    let meta = MetaStore::open_temporary().unwrap();

    let result = read_object("bucket", "ghost.bin", 2, 1, &stores, &meta).await;
    assert_eq!(result, None);
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_shard_integrity_verified() {
    let (stores, ring, _nodes) = setup_cluster();
    let meta = MetaStore::open_temporary().unwrap();

    let data = test_data(2048);
    write_object(
        &data,
        "bucket",
        "verify.bin",
        1024,
        2,
        1,
        &stores,
        &ring,
        &meta,
    )
    .await;

    for store in stores.values() {
        let shard_ids = store.list().await.unwrap();
        for sid in shard_ids {
            assert!(
                store.verify(sid).await.unwrap(),
                "shard {sid} failed integrity check"
            );
        }
    }
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_parity_shard_reconstruction() {
    // Delete the first DATA shard (index 0) of each chunk.
    // The decoder must use the parity shard to reconstruct.
    let (stores, ring, _nodes) = setup_cluster();
    let meta = MetaStore::open_temporary().unwrap();

    let data = test_data(5000);
    write_object(
        &data,
        "bucket",
        "parity.bin",
        1024,
        2,
        1,
        &stores,
        &ring,
        &meta,
    )
    .await;

    let oid = meta
        .get_object_key("bucket", "parity.bin")
        .unwrap()
        .unwrap();
    let manifest = meta.get_manifest(&oid).unwrap().unwrap();

    // Delete shard index 0 (data shard) from each chunk, forcing parity decode.
    for chunk_meta in &manifest.chunks {
        let shard = &chunk_meta.shards[0];
        assert_eq!(shard.index, 0, "first shard should be index 0");
        for store in stores.values() {
            if store.contains(shard.shard_id).await.unwrap() {
                store.delete(shard.shard_id).await.unwrap();
                break;
            }
        }
    }

    // Read should succeed using data shard 1 + parity shard 2.
    let result = read_object("bucket", "parity.bin", 2, 1, &stores, &meta).await;
    assert_eq!(
        result,
        Some(data),
        "decode should succeed using parity shard"
    );
}
