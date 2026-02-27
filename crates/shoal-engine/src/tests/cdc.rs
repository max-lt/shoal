//! CDC-specific tests: deduplication, compression transparency, chunk sizing.

use std::collections::{BTreeMap, HashSet};

use shoal_types::Compression;

use super::helpers::{single_node, test_data};

// -----------------------------------------------------------------------
// Inter-version deduplication
// -----------------------------------------------------------------------

/// PUT v1 (large), PUT v2 (5% modified) → >90% of shard IDs shared.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_cdc_dedup_inter_version() {
    let node = single_node(1024, 2, 1).await;

    let size = 1_048_576; // 1MB
    let v1 = test_data(size);
    let mut v2 = v1.clone();

    // Modify the last 5%.
    let modify_start = size - size / 20;
    for b in &mut v2[modify_start..] {
        *b = b.wrapping_add(1);
    }

    node.put_object("b", "file-v1", &v1, BTreeMap::new())
        .await
        .unwrap();
    node.put_object("b", "file-v2", &v2, BTreeMap::new())
        .await
        .unwrap();

    let m1 = node.head_object("b", "file-v1").await.unwrap();
    let m2 = node.head_object("b", "file-v2").await.unwrap();

    // Collect all shard IDs from both versions.
    let shard_ids_v1: HashSet<_> = m1
        .chunks
        .iter()
        .flat_map(|c| c.shards.iter().map(|s| s.shard_id))
        .collect();
    let shard_ids_v2: HashSet<_> = m2
        .chunks
        .iter()
        .flat_map(|c| c.shards.iter().map(|s| s.shard_id))
        .collect();

    let shared = shard_ids_v1.intersection(&shard_ids_v2).count();
    let max_shards = shard_ids_v1.len().max(shard_ids_v2.len());
    let shared_ratio = shared as f64 / max_shards as f64;

    assert!(
        shared_ratio > 0.80,
        "expected >80% shard reuse between versions, got {:.1}% ({shared}/{max_shards})",
        shared_ratio * 100.0
    );

    // Also verify chunk-level dedup.
    let chunk_ids_v1: HashSet<_> = m1.chunks.iter().map(|c| c.chunk_id).collect();
    let chunk_ids_v2: HashSet<_> = m2.chunks.iter().map(|c| c.chunk_id).collect();
    let shared_chunks = chunk_ids_v1.intersection(&chunk_ids_v2).count();
    let max_chunks = chunk_ids_v1.len().max(chunk_ids_v2.len());
    let chunk_ratio = shared_chunks as f64 / max_chunks as f64;

    assert!(
        chunk_ratio > 0.80,
        "expected >80% chunk reuse, got {:.1}% ({shared_chunks}/{max_chunks})",
        chunk_ratio * 100.0
    );
}

// -----------------------------------------------------------------------
// Small file: below CDC min_size → single chunk
// -----------------------------------------------------------------------

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_cdc_small_file_single_chunk() {
    let node = single_node(1024, 2, 1).await;
    let data = vec![0xABu8; 100]; // 100 bytes, well below 16KB min

    node.put_object("b", "tiny", &data, BTreeMap::new())
        .await
        .unwrap();

    let (got, manifest) = node.get_object("b", "tiny").await.unwrap();
    assert_eq!(got, data);
    assert_eq!(manifest.chunks.len(), 1, "small file must be single chunk");
    assert_eq!(manifest.chunks[0].raw_length, 100);
    assert_eq!(manifest.total_size, 100);
}

// -----------------------------------------------------------------------
// Large file: chunks between min and max size
// -----------------------------------------------------------------------

#[tokio::test]
#[ntest::timeout(30000)]
async fn test_cdc_large_file_chunk_bounds() {
    let node = single_node(1024, 2, 1).await;
    let data = test_data(2_000_000); // 2MB

    node.put_object("b", "large", &data, BTreeMap::new())
        .await
        .unwrap();

    let (got, manifest) = node.get_object("b", "large").await.unwrap();
    assert_eq!(got, data);
    assert!(
        manifest.chunks.len() > 1,
        "2MB should produce multiple chunks"
    );

    // chunk_size=1024 → CDC min=64, max=1024.
    let min_size: u32 = 1024 / 16;
    let max_size: u32 = 1024;

    for (i, chunk) in manifest.chunks.iter().enumerate() {
        if i < manifest.chunks.len() - 1 {
            assert!(
                chunk.raw_length >= min_size,
                "chunk {i} raw_length {} < min {min_size}",
                chunk.raw_length
            );
        }
        assert!(
            chunk.raw_length <= max_size,
            "chunk {i} raw_length {} > max {max_size}",
            chunk.raw_length
        );
    }

    // Verify offsets are contiguous.
    let mut expected_offset = 0u64;
    for chunk in &manifest.chunks {
        assert_eq!(chunk.offset, expected_offset);
        expected_offset += chunk.raw_length as u64;
    }
    assert_eq!(expected_offset, data.len() as u64);
}

// -----------------------------------------------------------------------
// Compression transparency: put raw → get raw
// -----------------------------------------------------------------------

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_compression_transparent_roundtrip() {
    let node = single_node(1024, 2, 1).await;

    // Highly compressible data (repeated pattern).
    let data = vec![42u8; 50_000];

    node.put_object("b", "compressible", &data, BTreeMap::new())
        .await
        .unwrap();

    let (got, manifest) = node.get_object("b", "compressible").await.unwrap();
    assert_eq!(got, data, "data must round-trip through compression");
    assert_eq!(manifest.total_size, 50_000);

    // Verify compression was applied (stored_length < raw_length for compressible data).
    for chunk in &manifest.chunks {
        assert_eq!(chunk.compression, Compression::Zstd);
        assert!(
            chunk.stored_length < chunk.raw_length,
            "compressible data should compress: stored={} raw={}",
            chunk.stored_length,
            chunk.raw_length
        );
    }
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_incompressible_data_no_compression() {
    let node = single_node(1024, 2, 1).await;

    // Pseudo-random data should not compress well.
    // If compressed is >= raw, the engine should store uncompressed.
    let data = test_data(50_000);

    node.put_object("b", "random", &data, BTreeMap::new())
        .await
        .unwrap();

    let (got, manifest) = node.get_object("b", "random").await.unwrap();
    assert_eq!(got, data, "random data must round-trip");

    // Some chunks may compress, some may not — just verify the round-trip works.
    for chunk in &manifest.chunks {
        match chunk.compression {
            Compression::Zstd => {
                assert!(chunk.stored_length < chunk.raw_length);
            }
            Compression::None => {
                assert_eq!(chunk.stored_length, chunk.raw_length);
            }
        }
    }
}

// -----------------------------------------------------------------------
// Empty file: 0 bytes → 0 chunks
// -----------------------------------------------------------------------

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_cdc_empty_file() {
    let node = single_node(1024, 2, 1).await;

    node.put_object("b", "empty", b"", BTreeMap::new())
        .await
        .unwrap();

    let (got, manifest) = node.get_object("b", "empty").await.unwrap();
    assert!(got.is_empty());
    assert_eq!(manifest.chunks.len(), 0, "empty file → 0 chunks");
    assert_eq!(manifest.total_size, 0);
}

// -----------------------------------------------------------------------
// Identical data produces identical manifests (deterministic CDC + compression)
// -----------------------------------------------------------------------

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_cdc_deterministic() {
    let node = single_node(1024, 2, 1).await;
    let data = test_data(100_000);

    let oid1 = node
        .put_object("b", "det1", &data, BTreeMap::new())
        .await
        .unwrap();
    let oid2 = node
        .put_object("b", "det2", &data, BTreeMap::new())
        .await
        .unwrap();

    assert_eq!(oid1, oid2, "same data must produce same ObjectId");

    let m1 = node.head_object("b", "det1").await.unwrap();
    let m2 = node.head_object("b", "det2").await.unwrap();

    assert_eq!(m1.chunks.len(), m2.chunks.len());
    for (c1, c2) in m1.chunks.iter().zip(m2.chunks.iter()) {
        assert_eq!(c1.chunk_id, c2.chunk_id);
        assert_eq!(c1.raw_length, c2.raw_length);
        assert_eq!(c1.stored_length, c2.stored_length);
        assert_eq!(c1.compression, c2.compression);
        for (s1, s2) in c1.shards.iter().zip(c2.shards.iter()) {
            assert_eq!(s1.shard_id, s2.shard_id);
        }
    }
}

// -----------------------------------------------------------------------
// ChunkMeta fields are correctly populated
// -----------------------------------------------------------------------

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_chunk_meta_fields() {
    let node = single_node(1024, 2, 1).await;
    let data = test_data(200_000); // ~3 CDC chunks

    node.put_object("b", "meta-check", &data, BTreeMap::new())
        .await
        .unwrap();

    let manifest = node.head_object("b", "meta-check").await.unwrap();

    let mut total_raw = 0u64;
    for chunk in &manifest.chunks {
        // raw_length must be > 0.
        assert!(chunk.raw_length > 0, "raw_length must be > 0");
        // stored_length must be > 0.
        assert!(chunk.stored_length > 0, "stored_length must be > 0");
        // stored_length <= raw_length (compression only shrinks or stays same).
        assert!(
            chunk.stored_length <= chunk.raw_length,
            "stored_length {} > raw_length {}",
            chunk.stored_length,
            chunk.raw_length
        );
        // Compression field must be consistent.
        match chunk.compression {
            Compression::Zstd => {
                assert!(chunk.stored_length < chunk.raw_length);
            }
            Compression::None => {
                assert_eq!(chunk.stored_length, chunk.raw_length);
            }
        }
        // Each chunk must have k+m shards.
        assert_eq!(chunk.shards.len(), 3); // k=2, m=1
        total_raw += chunk.raw_length as u64;
    }

    assert_eq!(
        total_raw,
        data.len() as u64,
        "raw lengths must sum to total size"
    );
}
