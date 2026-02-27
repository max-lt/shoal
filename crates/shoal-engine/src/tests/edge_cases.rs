//! Edge cases: special characters, boundary chunk sizes, empty objects, large objects.

use std::collections::BTreeMap;

use super::helpers::{single_node, test_data};

// -----------------------------------------------------------------------
// Empty data edge case
// -----------------------------------------------------------------------

#[tokio::test]
#[ntest::timeout(10000)]
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
// Special characters in bucket/key names
// -----------------------------------------------------------------------

#[tokio::test]
#[ntest::timeout(10000)]
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
#[ntest::timeout(10000)]
async fn test_unicode_bucket_and_key() {
    let node = single_node(1024, 2, 1).await;
    let data = b"unicode test".to_vec();

    node.put_object(
        "donn\u{e9}es",
        "fichier/\u{e0}/stocker",
        &data,
        BTreeMap::new(),
    )
    .await
    .unwrap();
    let (got, _) = node
        .get_object("donn\u{e9}es", "fichier/\u{e0}/stocker")
        .await
        .unwrap();
    assert_eq!(got, data);
}

#[tokio::test]
#[ntest::timeout(10000)]
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
// Boundary chunk sizes (CDC: min=16KB, avg=64KB, max=256KB)
// -----------------------------------------------------------------------

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_data_below_cdc_min() {
    // Data smaller than CDC min_size (chunk_size/16 = 64) → single chunk.
    let node = single_node(1024, 2, 1).await;
    let data = test_data(50); // < 64 byte min

    node.put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    let (got, manifest) = node.get_object("b", "k").await.unwrap();
    assert_eq!(got, data);
    assert_eq!(manifest.chunks.len(), 1);
    assert_eq!(manifest.chunks[0].raw_length, 50);
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_data_at_cdc_min_boundary() {
    // Data exactly at CDC min_size (chunk_size/16 = 64) → single chunk.
    let node = single_node(1024, 2, 1).await;
    let data = test_data(64);

    node.put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    let (got, manifest) = node.get_object("b", "k").await.unwrap();
    assert_eq!(got, data);
    // At exactly min_size, CDC produces 1 chunk.
    assert_eq!(manifest.chunks.len(), 1);
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_data_above_cdc_max() {
    // Data larger than CDC max_size (chunk_size = 1024) → multiple chunks.
    let node = single_node(1024, 2, 1).await;
    let data = test_data(5000); // > 1024 max

    node.put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    let (got, manifest) = node.get_object("b", "k").await.unwrap();
    assert_eq!(got, data);
    assert!(manifest.chunks.len() >= 2, "should produce multiple chunks");
}

// -----------------------------------------------------------------------
// Large objects
// -----------------------------------------------------------------------

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_large_object() {
    let node = single_node(1024, 2, 1).await;
    let data = test_data(100_000);

    node.put_object("b", "large", &data, BTreeMap::new())
        .await
        .unwrap();

    let (got, manifest) = node.get_object("b", "large").await.unwrap();
    assert_eq!(got, data);
    assert_eq!(manifest.total_size, 100_000);
    // CDC produces variable-size chunks; just verify data round-trips.
    assert!(!manifest.chunks.is_empty());
}

#[tokio::test]
#[ntest::timeout(10000)]
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
    // CDC chunks — verify they exist and data round-trips.
    assert!(!manifest.chunks.is_empty());
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_1mb_object() {
    let node = single_node(8192, 4, 2).await;
    let data = test_data(1_000_000);

    node.put_object("b", "1mb", &data, BTreeMap::new())
        .await
        .unwrap();
    let (got, manifest) = node.get_object("b", "1mb").await.unwrap();
    assert_eq!(got.len(), 1_000_000);
    assert_eq!(got, data);
    // 1MB with CDC avg=64KB → expect ~15 chunks (varies).
    assert!(
        manifest.chunks.len() > 1,
        "1MB should produce multiple chunks"
    );
}

// -----------------------------------------------------------------------
// Many objects in one bucket
// -----------------------------------------------------------------------

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_many_objects_same_bucket() {
    let node = single_node(1024, 2, 1).await;

    for i in 0..100u32 {
        let data = test_data(200 + i as usize);
        node.put_object("b", &format!("obj-{i:04}"), &data, BTreeMap::new())
            .await
            .unwrap();
    }

    let keys = node.list_objects("b", "").await.unwrap();
    assert_eq!(keys.len(), 100);

    // Spot check a few.
    for i in [0, 42, 99] {
        let expected = test_data(200 + i as usize);
        let (got, _) = node.get_object("b", &format!("obj-{i:04}")).await.unwrap();
        assert_eq!(got, expected);
    }
}
