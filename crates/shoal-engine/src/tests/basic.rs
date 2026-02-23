//! Basic single-node CRUD, metadata, determinism, and listing tests.

use std::collections::BTreeMap;

use crate::error::EngineError;

use super::helpers::{single_node, test_data};

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
// Small objects
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

    // Same data, same metadata -> same ObjectId.
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
// Manifest version field
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
