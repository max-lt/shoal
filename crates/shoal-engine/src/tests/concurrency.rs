//! Concurrent put/get tests (race conditions, shared state).

use std::collections::BTreeMap;
use std::sync::Arc;

use super::helpers::{single_node, test_data};

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
