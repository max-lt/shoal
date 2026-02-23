//! In-memory shard storage backend.

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use shoal_types::ShardId;
use tracing::debug;

use crate::error::StoreError;
use crate::traits::{ShardStore, StorageCapacity};

/// In-memory shard store backed by a `RwLock<HashMap>`.
///
/// Useful for testing and for nodes configured to run in memory-only mode.
/// Tracks total bytes stored against a configurable maximum.
/// Used bytes are maintained incrementally via an atomic counter (O(1) per operation).
pub struct MemoryStore {
    shards: RwLock<HashMap<ShardId, Bytes>>,
    max_bytes: u64,
    used_bytes: AtomicU64,
}

impl MemoryStore {
    /// Create a new in-memory store with the given capacity limit.
    pub fn new(max_bytes: u64) -> Self {
        Self {
            shards: RwLock::new(HashMap::new()),
            max_bytes,
            used_bytes: AtomicU64::new(0),
        }
    }

    /// Return a reference to the inner map (for testing purposes).
    #[cfg(test)]
    pub(crate) fn inner(&self) -> &RwLock<HashMap<ShardId, Bytes>> {
        &self.shards
    }
}

#[async_trait::async_trait]
impl ShardStore for MemoryStore {
    async fn put(&self, id: ShardId, data: Bytes) -> Result<(), StoreError> {
        let mut map = self.shards.write().expect("lock poisoned");
        let data_len = data.len() as u64;
        let used = self.used_bytes.load(Ordering::Relaxed);

        // If we're replacing an existing shard, account for freed space.
        let existing_len = map.get(&id).map_or(0, |v| v.len() as u64);
        let net_increase = data_len.saturating_sub(existing_len);

        if used + net_increase > self.max_bytes {
            return Err(StoreError::CapacityExceeded {
                needed: net_increase,
                available: self.max_bytes.saturating_sub(used),
            });
        }

        debug!(%id, size = data.len(), "storing shard in memory");
        map.insert(id, data);
        // Update used bytes: add new size, subtract old size.
        self.used_bytes
            .store(used - existing_len + data_len, Ordering::Relaxed);
        Ok(())
    }

    async fn get(&self, id: ShardId) -> Result<Option<Bytes>, StoreError> {
        let map = self.shards.read().expect("lock poisoned");
        Ok(map.get(&id).cloned())
    }

    async fn delete(&self, id: ShardId) -> Result<(), StoreError> {
        let mut map = self.shards.write().expect("lock poisoned");
        if let Some(removed) = map.remove(&id) {
            self.used_bytes
                .fetch_sub(removed.len() as u64, Ordering::Relaxed);
        }
        debug!(%id, "deleted shard from memory");
        Ok(())
    }

    async fn contains(&self, id: ShardId) -> Result<bool, StoreError> {
        let map = self.shards.read().expect("lock poisoned");
        Ok(map.contains_key(&id))
    }

    async fn list(&self) -> Result<Vec<ShardId>, StoreError> {
        let map = self.shards.read().expect("lock poisoned");
        Ok(map.keys().copied().collect())
    }

    async fn capacity(&self) -> Result<StorageCapacity, StoreError> {
        let used = self.used_bytes.load(Ordering::Relaxed);
        Ok(StorageCapacity {
            total_bytes: self.max_bytes,
            used_bytes: used,
            available_bytes: self.max_bytes.saturating_sub(used),
            inodes_total: 0,
            inodes_free: 0,
        })
    }

    async fn verify(&self, id: ShardId) -> Result<bool, StoreError> {
        let map = self.shards.read().expect("lock poisoned");
        match map.get(&id) {
            Some(data) => {
                let computed = ShardId::from_data(data);
                Ok(computed == id)
            }
            None => Err(StoreError::NotFound(id)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_put_get_roundtrip() {
        let store = MemoryStore::new(1024 * 1024);
        let data = Bytes::from_static(b"hello shard");
        let id = ShardId::from_data(&data);

        store.put(id, data.clone()).await.unwrap();
        let result = store.get(id).await.unwrap();
        assert_eq!(result, Some(data));
    }

    #[tokio::test]
    async fn test_get_nonexistent_returns_none() {
        let store = MemoryStore::new(1024 * 1024);
        let id = ShardId::from_data(b"does not exist");
        let result = store.get(id).await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_delete_then_get_returns_none() {
        let store = MemoryStore::new(1024 * 1024);
        let data = Bytes::from_static(b"to be deleted");
        let id = ShardId::from_data(&data);

        store.put(id, data).await.unwrap();
        store.delete(id).await.unwrap();
        let result = store.get(id).await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_contains_true_false() {
        let store = MemoryStore::new(1024 * 1024);
        let data = Bytes::from_static(b"exists");
        let id = ShardId::from_data(&data);

        assert!(!store.contains(id).await.unwrap());
        store.put(id, data).await.unwrap();
        assert!(store.contains(id).await.unwrap());
    }

    #[tokio::test]
    async fn test_list_returns_all_stored_ids() {
        let store = MemoryStore::new(1024 * 1024);
        let data1 = Bytes::from_static(b"shard one");
        let data2 = Bytes::from_static(b"shard two");
        let data3 = Bytes::from_static(b"shard three");
        let id1 = ShardId::from_data(&data1);
        let id2 = ShardId::from_data(&data2);
        let id3 = ShardId::from_data(&data3);

        store.put(id1, data1).await.unwrap();
        store.put(id2, data2).await.unwrap();
        store.put(id3, data3).await.unwrap();

        let mut listed = store.list().await.unwrap();
        listed.sort();
        let mut expected = vec![id1, id2, id3];
        expected.sort();
        assert_eq!(listed, expected);
    }

    #[tokio::test]
    async fn test_verify_valid_shard() {
        let store = MemoryStore::new(1024 * 1024);
        let data = Bytes::from_static(b"valid shard data");
        let id = ShardId::from_data(&data);

        store.put(id, data).await.unwrap();
        assert!(store.verify(id).await.unwrap());
    }

    #[tokio::test]
    async fn test_verify_corrupted_shard() {
        let store = MemoryStore::new(1024 * 1024);
        let data = Bytes::from_static(b"original data");
        let id = ShardId::from_data(&data);

        store.put(id, data).await.unwrap();

        // Corrupt the internal data directly.
        {
            let mut map = store.inner().write().unwrap();
            map.insert(id, Bytes::from_static(b"corrupted data"));
        }

        assert!(!store.verify(id).await.unwrap());
    }

    #[tokio::test]
    async fn test_verify_nonexistent_returns_error() {
        let store = MemoryStore::new(1024 * 1024);
        let id = ShardId::from_data(b"missing");
        let result = store.verify(id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_capacity_tracking() {
        let store = MemoryStore::new(1024);
        let data = Bytes::from_static(b"some data here"); // 14 bytes

        let cap = store.capacity().await.unwrap();
        assert_eq!(cap.total_bytes, 1024);
        assert_eq!(cap.used_bytes, 0);
        assert_eq!(cap.available_bytes, 1024);

        let id = ShardId::from_data(&data);
        store.put(id, data).await.unwrap();

        let cap = store.capacity().await.unwrap();
        assert_eq!(cap.total_bytes, 1024);
        assert_eq!(cap.used_bytes, 14);
        assert_eq!(cap.available_bytes, 1010);
    }

    #[tokio::test]
    async fn test_capacity_exceeded() {
        let store = MemoryStore::new(10); // tiny store
        let data = Bytes::from_static(b"this is way too large for the store");
        let id = ShardId::from_data(&data);

        let result = store.put(id, data).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            StoreError::CapacityExceeded { .. }
        ));
    }

    #[tokio::test]
    async fn test_capacity_after_delete() {
        let store = MemoryStore::new(1024);
        let data = Bytes::from_static(b"track me"); // 8 bytes
        let id = ShardId::from_data(&data);

        store.put(id, data).await.unwrap();
        assert_eq!(store.capacity().await.unwrap().used_bytes, 8);

        store.delete(id).await.unwrap();
        assert_eq!(store.capacity().await.unwrap().used_bytes, 0);
    }

    #[tokio::test]
    async fn test_put_overwrite_updates_capacity() {
        let store = MemoryStore::new(1024);
        let data_small = Bytes::from_static(b"small");
        let data_big = Bytes::from_static(b"bigger data here");

        // Use a synthetic ID so we can overwrite with different data.
        let fixed_id = ShardId::from([0xAA; 32]);
        store.put(fixed_id, data_small).await.unwrap();
        assert_eq!(store.capacity().await.unwrap().used_bytes, 5);

        store.put(fixed_id, data_big).await.unwrap();
        assert_eq!(store.capacity().await.unwrap().used_bytes, 16);
    }

    // -----------------------------------------------------------------------
    // Concurrent access
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_concurrent_put_different_shards() {
        let store = std::sync::Arc::new(MemoryStore::new(10 * 1024 * 1024));
        let mut handles = Vec::new();

        for i in 0..50u32 {
            let s = store.clone();
            handles.push(tokio::spawn(async move {
                let data = Bytes::from(vec![i as u8; 100]);
                let id = ShardId::from_data(&data);
                s.put(id, data.clone()).await.unwrap();
                let got = s.get(id).await.unwrap();
                assert_eq!(got, Some(data));
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let listed = store.list().await.unwrap();
        assert_eq!(listed.len(), 50);
    }

    #[tokio::test]
    async fn test_concurrent_reads() {
        let store = std::sync::Arc::new(MemoryStore::new(1024 * 1024));
        let data = Bytes::from_static(b"shared shard data");
        let id = ShardId::from_data(&data);
        store.put(id, data.clone()).await.unwrap();

        let mut handles = Vec::new();
        for _ in 0..20 {
            let s = store.clone();
            let expected = data.clone();
            handles.push(tokio::spawn(async move {
                let got = s.get(id).await.unwrap();
                assert_eq!(got, Some(expected));
            }));
        }

        for h in handles {
            h.await.unwrap();
        }
    }

    // -----------------------------------------------------------------------
    // Delete nonexistent shard (should not error)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_delete_nonexistent_shard() {
        let store = MemoryStore::new(1024);
        let id = ShardId::from_data(b"ghost");
        // Should not error.
        store.delete(id).await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Capacity: exact boundary
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_capacity_exact_boundary() {
        let store = MemoryStore::new(10);
        let data = Bytes::from(vec![0u8; 10]);
        let id = ShardId::from_data(&data);

        // Exactly 10 bytes in a 10-byte store should succeed.
        store.put(id, data).await.unwrap();
        assert_eq!(store.capacity().await.unwrap().used_bytes, 10);
        assert_eq!(store.capacity().await.unwrap().available_bytes, 0);

        // Any additional data should fail.
        let extra = Bytes::from_static(b"x");
        let extra_id = ShardId::from_data(&extra);
        let result = store.put(extra_id, extra).await;
        assert!(matches!(
            result.unwrap_err(),
            StoreError::CapacityExceeded { .. }
        ));
    }

    // -----------------------------------------------------------------------
    // Capacity freed on overwrite with smaller data
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_overwrite_with_smaller_data_frees_capacity() {
        let store = MemoryStore::new(1024);
        let fixed_id = ShardId::from([0xBB; 32]);

        let big = Bytes::from(vec![0; 500]);
        store.put(fixed_id, big).await.unwrap();
        assert_eq!(store.capacity().await.unwrap().used_bytes, 500);

        let small = Bytes::from(vec![0; 100]);
        store.put(fixed_id, small).await.unwrap();
        assert_eq!(store.capacity().await.unwrap().used_bytes, 100);
    }

    // -----------------------------------------------------------------------
    // Empty shard (zero bytes)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_put_get_empty_shard() {
        let store = MemoryStore::new(1024);
        let data = Bytes::from_static(b"");
        let id = ShardId::from_data(&data);

        store.put(id, data.clone()).await.unwrap();
        let got = store.get(id).await.unwrap();
        assert_eq!(got, Some(data));
        assert_eq!(store.capacity().await.unwrap().used_bytes, 0);
    }

    // -----------------------------------------------------------------------
    // Verify after put (happy path)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_verify_immediately_after_put() {
        let store = MemoryStore::new(1024 * 1024);
        for i in 0..10u32 {
            let data = Bytes::from(i.to_le_bytes().to_vec());
            let id = ShardId::from_data(&data);
            store.put(id, data).await.unwrap();
            assert!(store.verify(id).await.unwrap());
        }
    }

    // -----------------------------------------------------------------------
    // List empty store
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_list_empty_store() {
        let store = MemoryStore::new(1024);
        let listed = store.list().await.unwrap();
        assert!(listed.is_empty());
    }
}
