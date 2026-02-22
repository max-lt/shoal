//! In-memory shard storage backend.

use std::collections::HashMap;
use std::sync::RwLock;

use shoal_types::ShardId;
use tracing::debug;

use crate::error::StoreError;
use crate::traits::{ShardStore, StorageCapacity};

/// In-memory shard store backed by a `RwLock<HashMap>`.
///
/// Useful for testing and for nodes configured to run in memory-only mode.
/// Tracks total bytes stored against a configurable maximum.
pub struct MemoryStore {
    shards: RwLock<HashMap<ShardId, Vec<u8>>>,
    max_bytes: u64,
}

impl MemoryStore {
    /// Create a new in-memory store with the given capacity limit.
    pub fn new(max_bytes: u64) -> Self {
        Self {
            shards: RwLock::new(HashMap::new()),
            max_bytes,
        }
    }

    /// Return a reference to the inner map (for testing purposes).
    #[cfg(test)]
    pub(crate) fn inner(&self) -> &RwLock<HashMap<ShardId, Vec<u8>>> {
        &self.shards
    }

    fn used_bytes_unlocked(map: &HashMap<ShardId, Vec<u8>>) -> u64 {
        map.values().map(|v| v.len() as u64).sum()
    }
}

#[async_trait::async_trait]
impl ShardStore for MemoryStore {
    async fn put(&self, id: ShardId, data: &[u8]) -> Result<(), StoreError> {
        let mut map = self.shards.write().expect("lock poisoned");
        let used = Self::used_bytes_unlocked(&map);
        let data_len = data.len() as u64;

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
        map.insert(id, data.to_vec());
        Ok(())
    }

    async fn get(&self, id: ShardId) -> Result<Option<Vec<u8>>, StoreError> {
        let map = self.shards.read().expect("lock poisoned");
        Ok(map.get(&id).cloned())
    }

    async fn delete(&self, id: ShardId) -> Result<(), StoreError> {
        let mut map = self.shards.write().expect("lock poisoned");
        map.remove(&id);
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
        let map = self.shards.read().expect("lock poisoned");
        let used = Self::used_bytes_unlocked(&map);
        Ok(StorageCapacity {
            total_bytes: self.max_bytes,
            used_bytes: used,
            available_bytes: self.max_bytes.saturating_sub(used),
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
        let data = b"hello shard";
        let id = ShardId::from_data(data);

        store.put(id, data).await.unwrap();
        let result = store.get(id).await.unwrap();
        assert_eq!(result, Some(data.to_vec()));
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
        let data = b"to be deleted";
        let id = ShardId::from_data(data);

        store.put(id, data).await.unwrap();
        store.delete(id).await.unwrap();
        let result = store.get(id).await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_contains_true_false() {
        let store = MemoryStore::new(1024 * 1024);
        let data = b"exists";
        let id = ShardId::from_data(data);

        assert!(!store.contains(id).await.unwrap());
        store.put(id, data).await.unwrap();
        assert!(store.contains(id).await.unwrap());
    }

    #[tokio::test]
    async fn test_list_returns_all_stored_ids() {
        let store = MemoryStore::new(1024 * 1024);
        let data1 = b"shard one";
        let data2 = b"shard two";
        let data3 = b"shard three";
        let id1 = ShardId::from_data(data1);
        let id2 = ShardId::from_data(data2);
        let id3 = ShardId::from_data(data3);

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
        let data = b"valid shard data";
        let id = ShardId::from_data(data);

        store.put(id, data).await.unwrap();
        assert!(store.verify(id).await.unwrap());
    }

    #[tokio::test]
    async fn test_verify_corrupted_shard() {
        let store = MemoryStore::new(1024 * 1024);
        let data = b"original data";
        let id = ShardId::from_data(data);

        store.put(id, data).await.unwrap();

        // Corrupt the internal data directly.
        {
            let mut map = store.inner().write().unwrap();
            map.insert(id, b"corrupted data".to_vec());
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
        let data = b"some data here"; // 14 bytes

        let cap = store.capacity().await.unwrap();
        assert_eq!(cap.total_bytes, 1024);
        assert_eq!(cap.used_bytes, 0);
        assert_eq!(cap.available_bytes, 1024);

        let id = ShardId::from_data(data);
        store.put(id, data).await.unwrap();

        let cap = store.capacity().await.unwrap();
        assert_eq!(cap.total_bytes, 1024);
        assert_eq!(cap.used_bytes, 14);
        assert_eq!(cap.available_bytes, 1010);
    }

    #[tokio::test]
    async fn test_capacity_exceeded() {
        let store = MemoryStore::new(10); // tiny store
        let data = b"this is way too large for the store";
        let id = ShardId::from_data(data);

        let result = store.put(id, data).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            StoreError::CapacityExceeded { .. }
        ));
    }
}
