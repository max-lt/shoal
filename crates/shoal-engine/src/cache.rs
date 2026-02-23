//! LRU shard cache for non-owned shards pulled during reads.
//!
//! Owned shards (placed by the ring on this node) live in the main
//! [`ShardStore`] and are never evicted. Shards pulled from remote
//! nodes during reads are placed here instead, bounded by a
//! configurable `max_bytes`. When the cache is full, the least
//! recently accessed entry is evicted.
//!
//! [`ShardStore`]: shoal_store::ShardStore

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use bytes::Bytes;
use shoal_types::ShardId;
use tracing::debug;

/// Thread-safe LRU cache for shard data.
///
/// All operations acquire a single lock — this is fine because the
/// critical section is pure in-memory work (HashMap lookup / VecDeque
/// manipulation) with no I/O.
pub struct ShardCache {
    max_bytes: u64,
    inner: Mutex<CacheInner>,
}

struct CacheInner {
    /// Access order: front = oldest (eviction candidate), back = newest.
    order: VecDeque<ShardId>,
    /// Shard data.
    data: HashMap<ShardId, Bytes>,
    /// Current total size of cached data in bytes.
    used_bytes: u64,
}

impl ShardCache {
    /// Create a new cache with the given byte limit.
    ///
    /// A `max_bytes` of 0 disables caching entirely.
    pub fn new(max_bytes: u64) -> Self {
        Self {
            max_bytes,
            inner: Mutex::new(CacheInner {
                order: VecDeque::new(),
                data: HashMap::new(),
                used_bytes: 0,
            }),
        }
    }

    /// Insert a shard into the cache, evicting old entries if needed.
    ///
    /// If the shard is larger than `max_bytes`, it is not cached.
    pub fn put(&self, id: ShardId, data: Bytes) {
        let data_len = data.len() as u64;
        if data_len > self.max_bytes {
            return;
        }

        let mut inner = self.inner.lock().expect("cache lock poisoned");

        // If already cached, remove old entry first.
        if let Some(old) = inner.data.remove(&id) {
            inner.used_bytes -= old.len() as u64;
            inner.order.retain(|s| *s != id);
        }

        // Evict until there is room.
        while inner.used_bytes + data_len > self.max_bytes {
            let Some(evict_id) = inner.order.pop_front() else {
                break;
            };
            if let Some(evicted) = inner.data.remove(&evict_id) {
                inner.used_bytes -= evicted.len() as u64;
                debug!(%evict_id, "evicted cached shard");
            }
        }

        inner.used_bytes += data_len;
        inner.data.insert(id, data);
        inner.order.push_back(id);
    }

    /// Look up a cached shard and promote it to most-recently-used.
    pub fn get(&self, id: &ShardId) -> Option<Bytes> {
        let mut inner = self.inner.lock().expect("cache lock poisoned");
        let data = inner.data.get(id)?.clone();

        // Promote: remove from current position, push to back.
        inner.order.retain(|s| s != id);
        inner.order.push_back(*id);

        Some(data)
    }

    /// Current number of cached entries.
    pub fn len(&self) -> usize {
        self.inner.lock().expect("cache lock poisoned").data.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.inner
            .lock()
            .expect("cache lock poisoned")
            .data
            .is_empty()
    }

    /// Current bytes used by cached data.
    pub fn used_bytes(&self) -> u64 {
        self.inner.lock().expect("cache lock poisoned").used_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard(data: &[u8]) -> (ShardId, Bytes) {
        let id = ShardId::from_data(data);
        (id, Bytes::from(data.to_vec()))
    }

    #[test]
    fn test_put_get_roundtrip() {
        let cache = ShardCache::new(1024);
        let (id, data) = shard(b"hello");

        cache.put(id, data.clone());
        assert_eq!(cache.get(&id), Some(data));
    }

    #[test]
    fn test_get_miss_returns_none() {
        let cache = ShardCache::new(1024);
        let id = ShardId::from_data(b"missing");
        assert_eq!(cache.get(&id), None);
    }

    #[test]
    fn test_eviction_when_full() {
        // Cache fits exactly 10 bytes.
        let cache = ShardCache::new(10);

        let (id1, d1) = shard(b"aaaa"); // 4 bytes
        let (id2, d2) = shard(b"bbbb"); // 4 bytes
        let (id3, d3) = shard(b"cccc"); // 4 bytes

        cache.put(id1, d1.clone());
        cache.put(id2, d2.clone());
        // 8 bytes used. Adding 4 more = 12 > 10, so id1 gets evicted.
        cache.put(id3, d3.clone());

        assert!(cache.get(&id1).is_none(), "id1 should be evicted");
        assert_eq!(cache.get(&id2), Some(d2));
        assert_eq!(cache.get(&id3), Some(d3));
    }

    #[test]
    fn test_lru_order_respected() {
        let cache = ShardCache::new(12);

        let (id1, d1) = shard(b"aaaa"); // 4 bytes
        let (id2, d2) = shard(b"bbbb"); // 4 bytes
        let (id3, d3) = shard(b"cccc"); // 4 bytes

        cache.put(id1, d1.clone());
        cache.put(id2, d2);
        cache.put(id3, d3);
        // 12 bytes used. Access id1 to promote it.
        let _ = cache.get(&id1);

        // Now add id4 — should evict id2 (oldest), not id1 (just accessed).
        let (id4, d4) = shard(b"dddd");
        cache.put(id4, d4.clone());

        assert_eq!(
            cache.get(&id1),
            Some(d1),
            "id1 was promoted, should survive"
        );
        assert!(cache.get(&id2).is_none(), "id2 should be evicted (oldest)");
        assert_eq!(cache.get(&id4), Some(d4));
    }

    #[test]
    fn test_oversize_item_not_cached() {
        let cache = ShardCache::new(5);
        let (id, data) = shard(b"way too big for this cache");

        cache.put(id, data);
        assert!(cache.get(&id).is_none());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_zero_capacity_disables_cache() {
        let cache = ShardCache::new(0);
        let (id, data) = shard(b"x");

        cache.put(id, data);
        assert!(cache.get(&id).is_none());
    }

    #[test]
    fn test_duplicate_put_updates_data() {
        let cache = ShardCache::new(1024);
        let id = ShardId::from([0xAA; 32]);

        cache.put(id, Bytes::from_static(b"old"));
        cache.put(id, Bytes::from_static(b"new"));

        assert_eq!(cache.get(&id), Some(Bytes::from_static(b"new")));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_used_bytes_tracked() {
        let cache = ShardCache::new(1024);
        let (id1, d1) = shard(b"hello"); // 5 bytes
        let (id2, d2) = shard(b"world!"); // 6 bytes

        assert_eq!(cache.used_bytes(), 0);
        cache.put(id1, d1);
        assert_eq!(cache.used_bytes(), 5);
        cache.put(id2, d2);
        assert_eq!(cache.used_bytes(), 11);
    }
}
