//! A [`ShardStore`] wrapper that adds configurable random IO latency.
//!
//! `SlowStore` wraps any `Arc<dyn ShardStore>` and sleeps for a random
//! duration before each read or write operation. The RNG is seeded for
//! deterministic, reproducible behaviour across test runs.
//!
//! # Example
//!
//! ```ignore
//! let slow = SlowStore::new(inner)
//!     .read_latency(5, 20)    // 5–20 ms per read
//!     .write_latency(10, 30)  // 10–30 ms per write
//!     .seed(42);
//! ```

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use shoal_types::ShardId;

use crate::error::StoreError;
use crate::traits::{ShardStore, StorageCapacity};

/// A [`ShardStore`] wrapper that injects random latency before IO operations.
///
/// Useful for chaos testing to surface race conditions and timing bugs that
/// don't appear with an instant in-memory store.
pub struct SlowStore {
    inner: Arc<dyn ShardStore>,
    read_latency_ms: (u64, u64),
    write_latency_ms: (u64, u64),
    rng: Mutex<StdRng>,
}

impl SlowStore {
    /// Wrap an existing store with zero latency (pass-through) by default.
    pub fn new(inner: Arc<dyn ShardStore>) -> Self {
        Self {
            inner,
            read_latency_ms: (0, 0),
            write_latency_ms: (0, 0),
            rng: Mutex::new(StdRng::seed_from_u64(0)),
        }
    }

    /// Set the read latency range in milliseconds (uniform random).
    pub fn read_latency(mut self, min_ms: u64, max_ms: u64) -> Self {
        self.read_latency_ms = (min_ms, max_ms);
        self
    }

    /// Set the write latency range in milliseconds (uniform random).
    pub fn write_latency(mut self, min_ms: u64, max_ms: u64) -> Self {
        self.write_latency_ms = (min_ms, max_ms);
        self
    }

    /// Set the RNG seed for deterministic behaviour.
    pub fn seed(self, seed: u64) -> Self {
        Self {
            rng: Mutex::new(StdRng::seed_from_u64(seed)),
            ..self
        }
    }

    /// Sleep for a random duration in `[min, max]` milliseconds.
    async fn delay(&self, range: (u64, u64)) {
        let (min, max) = range;

        if max == 0 {
            return;
        }

        let ms = if min == max {
            min
        } else {
            self.rng.lock().unwrap().random_range(min..=max)
        };

        if ms > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(ms)).await;
        }
    }
}

#[async_trait::async_trait]
impl ShardStore for SlowStore {
    async fn put(&self, id: ShardId, data: Bytes) -> Result<(), StoreError> {
        self.delay(self.write_latency_ms).await;
        self.inner.put(id, data).await
    }

    async fn get(&self, id: ShardId) -> Result<Option<Bytes>, StoreError> {
        self.delay(self.read_latency_ms).await;
        self.inner.get(id).await
    }

    async fn delete(&self, id: ShardId) -> Result<(), StoreError> {
        self.delay(self.write_latency_ms).await;
        self.inner.delete(id).await
    }

    async fn contains(&self, id: ShardId) -> Result<bool, StoreError> {
        self.delay(self.read_latency_ms).await;
        self.inner.contains(id).await
    }

    async fn list(&self) -> Result<Vec<ShardId>, StoreError> {
        self.inner.list().await
    }

    async fn capacity(&self) -> Result<StorageCapacity, StoreError> {
        self.inner.capacity().await
    }

    async fn verify(&self, id: ShardId) -> Result<bool, StoreError> {
        self.delay(self.read_latency_ms).await;
        self.inner.verify(id).await
    }
}
