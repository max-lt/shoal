//! Chaos testing infrastructure for simulating network faults.
//!
//! Provides [`ChaosController`] (shared fault injection state) and
//! [`ChaosTransport`] (per-node wrapper that applies faults before
//! delegating to the inner transport).

use std::collections::HashSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use shoal_net::{ManifestSyncEntry, NetError, ShoalMessage, Transport};
use shoal_types::{NodeId, ObjectId, ShardId};
use tokio::sync::RwLock;

/// Configuration for chaos fault injection.
#[derive(Debug, Clone)]
pub struct ChaosConfig {
    /// Per-message latency range in milliseconds (min, max).
    pub latency_ms: (u64, u64),
    /// Probability of dropping a message (0.0 = no drops, 1.0 = drop all).
    pub drop_rate: f64,
    /// Deterministic RNG seed for reproducibility.
    pub seed: u64,
    /// Per-IO read latency for the store layer (min, max) ms. (0, 0) = disabled.
    pub store_read_latency_ms: (u64, u64),
    /// Per-IO write latency for the store layer (min, max) ms. (0, 0) = disabled.
    pub store_write_latency_ms: (u64, u64),
}

impl Default for ChaosConfig {
    fn default() -> Self {
        Self {
            latency_ms: (0, 0),
            drop_rate: 0.0,
            seed: 0,
            store_read_latency_ms: (0, 0),
            store_write_latency_ms: (0, 0),
        }
    }
}

/// Delivery statistics from the chaos layer.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub struct ChaosStats {
    /// Messages successfully delivered.
    pub delivered: u64,
    /// Messages intentionally dropped.
    pub dropped: u64,
    /// Messages blocked by partitions.
    pub partitioned: u64,
}

/// Shared chaos state controlling fault injection across all nodes.
pub struct ChaosController {
    config: ChaosConfig,
    rng: Mutex<StdRng>,
    /// Set of directed partitions: (source, target) pairs that are blocked.
    partitions: RwLock<HashSet<(NodeId, NodeId)>>,
    delivered: AtomicU64,
    dropped: AtomicU64,
    partitioned: AtomicU64,
}

impl ChaosController {
    /// Create a new chaos controller from the given config.
    pub fn new(config: ChaosConfig) -> Self {
        let rng = StdRng::seed_from_u64(config.seed);
        Self {
            config,
            rng: Mutex::new(rng),
            partitions: RwLock::new(HashSet::new()),
            delivered: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            partitioned: AtomicU64::new(0),
        }
    }

    /// Block traffic in both directions between `a` and `b`.
    pub async fn partition(&self, a: NodeId, b: NodeId) {
        let mut parts = self.partitions.write().await;
        parts.insert((a, b));
        parts.insert((b, a));
    }

    /// Restore traffic in both directions between `a` and `b`.
    pub async fn heal(&self, a: NodeId, b: NodeId) {
        let mut parts = self.partitions.write().await;
        parts.remove(&(a, b));
        parts.remove(&(b, a));
    }

    /// Return delivery statistics.
    pub fn stats(&self) -> ChaosStats {
        ChaosStats {
            delivered: self.delivered.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            partitioned: self.partitioned.load(Ordering::Relaxed),
        }
    }

    /// Decide whether a message from `source` to `target` should be
    /// delivered, dropped, or blocked by a partition. If delivered,
    /// returns the latency to inject.
    async fn gate(&self, source: NodeId, target: NodeId) -> Result<u64, NetError> {
        // Check partitions.
        if self.partitions.read().await.contains(&(source, target)) {
            self.partitioned.fetch_add(1, Ordering::Relaxed);
            return Err(NetError::Endpoint(format!(
                "chaos: partitioned {source} -> {target}"
            )));
        }

        // Roll for drop.
        if self.config.drop_rate > 0.0 {
            let roll: f64 = self.rng.lock().unwrap().random();

            if roll < self.config.drop_rate {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                return Err(NetError::Endpoint("chaos: message dropped".into()));
            }
        }

        // Compute latency.
        let (min, max) = self.config.latency_ms;
        let latency_ms = if max == 0 {
            0
        } else if min == max {
            min
        } else {
            self.rng.lock().unwrap().random_range(min..=max)
        };

        self.delivered.fetch_add(1, Ordering::Relaxed);
        Ok(latency_ms)
    }
}

/// Per-node transport wrapper that applies chaos faults before delegating
/// to the real (mock) transport.
pub struct ChaosTransport {
    /// This node's identity.
    source: NodeId,
    /// Shared chaos controller.
    controller: std::sync::Arc<ChaosController>,
    /// The underlying transport to delegate to.
    inner: std::sync::Arc<dyn Transport>,
}

impl ChaosTransport {
    /// Create a new chaos-wrapped transport for a given node.
    pub fn new(
        source: NodeId,
        controller: std::sync::Arc<ChaosController>,
        inner: std::sync::Arc<dyn Transport>,
    ) -> Self {
        Self {
            source,
            controller,
            inner,
        }
    }

    /// Extract the target `NodeId` from an `EndpointAddr`.
    fn target_node(addr: &iroh::EndpointAddr) -> NodeId {
        NodeId::from(*addr.id.as_bytes())
    }

    /// Run the chaos gate (partition check, drop check, latency injection).
    async fn apply_chaos(&self, addr: &iroh::EndpointAddr) -> Result<(), NetError> {
        let target = Self::target_node(addr);
        let latency_ms = self.controller.gate(self.source, target).await?;

        if latency_ms > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(latency_ms)).await;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl Transport for ChaosTransport {
    async fn push_shard(
        &self,
        addr: iroh::EndpointAddr,
        shard_id: ShardId,
        data: Bytes,
    ) -> Result<(), NetError> {
        self.apply_chaos(&addr).await?;
        self.inner.push_shard(addr, shard_id, data).await
    }

    async fn pull_shard(
        &self,
        addr: iroh::EndpointAddr,
        shard_id: ShardId,
    ) -> Result<Option<Bytes>, NetError> {
        self.apply_chaos(&addr).await?;
        self.inner.pull_shard(addr, shard_id).await
    }

    async fn send_to(&self, addr: iroh::EndpointAddr, msg: &ShoalMessage) -> Result<(), NetError> {
        self.apply_chaos(&addr).await?;
        self.inner.send_to(addr, msg).await
    }

    async fn pull_manifest(
        &self,
        addr: iroh::EndpointAddr,
        bucket: &str,
        key: &str,
    ) -> Result<Option<Vec<u8>>, NetError> {
        self.apply_chaos(&addr).await?;
        self.inner.pull_manifest(addr, bucket, key).await
    }

    async fn pull_all_manifests(
        &self,
        addr: iroh::EndpointAddr,
    ) -> Result<Vec<ManifestSyncEntry>, NetError> {
        self.apply_chaos(&addr).await?;
        self.inner.pull_all_manifests(addr).await
    }

    async fn pull_log_entries(
        &self,
        addr: iroh::EndpointAddr,
        my_tips: &[[u8; 32]],
    ) -> Result<(Vec<Vec<u8>>, Vec<(ObjectId, Vec<u8>)>), NetError> {
        self.apply_chaos(&addr).await?;
        self.inner.pull_log_entries(addr, my_tips).await
    }
}
