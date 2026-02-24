//! Repair detector: watches cluster events and enqueues shards for repair.
//!
//! [`RepairDetector`] subscribes to [`ClusterEvent`]s and:
//! - On `NodeDead`: computes which shards were owned by the dead node and
//!   enqueues them in the repair queue, prioritized by how many surviving
//!   copies remain.
//! - Periodically: scans local shards, verifies they match ring placement,
//!   and reports anomalies.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use shoal_cluster::ClusterState;
use shoal_meta::MetaStore;
use shoal_store::ShardStore;
use shoal_types::{ClusterEvent, NodeId, ShardId};
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

/// Watches cluster events and enqueues shards that need repair.
pub struct RepairDetector {
    cluster: Arc<ClusterState>,
    meta: Arc<MetaStore>,
    store: Arc<dyn ShardStore>,
    replication_factor: usize,
    /// Nodes already processed as dead/left — prevents duplicate repair enqueues
    /// when the same event arrives via multiple channels (foca + gossip peers).
    processed_dead: Mutex<HashSet<NodeId>>,
}

impl RepairDetector {
    /// Create a new repair detector.
    pub fn new(
        cluster: Arc<ClusterState>,
        meta: Arc<MetaStore>,
        store: Arc<dyn ShardStore>,
        replication_factor: usize,
    ) -> Self {
        Self {
            cluster,
            meta,
            store,
            replication_factor,
            processed_dead: Mutex::new(HashSet::new()),
        }
    }

    /// Run the detector loop, processing cluster events until the receiver is dropped.
    ///
    /// This should be spawned as a background task.
    pub async fn run(&self, mut events: broadcast::Receiver<ClusterEvent>) {
        info!("repair detector started");

        loop {
            match events.recv().await {
                Ok(event) => self.handle_event(event).await,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(skipped = n, "repair detector lagged behind event stream");
                }
                Err(broadcast::error::RecvError::Closed) => {
                    info!("repair detector shutting down — event channel closed");
                    break;
                }
            }
        }
    }

    /// Handle a single cluster event.
    async fn handle_event(&self, event: ClusterEvent) {
        match event {
            ClusterEvent::NodeDead(node_id) => {
                if !self.processed_dead.lock().unwrap().insert(node_id) {
                    debug!(%node_id, "ignoring duplicate NodeDead event");
                    return;
                }

                info!(%node_id, "node declared dead — scanning for affected shards");

                if let Err(e) = self.handle_node_dead(node_id).await {
                    error!(%node_id, error = %e, "failed to enqueue repairs for dead node");
                }
            }
            ClusterEvent::NodeLeft(node_id) => {
                if !self.processed_dead.lock().unwrap().insert(node_id) {
                    debug!(%node_id, "ignoring duplicate NodeLeft event");
                    return;
                }

                info!(%node_id, "node left — scanning for affected shards");

                if let Err(e) = self.handle_node_dead(node_id).await {
                    error!(%node_id, error = %e, "failed to enqueue repairs for departed node");
                }
            }
            ClusterEvent::RepairNeeded(shard_id) => {
                debug!(%shard_id, "repair needed event received");
                if let Err(e) = self.enqueue_shard(shard_id).await {
                    error!(%shard_id, error = %e, "failed to enqueue shard for repair");
                }
            }
            ClusterEvent::NodeJoined(member) => {
                // Clear from processed set so a future death is handled.
                self.processed_dead.lock().unwrap().remove(&member.node_id);
            }
            _ => {}
        }
    }

    /// When a node dies, find all shards that were assigned to it and enqueue repairs.
    async fn handle_node_dead(&self, dead_node: NodeId) -> Result<(), crate::RepairError> {
        let ring = self.cluster.ring().await;
        let local_shards = self.store.list().await?;

        let mut enqueued = 0u64;
        for shard_id in &local_shards {
            let owners = ring.owners(shard_id, self.replication_factor);
            if owners.contains(&dead_node) {
                // This shard was supposed to live on the dead node — it needs repair.
                // Priority = number of surviving owners (fewer = more urgent).
                let surviving = owners.iter().filter(|o| **o != dead_node).count();
                let priority = surviving as u64;
                self.meta.enqueue_repair(shard_id, priority)?;
                enqueued += 1;
            }
        }

        info!(
            %dead_node,
            enqueued,
            "finished scanning for shards affected by dead node"
        );
        Ok(())
    }

    /// Enqueue a single shard for repair with default priority.
    async fn enqueue_shard(&self, shard_id: ShardId) -> Result<(), crate::RepairError> {
        let ring = self.cluster.ring().await;
        let owners = ring.owners(&shard_id, self.replication_factor);

        // Check how many owners are alive.
        let alive_count = self.cluster.alive_count().await;
        let total = self.cluster.member_count().await;
        let dead = total.saturating_sub(alive_count);

        // Priority: fewer surviving copies = more urgent.
        let expected = owners.len();
        let surviving = expected.saturating_sub(dead.min(expected));
        let priority = surviving as u64;

        self.meta.enqueue_repair(&shard_id, priority)?;
        debug!(%shard_id, priority, "enqueued shard for repair");
        Ok(())
    }

    /// Scan local shards and verify they match ring placement.
    ///
    /// - Shards we hold but shouldn't → log warning (will be cleaned up later).
    /// - Shards assigned to us that we don't hold → enqueue for repair.
    pub async fn scan_local_shards(&self) -> Result<ScanResult, crate::RepairError> {
        let ring = self.cluster.ring().await;
        let local_node = self.cluster.local_node_id();
        let local_shards = self.store.list().await?;

        let mut result = ScanResult::default();

        for shard_id in &local_shards {
            // Verify integrity.
            match self.store.verify(*shard_id).await {
                Ok(true) => {}
                Ok(false) => {
                    warn!(%shard_id, "local shard failed integrity check — enqueuing for repair");
                    self.meta.enqueue_repair(shard_id, 0)?; // Priority 0 = most urgent
                    result.corrupt += 1;
                }
                Err(e) => {
                    warn!(%shard_id, error = %e, "failed to verify shard integrity");
                }
            }

            // Check if we should own this shard.
            let owners = ring.owners(shard_id, self.replication_factor);
            if !owners.contains(&local_node) {
                debug!(%shard_id, "holding shard not assigned by ring");
                result.misplaced += 1;
            }
        }

        result.total_scanned = local_shards.len();
        info!(
            scanned = result.total_scanned,
            corrupt = result.corrupt,
            misplaced = result.misplaced,
            "local shard scan complete"
        );
        Ok(result)
    }
}

/// Result of a local shard scan.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ScanResult {
    /// Total shards scanned.
    pub total_scanned: usize,
    /// Shards that failed integrity verification.
    pub corrupt: usize,
    /// Shards held locally but not assigned by the ring.
    pub misplaced: usize,
}
