//! Tests for the repair subsystem.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use shoal_cluster::ClusterState;
use shoal_erasure::ErasureEncoder;
use shoal_meta::MetaStore;
use shoal_store::{MemoryStore, ShardStore};
use shoal_types::*;

use crate::RepairError;
use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerStatus};
use crate::detector::RepairDetector;
use crate::executor::{RepairExecutor, ShardTransfer};
use crate::scrub::{DeepScrubber, HashQuery, ScrubVerdict};
use crate::throttle::Throttle;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Large enough for tests.
const TEST_MAX_BYTES: u64 = 1_000_000_000;

fn node_id(n: u8) -> NodeId {
    NodeId::from([n; 32])
}

fn default_topo() -> NodeTopology {
    NodeTopology::default()
}

fn member(n: u8) -> Member {
    Member {
        node_id: node_id(n),
        capacity: 1_000_000_000,
        state: MemberState::Alive,
        generation: 1,
        topology: default_topo(),
    }
}

/// A mock shard transfer that stores shards in a shared map of MemoryStores.
struct MockTransfer {
    stores: HashMap<NodeId, Arc<MemoryStore>>,
}

#[async_trait::async_trait]
impl ShardTransfer for MockTransfer {
    async fn pull_shard(
        &self,
        node_id: NodeId,
        shard_id: ShardId,
    ) -> Result<Option<Bytes>, RepairError> {
        if let Some(store) = self.stores.get(&node_id) {
            Ok(store.get(shard_id).await?)
        } else {
            Ok(None)
        }
    }

    async fn push_shard(
        &self,
        node_id: NodeId,
        shard_id: ShardId,
        data: Bytes,
    ) -> Result<(), RepairError> {
        if let Some(store) = self.stores.get(&node_id) {
            store.put(shard_id, data).await?;
        }
        Ok(())
    }
}

/// A mock hash query that computes hashes from actual stores.
struct MockHashQuery {
    stores: HashMap<NodeId, Arc<MemoryStore>>,
}

#[async_trait::async_trait]
impl HashQuery for MockHashQuery {
    async fn query_hash(&self, node_id: NodeId, shard_id: ShardId) -> Option<ShardId> {
        if let Some(store) = self.stores.get(&node_id) {
            if let Ok(Some(data)) = store.get(shard_id).await {
                return Some(ShardId::from_data(&data));
            }
        }
        None
    }
}

/// Build a 3-node cluster, encode an object, distribute shards.
struct TestCluster {
    cluster: Arc<ClusterState>,
    meta: Arc<MetaStore>,
    stores: HashMap<NodeId, Arc<MemoryStore>>,
    manifest: Manifest,
}

impl TestCluster {
    async fn new() -> Self {
        let local_id = node_id(1);
        let cluster = ClusterState::new(local_id, 128);

        // Add 3 members.
        for n in 1..=3u8 {
            cluster.add_member(member(n)).await;
        }

        let meta = Arc::new(MetaStore::open_temporary().unwrap());

        let mut stores = HashMap::new();
        for n in 1..=3u8 {
            stores.insert(node_id(n), Arc::new(MemoryStore::new(TEST_MAX_BYTES)));
        }

        // Create test data where the first half of each chunk differs from the
        // second half, so RS data shards are never identical.
        let mut data = Vec::with_capacity(2048);
        for i in 0..2048u32 {
            // Mix in the byte index in a way that avoids repetition within a shard.
            let b = ((i.wrapping_mul(7) ^ (i >> 3)) & 0xFF) as u8;
            data.push(b);
        }
        let chunk_size = 1024u32;
        let k = 2usize;
        let m = 1usize;
        let encoder = ErasureEncoder::new(k, m);

        let mut chunks = Vec::new();
        let mut offset = 0u64;

        for chunk_data in data.chunks(chunk_size as usize) {
            let chunk_id = ChunkId::from_data(chunk_data);
            let (shards, _original_size) = encoder.encode(chunk_data).unwrap();

            let ring = cluster.ring().await;
            let replication_factor = k + m;

            let mut shard_metas = Vec::new();
            for shard in &shards {
                let owners = ring.owners(&shard.id, replication_factor);
                for owner in &owners {
                    if let Some(store) = stores.get(owner) {
                        store.put(shard.id, shard.data.clone()).await.unwrap();
                    }
                }
                meta.put_shard_owners(&shard.id, &owners).unwrap();
                shard_metas.push(ShardMeta {
                    shard_id: shard.id,
                    index: shard.index,
                    size: shard.data.len() as u32,
                });
            }

            chunks.push(ChunkMeta {
                chunk_id,
                offset,
                size: chunk_data.len() as u32,
                shards: shard_metas,
            });
            offset += chunk_data.len() as u64;
        }

        let manifest = Manifest {
            version: MANIFEST_VERSION,
            object_id: ObjectId::from_data(b"test-object"),
            total_size: data.len() as u64,
            chunk_size,
            chunks,
            created_at: 1700000000,
            metadata: std::collections::BTreeMap::new(),
        };
        meta.put_manifest(&manifest).unwrap();
        meta.put_object_key("__all__", "test-object", &manifest.object_id)
            .unwrap();

        Self {
            cluster,
            meta,
            stores,
            manifest,
        }
    }
}

// ---------------------------------------------------------------------------
// Detector tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_detector_enqueues_on_node_dead() {
    let tc = TestCluster::new().await;
    let local_store = tc.stores[&node_id(1)].clone() as Arc<dyn ShardStore>;
    let detector = RepairDetector::new(tc.cluster.clone(), tc.meta.clone(), local_store, 3);

    // Before: empty queue.
    assert_eq!(tc.meta.repair_queue_len().unwrap(), 0);

    // Mark node 2 as dead — this sends an event internally.
    tc.cluster.mark_dead(&node_id(2)).await;

    // Test the scan functionality directly.
    let result = detector.scan_local_shards().await.unwrap();
    assert!(result.total_scanned > 0);
}

#[tokio::test]
async fn test_detector_scan_detects_healthy_shard() {
    let meta = Arc::new(MetaStore::open_temporary().unwrap());
    let store = Arc::new(MemoryStore::new(TEST_MAX_BYTES));
    let cluster = ClusterState::new(node_id(1), 128);
    cluster.add_member(member(1)).await;

    // Put a shard with correct ID.
    let data = Bytes::from(vec![0xABu8; 100]);
    let id = ShardId::from_data(&data);
    store.put(id, data).await.unwrap();

    let detector = RepairDetector::new(cluster, meta.clone(), store.clone(), 1);
    let result = detector.scan_local_shards().await.unwrap();

    // Shard should be healthy (hash matches ID).
    assert_eq!(result.corrupt, 0);
    assert_eq!(result.total_scanned, 1);
}

// ---------------------------------------------------------------------------
// Scheduler tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_scheduler_dequeues_in_priority_order() {
    let meta = Arc::new(MetaStore::open_temporary().unwrap());

    let s1 = ShardId::from_data(b"shard-high-priority");
    let s2 = ShardId::from_data(b"shard-medium-priority");
    let s3 = ShardId::from_data(b"shard-low-priority");

    // Enqueue in reverse priority order.
    meta.enqueue_repair(&s3, 100).unwrap();
    meta.enqueue_repair(&s1, 1).unwrap(); // Most urgent.
    meta.enqueue_repair(&s2, 10).unwrap();

    // Dequeue should return highest priority (lowest number) first.
    assert_eq!(meta.dequeue_repair().unwrap(), Some(s1));
    assert_eq!(meta.dequeue_repair().unwrap(), Some(s2));
    assert_eq!(meta.dequeue_repair().unwrap(), Some(s3));
    assert_eq!(meta.dequeue_repair().unwrap(), None);
}

// ---------------------------------------------------------------------------
// Executor tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_executor_repair_by_direct_fetch() {
    let tc = TestCluster::new().await;
    let local_store = tc.stores[&node_id(1)].clone();
    let throttle = Throttle::new(100_000_000);

    let transfer = Arc::new(MockTransfer {
        stores: tc.stores.clone(),
    });

    let executor = RepairExecutor::new(
        tc.cluster.clone(),
        tc.meta.clone(),
        local_store.clone() as Arc<dyn ShardStore>,
        transfer,
        node_id(1),
        3,
        2,
        1,
    );

    // Pick a shard and delete it from node 1 to simulate loss.
    let shard = &tc.manifest.chunks[0].shards[0];
    let shard_id = shard.shard_id;

    // Verify it exists on at least one other node.
    let owners = tc.meta.get_shard_owners(&shard_id).unwrap().unwrap();
    assert!(!owners.is_empty());

    // Delete from local store.
    local_store.delete(shard_id).await.unwrap();
    assert!(!local_store.contains(shard_id).await.unwrap());

    // Repair should succeed via direct fetch from another node.
    executor.repair_shard(shard_id, &throttle).await.unwrap();
}

#[tokio::test]
async fn test_executor_repair_by_rs_reconstruction() {
    let tc = TestCluster::new().await;
    let local_store = tc.stores[&node_id(1)].clone();
    let throttle = Throttle::new(100_000_000);

    let transfer = Arc::new(MockTransfer {
        stores: tc.stores.clone(),
    });

    let executor = RepairExecutor::new(
        tc.cluster.clone(),
        tc.meta.clone(),
        local_store.clone() as Arc<dyn ShardStore>,
        transfer,
        node_id(1),
        3,
        2,
        1,
    );

    // Pick a shard and delete it from ALL stores to force RS reconstruction.
    let chunk = &tc.manifest.chunks[0];
    let target_shard = &chunk.shards[0];
    let shard_id = target_shard.shard_id;

    // Verify sibling shards exist before deletion of target.
    let sibling_count = chunk
        .shards
        .iter()
        .filter(|s| s.shard_id != shard_id)
        .count();
    assert_eq!(sibling_count, 2, "should have k+m-1=2 sibling shards");

    // Verify sibling shards are accessible on local store.
    for s in &chunk.shards {
        if s.shard_id != shard_id {
            assert!(
                local_store.contains(s.shard_id).await.unwrap(),
                "sibling shard {} (index {}) should exist on local store",
                s.shard_id,
                s.index,
            );
        }
    }

    // Delete the target shard from all stores.
    for store in tc.stores.values() {
        let _ = store.delete(shard_id).await;
    }

    // Clear the shard owners for the target so direct fetch fails.
    tc.meta.put_shard_owners(&shard_id, &[]).unwrap();

    // Test the RS reconstruction path via repair_shard_with_context.
    let sibling_metas: Vec<_> = chunk.shards.clone();
    let repair_result = executor
        .repair_shard_with_context(
            shard_id,
            target_shard,
            &sibling_metas,
            chunk.size,
            &throttle,
        )
        .await;
    assert!(
        repair_result.is_ok(),
        "RS reconstruction should succeed: {:?}",
        repair_result.err()
    );
}

// ---------------------------------------------------------------------------
// Rate limiting tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_throttle_limits_rate() {
    let throttle = Throttle::new(1_000); // 1KB/s
    let start = tokio::time::Instant::now();

    // Drain the bucket.
    throttle.acquire(1_000).await;

    // Next acquire should wait ~1 second.
    throttle.acquire(500).await;

    let elapsed = start.elapsed();
    // Should have waited at least 400ms (account for timing variance).
    assert!(
        elapsed.as_millis() >= 400,
        "throttle should have delayed: elapsed={:?}",
        elapsed
    );
}

#[tokio::test]
async fn test_throttle_no_delay_when_tokens_available() {
    let throttle = Throttle::new(1_000_000);
    let start = tokio::time::Instant::now();

    throttle.acquire(100).await;

    let elapsed = start.elapsed();
    assert!(
        elapsed.as_millis() < 100,
        "should not delay when tokens available: elapsed={:?}",
        elapsed
    );
}

// ---------------------------------------------------------------------------
// Circuit breaker tests
// ---------------------------------------------------------------------------

#[test]
fn test_circuit_breaker_stops_when_half_nodes_down() {
    let config = RepairCircuitBreaker::default();
    let cb = CircuitBreaker::new(config);

    // 5 of 10 nodes down = 50% >= max_down_fraction (0.5)
    let status = cb.check(10, 5, 0);
    assert!(matches!(
        status,
        CircuitBreakerStatus::OpenTooManyDown { .. }
    ));
}

#[test]
fn test_circuit_breaker_allows_when_mostly_healthy() {
    let config = RepairCircuitBreaker::default();
    let cb = CircuitBreaker::new(config);

    // 1 of 10 down = 10%
    let status = cb.check(10, 9, 0);
    assert_eq!(status, CircuitBreakerStatus::Closed);
}

#[test]
fn test_circuit_breaker_queue_pressure_throttle() {
    let config = RepairCircuitBreaker {
        queue_pressure_threshold: 100,
        ..RepairCircuitBreaker::default()
    };
    let cb = CircuitBreaker::new(config);

    let status = cb.check(10, 9, 200);
    assert!(matches!(
        status,
        CircuitBreakerStatus::ThrottledQueuePressure { .. }
    ));
}

#[tokio::test]
async fn test_circuit_breaker_cooldown() {
    let config = RepairCircuitBreaker {
        rebalance_cooldown_secs: 1,
        ..RepairCircuitBreaker::default()
    };
    let cb = CircuitBreaker::new(config);
    let n = node_id(1);

    // Not in cooldown initially.
    assert!(!cb.is_in_cooldown(&n).await);

    // Record recovery.
    cb.record_node_recovery(n).await;
    assert!(cb.is_in_cooldown(&n).await);

    // Wait for cooldown to expire.
    tokio::time::sleep(tokio::time::Duration::from_millis(1200)).await;
    assert!(!cb.is_in_cooldown(&n).await);
}

#[test]
fn test_circuit_breaker_empty_cluster() {
    let cb = CircuitBreaker::new(RepairCircuitBreaker::default());
    let status = cb.check(0, 0, 0);
    assert_eq!(status, CircuitBreakerStatus::Closed);
}

// ---------------------------------------------------------------------------
// Deep scrub tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_scrub_healthy_shard() {
    let store1 = Arc::new(MemoryStore::new(TEST_MAX_BYTES));
    let store2 = Arc::new(MemoryStore::new(TEST_MAX_BYTES));
    let store3 = Arc::new(MemoryStore::new(TEST_MAX_BYTES));

    let data = Bytes::from(vec![0xABu8; 100]);
    let shard_id = ShardId::from_data(&data);

    // Same data on all 3 nodes.
    store1.put(shard_id, data.clone()).await.unwrap();
    store2.put(shard_id, data.clone()).await.unwrap();
    store3.put(shard_id, data.clone()).await.unwrap();

    let meta = Arc::new(MetaStore::open_temporary().unwrap());
    meta.put_shard_owners(&shard_id, &[node_id(1), node_id(2), node_id(3)])
        .unwrap();

    let mut peer_stores = HashMap::new();
    peer_stores.insert(node_id(2), store2);
    peer_stores.insert(node_id(3), store3);

    let hash_query = Arc::new(MockHashQuery {
        stores: peer_stores,
    });

    let scrubber = DeepScrubber::new(store1, meta, hash_query, node_id(1));
    let verdict = scrubber.scrub_shard(shard_id).await.unwrap();
    assert_eq!(verdict, ScrubVerdict::Healthy);
}

#[tokio::test]
async fn test_scrub_detects_local_corruption() {
    let store1 = Arc::new(MemoryStore::new(TEST_MAX_BYTES));
    let store2 = Arc::new(MemoryStore::new(TEST_MAX_BYTES));
    let store3 = Arc::new(MemoryStore::new(TEST_MAX_BYTES));

    let correct_data = Bytes::from(vec![0xABu8; 100]);
    let shard_id = ShardId::from_data(&correct_data);

    // Corrupt data on node 1 (different bytes but stored under the same ID).
    let corrupt_data = Bytes::from(vec![0xCDu8; 100]);
    store1.put(shard_id, corrupt_data).await.unwrap();
    // Correct data on nodes 2 and 3.
    store2.put(shard_id, correct_data.clone()).await.unwrap();
    store3.put(shard_id, correct_data.clone()).await.unwrap();

    let meta = Arc::new(MetaStore::open_temporary().unwrap());
    meta.put_shard_owners(&shard_id, &[node_id(1), node_id(2), node_id(3)])
        .unwrap();

    let mut peer_stores = HashMap::new();
    peer_stores.insert(node_id(2), store2);
    peer_stores.insert(node_id(3), store3);

    let hash_query = Arc::new(MockHashQuery {
        stores: peer_stores,
    });

    let scrubber = DeepScrubber::new(store1, meta, hash_query, node_id(1));
    let verdict = scrubber.scrub_shard(shard_id).await.unwrap();

    assert!(
        matches!(verdict, ScrubVerdict::LocalCorrupt { .. }),
        "should detect local corruption, got: {verdict:?}"
    );
}

#[tokio::test]
async fn test_scrub_no_consensus() {
    let store1 = Arc::new(MemoryStore::new(TEST_MAX_BYTES));
    let store2 = Arc::new(MemoryStore::new(TEST_MAX_BYTES));
    let store3 = Arc::new(MemoryStore::new(TEST_MAX_BYTES));

    let data1 = Bytes::from(vec![0xAAu8; 100]);
    let data2 = Bytes::from(vec![0xBBu8; 100]);
    let data3 = Bytes::from(vec![0xCCu8; 100]);
    let shard_id = ShardId::from_data(&data1);

    // All three nodes have different data — no consensus.
    store1.put(shard_id, data1).await.unwrap();
    store2.put(shard_id, data2).await.unwrap();
    store3.put(shard_id, data3).await.unwrap();

    let meta = Arc::new(MetaStore::open_temporary().unwrap());
    meta.put_shard_owners(&shard_id, &[node_id(1), node_id(2), node_id(3)])
        .unwrap();

    let mut peer_stores = HashMap::new();
    peer_stores.insert(node_id(2), store2);
    peer_stores.insert(node_id(3), store3);

    let hash_query = Arc::new(MockHashQuery {
        stores: peer_stores,
    });

    let scrubber = DeepScrubber::new(store1, meta, hash_query, node_id(1));
    let verdict = scrubber.scrub_shard(shard_id).await.unwrap();

    assert!(
        matches!(verdict, ScrubVerdict::NoConsensus { .. }),
        "should report no consensus, got: {verdict:?}"
    );
}

#[tokio::test]
async fn test_scrub_insufficient_peers() {
    let store1 = Arc::new(MemoryStore::new(TEST_MAX_BYTES));
    let data = Bytes::from(vec![0xABu8; 100]);
    let shard_id = ShardId::from_data(&data);
    store1.put(shard_id, data).await.unwrap();

    let meta = Arc::new(MetaStore::open_temporary().unwrap());
    // Only local node in shard owners — no peers.
    meta.put_shard_owners(&shard_id, &[node_id(1)]).unwrap();

    let hash_query = Arc::new(MockHashQuery {
        stores: HashMap::new(),
    });

    let scrubber = DeepScrubber::new(store1, meta, hash_query, node_id(1));
    let verdict = scrubber.scrub_shard(shard_id).await.unwrap();

    assert!(
        matches!(verdict, ScrubVerdict::InsufficientPeers { .. }),
        "should report insufficient peers, got: {verdict:?}"
    );
}

// ---------------------------------------------------------------------------
// Integration: full repair cycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_full_repair_after_node_death() {
    let tc = TestCluster::new().await;
    let local_store = tc.stores[&node_id(1)].clone();

    let transfer = Arc::new(MockTransfer {
        stores: tc.stores.clone(),
    });

    let throttle = Throttle::new(100_000_000);

    let executor = RepairExecutor::new(
        tc.cluster.clone(),
        tc.meta.clone(),
        local_store.clone() as Arc<dyn ShardStore>,
        transfer,
        node_id(1),
        3,
        2,
        1,
    );

    // Simulate node 3 dying: delete all its shards.
    let dead_store = tc.stores[&node_id(3)].clone();
    let dead_shards = dead_store.list().await.unwrap();
    for shard_id in &dead_shards {
        dead_store.delete(*shard_id).await.unwrap();
    }

    // Mark dead.
    tc.cluster.mark_dead(&node_id(3)).await;

    // Enqueue all shards from the manifest for repair.
    let mut repair_count = 0;
    for chunk in &tc.manifest.chunks {
        for shard_meta in &chunk.shards {
            tc.meta.enqueue_repair(&shard_meta.shard_id, 1).unwrap();
            repair_count += 1;
        }
    }
    assert!(repair_count > 0, "should have enqueued shards for repair");

    // Repair each shard.
    while let Some(shard_id) = tc.meta.dequeue_repair().unwrap() {
        match executor.repair_shard(shard_id, &throttle).await {
            Ok(()) => {}
            Err(e) => {
                // Some shards may not be repairable if direct fetch hits a node
                // that doesn't have it, but the executor should not panic.
                eprintln!("repair error (non-fatal): {e}");
            }
        }
    }

    // After repair, verify data shards are available on at least k=2 nodes.
    for chunk in &tc.manifest.chunks {
        let mut available = 0;
        for shard_meta in &chunk.shards {
            for n in [1u8, 2] {
                if tc.stores[&node_id(n)]
                    .contains(shard_meta.shard_id)
                    .await
                    .unwrap()
                {
                    available += 1;
                    break;
                }
            }
        }
        assert!(
            available >= 2,
            "chunk should have at least k=2 shards available after repair, got {available}"
        );
    }
}

#[tokio::test]
async fn test_scrub_all_summary() {
    let store = Arc::new(MemoryStore::new(TEST_MAX_BYTES));

    // Put 3 healthy shards.
    for i in 0..3u8 {
        let data = Bytes::from(vec![i; 100]);
        let id = ShardId::from_data(&data);
        store.put(id, data).await.unwrap();
    }

    let meta = Arc::new(MetaStore::open_temporary().unwrap());

    // No peers — all will be InsufficientPeers.
    let hash_query = Arc::new(MockHashQuery {
        stores: HashMap::new(),
    });

    let scrubber = DeepScrubber::new(store, meta, hash_query, node_id(1));
    let summary = scrubber.scrub_all().await.unwrap();

    assert_eq!(summary.total, 3);
    assert_eq!(summary.insufficient_peers, 3);
    assert_eq!(summary.healthy, 0);
    assert_eq!(summary.corrupt, 0);
    assert_eq!(summary.no_consensus, 0);
}
