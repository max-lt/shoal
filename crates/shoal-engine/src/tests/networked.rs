//! Comprehensive networked tests with mock transport.
//!
//! Tests cover: N-node clusters, node failures, erasure config variations,
//! node recovery, edge cases (tiny objects, boundary sizes), and large clusters.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use shoal_cluster::ClusterState;
use shoal_logtree::LogTree;
use shoal_meta::MetaStore;
use shoal_net::{NetError, ShoalMessage, Transport};
use shoal_store::{MemoryStore, ShardStore, SlowStore};
use shoal_types::*;
use tokio::sync::RwLock;

use crate::node::{ShoalNode, ShoalNodeConfig};

use super::chaos::{ChaosConfig, ChaosController, ChaosTransport};
use super::helpers::{TEST_MAX_BYTES, test_data};

// =========================================================================
// Test infrastructure
// =========================================================================

/// A mock transport that routes shard requests directly to in-memory stores,
/// with support for simulating node failures.
///
/// When a node is in `down_nodes`, push_shard silently drops the data and
/// pull_shard returns `None` (simulating an unreachable node).
struct FailableMockTransport {
    /// Maps NodeId → store for all nodes in the cluster.
    stores: HashMap<NodeId, Arc<dyn ShardStore>>,
    /// Maps NodeId → MetaStore for manifest sync.
    metas: HashMap<NodeId, Arc<MetaStore>>,
    /// Maps NodeId → LogTree for log entry sync.
    log_trees: HashMap<NodeId, Arc<LogTree>>,
    /// Set of nodes currently "down" (unreachable).
    down_nodes: Arc<RwLock<HashSet<NodeId>>>,
}

#[async_trait::async_trait]
impl Transport for FailableMockTransport {
    async fn push_shard(
        &self,
        addr: iroh::EndpointAddr,
        shard_id: ShardId,
        data: Bytes,
    ) -> Result<(), NetError> {
        let node_id = NodeId::from(*addr.id.as_bytes());
        if self.down_nodes.read().await.contains(&node_id) {
            return Err(NetError::Endpoint("node is down".into()));
        }
        if let Some(store) = self.stores.get(&node_id) {
            store
                .put(shard_id, data)
                .await
                .map_err(|e| NetError::Endpoint(e.to_string()))?;
        }
        Ok(())
    }

    async fn pull_shard(
        &self,
        addr: iroh::EndpointAddr,
        shard_id: ShardId,
    ) -> Result<Option<Bytes>, NetError> {
        let node_id = NodeId::from(*addr.id.as_bytes());
        if self.down_nodes.read().await.contains(&node_id) {
            return Err(NetError::Endpoint("node is down".into()));
        }
        if let Some(store) = self.stores.get(&node_id) {
            return store
                .get(shard_id)
                .await
                .map_err(|e| NetError::Endpoint(e.to_string()));
        }
        Ok(None)
    }

    async fn send_to(&self, addr: iroh::EndpointAddr, msg: &ShoalMessage) -> Result<(), NetError> {
        let node_id = NodeId::from(*addr.id.as_bytes());
        if self.down_nodes.read().await.contains(&node_id) {
            return Err(NetError::Endpoint("node is down".into()));
        }

        match msg {
            ShoalMessage::LogEntryBroadcast { entry_bytes } => {
                if let Some(log_tree) = self.log_trees.get(&node_id)
                    && let Ok(entry) = postcard::from_bytes::<shoal_logtree::LogEntry>(entry_bytes)
                {
                    let _ = log_tree.receive_entry(&entry, None);
                }
            }
            _ => {}
        }

        Ok(())
    }

    async fn pull_manifests(
        &self,
        addr: iroh::EndpointAddr,
        manifest_ids: &[ObjectId],
    ) -> Result<Vec<(ObjectId, Vec<u8>)>, NetError> {
        let node_id = NodeId::from(*addr.id.as_bytes());
        if self.down_nodes.read().await.contains(&node_id) {
            return Err(NetError::Endpoint("node is down".into()));
        }

        let Some(meta) = self.metas.get(&node_id) else {
            return Ok(vec![]);
        };

        let mut result = Vec::new();
        for oid in manifest_ids {
            if let Ok(Some(manifest)) = meta.get_manifest(oid) {
                if let Ok(bytes) = postcard::to_allocvec(&manifest) {
                    result.push((*oid, bytes));
                }
            }
        }
        Ok(result)
    }

    async fn pull_log_entries(
        &self,
        addr: iroh::EndpointAddr,
        my_tips: &[[u8; 32]],
    ) -> Result<Vec<Vec<u8>>, NetError> {
        let node_id = NodeId::from(*addr.id.as_bytes());
        if self.down_nodes.read().await.contains(&node_id) {
            return Err(NetError::Endpoint("node is down".into()));
        }

        let Some(log_tree) = self.log_trees.get(&node_id) else {
            return Ok(vec![]);
        };

        let delta = log_tree
            .compute_delta(my_tips)
            .map_err(|e| NetError::Endpoint(e.to_string()))?;

        let entries: Vec<Vec<u8>> = delta
            .iter()
            .filter_map(|e| postcard::to_allocvec(e).ok())
            .collect();

        Ok(entries)
    }

    async fn pull_log_sync(
        &self,
        addr: iroh::EndpointAddr,
        entry_hashes: &[[u8; 32]],
        my_tips: &[[u8; 32]],
    ) -> Result<Vec<Vec<u8>>, NetError> {
        // Delegate to pull_log_entries as a simple implementation
        let _ = entry_hashes;
        self.pull_log_entries(addr, my_tips).await
    }

    async fn pull_api_keys(
        &self,
        addr: iroh::EndpointAddr,
        access_key_ids: &[String],
    ) -> Result<Vec<(String, String)>, NetError> {
        let node_id = NodeId::from(*addr.id.as_bytes());
        if self.down_nodes.read().await.contains(&node_id) {
            return Err(NetError::Endpoint("node is down".into()));
        }

        // The mock transport does not store API keys, so return empty.
        let _ = access_key_ids;
        Ok(vec![])
    }

    async fn lookup_key(
        &self,
        addr: iroh::EndpointAddr,
        bucket: &str,
        key: &str,
    ) -> Result<Option<Vec<u8>>, NetError> {
        let node_id = NodeId::from(*addr.id.as_bytes());
        if self.down_nodes.read().await.contains(&node_id) {
            return Err(NetError::Endpoint("node is down".into()));
        }

        let Some(meta) = self.metas.get(&node_id) else {
            return Ok(None);
        };

        let manifest = meta
            .get_object_key(bucket, key)
            .ok()
            .flatten()
            .and_then(|oid| meta.get_manifest(&oid).ok().flatten());

        Ok(manifest.and_then(|m| postcard::to_allocvec(&m).ok()))
    }

    async fn request_response(
        &self,
        addr: iroh::EndpointAddr,
        msg: &ShoalMessage,
    ) -> Result<ShoalMessage, NetError> {
        let node_id = NodeId::from(*addr.id.as_bytes());
        if self.down_nodes.read().await.contains(&node_id) {
            return Err(NetError::Endpoint("node is down".into()));
        }
        match msg {
            ShoalMessage::Ping { timestamp } => Ok(ShoalMessage::Pong {
                timestamp: *timestamp,
            }),
            _ => Err(NetError::Serialization("unexpected message".into())),
        }
    }
}

/// Derive a valid (NodeId, EndpointAddr) pair from a seed byte.
///
/// Uses a SecretKey to ensure the bytes represent a valid ed25519 point.
fn valid_identity(seed: u8) -> (NodeId, iroh::EndpointAddr) {
    let secret = iroh::SecretKey::from([seed; 32]);
    let public = secret.public();
    let node_id = NodeId::from(*public.as_bytes());
    let addr = iroh::EndpointAddr::new(public);
    (node_id, addr)
}

/// A simulated N-node cluster with mock transport and failure injection.
struct TestCluster {
    nodes: Vec<ShoalNode>,
    node_ids: Vec<NodeId>,
    stores: Vec<Arc<dyn ShardStore>>,
    cluster: Arc<ClusterState>,
    down_nodes: Arc<RwLock<HashSet<NodeId>>>,
    /// LogTree instances — nodes hold their own Arc clones via `.with_log_tree()`.
    log_trees: Vec<Arc<LogTree>>,
    /// Optional chaos controller for fault injection tests.
    #[allow(dead_code)]
    chaos_controller: Option<Arc<ChaosController>>,
}

impl TestCluster {
    /// Create an N-node cluster. Seeds start at 1 (seed=0 gives an invalid key).
    /// Uses shard_replication=1 (each shard on exactly one node).
    async fn new(n: usize, chunk_size: u32, k: usize, m: usize) -> Self {
        Self::build(n, chunk_size, k, m, 1, None).await
    }

    /// Create an N-node cluster with a custom shard replication factor.
    /// For node failure tests, use replication >= 2 so shards survive node loss.
    async fn with_replication(
        n: usize,
        chunk_size: u32,
        k: usize,
        m: usize,
        shard_replication: usize,
    ) -> Self {
        Self::build(n, chunk_size, k, m, shard_replication, None).await
    }

    /// Create an N-node cluster with chaos fault injection.
    async fn with_chaos(n: usize, chunk_size: u32, k: usize, m: usize, chaos: ChaosConfig) -> Self {
        Self::build(n, chunk_size, k, m, 1, Some(chaos)).await
    }

    /// Create an N-node cluster with chaos fault injection AND custom
    /// shard replication. Combines the two for tests that need redundancy
    /// to survive chaotic transport failures.
    async fn with_chaos_and_replication(
        n: usize,
        chunk_size: u32,
        k: usize,
        m: usize,
        shard_replication: usize,
        chaos: ChaosConfig,
    ) -> Self {
        Self::build(n, chunk_size, k, m, shard_replication, Some(chaos)).await
    }

    /// Internal builder.
    async fn build(
        n: usize,
        chunk_size: u32,
        k: usize,
        m: usize,
        shard_replication: usize,
        chaos_config: Option<ChaosConfig>,
    ) -> Self {
        assert!(n >= 2, "need at least 2 nodes");
        assert!(n <= 200, "seed byte overflow");

        let identities: Vec<_> = (1..=n as u8).map(valid_identity).collect();
        let node_ids: Vec<_> = identities.iter().map(|(nid, _)| *nid).collect();
        let addrs: Vec<_> = identities.iter().map(|(_, addr)| addr.clone()).collect();

        let cluster = ClusterState::new(node_ids[0], 128);
        for &nid in &node_ids {
            cluster
                .add_member(Member {
                    node_id: nid,
                    capacity: TEST_MAX_BYTES,
                    state: MemberState::Alive,
                    generation: 1,
                    topology: NodeTopology::default(),
                })
                .await;
        }

        // Create the chaos controller if configured.
        let chaos_controller = chaos_config
            .as_ref()
            .map(|c| Arc::new(ChaosController::new(c.clone())));

        let stores: Vec<Arc<dyn ShardStore>> = (0..n)
            .map(|i| {
                let mem: Arc<dyn ShardStore> = Arc::new(MemoryStore::new(TEST_MAX_BYTES));

                if let Some(ref cfg) = chaos_config {
                    let (rmin, rmax) = cfg.store_read_latency_ms;
                    let (wmin, wmax) = cfg.store_write_latency_ms;

                    if rmax > 0 || wmax > 0 {
                        return Arc::new(
                            SlowStore::new(mem)
                                .read_latency(rmin, rmax)
                                .write_latency(wmin, wmax)
                                .seed(cfg.seed.wrapping_add(i as u64)),
                        ) as Arc<dyn ShardStore>;
                    }
                }

                mem
            })
            .collect();

        let down_nodes: Arc<RwLock<HashSet<NodeId>>> = Arc::new(RwLock::new(HashSet::new()));

        // Build a store map for the shared transport.
        let store_map: HashMap<NodeId, Arc<dyn ShardStore>> = node_ids
            .iter()
            .zip(stores.iter())
            .map(|(&nid, store)| (nid, store.clone()))
            .collect();

        // Build address book (all nodes know about all other nodes).
        let full_book: HashMap<NodeId, iroh::EndpointAddr> = node_ids
            .iter()
            .zip(addrs.iter())
            .map(|(&nid, addr)| (nid, addr.clone()))
            .collect();

        // Create MetaStores upfront so we can share them with the transport.
        let metas: Vec<Arc<MetaStore>> = (0..n)
            .map(|_| Arc::new(MetaStore::open_temporary().unwrap()))
            .collect();

        let meta_map: HashMap<NodeId, Arc<MetaStore>> = node_ids
            .iter()
            .zip(metas.iter())
            .map(|(&nid, meta)| (nid, meta.clone()))
            .collect();

        let log_trees: Vec<Arc<LogTree>> = (0..n)
            .map(|i| {
                let signing_key = ed25519_dalek::SigningKey::from_bytes(&[(i + 1) as u8; 32]);
                let store = shoal_logtree::LogTreeStore::open_temporary().unwrap();
                Arc::new(LogTree::new(store, node_ids[i], signing_key))
            })
            .collect();

        let log_tree_map: HashMap<NodeId, Arc<LogTree>> = node_ids
            .iter()
            .zip(log_trees.iter())
            .map(|(&nid, lt)| (nid, lt.clone()))
            .collect();

        let mut nodes = Vec::with_capacity(n);
        for i in 0..n {
            let base_transport: Arc<dyn Transport> = Arc::new(FailableMockTransport {
                stores: store_map.clone(),
                metas: meta_map.clone(),
                log_trees: log_tree_map.clone(),
                down_nodes: down_nodes.clone(),
            });

            let transport: Arc<dyn Transport> = if let Some(ref ctrl) = chaos_controller {
                Arc::new(ChaosTransport::new(
                    node_ids[i],
                    ctrl.clone(),
                    base_transport,
                ))
            } else {
                base_transport
            };
            let book: HashMap<NodeId, iroh::EndpointAddr> = full_book
                .iter()
                .filter(|(nid, _)| **nid != node_ids[i])
                .map(|(&nid, addr)| (nid, addr.clone()))
                .collect();

            let node = ShoalNode::new(
                ShoalNodeConfig {
                    node_id: node_ids[i],
                    chunk_size,
                    erasure_k: k,
                    erasure_m: m,
                    vnodes_per_node: 128,
                    shard_replication,
                    cache_max_bytes: u64::MAX,
                },
                stores[i].clone(),
                metas[i].clone(),
                cluster.clone(),
            )
            .with_transport(transport)
            .with_address_book(Arc::new(RwLock::new(book)))
            .with_log_tree(log_trees[i].clone());

            nodes.push(node);
        }

        Self {
            nodes,
            node_ids,
            stores,
            cluster,
            down_nodes,
            log_trees,
            chaos_controller,
        }
    }

    fn node(&self, i: usize) -> &ShoalNode {
        &self.nodes[i]
    }

    /// Simulate manifest broadcast from one node to all others.
    ///
    /// Syncs the LogTree entries from `from` to every other node and
    /// caches the manifest in both the LogTree store and MetaStore.
    async fn broadcast_manifest(&self, from: usize, bucket: &str, key: &str) {
        let manifest = self.nodes[from].head_object(bucket, key).await.unwrap();
        let src_lt = &self.log_trees[from];

        for (i, _node) in self.nodes.iter().enumerate() {
            if i == from {
                continue;
            }

            let dst_lt = &self.log_trees[i];
            let tips = dst_lt.tips().unwrap_or_default();
            let delta = src_lt.compute_delta(&tips).unwrap_or_default();
            let _ = dst_lt.apply_sync_entries(&delta);
            dst_lt.store().put_manifest(&manifest).unwrap();

            self.nodes[i].meta().put_manifest(&manifest).unwrap();
            self.nodes[i]
                .meta()
                .put_object_key(bucket, key, &manifest.object_id)
                .unwrap();
        }
    }

    /// Mark a node as "down" — transport calls to it will fail and the ring
    /// is updated to remove it.
    async fn kill_node(&self, i: usize) {
        self.down_nodes.write().await.insert(self.node_ids[i]);
        self.cluster.mark_dead(&self.node_ids[i]).await;
    }

    /// Make a node unreachable at the transport level WITHOUT removing it
    /// from the ring. This simulates a transient network failure: the node
    /// is still in the ring (shards are assigned to it) but pushes/pulls fail.
    async fn disconnect_node(&self, i: usize) {
        self.down_nodes.write().await.insert(self.node_ids[i]);
    }

    /// Reconnect a previously disconnected node (transport only).
    async fn reconnect_node(&self, i: usize) {
        self.down_nodes.write().await.remove(&self.node_ids[i]);
    }

    /// Mark a node as "alive" again after being killed.
    async fn revive_node(&self, i: usize) {
        self.down_nodes.write().await.remove(&self.node_ids[i]);
        self.cluster
            .add_member(Member {
                node_id: self.node_ids[i],
                capacity: TEST_MAX_BYTES,
                state: MemberState::Alive,
                generation: 2, // bumped generation after restart
                topology: NodeTopology::default(),
            })
            .await;
    }

    /// Count shards stored locally on a given node.
    async fn local_shard_count(&self, i: usize) -> usize {
        self.stores[i].list().await.unwrap().len()
    }
}

// =========================================================================
// Regression tests (preserved from original)
// =========================================================================

/// Simulate what the manifest broadcast handler does:
/// store the manifest and key mapping on the target node, but do NOT
/// store shard owners.
async fn simulate_manifest_broadcast(
    source: &ShoalNode,
    target: &ShoalNode,
    bucket: &str,
    key: &str,
) {
    let manifest = source.head_object(bucket, key).await.unwrap();
    target.meta().put_manifest(&manifest).unwrap();
    target
        .meta()
        .put_object_key(bucket, key, &manifest.object_id)
        .unwrap();
}

/// Set up 2 nodes sharing a cluster state, connected via mock transport.
async fn writer_reader_pair_with_transport(
    chunk_size: u32,
    k: usize,
    m: usize,
) -> (ShoalNode, ShoalNode) {
    let (nid_a, addr_a) = valid_identity(1);
    let (nid_b, addr_b) = valid_identity(2);

    let cluster = ClusterState::new(nid_a, 128);
    for &nid in &[nid_a, nid_b] {
        cluster
            .add_member(Member {
                node_id: nid,
                capacity: TEST_MAX_BYTES,
                state: MemberState::Alive,
                generation: 1,
                topology: NodeTopology::default(),
            })
            .await;
    }

    let store_a: Arc<dyn ShardStore> = Arc::new(MemoryStore::new(TEST_MAX_BYTES));
    let store_b: Arc<dyn ShardStore> = Arc::new(MemoryStore::new(TEST_MAX_BYTES));
    let meta_a = Arc::new(MetaStore::open_temporary().unwrap());
    let meta_b = Arc::new(MetaStore::open_temporary().unwrap());

    let down: Arc<RwLock<HashSet<NodeId>>> = Arc::new(RwLock::new(HashSet::new()));

    let transport_a: Arc<dyn Transport> = Arc::new(FailableMockTransport {
        stores: HashMap::from([(nid_b, store_b.clone())]),
        metas: HashMap::from([(nid_b, meta_b.clone())]),
        log_trees: HashMap::new(),
        down_nodes: down.clone(),
    });
    let transport_b: Arc<dyn Transport> = Arc::new(FailableMockTransport {
        stores: HashMap::from([(nid_a, store_a.clone())]),
        metas: HashMap::from([(nid_a, meta_a.clone())]),
        log_trees: HashMap::new(),
        down_nodes: down,
    });

    let book_a = Arc::new(RwLock::new(HashMap::from([(nid_b, addr_b)])));
    let book_b = Arc::new(RwLock::new(HashMap::from([(nid_a, addr_a)])));

    let node_a = ShoalNode::new(
        ShoalNodeConfig {
            node_id: nid_a,
            chunk_size,
            erasure_k: k,
            erasure_m: m,
            vnodes_per_node: 128,
            shard_replication: 1,
            cache_max_bytes: u64::MAX,
        },
        store_a,
        meta_a,
        cluster.clone(),
    )
    .with_transport(transport_a)
    .with_address_book(book_a);

    let node_b = ShoalNode::new(
        ShoalNodeConfig {
            node_id: nid_b,
            chunk_size,
            erasure_k: k,
            erasure_m: m,
            vnodes_per_node: 128,
            shard_replication: 1,
            cache_max_bytes: u64::MAX,
        },
        store_b,
        meta_b,
        cluster,
    )
    .with_transport(transport_b)
    .with_address_book(book_b);

    (node_a, node_b)
}

// -----------------------------------------------------------------------
// Regression: non-writer read with mock transport
// -----------------------------------------------------------------------

#[tokio::test]
#[ntest::timeout(30000)]
async fn test_non_writer_read_after_manifest_broadcast() {
    let (node_a, node_b) = writer_reader_pair_with_transport(1024, 2, 1).await;
    let data = test_data(5000);

    node_a
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    simulate_manifest_broadcast(&node_a, &node_b, "b", "k").await;

    let (got, _) = node_b.get_object("b", "k").await.unwrap();
    assert_eq!(got, data, "non-writer node should reconstruct the object");
}

#[tokio::test]
#[ntest::timeout(30000)]
async fn test_non_writer_read_k4_m2_after_broadcast() {
    let (node_a, node_b) = writer_reader_pair_with_transport(1024, 4, 2).await;
    let data = test_data(10_000);

    node_a
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    simulate_manifest_broadcast(&node_a, &node_b, "b", "k").await;

    let (got, _) = node_b.get_object("b", "k").await.unwrap();
    assert_eq!(got, data);
}

#[tokio::test]
#[ntest::timeout(30000)]
async fn test_writer_stores_shard_owners_in_meta() {
    let (node_a, _node_b) = writer_reader_pair_with_transport(1024, 2, 1).await;
    let data = test_data(2000);

    node_a
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    let manifest = node_a.head_object("b", "k").await.unwrap();
    for chunk_meta in &manifest.chunks {
        for shard_meta in &chunk_meta.shards {
            let owners = node_a
                .meta()
                .get_shard_owners(&shard_meta.shard_id)
                .unwrap();
            assert!(
                owners.is_some(),
                "writer should store shard owners for {}",
                shard_meta.shard_id
            );
        }
    }
}

#[tokio::test]
#[ntest::timeout(30000)]
async fn test_manifest_broadcast_does_not_store_shard_owners() {
    let (node_a, node_b) = writer_reader_pair_with_transport(1024, 2, 1).await;
    let data = test_data(2000);

    node_a
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    simulate_manifest_broadcast(&node_a, &node_b, "b", "k").await;

    let manifest = node_b.head_object("b", "k").await.unwrap();
    for chunk_meta in &manifest.chunks {
        for shard_meta in &chunk_meta.shards {
            let owners = node_b
                .meta()
                .get_shard_owners(&shard_meta.shard_id)
                .unwrap();
            assert!(
                owners.is_none(),
                "broadcast receiver should NOT have shard owners for {}",
                shard_meta.shard_id
            );
        }
    }
}

// =========================================================================
// Multi-node cluster tests
// =========================================================================

/// 3-node cluster: write on node 0, read from all nodes.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_3_node_write_read_from_all() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let data = test_data(5000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k").await;

    for i in 0..3 {
        let (got, _) = c.node(i).get_object("b", "k").await.unwrap();
        assert_eq!(got, data, "node {i} should read the object");
    }
}

/// 5-node cluster: write on node 2, read from all nodes.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_5_node_write_read_from_all() {
    let c = TestCluster::new(5, 1024, 4, 2).await;
    let data = test_data(20_000);

    c.node(2)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(2, "b", "k").await;

    for i in 0..5 {
        let (got, _) = c.node(i).get_object("b", "k").await.unwrap();
        assert_eq!(got, data, "node {i} should read the object");
    }
}

/// 10-node cluster: write and read.
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_10_node_cluster() {
    let c = TestCluster::new(10, 2048, 4, 2).await;
    let data = test_data(50_000);

    c.node(7)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(7, "b", "k").await;

    // Every node can read.
    for i in 0..10 {
        let (got, _) = c.node(i).get_object("b", "k").await.unwrap();
        assert_eq!(got, data, "node {i} should read correctly");
    }
}

/// 10-node cluster: shards should be distributed across multiple nodes.
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_10_node_shard_distribution() {
    let c = TestCluster::new(10, 1024, 4, 2).await;
    let data = test_data(10_000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    // Check how many nodes have at least one shard.
    let mut nodes_with_shards = 0;
    for i in 0..10 {
        if c.local_shard_count(i).await > 0 {
            nodes_with_shards += 1;
        }
    }
    // With 10 nodes and 6 shards per chunk (k=4, m=2), shards should spread.
    assert!(
        nodes_with_shards >= 3,
        "shards should be distributed across at least 3 of 10 nodes, got {nodes_with_shards}"
    );
}

// =========================================================================
// Erasure coding configuration variations
// =========================================================================

/// k=1, m=1 (mirroring): every chunk has 2 copies.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_3_node_k1_m1_mirroring() {
    let c = TestCluster::new(3, 1024, 1, 1).await;
    let data = test_data(3000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k").await;

    for i in 0..3 {
        let (got, _) = c.node(i).get_object("b", "k").await.unwrap();
        assert_eq!(got, data, "k1m1 node {i}");
    }
}

/// k=2, m=2: can lose 2 shards per chunk.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_5_node_k2_m2() {
    let c = TestCluster::new(5, 1024, 2, 2).await;
    let data = test_data(4000);

    c.node(1)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(1, "b", "k").await;

    for i in 0..5 {
        let (got, _) = c.node(i).get_object("b", "k").await.unwrap();
        assert_eq!(got, data, "k2m2 node {i}");
    }
}

/// k=8, m=4: large erasure group.
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_12_node_k8_m4() {
    let c = TestCluster::new(12, 4096, 8, 4).await;
    let data = test_data(100_000);

    c.node(5)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(5, "b", "k").await;

    // Read from a few nodes.
    for &i in &[0, 3, 7, 11] {
        let (got, _) = c.node(i).get_object("b", "k").await.unwrap();
        assert_eq!(got, data, "k8m4 node {i}");
    }
}

// =========================================================================
// Node failure tests
// =========================================================================

/// Kill 1 node in a 3-node k=2,m=1 cluster with replication=2:
/// each shard is on 2 nodes, so losing 1 node still leaves enough copies.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_3_node_kill_one_still_reads() {
    let c = TestCluster::with_replication(3, 1024, 2, 1, 2).await;
    let data = test_data(5000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k").await;

    // Kill node 1.
    c.kill_node(1).await;

    // Node 2 should still read (pulling from node 0, even if node 1 is down).
    let (got, _) = c.node(2).get_object("b", "k").await.unwrap();
    assert_eq!(got, data, "should read after 1 node failure");
}

/// Kill 2 nodes in a 5-node k=4,m=2 cluster with replication=3:
/// each shard is on 3 nodes, so losing 2 still leaves copies available.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_5_node_kill_two_still_reads() {
    let c = TestCluster::with_replication(5, 1024, 4, 2, 3).await;
    let data = test_data(20_000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k").await;

    // Kill 2 nodes.
    c.kill_node(3).await;
    c.kill_node(4).await;

    // Surviving nodes should still read.
    for i in 0..3 {
        let (got, _) = c.node(i).get_object("b", "k").await.unwrap();
        assert_eq!(got, data, "node {i} should read after 2 failures (k=4,m=2)");
    }
}

/// Kill the writer node: other nodes should still read via remaining shards.
/// replication=2 ensures shards survive writer death.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_5_node_kill_writer() {
    let c = TestCluster::with_replication(5, 1024, 4, 2, 2).await;
    let data = test_data(15_000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k").await;

    // Kill the writer.
    c.kill_node(0).await;

    // Non-writer nodes should still read.
    for i in 1..5 {
        let (got, _) = c.node(i).get_object("b", "k").await.unwrap();
        assert_eq!(got, data, "node {i} should read after writer died");
    }
}

/// Kill 2 of 3 nodes: with replication=3 (full replication), the writer
/// has all shards locally and can still read.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_3_node_kill_two_writer_still_reads() {
    let c = TestCluster::with_replication(3, 1024, 2, 1, 3).await;
    let data = test_data(5000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k").await;

    // Kill 2 out of 3 nodes. With k=2,m=1, we need at least 2 shards per chunk.
    // If 2 of 3 nodes are down and the reading node doesn't have enough local
    // shards, it will fail.
    c.kill_node(1).await;
    c.kill_node(2).await;

    // Node 0 (writer) should still read because it has all shards locally.
    let (got, _) = c.node(0).get_object("b", "k").await.unwrap();
    assert_eq!(got, data, "writer should read from local shards");
}

// =========================================================================
// Node recovery tests
// =========================================================================

/// Kill a node, then revive it: reads should work before and after.
/// replication=2 ensures shards survive the temporary node loss.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_node_kill_and_revive() {
    let c = TestCluster::with_replication(3, 1024, 2, 1, 2).await;
    let data = test_data(5000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k").await;

    // Kill node 1.
    c.kill_node(1).await;

    // Node 2 reads fine.
    let (got, _) = c.node(2).get_object("b", "k").await.unwrap();
    assert_eq!(got, data, "should read with node 1 down");

    // Revive node 1.
    c.revive_node(1).await;

    // Node 1 should read again (still has manifest, ring recomputed).
    let (got, _) = c.node(1).get_object("b", "k").await.unwrap();
    assert_eq!(got, data, "node 1 should read after revival");
}

/// Kill all nodes except the writer, revive them all, verify reads.
/// replication=5 (all nodes) ensures the writer has all shards locally.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_mass_failure_and_recovery() {
    let c = TestCluster::with_replication(5, 1024, 4, 2, 5).await;
    let data = test_data(20_000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k").await;

    // Kill everyone except node 0.
    for i in 1..5 {
        c.kill_node(i).await;
    }

    // Writer still reads from local shards.
    let (got, _) = c.node(0).get_object("b", "k").await.unwrap();
    assert_eq!(got, data, "writer reads locally during mass failure");

    // Revive all nodes.
    for i in 1..5 {
        c.revive_node(i).await;
    }

    // Everyone can read again.
    for i in 0..5 {
        let (got, _) = c.node(i).get_object("b", "k").await.unwrap();
        assert_eq!(got, data, "node {i} should read after mass recovery");
    }
}

// =========================================================================
// Edge cases: object sizes
// =========================================================================

/// 1-byte object across a 3-node cluster.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_3_node_single_byte_object() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let data = vec![42u8];

    c.node(0)
        .put_object("b", "tiny", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "tiny").await;

    for i in 0..3 {
        let (got, _) = c.node(i).get_object("b", "tiny").await.unwrap();
        assert_eq!(got, data, "single byte on node {i}");
    }
}

/// Empty object across a 3-node cluster.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_3_node_empty_object() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let data: Vec<u8> = vec![];

    c.node(1)
        .put_object("b", "empty", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(1, "b", "empty").await;

    for i in 0..3 {
        let (got, _) = c.node(i).get_object("b", "empty").await.unwrap();
        assert_eq!(got, data, "empty object on node {i}");
    }
}

/// Object exactly chunk_size bytes.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_5_node_exact_chunk_size() {
    let c = TestCluster::new(5, 1024, 4, 2).await;
    let data = test_data(1024);

    c.node(3)
        .put_object("b", "exact", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(3, "b", "exact").await;

    for i in 0..5 {
        let (got, _) = c.node(i).get_object("b", "exact").await.unwrap();
        assert_eq!(got, data, "exact chunk_size on node {i}");
    }
}

/// Object of chunk_size - 1 bytes.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_3_node_one_byte_under_chunk() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let data = test_data(1023);

    c.node(0)
        .put_object("b", "under", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "under").await;

    for i in 0..3 {
        let (got, _) = c.node(i).get_object("b", "under").await.unwrap();
        assert_eq!(got, data, "under chunk_size on node {i}");
    }
}

/// Small object (below CDC min) produces a single chunk.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_3_node_small_object_single_chunk() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let data = test_data(1025);

    c.node(2)
        .put_object("b", "small", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(2, "b", "small").await;

    let manifest = c.node(2).head_object("b", "small").await.unwrap();
    // 1025 bytes < CDC min (16KB) → single chunk.
    assert_eq!(manifest.chunks.len(), 1, "small data should be 1 CDC chunk");

    for i in 0..3 {
        let (got, _) = c.node(i).get_object("b", "small").await.unwrap();
        assert_eq!(got, data, "small object on node {i}");
    }
}

/// 1MB object produces multiple CDC chunks.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_5_node_1mb_many_chunks() {
    let c = TestCluster::new(5, 4096, 4, 2).await;
    let data = test_data(1_048_576);

    c.node(0)
        .put_object("b", "big", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "big").await;

    let manifest = c.node(0).head_object("b", "big").await.unwrap();
    // CDC avg=64KB → ~16 chunks for 1MB (varies).
    assert!(
        manifest.chunks.len() > 1,
        "1MB should produce multiple CDC chunks, got {}",
        manifest.chunks.len()
    );

    // Read from two different non-writer nodes.
    for &i in &[2, 4] {
        let (got, _) = c.node(i).get_object("b", "big").await.unwrap();
        assert_eq!(got, data, "1MB on node {i}");
    }
}

// =========================================================================
// Multiple objects
// =========================================================================

/// Write 50 objects from different nodes, read all from every node.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_5_node_50_objects() {
    let c = TestCluster::new(5, 1024, 2, 1).await;
    let mut objects = Vec::new();

    for i in 0..50 {
        let writer = i % 5;
        let data = test_data(500 + i * 100);
        let key = format!("obj-{i}");

        c.node(writer)
            .put_object("b", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        c.broadcast_manifest(writer, "b", &key).await;

        objects.push((key, data));
    }

    // Every node reads every object.
    for reader in 0..5 {
        for (key, expected) in &objects {
            let (got, _) = c.node(reader).get_object("b", key).await.unwrap();
            assert_eq!(&got, expected, "node {reader} reading {key}");
        }
    }
}

/// Write from different nodes, kill one node, all objects still readable.
/// replication=2 ensures enough shard copies survive.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_5_node_write_kill_read_all() {
    let c = TestCluster::with_replication(5, 1024, 4, 2, 2).await;
    let mut objects = Vec::new();

    for i in 0..20 {
        let writer = i % 5;
        let data = test_data(2000 + i * 50);
        let key = format!("obj-{i}");

        c.node(writer)
            .put_object("b", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        c.broadcast_manifest(writer, "b", &key).await;

        objects.push((key, data));
    }

    // Kill node 2.
    c.kill_node(2).await;

    // All objects still readable from surviving nodes.
    for reader in [0, 1, 3, 4] {
        for (key, expected) in &objects {
            let (got, _) = c.node(reader).get_object("b", key).await.unwrap();
            assert_eq!(&got, expected, "node {reader} reading {key} after kill");
        }
    }
}

// =========================================================================
// Metadata preservation across nodes
// =========================================================================

/// Verify user metadata is preserved when reading from a different node.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_metadata_preserved_across_nodes() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let data = test_data(2000);
    let mut meta = BTreeMap::new();
    meta.insert("content-type".into(), "application/json".into());
    meta.insert("x-custom".into(), "hello".into());

    c.node(0)
        .put_object("b", "k", &data, meta.clone())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k").await;

    // Read manifest from another node, check metadata is intact.
    let manifest = c.node(1).head_object("b", "k").await.unwrap();
    assert_eq!(manifest.metadata, meta);
}

// =========================================================================
// Write from one node, delete from another
// =========================================================================

/// Node 0 writes, node 1 deletes, node 2 should see 404.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_delete_from_different_node() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let data = test_data(2000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k").await;

    // Node 1 can read.
    let (got, _) = c.node(1).get_object("b", "k").await.unwrap();
    assert_eq!(got, data);

    // Node 1 deletes.
    c.node(1).delete_object("b", "k").await.unwrap();

    // Node 1 should get 404.
    let result = c.node(1).get_object("b", "k").await;
    assert!(result.is_err(), "deleted object should not be found");
}

// =========================================================================
// Different chunk sizes
// =========================================================================

/// Verify various data sizes produce correct results with CDC chunking.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_various_data_sizes() {
    for data_size in [100, 1_000, 10_000, 100_000, 500_000] {
        let c = TestCluster::new(3, 1024, 2, 1).await;
        let data = test_data(data_size);

        c.node(0)
            .put_object("b", "k", &data, BTreeMap::new())
            .await
            .unwrap();
        c.broadcast_manifest(0, "b", "k").await;

        let (got, _) = c.node(2).get_object("b", "k").await.unwrap();
        assert_eq!(got, data, "data_size={data_size}");

        let manifest = c.node(0).head_object("b", "k").await.unwrap();
        assert!(
            !manifest.chunks.is_empty(),
            "must have at least 1 chunk for size={data_size}"
        );
        assert_eq!(manifest.total_size, data_size as u64);
    }
}

// =========================================================================
// Overwrite from a different node
// =========================================================================

/// Node 0 writes version 1, node 1 overwrites with version 2, all nodes
/// should see the new version.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_overwrite_from_different_node() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let v1 = test_data(3000);
    let v2 = test_data(5000);

    // Node 0 writes v1.
    c.node(0)
        .put_object("b", "k", &v1, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k").await;

    // Verify v1 readable.
    let (got, _) = c.node(2).get_object("b", "k").await.unwrap();
    assert_eq!(got, v1);

    // Node 1 overwrites with v2.
    c.node(1)
        .put_object("b", "k", &v2, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(1, "b", "k").await;

    // All nodes see v2.
    for i in 0..3 {
        let (got, _) = c.node(i).get_object("b", "k").await.unwrap();
        assert_eq!(got, v2, "node {i} should see v2 after overwrite");
    }
}

// =========================================================================
// Concurrent writes from multiple nodes
// =========================================================================

/// Multiple nodes write different objects concurrently. All should be readable.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_5_node_concurrent_writes() {
    let c = Arc::new(TestCluster::new(5, 1024, 2, 1).await);

    let mut handles = Vec::new();
    for writer in 0..5 {
        let cluster = c.clone();
        handles.push(tokio::spawn(async move {
            for j in 0..10 {
                let key = format!("w{writer}-obj{j}");
                let data = test_data(1000 + writer * 100 + j * 10);
                cluster
                    .node(writer)
                    .put_object("b", &key, &data, BTreeMap::new())
                    .await
                    .unwrap();
                cluster.broadcast_manifest(writer, "b", &key).await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Every object readable from every node.
    for writer in 0..5 {
        for j in 0..10 {
            let key = format!("w{writer}-obj{j}");
            let expected = test_data(1000 + writer * 100 + j * 10);
            for reader in 0..5 {
                let (got, _) = c.node(reader).get_object("b", &key).await.unwrap();
                assert_eq!(got, expected, "reader={reader} key={key}");
            }
        }
    }
}

// =========================================================================
// Shard caching on read
// =========================================================================

/// After a non-writer reads an object, the pulled shards should be cached
/// in the bounded LRU shard cache for future reads.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_pulled_shards_cached_locally() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let data = test_data(2000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k").await;

    // Before read: the shard cache on node 2 should be empty.
    let cache_before = c.node(2).shard_cache().len();

    // Read triggers remote pulls — pulled shards go to the LRU cache.
    let (got, _) = c.node(2).get_object("b", "k").await.unwrap();
    assert_eq!(got, data);

    // After read: node 2's shard cache should hold the pulled shards.
    let cache_after = c.node(2).shard_cache().len();
    assert!(
        cache_after > cache_before,
        "shard cache should grow after read: before={cache_before} after={cache_after}"
    );
}

// =========================================================================
// 2-node minimal cluster
// =========================================================================

/// Minimal 2-node cluster with k=1, m=1 (mirroring).
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_2_node_k1_m1() {
    let c = TestCluster::new(2, 512, 1, 1).await;
    let data = test_data(3000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k").await;

    let (got, _) = c.node(1).get_object("b", "k").await.unwrap();
    assert_eq!(got, data);
}

/// 2-node cluster with replication=2: both nodes have all shards.
/// Kill one, the other should still read.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_2_node_kill_one() {
    let c = TestCluster::with_replication(2, 512, 1, 1, 2).await;
    let data = test_data(3000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k").await;

    c.kill_node(1).await;

    // Writer should still read (all shards are local).
    let (got, _) = c.node(0).get_object("b", "k").await.unwrap();
    assert_eq!(got, data);
}

// =========================================================================
// Multiple buckets
// =========================================================================

/// Objects in different buckets don't interfere across nodes.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_3_node_multiple_buckets() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let data_a = test_data(2000);
    let data_b = test_data(3000);

    c.node(0)
        .put_object("photos", "img.jpg", &data_a, BTreeMap::new())
        .await
        .unwrap();
    c.node(1)
        .put_object("docs", "readme.md", &data_b, BTreeMap::new())
        .await
        .unwrap();

    c.broadcast_manifest(0, "photos", "img.jpg").await;
    c.broadcast_manifest(1, "docs", "readme.md").await;

    // Node 2 reads both.
    let (got_a, _) = c.node(2).get_object("photos", "img.jpg").await.unwrap();
    let (got_b, _) = c.node(2).get_object("docs", "readme.md").await.unwrap();
    assert_eq!(got_a, data_a);
    assert_eq!(got_b, data_b);

    // Cross-bucket isolation.
    let result = c.node(2).get_object("photos", "readme.md").await;
    assert!(result.is_err(), "should not find docs key in photos bucket");
}

// =========================================================================
// Object listing across nodes
// =========================================================================

/// After broadcast, listing on any node returns all keys.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_list_objects_after_broadcast() {
    let c = TestCluster::new(3, 1024, 2, 1).await;

    for i in 0..10 {
        let key = format!("item-{i:03}");
        let writer = i % 3;
        let data = test_data(500 + i * 10);
        c.node(writer)
            .put_object("b", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        c.broadcast_manifest(writer, "b", &key).await;
    }

    // All 3 nodes should list all 10 items.
    for i in 0..3 {
        let keys = c.node(i).list_objects("b", "").await.unwrap();
        assert_eq!(keys.len(), 10, "node {i} should list 10 objects");
    }
}

// =========================================================================
// Manifest sync (regression test for gossip catch-up bug)
// =========================================================================

/// Bug regression: a node that didn't receive manifest broadcasts (e.g.
/// because it joined after the objects were stored) can't list objects.
/// After calling `sync_manifests_from_peers`, it should see everything.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_new_node_lists_objects_after_manifest_sync() {
    let c = TestCluster::new(3, 1024, 2, 1).await;

    // Kill node 2 to simulate it joining AFTER the objects are stored.
    // put_object's broadcast via send_to will skip the dead node.
    c.kill_node(2).await;

    // Write 2 objects from different nodes.
    let data1 = test_data(2000);
    let data2 = test_data(3000);

    c.node(0)
        .put_object("b", "key.txt", &data1, BTreeMap::new())
        .await
        .unwrap();
    c.node(1)
        .put_object("b", "w.txt", &data2, BTreeMap::new())
        .await
        .unwrap();

    // Revive node 2 — simulating a late join.
    c.revive_node(2).await;

    // Before sync: node 2 knows nothing (list_objects is local-only).
    let before = c.node(2).list_objects("b", "").await.unwrap();
    assert!(
        before.is_empty(),
        "node 2 should have 0 objects before sync"
    );

    // Explicit sync pulls manifests from peers.
    c.node(2).sync_manifests_from_peers().await.unwrap();

    let mut keys_after = c.node(2).list_objects("b", "").await.unwrap();
    keys_after.sort();
    assert_eq!(
        keys_after,
        vec!["key.txt", "w.txt"],
        "node 2 should list both objects after sync"
    );

    // Node 2 should also be able to read the objects via shard pull.
    let (got1, _) = c.node(2).get_object("b", "key.txt").await.unwrap();
    assert_eq!(got1, data1);
    let (got2, _) = c.node(2).get_object("b", "w.txt").await.unwrap();
    assert_eq!(got2, data2);
}

/// Sync is idempotent: calling it twice doesn't duplicate entries.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_manifest_sync_idempotent() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let data = test_data(2000);

    // Kill node 2 so it misses the broadcast.
    c.kill_node(2).await;

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    c.revive_node(2).await;

    // First sync: picks up the manifest.
    let synced1 = c.node(2).sync_manifests_from_peers().await.unwrap();
    assert_eq!(synced1, 1);

    // Second sync: nothing new.
    let synced2 = c.node(2).sync_manifests_from_peers().await.unwrap();
    assert_eq!(synced2, 0);

    let keys = c.node(2).list_objects("b", "").await.unwrap();
    assert_eq!(keys.len(), 1);
}

/// Bug regression: when a node has a stale key mapping from before a
/// restart, `sync_manifests_from_peers` should update it to the latest
/// version from peers. Previously, sync only stored manifests for keys
/// that didn't exist locally, silently ignoring overwrites.
///
/// Scenario:
/// 1. Node 0 writes v1 of key.txt — all nodes receive the broadcast
/// 2. Node 2 goes down
/// 3. Node 0 overwrites key.txt with v2 — node 2 misses the broadcast
/// 4. Node 2 comes back up and syncs — should get v2, not stay on v1
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_manifest_sync_updates_stale_key_after_overwrite() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let v1 = test_data(2000);
    let v2 = test_data(3000);

    // Node 0 writes v1. put_object broadcasts via send_to → node 2 gets v1.
    c.node(0)
        .put_object("b", "key.txt", &v1, BTreeMap::new())
        .await
        .unwrap();

    // Verify node 2 has v1.
    let (got, _) = c.node(2).get_object("b", "key.txt").await.unwrap();
    assert_eq!(got, v1, "node 2 should have v1 from broadcast");

    // Kill node 2 — simulates a restart (it will miss subsequent broadcasts).
    c.kill_node(2).await;

    // Node 0 overwrites key.txt with v2. Broadcast won't reach node 2.
    c.node(0)
        .put_object("b", "key.txt", &v2, BTreeMap::new())
        .await
        .unwrap();

    // Verify other nodes have v2.
    let (got, _) = c.node(1).get_object("b", "key.txt").await.unwrap();
    assert_eq!(got, v2, "node 1 should have v2");

    // Revive node 2.
    c.revive_node(2).await;

    // Node 2 syncs from peers. It already has key.txt → v1_object_id.
    // The sync SHOULD detect that peers have a different (newer) ObjectId
    // and update the mapping.
    let synced = c.node(2).sync_manifests_from_peers().await.unwrap();
    assert!(
        synced >= 1,
        "sync should update stale key mapping (got {synced})"
    );

    // Node 2 should now read v2, not the stale v1.
    let (got, _) = c.node(2).get_object("b", "key.txt").await.unwrap();
    assert_eq!(got, v2, "node 2 should read v2 after sync, not stale v1");
}

/// Sync across multiple buckets works correctly.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_manifest_sync_multiple_buckets() {
    let c = TestCluster::new(3, 1024, 2, 1).await;

    // Kill node 2 so it misses the broadcasts.
    c.kill_node(2).await;

    c.node(0)
        .put_object("photos", "cat.jpg", &test_data(1000), BTreeMap::new())
        .await
        .unwrap();
    c.node(0)
        .put_object("docs", "readme.md", &test_data(500), BTreeMap::new())
        .await
        .unwrap();

    c.revive_node(2).await;

    // Node 2 syncs.
    let synced = c.node(2).sync_manifests_from_peers().await.unwrap();
    assert_eq!(synced, 2);

    assert_eq!(c.node(2).list_objects("photos", "").await.unwrap().len(), 1);
    assert_eq!(c.node(2).list_objects("docs", "").await.unwrap().len(), 1);
}

// =========================================================================
// LogTree integration tests
// =========================================================================

/// With LogTree: put_object appends a log entry and broadcasts to peers.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_logtree_put_broadcasts_to_peers() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let data = test_data(5000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    // The LogEntryBroadcast via send_to should have delivered the entry
    // to all peers' LogTrees. All nodes should resolve the key.
    for i in 0..3 {
        let (got, _) = c.node(i).get_object("b", "k").await.unwrap();
        assert_eq!(got, data, "logtree node {i} should read the object");
    }
}

/// With LogTree: delete_object appends a delete log entry.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_logtree_delete_object() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let data = test_data(2000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    // All nodes see it.
    let (got, _) = c.node(1).get_object("b", "k").await.unwrap();
    assert_eq!(got, data);

    // Node 1 deletes.
    c.node(1).delete_object("b", "k").await.unwrap();

    // Node 1 should see the delete (resolve returns None).
    let result = c.node(1).get_object("b", "k").await;
    assert!(result.is_err(), "deleted object should not be found");
}

/// With LogTree: get_object reads from LogTree's materialized state.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_logtree_get_reads_from_materialized_state() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let data = test_data(3000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    // Verify has_object and list_objects also use LogTree.
    assert!(c.node(0).has_object("b", "k").await.unwrap());
    assert!(c.node(1).has_object("b", "k").await.unwrap());

    let keys = c.node(2).list_objects("b", "").await.unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0], "k");
}

/// With LogTree: sync_log_from_peers recovers missed entries.
/// With LogTree: put_object also stores manifest in MetaStore so that
/// ManifestRequest handlers (peer pulls) can serve it immediately.
/// This is critical for read-after-write consistency: when node B asks
/// the writer for a manifest via QUIC, the handler looks in MetaStore.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_logtree_put_also_writes_metastore() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let data = test_data(5000);

    let oid = c
        .node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    // The manifest must be in MetaStore (not just LogTree) so the
    // ManifestRequest handler can serve it to peers.
    let meta = c.node(0).meta();
    let got_oid = meta.get_object_key("b", "k").unwrap();
    assert_eq!(
        got_oid,
        Some(oid),
        "MetaStore should have object key after LogTree put"
    );

    let got_manifest = meta.get_manifest(&oid).unwrap();
    assert!(
        got_manifest.is_some(),
        "MetaStore should have manifest after LogTree put"
    );
}

/// With LogTree: delete_object also removes from MetaStore so that
/// ManifestRequest handlers don't serve stale manifests.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_logtree_delete_also_clears_metastore() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let data = test_data(2000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    assert!(c.node(0).meta().get_object_key("b", "k").unwrap().is_some());

    c.node(0).delete_object("b", "k").await.unwrap();

    assert!(
        c.node(0).meta().get_object_key("b", "k").unwrap().is_none(),
        "MetaStore should not have object key after delete"
    );
}

/// With LogTree: sync_log_from_peers recovers missed entries.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_logtree_sync_from_peers() {
    let c = TestCluster::new(3, 1024, 2, 1).await;

    // Kill node 2 so it misses broadcasts.
    c.kill_node(2).await;

    let data1 = test_data(2000);
    let data2 = test_data(3000);

    c.node(0)
        .put_object("b", "key1.txt", &data1, BTreeMap::new())
        .await
        .unwrap();
    c.node(1)
        .put_object("b", "key2.txt", &data2, BTreeMap::new())
        .await
        .unwrap();

    // Revive node 2.
    c.revive_node(2).await;

    // Node 2 has no objects before sync.
    let keys_before = c.node(2).list_objects("b", "").await.unwrap();
    assert_eq!(
        keys_before.len(),
        0,
        "node 2 should have 0 objects before sync"
    );

    // Sync log entries from peers.
    let synced = c.node(2).sync_log_from_peers().await.unwrap();
    assert!(
        synced >= 2,
        "should have synced at least 2 entries, got {synced}"
    );

    // Node 2 should now see both objects.
    let mut keys_after = c.node(2).list_objects("b", "").await.unwrap();
    keys_after.sort();
    assert_eq!(keys_after, vec!["key1.txt", "key2.txt"]);

    // And should be able to read them.
    let (got1, _) = c.node(2).get_object("b", "key1.txt").await.unwrap();
    assert_eq!(got1, data1);
    let (got2, _) = c.node(2).get_object("b", "key2.txt").await.unwrap();
    assert_eq!(got2, data2);
}

/// With LogTree: overwrite scenario — put v1, kill node, put v2, revive, sync → reads v2.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_logtree_overwrite_after_sync() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let v1 = test_data(2000);
    let v2 = test_data(3000);

    // Node 0 writes v1, all nodes get it.
    c.node(0)
        .put_object("b", "key.txt", &v1, BTreeMap::new())
        .await
        .unwrap();

    let (got, _) = c.node(2).get_object("b", "key.txt").await.unwrap();
    assert_eq!(got, v1, "node 2 should have v1");

    // Kill node 2.
    c.kill_node(2).await;

    // Node 0 overwrites with v2.
    c.node(0)
        .put_object("b", "key.txt", &v2, BTreeMap::new())
        .await
        .unwrap();

    // Revive node 2, sync.
    c.revive_node(2).await;
    let synced = c.node(2).sync_log_from_peers().await.unwrap();
    assert!(synced >= 1, "should sync the v2 entry");

    // Node 2 should read v2 via LWW.
    let (got, _) = c.node(2).get_object("b", "key.txt").await.unwrap();
    assert_eq!(got, v2, "node 2 should read v2 after sync");
}

// =========================================================================
// Regression: writer stores all shards locally (shard push failure durability)
// =========================================================================

/// When ALL remote shard pushes fail during write (all peers disconnected),
/// the writer must hold ALL shards locally so other nodes can pull them later.
///
/// Regression test for: "not enough shards for chunk 0: need 2, found 1"
/// when reading from a non-writer node after push targets were unreachable.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_write_with_disconnected_peers_then_read_from_other() {
    // 3-node cluster, k=2 m=1 → 3 shards per chunk, shard_replication=1.
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let data = test_data(10_000); // ~10 chunks

    // Disconnect nodes 1 and 2 at the transport level — they stay in the
    // ring (shards are assigned to them) but pushes will fail.
    c.disconnect_node(1).await;
    c.disconnect_node(2).await;

    // Write from node 0. The ring assigns shards to all 3 nodes, but
    // pushes to nodes 1 and 2 fail. All pushes fail, so ACK cleanup
    // doesn't trigger and the writer keeps everything.
    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    // Verify: the writer should have stored ALL shards locally because
    // every push failed (nothing was ACK'd and cleaned up).
    let writer_shards = c.local_shard_count(0).await;
    let manifest = c.node(0).head_object("b", "k").await.unwrap();
    let total_shards: usize = manifest.chunks.iter().map(|ch| ch.shards.len()).sum();
    assert_eq!(
        writer_shards, total_shards,
        "writer must store ALL {total_shards} shards locally when all pushes fail, but only has {writer_shards}"
    );

    // Reconnect nodes and propagate manifest.
    c.reconnect_node(1).await;
    c.reconnect_node(2).await;
    c.broadcast_manifest(0, "b", "k").await;

    // Non-writer (node 1) should pull all shards from the writer and
    // read successfully.
    let (got, _) = c.node(1).get_object("b", "k").await.unwrap();
    assert_eq!(got, data, "non-writer should read by pulling from writer");
}

// =========================================================================
// ACK-based shard cleanup
// =========================================================================

/// When all peers are reachable, the writer should delete local copies of
/// shards that were successfully pushed to their ring owners (and which the
/// writer does not own). This prevents the writer from keeping all k+m
/// shards per chunk, which would defeat erasure coding distribution.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_ack_cleanup_frees_non_owned_shards() {
    // 6-node cluster, k=4 m=2 → 6 shards per chunk. All nodes reachable.
    let c = TestCluster::new(6, 1024, 4, 2).await;
    let data = test_data(5000); // ~5 chunks → 30 total shards

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k").await;

    let manifest = c.node(0).head_object("b", "k").await.unwrap();
    let total_shards: usize = manifest.chunks.iter().map(|ch| ch.shards.len()).sum();

    // The writer should have FEWER shards than total because ACK'd
    // non-owned copies were deleted.
    let writer_shards = c.local_shard_count(0).await;
    assert!(
        writer_shards < total_shards,
        "writer should have fewer than {total_shards} shards after ACK cleanup, got {writer_shards}"
    );

    // All 6 nodes can still read the object.
    for i in 0..6 {
        let (got, _) = c.node(i).get_object("b", "k").await.unwrap();
        assert_eq!(got, data, "node {i} should read the object");
    }
}

/// When some peers are disconnected during write, the writer queues failed
/// pushes for background retry while still cleaning up ACK'd pushes.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_partial_disconnect_queues_failed_pushes() {
    // 3-node cluster, k=2 m=1 → 3 shards per chunk.
    let c = TestCluster::new(3, 1024, 2, 1).await;

    // Disconnect node 2 only — node 1 stays reachable.
    c.disconnect_node(2).await;

    let data = test_data(5000); // ~5 chunks

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    // Writer should have pending pushes (shards destined for node 2).
    let pending = c.node(0).pending_push_count();
    assert!(
        pending > 0,
        "writer should have pending pushes for disconnected node 2, got {pending}"
    );

    let manifest = c.node(0).head_object("b", "k").await.unwrap();
    let total_shards: usize = manifest.chunks.iter().map(|ch| ch.shards.len()).sum();

    // The writer should NOT have all shards — ACK'd pushes to node 1
    // should have been cleaned up.
    let writer_shards = c.local_shard_count(0).await;
    assert!(
        writer_shards < total_shards,
        "writer should have fewer than {total_shards} shards (ACK'd ones cleaned), got {writer_shards}"
    );
}

/// After reconnecting a previously disconnected node, calling
/// `retry_pending_pushes()` should successfully push the queued shards
/// and clean up local copies.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_retry_pending_pushes_cleans_up() {
    // 3-node cluster, k=2 m=1 → 3 shards per chunk.
    let c = TestCluster::new(3, 1024, 2, 1).await;

    // Disconnect node 2.
    c.disconnect_node(2).await;

    let data = test_data(5000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k").await;

    // Verify pending pushes exist.
    assert!(
        c.node(0).pending_push_count() > 0,
        "should have pending pushes before retry"
    );

    // Reconnect node 2 and retry.
    c.reconnect_node(2).await;
    let succeeded = c.node(0).retry_pending_pushes().await;
    assert!(succeeded > 0, "retry should succeed after reconnect");

    // No more pending pushes.
    assert_eq!(
        c.node(0).pending_push_count(),
        0,
        "pending pushes should be drained after retry"
    );

    // All nodes can read the object.
    for i in 0..3 {
        let (got, _) = c.node(i).get_object("b", "k").await.unwrap();
        assert_eq!(got, data, "node {i} should read after retry");
    }
}

// =========================================================================
// Torture test bug reproductions
// =========================================================================

/// Reproduction of torture test Bug 1: read fails on large binary objects
/// after ACK-based cleanup.
///
/// Error from torture test:
///   `engine error: read failed: not enough shards for chunk 0: need 2, found 1`
///
/// The hypothesis: ACK cleanup deletes non-owned shards from the writer
/// after successful push. With shard_replication=1, each shard lives on
/// exactly one ring owner. If the reader can't reach some ring owners
/// (or the ring assigns shards to a node that doesn't have them), reads fail.
///
/// With mock transport (all nodes reachable), this should pass. If it fails,
/// the ACK cleanup logic has a bug. In the real cluster, this manifests
/// because QUIC pushes may succeed from the transport's perspective but the
/// remote node drops the shard (e.g. backpressure, disk full).
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_bug1_large_object_read_after_ack_cleanup() {
    // 4-node cluster matching torture test: k=2 m=2 → 4 shards/chunk.
    // shard_replication=1 (each shard on exactly one node).
    let c = TestCluster::new(4, 262_144, 2, 2).await;

    // 15 MB of random-ish data (matches torture test's failing case).
    let data = test_data(15 * 1024 * 1024);

    // Write from node 0.
    c.node(0)
        .put_object("b", "large-binary", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "large-binary").await;

    // After ACK cleanup, the writer should NOT have all shards — only the
    // ones it's a ring owner for.
    let manifest = c.node(0).head_object("b", "large-binary").await.unwrap();
    let total_shards: usize = manifest.chunks.iter().map(|ch| ch.shards.len()).sum();
    let writer_shards = c.local_shard_count(0).await;
    assert!(
        writer_shards < total_shards,
        "ACK cleanup should have removed some shards from writer: {writer_shards}/{total_shards}"
    );

    // BUG 1: read from writer — if ACK cleanup was too aggressive or shards
    // weren't properly stored on remote nodes, this fails with:
    //   "not enough shards for chunk N: need 2, found 1"
    let (got, _) = c
        .node(0)
        .get_object("b", "large-binary")
        .await
        .expect("BUG 1: read failed after ACK cleanup on large object");
    assert_eq!(got.len(), data.len(), "data length mismatch");
    assert_eq!(got, data, "data content mismatch");

    // Also verify all other nodes can read.
    for i in 1..4 {
        let (got, _) = c
            .node(i)
            .get_object("b", "large-binary")
            .await
            .unwrap_or_else(|e| panic!("BUG 1: node {i} failed to read large object: {e}"));
        assert_eq!(got, data, "node {i} data mismatch");
    }
}

/// Reproduction of torture test Bug 4: no cross-node replication.
///
/// An object written to node 0 should be visible from node 1 WITHOUT
/// manual `broadcast_manifest`. In the mock transport, `send_to` delivers
/// manifest messages directly to the peer's MetaStore. If this test
/// passes with mock transport but fails in the real cluster, the bug is
/// in the iroh QUIC transport layer (messages not being delivered).
///
/// Note: the existing `test_logtree_put_broadcasts_to_peers` tests this
/// for LogTree mode. This test covers the non-LogTree (MetaStore) path
/// which is what the torture test's `shoald` cluster uses by default.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_bug4_cross_node_visibility_without_manual_broadcast() {
    // 4-node cluster matching torture test setup (ports 4821-4824).
    let c = TestCluster::new(4, 1024, 2, 2).await;
    let data = test_data(5000);

    // Write on node 0 — put_object internally broadcasts the manifest
    // to all peers via transport.send_to (LogEntryBroadcast message).
    c.node(0)
        .put_object("b", "cross-node-test", &data, BTreeMap::new())
        .await
        .unwrap();

    // BUG 4: read from other nodes WITHOUT calling broadcast_manifest.
    // In the real cluster, this fails with NoSuchKey because the LogTree/
    // manifest broadcast never reaches the other nodes.
    for i in 1..4 {
        let result = c.node(i).get_object("b", "cross-node-test").await;
        assert!(
            result.is_ok(),
            "BUG 4: node {i} cannot see object written to node 0 — \
             cross-node replication failed: {:?}",
            result.err()
        );
        let (got, _) = result.unwrap();
        assert_eq!(got, data, "node {i} data mismatch");
    }

    // Also verify list_objects works across nodes.
    for i in 1..4 {
        let keys = c.node(i).list_objects("b", "").await.unwrap();
        assert!(
            keys.contains(&"cross-node-test".to_string()),
            "BUG 4: node {i} list_objects doesn't include the key"
        );
    }
}

/// Same as Bug 4 but with LogTree mode (which is what the real shoald uses).
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_bug4_cross_node_visibility_logtree_mode() {
    let c = TestCluster::new(4, 1024, 2, 2).await;
    let data = test_data(5000);

    // Write on node 0.
    c.node(0)
        .put_object("b", "cross-node-lt", &data, BTreeMap::new())
        .await
        .unwrap();

    // Read from all other nodes without any manual sync.
    for i in 1..4 {
        let result = c.node(i).get_object("b", "cross-node-lt").await;
        assert!(
            result.is_ok(),
            "BUG 4 (LogTree): node {i} cannot see object written to node 0: {:?}",
            result.err()
        );
        let (got, _) = result.unwrap();
        assert_eq!(got, data, "node {i} data mismatch");
    }
}

// =========================================================================
// Chaos tests — fault injection scenarios
// =========================================================================

/// Chaos test for Bug 1: large object read with network latency.
///
/// With transport latency injected, shards must still be fully distributed
/// before PUT returns, and reads from all nodes must succeed.
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_chaos_large_object_read_with_latency() {
    let chaos = ChaosConfig {
        latency_ms: (10, 50),
        drop_rate: 0.0,
        seed: 42,
        ..Default::default()
    };

    let c = TestCluster::with_chaos(4, 4096, 2, 2, chaos).await;

    // 1 MB object — enough chunks to exercise the pipeline under latency.
    let data = test_data(1_048_576);

    c.node(0)
        .put_object("b", "large", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "large").await;

    // Read from the writer.
    let (got, _) = c
        .node(0)
        .get_object("b", "large")
        .await
        .expect("writer should read its own large object under latency");
    assert_eq!(got, data);

    // Read from all other nodes.
    for i in 1..4 {
        let (got, _) = c
            .node(i)
            .get_object("b", "large")
            .await
            .unwrap_or_else(|e| panic!("node {i} failed to read large object under latency: {e}"));
        assert_eq!(got, data, "node {i} data mismatch");
    }
}

/// Chaos test for Bug 2: concurrent writes with slow store IO.
///
/// With per-operation store latency, 50 concurrent writes must all succeed
/// and be readable afterward.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_chaos_concurrent_writes_with_slow_store() {
    let chaos = ChaosConfig {
        store_write_latency_ms: (5, 15),
        store_read_latency_ms: (2, 8),
        seed: 42,
        ..Default::default()
    };

    let c = Arc::new(TestCluster::with_chaos(3, 1024, 2, 1, chaos).await);
    let n_objects = 50;

    // Spawn concurrent writers.
    let mut handles = Vec::new();

    for j in 0..n_objects {
        let cluster = c.clone();
        handles.push(tokio::spawn(async move {
            let key = format!("obj-{j}");
            let data = test_data(500 + j * 37);
            cluster
                .node(j % 3)
                .put_object("b", &key, &data, BTreeMap::new())
                .await
                .unwrap_or_else(|e| panic!("concurrent write {key} failed: {e}"));
            cluster.broadcast_manifest(j % 3, "b", &key).await;
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Read back all objects from each node.
    for j in 0..n_objects {
        let key = format!("obj-{j}");
        let expected = test_data(500 + j * 37);

        for reader in 0..3 {
            let (got, _) = c
                .node(reader)
                .get_object("b", &key)
                .await
                .unwrap_or_else(|e| panic!("node {reader} failed to read {key}: {e}"));
            assert_eq!(got, expected, "node {reader} data mismatch for {key}");
        }
    }
}

/// Chaos test for Bug 4: cross-node visibility with packet drops.
///
/// With a 10% drop rate, the manifest broadcast from `put_object` may not
/// reach all nodes. After retrying via `sync_manifests_from_peers`, the
/// object must become visible.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_chaos_cross_node_with_drop() {
    let chaos = ChaosConfig {
        latency_ms: (10, 30),
        drop_rate: 0.10,
        seed: 42,
        ..Default::default()
    };

    let c = TestCluster::with_chaos(4, 1024, 2, 2, chaos).await;
    let data = test_data(5000);

    // Write on node 0 — broadcast may be dropped for some peers.
    c.node(0)
        .put_object("b", "dropped", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "dropped").await;

    // Try to read from node 1. If the broadcast was dropped, sync first.
    let result = c.node(1).get_object("b", "dropped").await;

    if result.is_err() {
        // Manifest didn't arrive — sync from peers (this uses pull_manifests
        // which also goes through chaos, but retries internally).
        let synced = c.node(1).sync_manifests_from_peers().await.unwrap();
        assert!(synced >= 1, "sync should recover the dropped manifest");
    }

    // Now it must be readable.
    let (got, _) = c
        .node(1)
        .get_object("b", "dropped")
        .await
        .expect("node 1 should read after sync recovery from drops");
    assert_eq!(got, data);
}

/// Chaos test: network partition then heal.
///
/// Node 2 is partitioned from nodes 0 and 1. A new object written on
/// node 0 won't reach node 2 via broadcast. After healing, retrying
/// pending pushes, and syncing manifests, node 2 must see the object.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_chaos_partition_then_heal() {
    let chaos = ChaosConfig {
        latency_ms: (5, 10),
        seed: 42,
        ..Default::default()
    };

    let c = TestCluster::with_chaos(4, 1024, 2, 2, chaos).await;
    let data_before = test_data(3000);
    let data_during = test_data(5000);
    let controller = c.chaos_controller.as_ref().unwrap();

    // Write an object while all nodes are reachable.
    c.node(0)
        .put_object("b", "before", &data_before, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "before").await;

    // All nodes can read it.
    let (got, _) = c.node(2).get_object("b", "before").await.unwrap();
    assert_eq!(got, data_before, "node 2 should read 'before' object");

    // Partition node 2 from ALL other nodes (full isolation).
    controller.partition(c.node_ids[0], c.node_ids[2]).await;
    controller.partition(c.node_ids[1], c.node_ids[2]).await;
    controller.partition(c.node_ids[3], c.node_ids[2]).await;

    // Write a new object on node 0 — the transport broadcast to node 2
    // will be blocked by the partition. Shard pushes to node 2 also fail.
    c.node(0)
        .put_object("b", "during", &data_during, BTreeMap::new())
        .await
        .unwrap();

    // Node 2 should NOT see the "during" object (missed the broadcast).
    let keys = c.node(2).list_objects("b", "").await.unwrap();
    assert!(
        !keys.contains(&"during".to_string()),
        "node 2 should not see 'during' while partitioned"
    );

    // Node 2 can still read the pre-partition object.
    let (got, _) = c.node(2).get_object("b", "before").await.unwrap();
    assert_eq!(got, data_before, "pre-partition object still readable");

    // Heal the partition.
    controller.heal(c.node_ids[0], c.node_ids[2]).await;
    controller.heal(c.node_ids[1], c.node_ids[2]).await;
    controller.heal(c.node_ids[3], c.node_ids[2]).await;

    // Retry pending pushes on the writer — pushes that failed during the
    // partition (shards destined for node 2) will now succeed.
    let retried = c.node(0).retry_pending_pushes().await;
    assert!(
        retried > 0 || c.node(0).pending_push_count() == 0,
        "pending pushes should either succeed or already be drained"
    );

    // Node 2 syncs manifests from peers — should learn about "during".
    let synced = c.node(2).sync_manifests_from_peers().await.unwrap();
    assert!(synced >= 1, "sync should find the 'during' manifest");

    // Node 2 can now list both objects.
    let mut keys = c.node(2).list_objects("b", "").await.unwrap();
    keys.sort();
    assert!(
        keys.contains(&"during".to_string()),
        "node 2 should see 'during' after sync"
    );

    // Node 2 reads the new object (shards are available now via retry).
    let (got, _) = c
        .node(2)
        .get_object("b", "during")
        .await
        .expect("node 2 should read 'during' after partition healed");
    assert_eq!(got, data_during);

    // Verify chaos stats show some partitioned messages.
    let stats = controller.stats();
    assert!(
        stats.partitioned > 0,
        "chaos stats should show partitioned messages: {stats:?}"
    );
}

// =========================================================================
// Advanced chaos tests — QUIC-style network misbehaviour
// =========================================================================

/// Cascading node failures under network latency.
///
/// 7-node cluster with high latency. Write objects, then kill nodes one by one.
/// After each kill, verify that surviving nodes can still read all data.
/// Simulates the cascading failure pattern that QUIC connections experience
/// when a rack switch goes down and nodes timeout one after another.
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_chaos_cascading_node_failures_with_latency() {
    let chaos = ChaosConfig {
        latency_ms: (20, 80),
        drop_rate: 0.0,
        seed: 7777,
        ..Default::default()
    };

    // replication=4 so shards survive multiple node losses with k=2, m=1
    let c = TestCluster::with_chaos_and_replication(7, 1024, 2, 1, 4, chaos).await;

    // Write 10 objects from various nodes.
    let mut objects = Vec::new();
    for i in 0..10 {
        let writer = i % 7;
        let key = format!("cascade-{i}");
        let data = test_data(2000 + i * 100);
        c.node(writer)
            .put_object("b", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        c.broadcast_manifest(writer, "b", &key).await;
        objects.push((key, data));
    }

    // Kill nodes 6, 5, 4 one at a time (cascading), verifying after each.
    for kill_idx in (4..7).rev() {
        c.kill_node(kill_idx).await;

        // Pick a surviving reader.
        let reader = kill_idx.saturating_sub(4);
        for (key, expected) in &objects {
            let (got, _) = c
                .node(reader)
                .get_object("b", key)
                .await
                .unwrap_or_else(|e| {
                    panic!("node {reader} failed to read {key} after killing node {kill_idx}: {e}")
                });
            assert_eq!(
                &got, expected,
                "data mismatch for {key} after kill {kill_idx}"
            );
        }
    }
}

/// Flapping node: a node repeatedly goes down and comes back.
///
/// Simulates an unstable QUIC connection that keeps resetting —
/// the kind of thing you see with flaky NICs or overloaded NAT gateways.
/// Writes happen concurrently with the flapping. All data must be readable
/// after the flapping stops.
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_chaos_flapping_node() {
    let chaos = ChaosConfig {
        latency_ms: (5, 20),
        drop_rate: 0.0,
        seed: 1234,
        ..Default::default()
    };

    let c = Arc::new(TestCluster::with_chaos_and_replication(5, 1024, 2, 1, 2, chaos).await);

    let flapper_idx = 3;
    let n_objects = 30;

    // Spawn a task that flaps node 3 repeatedly.
    let flap_cluster = c.clone();
    let flap_handle = tokio::spawn(async move {
        for cycle in 0..6 {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            flap_cluster.disconnect_node(flapper_idx).await;
            tokio::time::sleep(tokio::time::Duration::from_millis(30)).await;
            flap_cluster.reconnect_node(flapper_idx).await;
            let _ = cycle; // suppress unused warning in release
        }
    });

    // Concurrently write objects from non-flapping nodes.
    let mut handles = Vec::new();
    for j in 0..n_objects {
        let cluster = c.clone();
        handles.push(tokio::spawn(async move {
            let writer = j % 3; // nodes 0, 1, 2 only
            let key = format!("flap-{j}");
            let data = test_data(800 + j * 50);
            cluster
                .node(writer)
                .put_object("b", &key, &data, BTreeMap::new())
                .await
                .unwrap_or_else(|e| panic!("write {key} from node {writer} failed: {e}"));
            cluster.broadcast_manifest(writer, "b", &key).await;
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
    flap_handle.await.unwrap();

    // Ensure flapper is reconnected.
    c.reconnect_node(flapper_idx).await;

    // Retry any pending pushes on all writers.
    for i in 0..3 {
        c.node(i).retry_pending_pushes().await;
    }

    // All objects must be readable from every surviving node.
    for j in 0..n_objects {
        let key = format!("flap-{j}");
        let expected = test_data(800 + j * 50);

        for reader in 0..5 {
            let (got, _) = c
                .node(reader)
                .get_object("b", &key)
                .await
                .unwrap_or_else(|e| panic!("node {reader} failed to read {key}: {e}"));
            assert_eq!(got, expected, "data mismatch for {key} on node {reader}");
        }
    }
}

/// High drop rate with replication: 30% packet loss, but replication=3
/// ensures enough shard copies land on enough nodes.
///
/// Simulates a badly congested network where QUIC streams experience
/// heavy packet loss. The replication factor compensates: even if a push
/// to one replica is dropped, others succeed.
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_chaos_high_drop_rate_with_replication() {
    let chaos = ChaosConfig {
        latency_ms: (5, 30),
        drop_rate: 0.30,
        seed: 999,
        ..Default::default()
    };

    let c = TestCluster::with_chaos_and_replication(5, 1024, 2, 1, 3, chaos).await;

    // Write 15 objects. Some shard pushes will be dropped, but enough
    // copies should land thanks to replication=3.
    let mut objects = Vec::new();
    for i in 0..15 {
        let writer = i % 5;
        let key = format!("drop-{i}");
        let data = test_data(1500 + i * 100);
        c.node(writer)
            .put_object("b", &key, &data, BTreeMap::new())
            .await
            .unwrap_or_else(|e| panic!("write {key} failed despite replication: {e}"));
        c.broadcast_manifest(writer, "b", &key).await;
        objects.push((key, data));
    }

    // Retry pending pushes — the dropped ones should succeed now
    // (the chaos RNG may still drop some, so retry a few rounds).
    for _round in 0..3 {
        for i in 0..5 {
            c.node(i).retry_pending_pushes().await;
        }
    }

    // The writer node always has all shards locally, so reads from
    // the writer must always work.
    for (i, (key, expected)) in objects.iter().enumerate() {
        let writer = i % 5;
        let (got, _) = c
            .node(writer)
            .get_object("b", key)
            .await
            .unwrap_or_else(|e| panic!("writer {writer} can't read {key}: {e}"));
        assert_eq!(&got, expected, "writer {writer} data mismatch for {key}");
    }
}

/// Simultaneous partitions isolating nodes into disjoint groups.
///
/// 6 nodes split into three groups: {0,1}, {2,3}, {4,5}.
/// Each group writes objects. After healing, manifest sync merges state.
/// Simulates a multi-rack datacenter where inter-rack links fail.
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_chaos_simultaneous_partitions() {
    let chaos = ChaosConfig {
        latency_ms: (5, 15),
        seed: 42,
        ..Default::default()
    };

    let c = TestCluster::with_chaos_and_replication(6, 1024, 2, 1, 2, chaos).await;
    let controller = c.chaos_controller.as_ref().unwrap();

    // Pre-partition: write a baseline object visible to all.
    let data_base = test_data(2000);
    c.node(0)
        .put_object("b", "baseline", &data_base, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "baseline").await;

    // Create 3 disjoint partitions: {0,1}, {2,3}, {4,5}.
    // Each group is isolated from the other two.
    let groups: Vec<Vec<usize>> = vec![vec![0, 1], vec![2, 3], vec![4, 5]];
    for (gi, group_a) in groups.iter().enumerate() {
        for (gj, group_b) in groups.iter().enumerate() {
            if gi >= gj {
                continue;
            }
            for &a in group_a {
                for &b in group_b {
                    controller.partition(c.node_ids[a], c.node_ids[b]).await;
                }
            }
        }
    }

    // Each group writes objects (only reachable within the group).
    let mut partition_objects: Vec<(String, Vec<u8>, usize)> = Vec::new();
    for (gi, group) in groups.iter().enumerate() {
        let writer = group[0];
        let key = format!("part-{gi}");
        let data = test_data(3000 + gi * 500);
        c.node(writer)
            .put_object("b", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        // Only broadcast within the group (the cross-group ones will be
        // blocked by the partition anyway).
        c.broadcast_manifest(writer, "b", &key).await;
        partition_objects.push((key, data, writer));
    }

    // Within each group, both nodes should see their local partition object.
    for (gi, group) in groups.iter().enumerate() {
        let key = format!("part-{gi}");
        for &node in group {
            let result = c.node(node).get_object("b", &key).await;
            assert!(
                result.is_ok(),
                "node {node} in group {gi} should read {key}"
            );
        }
    }

    // Heal all partitions.
    for (gi, group_a) in groups.iter().enumerate() {
        for (gj, group_b) in groups.iter().enumerate() {
            if gi >= gj {
                continue;
            }
            for &a in group_a {
                for &b in group_b {
                    controller.heal(c.node_ids[a], c.node_ids[b]).await;
                }
            }
        }
    }

    // Retry pending pushes on all writers.
    for i in 0..6 {
        c.node(i).retry_pending_pushes().await;
    }

    // Sync manifests so every node discovers objects from other partitions.
    for i in 0..6 {
        let _ = c.node(i).sync_manifests_from_peers().await;
    }

    // All 6 nodes should see the baseline object.
    for i in 0..6 {
        let (got, _) = c
            .node(i)
            .get_object("b", "baseline")
            .await
            .unwrap_or_else(|e| panic!("node {i} lost baseline after partition: {e}"));
        assert_eq!(got, data_base, "baseline mismatch on node {i}");
    }

    // All 6 nodes should see partition-local objects via their writers.
    for (key, expected, writer) in &partition_objects {
        let (got, _) = c
            .node(*writer)
            .get_object("b", key)
            .await
            .unwrap_or_else(|e| panic!("writer {writer} can't read {key} post-heal: {e}"));
        assert_eq!(
            &got, expected,
            "{key} mismatch on writer {writer} post-heal"
        );
    }

    // Stats should confirm some messages were blocked.
    let stats = controller.stats();
    assert!(
        stats.partitioned > 0,
        "should have partitioned messages: {stats:?}"
    );
}

/// Slow store reads during a write storm.
///
/// Simulates what happens when the underlying disk is saturated: reads
/// take 50-100ms while new writes keep arriving. Verifies no data
/// corruption and that both reads and writes eventually complete.
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_chaos_slow_store_during_write_storm() {
    let chaos = ChaosConfig {
        latency_ms: (2, 10),
        store_read_latency_ms: (20, 60),
        store_write_latency_ms: (10, 40),
        seed: 55,
        ..Default::default()
    };

    let c = Arc::new(TestCluster::with_chaos(5, 1024, 2, 1, chaos).await);

    let n_writers = 5;
    let objects_per_writer = 8;

    // Spawn concurrent writers.
    let mut handles = Vec::new();
    for w in 0..n_writers {
        let cluster = c.clone();
        handles.push(tokio::spawn(async move {
            for j in 0..objects_per_writer {
                let key = format!("storm-w{w}-{j}");
                let data = test_data(600 + w * 100 + j * 30);
                cluster
                    .node(w % 5)
                    .put_object("b", &key, &data, BTreeMap::new())
                    .await
                    .unwrap_or_else(|e| panic!("write {key} failed: {e}"));
                cluster.broadcast_manifest(w % 5, "b", &key).await;
            }
        }));
    }

    // Concurrently, some readers try to read objects as they become available.
    let read_cluster = c.clone();
    let reader_handle = tokio::spawn(async move {
        // Wait for some objects to be written and broadcast.
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        let mut successful_reads = 0u32;
        for attempt in 0..20 {
            let key = format!("storm-w0-{}", attempt % objects_per_writer);
            if read_cluster.node(1).get_object("b", &key).await.is_ok() {
                successful_reads += 1;
            }
            // Space out attempts so reads span the write storm duration.
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
        successful_reads
    });

    for h in handles {
        h.await.unwrap();
    }

    let reads_during_storm = reader_handle.await.unwrap();
    // Some reads should have succeeded even during the storm.
    assert!(
        reads_during_storm > 0,
        "at least some reads should succeed during write storm"
    );

    // After the storm, verify all objects are intact.
    for w in 0..n_writers {
        for j in 0..objects_per_writer {
            let key = format!("storm-w{w}-{j}");
            let expected = test_data(600 + w * 100 + j * 30);
            let writer = w % 5;
            let (got, _) = c
                .node(writer)
                .get_object("b", &key)
                .await
                .unwrap_or_else(|e| {
                    panic!("post-storm read {key} from writer {writer} failed: {e}")
                });
            assert_eq!(got, expected, "post-storm data mismatch for {key}");
        }
    }
}

/// Rolling restart under writes: nodes are killed and revived one at a
/// time while writes keep happening.
///
/// Simulates a rolling upgrade scenario where each node in turn restarts
/// its QUIC endpoint. With replication=3, the cluster must remain available
/// throughout.
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_chaos_write_during_rolling_restart() {
    let chaos = ChaosConfig {
        latency_ms: (5, 15),
        seed: 321,
        ..Default::default()
    };

    let c = Arc::new(TestCluster::with_chaos_and_replication(5, 1024, 2, 1, 3, chaos).await);

    let n_objects = 20;
    let mut all_objects: Vec<(String, Vec<u8>)> = Vec::new();
    let mut obj_counter = 0usize;

    // Rolling restart: kill node i, write some objects, revive node i.
    for restart_idx in 0..5 {
        c.kill_node(restart_idx).await;

        // Write a few objects while node is down, using a non-killed writer.
        let writer = (restart_idx + 1) % 5;
        for _ in 0..4 {
            let key = format!("rolling-{obj_counter}");
            let data = test_data(1000 + obj_counter * 60);
            c.node(writer)
                .put_object("b", &key, &data, BTreeMap::new())
                .await
                .unwrap_or_else(|e| {
                    panic!(
                        "write {key} on node {writer} failed while node {restart_idx} is down: {e}"
                    )
                });
            c.broadcast_manifest(writer, "b", &key).await;
            all_objects.push((key, data));
            obj_counter += 1;
        }

        // Revive the node.
        c.revive_node(restart_idx).await;

        // Retry pending pushes.
        for i in 0..5 {
            c.node(i).retry_pending_pushes().await;
        }
    }

    assert_eq!(all_objects.len(), n_objects);

    // After all restarts, every object should be readable from the writer.
    for (idx, (key, expected)) in all_objects.iter().enumerate() {
        let writer = ((idx / 4) + 1) % 5;
        let (got, _) = c
            .node(writer)
            .get_object("b", key)
            .await
            .unwrap_or_else(|e| panic!("writer {writer} can't read {key}: {e}"));
        assert_eq!(
            &got, expected,
            "data mismatch for {key} post-rolling-restart"
        );
    }
}

/// Split brain: both sides of a partition write to the same key.
///
/// During the partition each side can only read its own version. After
/// healing and syncing, every writer must still be able to read the
/// version it wrote (the shards are still on its local store). The key
/// invariant is **no data corruption** — whatever a node reads must be
/// one of the two valid versions, never a franken-mix.
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_chaos_split_brain_same_key() {
    let chaos = ChaosConfig {
        latency_ms: (5, 10),
        seed: 42,
        ..Default::default()
    };

    let c = TestCluster::with_chaos_and_replication(4, 1024, 2, 1, 2, chaos).await;
    let controller = c.chaos_controller.as_ref().unwrap();

    let v1 = test_data(3000);
    let v2 = test_data(4000);

    // Partition: {0,1} vs {2,3}.
    controller.partition(c.node_ids[0], c.node_ids[2]).await;
    controller.partition(c.node_ids[0], c.node_ids[3]).await;
    controller.partition(c.node_ids[1], c.node_ids[2]).await;
    controller.partition(c.node_ids[1], c.node_ids[3]).await;

    // Side A writes v1, side B writes v2 (same bucket/key).
    // Do NOT call broadcast_manifest — let the natural unicast from
    // put_object go through the chaos transport so the partition is
    // respected. Each side only sees its own version.
    c.node(0)
        .put_object("b", "conflict", &v1, BTreeMap::new())
        .await
        .unwrap();

    c.node(2)
        .put_object("b", "conflict", &v2, BTreeMap::new())
        .await
        .unwrap();

    // Each writer should read its own version (shards are local).
    let (got_a, _) = c.node(0).get_object("b", "conflict").await.unwrap();
    assert_eq!(got_a, v1, "side A should see v1 during partition");
    let (got_b, _) = c.node(2).get_object("b", "conflict").await.unwrap();
    assert_eq!(got_b, v2, "side B should see v2 during partition");

    // Heal.
    controller.heal(c.node_ids[0], c.node_ids[2]).await;
    controller.heal(c.node_ids[0], c.node_ids[3]).await;
    controller.heal(c.node_ids[1], c.node_ids[2]).await;
    controller.heal(c.node_ids[1], c.node_ids[3]).await;

    // Retry pending pushes so shards land on their ring owners.
    for _round in 0..3 {
        for i in 0..4 {
            c.node(i).retry_pending_pushes().await;
        }
    }

    // Each writer must STILL be able to read its version from local shards
    // (pending push retries may redistribute, but writers always have copies).
    let (got_a, _) = c.node(0).get_object("b", "conflict").await.unwrap();
    let (got_b, _) = c.node(2).get_object("b", "conflict").await.unwrap();

    // The key invariant: no corruption. Each writer reads either v1 or v2.
    assert!(
        got_a == v1 || got_a == v2,
        "node 0 post-heal: data must be v1 or v2, not corrupted (len={})",
        got_a.len()
    );
    assert!(
        got_b == v1 || got_b == v2,
        "node 2 post-heal: data must be v1 or v2, not corrupted (len={})",
        got_b.len()
    );

    // Verify the chaos layer exercised partitions.
    let stats = controller.stats();
    assert!(
        stats.partitioned > 0,
        "should have partitioned messages: {stats:?}"
    );
}

/// Intermittent transport with many objects: random drops + latency
/// combined with 100 objects. After retries and sync, all objects must
/// be readable from their writers.
///
/// Simulates a congested WAN link where QUIC streams experience both
/// latency spikes and packet drops.
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_chaos_intermittent_transport_many_objects() {
    let chaos = ChaosConfig {
        latency_ms: (10, 40),
        drop_rate: 0.15,
        seed: 808,
        ..Default::default()
    };

    let c = TestCluster::with_chaos_and_replication(5, 1024, 2, 1, 2, chaos).await;

    let n_objects = 100;
    let mut objects: Vec<(String, Vec<u8>, usize)> = Vec::new();

    for i in 0..n_objects {
        let writer = i % 5;
        let key = format!("inter-{i:03}");
        let data = test_data(500 + i * 30);
        c.node(writer)
            .put_object("b", &key, &data, BTreeMap::new())
            .await
            .unwrap_or_else(|e| panic!("write {key} on node {writer} failed: {e}"));
        c.broadcast_manifest(writer, "b", &key).await;
        objects.push((key, data, writer));
    }

    // Multiple retry rounds to flush pending pushes through the lossy link.
    for _round in 0..5 {
        for i in 0..5 {
            c.node(i).retry_pending_pushes().await;
        }
    }

    // Sync manifests in case broadcasts were dropped.
    for i in 0..5 {
        let _ = c.node(i).sync_manifests_from_peers().await;
    }

    // All objects must be readable from their writer (writers always have
    // local shards, so this must succeed regardless of network chaos).
    for (key, expected, writer) in &objects {
        let (got, _) = c
            .node(*writer)
            .get_object("b", key)
            .await
            .unwrap_or_else(|e| panic!("writer {writer} can't read {key}: {e}"));
        assert_eq!(&got, expected, "data mismatch for {key} on writer {writer}");
    }

    // Verify chaos stats show some dropped messages.
    let stats = c.chaos_controller.as_ref().unwrap().stats();
    assert!(stats.dropped > 0, "should have dropped messages: {stats:?}");
    assert!(
        stats.delivered > stats.dropped,
        "most messages should still be delivered: {stats:?}"
    );
}

/// All faults combined: latency + drops + slow store + partitions + node kills.
///
/// The kitchen sink test. If the system survives this, it can handle
/// whatever QUIC throws at it. A 7-node cluster with:
/// - 15-60ms transport latency
/// - 5% message drop rate
/// - 10-30ms store IO latency
/// - One node permanently killed
/// - One node temporarily partitioned then healed
///
/// 30 objects written and verified.
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_chaos_all_faults_combined() {
    let chaos = ChaosConfig {
        latency_ms: (15, 60),
        drop_rate: 0.05,
        store_read_latency_ms: (10, 30),
        store_write_latency_ms: (10, 30),
        seed: 1337,
    };

    let c = TestCluster::with_chaos_and_replication(7, 1024, 2, 1, 3, chaos).await;
    let controller = c.chaos_controller.as_ref().unwrap();

    // Phase 1: write 10 objects normally.
    let mut objects: Vec<(String, Vec<u8>, usize)> = Vec::new();
    for i in 0..10 {
        let writer = i % 7;
        let key = format!("chaos-all-{i}");
        let data = test_data(1000 + i * 100);
        c.node(writer)
            .put_object("b", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        c.broadcast_manifest(writer, "b", &key).await;
        objects.push((key, data, writer));
    }

    // Phase 2: kill node 6, partition node 5 from nodes 0-3.
    c.kill_node(6).await;
    controller.partition(c.node_ids[5], c.node_ids[0]).await;
    controller.partition(c.node_ids[5], c.node_ids[1]).await;
    controller.partition(c.node_ids[5], c.node_ids[2]).await;
    controller.partition(c.node_ids[5], c.node_ids[3]).await;

    // Phase 3: write 10 more objects under degraded conditions.
    for i in 10..20 {
        let writer = i % 5; // only nodes 0-4 as writers
        let key = format!("chaos-all-{i}");
        let data = test_data(1000 + i * 100);
        c.node(writer)
            .put_object("b", &key, &data, BTreeMap::new())
            .await
            .unwrap_or_else(|e| panic!("write {key} under chaos failed: {e}"));
        c.broadcast_manifest(writer, "b", &key).await;
        objects.push((key, data, writer));
    }

    // Phase 4: heal partition, write 10 more.
    controller.heal(c.node_ids[5], c.node_ids[0]).await;
    controller.heal(c.node_ids[5], c.node_ids[1]).await;
    controller.heal(c.node_ids[5], c.node_ids[2]).await;
    controller.heal(c.node_ids[5], c.node_ids[3]).await;

    for i in 20..30 {
        let writer = i % 6; // nodes 0-5, node 6 still dead
        let key = format!("chaos-all-{i}");
        let data = test_data(1000 + i * 100);
        c.node(writer)
            .put_object("b", &key, &data, BTreeMap::new())
            .await
            .unwrap_or_else(|e| panic!("write {key} post-heal failed: {e}"));
        c.broadcast_manifest(writer, "b", &key).await;
        objects.push((key, data, writer));
    }

    // Phase 5: retry and sync.
    for _round in 0..3 {
        for i in 0..6 {
            c.node(i).retry_pending_pushes().await;
        }
    }
    for i in 0..6 {
        let _ = c.node(i).sync_manifests_from_peers().await;
    }

    // Phase 6: verify all 30 objects from their writers.
    for (key, expected, writer) in &objects {
        let (got, _) = c
            .node(*writer)
            .get_object("b", key)
            .await
            .unwrap_or_else(|e| panic!("writer {writer} can't read {key}: {e}"));
        assert_eq!(&got, expected, "data mismatch for {key}");
    }

    // Verify the chaos layer exercised all fault types.
    let stats = controller.stats();
    assert!(stats.delivered > 0, "should have delivered: {stats:?}");
    assert!(stats.dropped > 0, "should have dropped: {stats:?}");
    assert!(stats.partitioned > 0, "should have partitioned: {stats:?}");
}

/// Writer reads from local shards even when all peers are partitioned.
///
/// When every QUIC connection is broken, the writer must still serve reads
/// from its own store. This validates the local-first read path.
///
/// The key insight: we partition BEFORE writing so all remote pushes fail,
/// forcing the writer to retain every shard locally (the ACK-based cleanup
/// only deletes local copies after a *successful* push).
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_chaos_read_under_full_partition_uses_local_shards() {
    let chaos = ChaosConfig {
        latency_ms: (5, 10),
        seed: 42,
        ..Default::default()
    };

    let c = TestCluster::with_chaos_and_replication(5, 1024, 2, 1, 3, chaos).await;
    let controller = c.chaos_controller.as_ref().unwrap();

    // Fully isolate node 0 from everyone BEFORE writing.
    // This ensures all pushes fail and node 0 retains all shards locally.
    for i in 1..5 {
        controller.partition(c.node_ids[0], c.node_ids[i]).await;
    }

    // Write objects while fully partitioned — all pushes will fail.
    let mut objects = Vec::new();
    for i in 0..5 {
        let key = format!("isolated-{i}");
        let data = test_data(2000 + i * 200);
        c.node(0)
            .put_object("b", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        objects.push((key, data));
    }

    // Node 0 should read all objects from its local shards despite isolation.
    for (key, expected) in &objects {
        let (got, _) = c
            .node(0)
            .get_object("b", key)
            .await
            .unwrap_or_else(|e| panic!("isolated node 0 can't read {key}: {e}"));
        assert_eq!(&got, expected, "isolated read mismatch for {key}");
    }

    // Heal the partition.
    for i in 1..5 {
        controller.heal(c.node_ids[0], c.node_ids[i]).await;
    }

    // Retry pending pushes so shards land on ring owners.
    for _round in 0..3 {
        c.node(0).retry_pending_pushes().await;
    }

    // Broadcast manifests so other nodes learn about the objects.
    for (key, _) in &objects {
        c.broadcast_manifest(0, "b", key).await;
    }

    // Other nodes should now be able to read.
    for (key, expected) in &objects {
        let (got, _) = c
            .node(1)
            .get_object("b", key)
            .await
            .unwrap_or_else(|e| panic!("node 1 can't read {key} post-heal: {e}"));
        assert_eq!(&got, expected, "post-heal mismatch for {key}");
    }

    // Verify the chaos layer saw partitioned messages.
    let stats = controller.stats();
    assert!(
        stats.partitioned > 0,
        "should have partitioned messages: {stats:?}"
    );
}
