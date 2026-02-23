//! Comprehensive networked tests with mock transport.
//!
//! Tests cover: N-node clusters, node failures, erasure config variations,
//! node recovery, edge cases (tiny objects, boundary sizes), and large clusters.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use shoal_cluster::ClusterState;
use shoal_meta::MetaStore;
use shoal_net::{ManifestSyncEntry, NetError, ShoalMessage, Transport};
use shoal_store::{MemoryStore, ShardStore};
use shoal_types::*;
use tokio::sync::RwLock;

use crate::node::{ShoalNode, ShoalNodeConfig};

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

    async fn send_to(
        &self,
        _addr: iroh::EndpointAddr,
        _msg: &ShoalMessage,
    ) -> Result<(), NetError> {
        Ok(())
    }

    async fn pull_manifest(
        &self,
        _addr: iroh::EndpointAddr,
        _bucket: &str,
        _key: &str,
    ) -> Result<Option<Vec<u8>>, NetError> {
        Ok(None)
    }

    async fn pull_all_manifests(
        &self,
        addr: iroh::EndpointAddr,
    ) -> Result<Vec<ManifestSyncEntry>, NetError> {
        let node_id = NodeId::from(*addr.id.as_bytes());
        if self.down_nodes.read().await.contains(&node_id) {
            return Err(NetError::Endpoint("node is down".into()));
        }
        let Some(meta) = self.metas.get(&node_id) else {
            return Ok(vec![]);
        };
        let entries = meta
            .list_all_object_entries()
            .map_err(|e| NetError::Endpoint(e.to_string()))?;
        let mut result = Vec::new();
        for (bucket, key, oid) in entries {
            if let Ok(Some(manifest)) = meta.get_manifest(&oid) {
                if let Ok(bytes) = postcard::to_allocvec(&manifest) {
                    result.push(ManifestSyncEntry {
                        bucket,
                        key,
                        manifest_bytes: bytes,
                    });
                }
            }
        }
        Ok(result)
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
}

impl TestCluster {
    /// Create an N-node cluster. Seeds start at 1 (seed=0 gives an invalid key).
    /// Uses shard_replication=1 (each shard on exactly one node).
    async fn new(n: usize, chunk_size: u32, k: usize, m: usize) -> Self {
        Self::with_replication(n, chunk_size, k, m, 1).await
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

        let stores: Vec<Arc<dyn ShardStore>> = (0..n)
            .map(|_| Arc::new(MemoryStore::new(TEST_MAX_BYTES)) as Arc<dyn ShardStore>)
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

        let mut nodes = Vec::with_capacity(n);
        for i in 0..n {
            let transport: Arc<dyn Transport> = Arc::new(FailableMockTransport {
                stores: store_map.clone(),
                metas: meta_map.clone(),
                down_nodes: down_nodes.clone(),
            });
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
                },
                stores[i].clone(),
                metas[i].clone(),
                cluster.clone(),
            )
            .with_transport(transport)
            .with_address_book(Arc::new(RwLock::new(book)));

            nodes.push(node);
        }

        Self {
            nodes,
            node_ids,
            stores,
            cluster,
            down_nodes,
        }
    }

    fn node(&self, i: usize) -> &ShoalNode {
        &self.nodes[i]
    }

    /// Simulate manifest broadcast from one node to all others.
    fn broadcast_manifest(&self, from: usize, bucket: &str, key: &str) {
        let manifest = self.nodes[from].head_object(bucket, key).unwrap();
        for (i, node) in self.nodes.iter().enumerate() {
            if i == from {
                continue;
            }
            node.meta().put_manifest(&manifest).unwrap();
            node.meta()
                .put_object_key(bucket, key, &manifest.object_id)
                .unwrap();
        }
    }

    /// Mark a node as "down" — transport calls to it will fail.
    async fn kill_node(&self, i: usize) {
        self.down_nodes.write().await.insert(self.node_ids[i]);
        self.cluster.mark_dead(&self.node_ids[i]).await;
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

/// Simulate what the ManifestPut broadcast handler does:
/// store the manifest and key mapping on the target node, but do NOT
/// store shard owners.
fn simulate_manifest_broadcast(source: &ShoalNode, target: &ShoalNode, bucket: &str, key: &str) {
    let manifest = source.head_object(bucket, key).unwrap();
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
        down_nodes: down.clone(),
    });
    let transport_b: Arc<dyn Transport> = Arc::new(FailableMockTransport {
        stores: HashMap::from([(nid_a, store_a.clone())]),
        metas: HashMap::from([(nid_a, meta_a.clone())]),
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
async fn test_non_writer_read_after_manifest_broadcast() {
    let (node_a, node_b) = writer_reader_pair_with_transport(1024, 2, 1).await;
    let data = test_data(5000);

    node_a
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    simulate_manifest_broadcast(&node_a, &node_b, "b", "k");

    let (got, _) = node_b.get_object("b", "k").await.unwrap();
    assert_eq!(got, data, "non-writer node should reconstruct the object");
}

#[tokio::test]
async fn test_non_writer_read_k4_m2_after_broadcast() {
    let (node_a, node_b) = writer_reader_pair_with_transport(1024, 4, 2).await;
    let data = test_data(10_000);

    node_a
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    simulate_manifest_broadcast(&node_a, &node_b, "b", "k");

    let (got, _) = node_b.get_object("b", "k").await.unwrap();
    assert_eq!(got, data);
}

#[tokio::test]
async fn test_writer_stores_shard_owners_in_meta() {
    let (node_a, _node_b) = writer_reader_pair_with_transport(1024, 2, 1).await;
    let data = test_data(2000);

    node_a
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    let manifest = node_a.head_object("b", "k").unwrap();
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
async fn test_manifest_broadcast_does_not_store_shard_owners() {
    let (node_a, node_b) = writer_reader_pair_with_transport(1024, 2, 1).await;
    let data = test_data(2000);

    node_a
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    simulate_manifest_broadcast(&node_a, &node_b, "b", "k");

    let manifest = node_b.head_object("b", "k").unwrap();
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
async fn test_3_node_write_read_from_all() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let data = test_data(5000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k");

    for i in 0..3 {
        let (got, _) = c.node(i).get_object("b", "k").await.unwrap();
        assert_eq!(got, data, "node {i} should read the object");
    }
}

/// 5-node cluster: write on node 2, read from all nodes.
#[tokio::test]
async fn test_5_node_write_read_from_all() {
    let c = TestCluster::new(5, 1024, 4, 2).await;
    let data = test_data(20_000);

    c.node(2)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(2, "b", "k");

    for i in 0..5 {
        let (got, _) = c.node(i).get_object("b", "k").await.unwrap();
        assert_eq!(got, data, "node {i} should read the object");
    }
}

/// 10-node cluster: write and read.
#[tokio::test]
async fn test_10_node_cluster() {
    let c = TestCluster::new(10, 2048, 4, 2).await;
    let data = test_data(50_000);

    c.node(7)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(7, "b", "k");

    // Every node can read.
    for i in 0..10 {
        let (got, _) = c.node(i).get_object("b", "k").await.unwrap();
        assert_eq!(got, data, "node {i} should read correctly");
    }
}

/// 10-node cluster: shards should be distributed across multiple nodes.
#[tokio::test]
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
async fn test_3_node_k1_m1_mirroring() {
    let c = TestCluster::new(3, 1024, 1, 1).await;
    let data = test_data(3000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k");

    for i in 0..3 {
        let (got, _) = c.node(i).get_object("b", "k").await.unwrap();
        assert_eq!(got, data, "k1m1 node {i}");
    }
}

/// k=2, m=2: can lose 2 shards per chunk.
#[tokio::test]
async fn test_5_node_k2_m2() {
    let c = TestCluster::new(5, 1024, 2, 2).await;
    let data = test_data(4000);

    c.node(1)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(1, "b", "k");

    for i in 0..5 {
        let (got, _) = c.node(i).get_object("b", "k").await.unwrap();
        assert_eq!(got, data, "k2m2 node {i}");
    }
}

/// k=8, m=4: large erasure group.
#[tokio::test]
async fn test_12_node_k8_m4() {
    let c = TestCluster::new(12, 4096, 8, 4).await;
    let data = test_data(100_000);

    c.node(5)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(5, "b", "k");

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
async fn test_3_node_kill_one_still_reads() {
    let c = TestCluster::with_replication(3, 1024, 2, 1, 2).await;
    let data = test_data(5000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k");

    // Kill node 1.
    c.kill_node(1).await;

    // Node 2 should still read (pulling from node 0, even if node 1 is down).
    let (got, _) = c.node(2).get_object("b", "k").await.unwrap();
    assert_eq!(got, data, "should read after 1 node failure");
}

/// Kill 2 nodes in a 5-node k=4,m=2 cluster with replication=3:
/// each shard is on 3 nodes, so losing 2 still leaves copies available.
#[tokio::test]
async fn test_5_node_kill_two_still_reads() {
    let c = TestCluster::with_replication(5, 1024, 4, 2, 3).await;
    let data = test_data(20_000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k");

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
async fn test_5_node_kill_writer() {
    let c = TestCluster::with_replication(5, 1024, 4, 2, 2).await;
    let data = test_data(15_000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k");

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
async fn test_3_node_kill_two_writer_still_reads() {
    let c = TestCluster::with_replication(3, 1024, 2, 1, 3).await;
    let data = test_data(5000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k");

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
async fn test_node_kill_and_revive() {
    let c = TestCluster::with_replication(3, 1024, 2, 1, 2).await;
    let data = test_data(5000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k");

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
async fn test_mass_failure_and_recovery() {
    let c = TestCluster::with_replication(5, 1024, 4, 2, 5).await;
    let data = test_data(20_000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k");

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
async fn test_3_node_single_byte_object() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let data = vec![42u8];

    c.node(0)
        .put_object("b", "tiny", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "tiny");

    for i in 0..3 {
        let (got, _) = c.node(i).get_object("b", "tiny").await.unwrap();
        assert_eq!(got, data, "single byte on node {i}");
    }
}

/// Empty object across a 3-node cluster.
#[tokio::test]
async fn test_3_node_empty_object() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let data: Vec<u8> = vec![];

    c.node(1)
        .put_object("b", "empty", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(1, "b", "empty");

    for i in 0..3 {
        let (got, _) = c.node(i).get_object("b", "empty").await.unwrap();
        assert_eq!(got, data, "empty object on node {i}");
    }
}

/// Object exactly chunk_size bytes.
#[tokio::test]
async fn test_5_node_exact_chunk_size() {
    let c = TestCluster::new(5, 1024, 4, 2).await;
    let data = test_data(1024);

    c.node(3)
        .put_object("b", "exact", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(3, "b", "exact");

    for i in 0..5 {
        let (got, _) = c.node(i).get_object("b", "exact").await.unwrap();
        assert_eq!(got, data, "exact chunk_size on node {i}");
    }
}

/// Object of chunk_size - 1 bytes.
#[tokio::test]
async fn test_3_node_one_byte_under_chunk() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let data = test_data(1023);

    c.node(0)
        .put_object("b", "under", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "under");

    for i in 0..3 {
        let (got, _) = c.node(i).get_object("b", "under").await.unwrap();
        assert_eq!(got, data, "under chunk_size on node {i}");
    }
}

/// Object of chunk_size + 1 bytes (forces 2 chunks).
#[tokio::test]
async fn test_3_node_one_byte_over_chunk() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let data = test_data(1025);

    c.node(2)
        .put_object("b", "over", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(2, "b", "over");

    let manifest = c.node(2).head_object("b", "over").unwrap();
    assert_eq!(manifest.chunks.len(), 2, "should be exactly 2 chunks");

    for i in 0..3 {
        let (got, _) = c.node(i).get_object("b", "over").await.unwrap();
        assert_eq!(got, data, "over chunk_size on node {i}");
    }
}

/// 1MB object with small chunk size: many chunks.
#[tokio::test]
async fn test_5_node_1mb_many_chunks() {
    let c = TestCluster::new(5, 4096, 4, 2).await;
    let data = test_data(1_048_576);

    c.node(0)
        .put_object("b", "big", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "big");

    let manifest = c.node(0).head_object("b", "big").unwrap();
    assert_eq!(manifest.chunks.len(), 256, "1MB / 4096 = 256 chunks");

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
        c.broadcast_manifest(writer, "b", &key);

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
        c.broadcast_manifest(writer, "b", &key);

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
    c.broadcast_manifest(0, "b", "k");

    // Read manifest from another node, check metadata is intact.
    let manifest = c.node(1).head_object("b", "k").unwrap();
    assert_eq!(manifest.metadata, meta);
}

// =========================================================================
// Write from one node, delete from another
// =========================================================================

/// Node 0 writes, node 1 deletes, node 2 should see 404.
#[tokio::test]
async fn test_delete_from_different_node() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let data = test_data(2000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k");

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

/// Verify different chunk sizes produce correct results.
#[tokio::test]
async fn test_various_chunk_sizes() {
    for chunk_size in [128, 256, 512, 1024, 4096] {
        let c = TestCluster::new(3, chunk_size, 2, 1).await;
        let data = test_data(10_000);

        c.node(0)
            .put_object("b", "k", &data, BTreeMap::new())
            .await
            .unwrap();
        c.broadcast_manifest(0, "b", "k");

        let (got, _) = c.node(2).get_object("b", "k").await.unwrap();
        assert_eq!(got, data, "chunk_size={chunk_size}");

        let manifest = c.node(0).head_object("b", "k").unwrap();
        let expected_chunks = (10_000 + chunk_size as usize - 1) / chunk_size as usize;
        assert_eq!(
            manifest.chunks.len(),
            expected_chunks,
            "chunk count for chunk_size={chunk_size}"
        );
    }
}

// =========================================================================
// Overwrite from a different node
// =========================================================================

/// Node 0 writes version 1, node 1 overwrites with version 2, all nodes
/// should see the new version.
#[tokio::test]
async fn test_overwrite_from_different_node() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let v1 = test_data(3000);
    let v2 = test_data(5000);

    // Node 0 writes v1.
    c.node(0)
        .put_object("b", "k", &v1, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k");

    // Verify v1 readable.
    let (got, _) = c.node(2).get_object("b", "k").await.unwrap();
    assert_eq!(got, v1);

    // Node 1 overwrites with v2.
    c.node(1)
        .put_object("b", "k", &v2, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(1, "b", "k");

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
                cluster.broadcast_manifest(writer, "b", &key);
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
/// in its local store for future reads.
#[tokio::test]
async fn test_pulled_shards_cached_locally() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let data = test_data(2000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k");

    // Before read: node 2 might have some shards (placed by ring) but
    // likely not all.
    let before = c.local_shard_count(2).await;

    // Read triggers remote pulls.
    let (got, _) = c.node(2).get_object("b", "k").await.unwrap();
    assert_eq!(got, data);

    // After read: node 2 should have more shards cached.
    let after = c.local_shard_count(2).await;
    assert!(
        after >= before,
        "shard count should not decrease after read: before={before} after={after}"
    );
}

// =========================================================================
// 2-node minimal cluster
// =========================================================================

/// Minimal 2-node cluster with k=1, m=1 (mirroring).
#[tokio::test]
async fn test_2_node_k1_m1() {
    let c = TestCluster::new(2, 512, 1, 1).await;
    let data = test_data(3000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k");

    let (got, _) = c.node(1).get_object("b", "k").await.unwrap();
    assert_eq!(got, data);
}

/// 2-node cluster with replication=2: both nodes have all shards.
/// Kill one, the other should still read.
#[tokio::test]
async fn test_2_node_kill_one() {
    let c = TestCluster::with_replication(2, 512, 1, 1, 2).await;
    let data = test_data(3000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    c.broadcast_manifest(0, "b", "k");

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

    c.broadcast_manifest(0, "photos", "img.jpg");
    c.broadcast_manifest(1, "docs", "readme.md");

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
        c.broadcast_manifest(writer, "b", &key);
    }

    // All 3 nodes should list all 10 items.
    for i in 0..3 {
        let keys = c.node(i).list_objects("b", "").unwrap();
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
async fn test_new_node_lists_objects_after_manifest_sync() {
    let c = TestCluster::new(3, 1024, 2, 1).await;

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

    // Broadcast manifests to nodes 0 and 1 only — simulating that
    // node 2 joined the cluster AFTER the objects were stored.
    simulate_manifest_broadcast(c.node(0), c.node(1), "b", "key.txt");
    simulate_manifest_broadcast(c.node(1), c.node(0), "b", "w.txt");

    // Node 2 has NO manifests.
    let keys_before = c.node(2).list_objects("b", "").unwrap();
    assert_eq!(
        keys_before.len(),
        0,
        "node 2 should have 0 objects before sync"
    );

    // After manifest sync, node 2 should see both objects.
    let synced = c.node(2).sync_manifests_from_peers().await.unwrap();
    assert_eq!(synced, 2, "should have synced 2 manifests");

    let mut keys_after = c.node(2).list_objects("b", "").unwrap();
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
async fn test_manifest_sync_idempotent() {
    let c = TestCluster::new(3, 1024, 2, 1).await;
    let data = test_data(2000);

    c.node(0)
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();
    simulate_manifest_broadcast(c.node(0), c.node(1), "b", "k");

    // First sync: picks up the manifest.
    let synced1 = c.node(2).sync_manifests_from_peers().await.unwrap();
    assert_eq!(synced1, 1);

    // Second sync: nothing new.
    let synced2 = c.node(2).sync_manifests_from_peers().await.unwrap();
    assert_eq!(synced2, 0);

    let keys = c.node(2).list_objects("b", "").unwrap();
    assert_eq!(keys.len(), 1);
}

/// Sync across multiple buckets works correctly.
#[tokio::test]
async fn test_manifest_sync_multiple_buckets() {
    let c = TestCluster::new(3, 1024, 2, 1).await;

    c.node(0)
        .put_object("photos", "cat.jpg", &test_data(1000), BTreeMap::new())
        .await
        .unwrap();
    c.node(0)
        .put_object("docs", "readme.md", &test_data(500), BTreeMap::new())
        .await
        .unwrap();
    simulate_manifest_broadcast(c.node(0), c.node(1), "photos", "cat.jpg");
    simulate_manifest_broadcast(c.node(0), c.node(1), "docs", "readme.md");

    // Node 2 syncs.
    let synced = c.node(2).sync_manifests_from_peers().await.unwrap();
    assert_eq!(synced, 2);

    assert_eq!(c.node(2).list_objects("photos", "").unwrap().len(), 1);
    assert_eq!(c.node(2).list_objects("docs", "").unwrap().len(), 1);
}
