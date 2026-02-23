//! Shared test harness for Shoal integration tests.
//!
//! Provides [`IntegrationCluster`] — an N-node cluster with mock transport
//! that tests the full engine pipeline: chunking → erasure coding →
//! shard distribution → manifest handling → read/decode.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use shoal_cluster::ClusterState;
use shoal_engine::{ShoalNode, ShoalNodeConfig};
use shoal_meta::MetaStore;
use shoal_net::{ManifestSyncEntry, NetError, ShoalMessage, Transport};
use shoal_store::{MemoryStore, ShardStore};
use shoal_types::*;
use tokio::sync::RwLock;

/// Maximum store capacity for test nodes.
const MAX_BYTES: u64 = u64::MAX;

// =========================================================================
// Mock transport
// =========================================================================

/// Mock transport that routes shard operations to in-memory stores.
///
/// Supports failure injection: nodes in `down_nodes` are unreachable.
struct MockTransport {
    stores: HashMap<NodeId, Arc<dyn ShardStore>>,
    metas: HashMap<NodeId, Arc<MetaStore>>,
    down_nodes: Arc<RwLock<HashSet<NodeId>>>,
}

#[async_trait]
impl Transport for MockTransport {
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
        // Deliver ManifestPut messages to the peer's MetaStore.
        if let ShoalMessage::ManifestPut {
            bucket,
            key,
            manifest_bytes,
        } = msg
            && let Some(meta) = self.metas.get(&node_id)
            && let Ok(manifest) = postcard::from_bytes::<Manifest>(manifest_bytes)
        {
            let _ = meta.put_manifest(&manifest);
            let _ = meta.put_object_key(bucket, key, &manifest.object_id);
        }
        Ok(())
    }

    async fn pull_manifest(
        &self,
        addr: iroh::EndpointAddr,
        bucket: &str,
        key: &str,
    ) -> Result<Option<Vec<u8>>, NetError> {
        let node_id = NodeId::from(*addr.id.as_bytes());
        if self.down_nodes.read().await.contains(&node_id) {
            return Err(NetError::Endpoint("node is down".into()));
        }
        if let Some(meta) = self.metas.get(&node_id)
            && let Ok(Some(oid)) = meta.get_object_key(bucket, key)
            && let Ok(Some(manifest)) = meta.get_manifest(&oid)
            && let Ok(bytes) = postcard::to_allocvec(&manifest)
        {
            return Ok(Some(bytes));
        }
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
            if let Ok(Some(manifest)) = meta.get_manifest(&oid)
                && let Ok(bytes) = postcard::to_allocvec(&manifest)
            {
                result.push(ManifestSyncEntry {
                    bucket,
                    key,
                    manifest_bytes: bytes,
                });
            }
        }
        Ok(result)
    }

    async fn pull_log_entries(
        &self,
        _addr: iroh::EndpointAddr,
        _my_tips: &[[u8; 32]],
    ) -> Result<(Vec<Vec<u8>>, Vec<(ObjectId, Vec<u8>)>), NetError> {
        // Integration tests use MetaStore mode, not LogTree.
        Ok((vec![], vec![]))
    }
}

// =========================================================================
// Identity helpers
// =========================================================================

/// Derive a valid (NodeId, EndpointAddr) pair from a seed byte.
fn valid_identity(seed: u8) -> (NodeId, iroh::EndpointAddr) {
    let secret = iroh::SecretKey::from([seed; 32]);
    let public = secret.public();
    let node_id = NodeId::from(*public.as_bytes());
    let addr = iroh::EndpointAddr::new(public);
    (node_id, addr)
}

// =========================================================================
// IntegrationCluster
// =========================================================================

/// A simulated N-node cluster for integration tests.
///
/// Provides the full engine pipeline with mock transport and failure
/// injection. Nodes share a `ClusterState` and a shard transport layer.
pub struct IntegrationCluster {
    nodes: Vec<ShoalNode>,
    node_ids: Vec<NodeId>,
    addrs: Vec<iroh::EndpointAddr>,
    stores: Vec<Arc<dyn ShardStore>>,
    metas: Vec<Arc<MetaStore>>,
    cluster: Arc<ClusterState>,
    down_nodes: Arc<RwLock<HashSet<NodeId>>>,
    chunk_size: u32,
    k: usize,
    m: usize,
    shard_replication: usize,
}

impl IntegrationCluster {
    /// Create an N-node cluster with the given erasure config.
    pub async fn new(n: usize, chunk_size: u32, k: usize, m: usize) -> Self {
        Self::with_replication(n, chunk_size, k, m, 1).await
    }

    /// Create an N-node cluster with custom shard replication.
    pub async fn with_replication(
        n: usize,
        chunk_size: u32,
        k: usize,
        m: usize,
        shard_replication: usize,
    ) -> Self {
        assert!(n >= 2, "need at least 2 nodes");
        assert!(n <= 200, "too many nodes");

        let identities: Vec<_> = (1..=n as u8).map(valid_identity).collect();
        let node_ids: Vec<_> = identities.iter().map(|(nid, _)| *nid).collect();
        let addrs: Vec<_> = identities.iter().map(|(_, a)| a.clone()).collect();

        let cluster = ClusterState::new(node_ids[0], 128);
        for &nid in &node_ids {
            cluster
                .add_member(Member {
                    node_id: nid,
                    capacity: MAX_BYTES,
                    state: MemberState::Alive,
                    generation: 1,
                    topology: NodeTopology::default(),
                })
                .await;
        }

        let stores: Vec<Arc<dyn ShardStore>> = (0..n)
            .map(|_| Arc::new(MemoryStore::new(MAX_BYTES)) as Arc<dyn ShardStore>)
            .collect();

        let metas: Vec<Arc<MetaStore>> = (0..n)
            .map(|_| Arc::new(MetaStore::open_temporary().unwrap()))
            .collect();

        let down_nodes: Arc<RwLock<HashSet<NodeId>>> = Arc::new(RwLock::new(HashSet::new()));

        let store_map: HashMap<NodeId, Arc<dyn ShardStore>> = node_ids
            .iter()
            .zip(stores.iter())
            .map(|(&nid, s)| (nid, s.clone()))
            .collect();

        let meta_map: HashMap<NodeId, Arc<MetaStore>> = node_ids
            .iter()
            .zip(metas.iter())
            .map(|(&nid, m)| (nid, m.clone()))
            .collect();

        let full_book: HashMap<NodeId, iroh::EndpointAddr> = node_ids
            .iter()
            .zip(addrs.iter())
            .map(|(&nid, a)| (nid, a.clone()))
            .collect();

        let mut nodes = Vec::with_capacity(n);
        for i in 0..n {
            let transport: Arc<dyn Transport> = Arc::new(MockTransport {
                stores: store_map.clone(),
                metas: meta_map.clone(),
                down_nodes: down_nodes.clone(),
            });
            let book: HashMap<NodeId, iroh::EndpointAddr> = full_book
                .iter()
                .filter(|(nid, _)| **nid != node_ids[i])
                .map(|(&nid, a)| (nid, a.clone()))
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
            .with_address_book(Arc::new(RwLock::new(book)));

            nodes.push(node);
        }

        Self {
            nodes,
            node_ids,
            addrs,
            stores,
            metas,
            cluster,
            down_nodes,
            chunk_size,
            k,
            m,
            shard_replication,
        }
    }

    /// Get a reference to node `i`.
    pub fn node(&self, i: usize) -> &ShoalNode {
        &self.nodes[i]
    }

    /// Get the NodeId of node `i`.
    pub fn node_id(&self, i: usize) -> NodeId {
        self.node_ids[i]
    }

    /// Number of nodes in the cluster.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Returns `true` if the cluster has no nodes.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Simulate manifest broadcast from one node to all others.
    pub fn broadcast_manifest(&self, from: usize, bucket: &str, key: &str) {
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
    pub async fn kill_node(&self, i: usize) {
        self.down_nodes.write().await.insert(self.node_ids[i]);
        self.cluster.mark_dead(&self.node_ids[i]).await;
    }

    /// Mark a node as "alive" again after being killed.
    pub async fn revive_node(&self, i: usize) {
        self.down_nodes.write().await.remove(&self.node_ids[i]);
        self.cluster
            .add_member(Member {
                node_id: self.node_ids[i],
                capacity: MAX_BYTES,
                state: MemberState::Alive,
                generation: 2,
                topology: NodeTopology::default(),
            })
            .await;
    }

    /// Count shards stored locally on a given node.
    pub async fn local_shard_count(&self, i: usize) -> usize {
        self.stores[i].list().await.unwrap().len()
    }

    /// Add a new node to the cluster dynamically.
    ///
    /// Returns the index of the new node.
    pub async fn add_node(&mut self) -> usize {
        let idx = self.nodes.len();
        let seed = (idx + 1) as u8;
        let (nid, addr) = valid_identity(seed);

        // This node may already be in the cluster (re-add as Alive).
        self.cluster
            .add_member(Member {
                node_id: nid,
                capacity: MAX_BYTES,
                state: MemberState::Alive,
                generation: 1,
                topology: NodeTopology::default(),
            })
            .await;

        let store: Arc<dyn ShardStore> = Arc::new(MemoryStore::new(MAX_BYTES));
        let meta = Arc::new(MetaStore::open_temporary().unwrap());

        // Build transport with ALL stores (including new node's).
        let mut store_map: HashMap<NodeId, Arc<dyn ShardStore>> = self
            .node_ids
            .iter()
            .zip(self.stores.iter())
            .map(|(&n, s)| (n, s.clone()))
            .collect();
        store_map.insert(nid, store.clone());

        let mut meta_map: HashMap<NodeId, Arc<MetaStore>> = self
            .node_ids
            .iter()
            .zip(self.metas.iter())
            .map(|(&n, m)| (n, m.clone()))
            .collect();
        meta_map.insert(nid, meta.clone());

        // Update existing nodes' transports to include the new node's store.
        // (In mock transport, stores are shared via HashMap, so we rebuild
        // the transport for the new node only. Existing nodes' transports
        // already have the shared store_map that was cloned at creation.)
        // Note: this means existing nodes won't see the new node's store
        // unless we rebuild them too. For simplicity, we rebuild all nodes'
        // transports.
        let mut full_book: HashMap<NodeId, iroh::EndpointAddr> = self
            .node_ids
            .iter()
            .zip(self.addrs.iter())
            .map(|(&n, a)| (n, a.clone()))
            .collect();
        full_book.insert(nid, addr.clone());

        // Rebuild all existing nodes with updated transport.
        for i in 0..self.nodes.len() {
            let transport: Arc<dyn Transport> = Arc::new(MockTransport {
                stores: store_map.clone(),
                metas: meta_map.clone(),
                down_nodes: self.down_nodes.clone(),
            });
            let book: HashMap<NodeId, iroh::EndpointAddr> = full_book
                .iter()
                .filter(|(n, _)| **n != self.node_ids[i])
                .map(|(&n, a)| (n, a.clone()))
                .collect();

            self.nodes[i] = ShoalNode::new(
                ShoalNodeConfig {
                    node_id: self.node_ids[i],
                    chunk_size: self.chunk_size,
                    erasure_k: self.k,
                    erasure_m: self.m,
                    vnodes_per_node: 128,
                    shard_replication: self.shard_replication,
                    cache_max_bytes: u64::MAX,
                },
                self.stores[i].clone(),
                self.metas[i].clone(),
                self.cluster.clone(),
            )
            .with_transport(transport)
            .with_address_book(Arc::new(RwLock::new(book)));
        }

        // Create the new node.
        let transport: Arc<dyn Transport> = Arc::new(MockTransport {
            stores: store_map,
            metas: meta_map,
            down_nodes: self.down_nodes.clone(),
        });
        let book: HashMap<NodeId, iroh::EndpointAddr> = full_book
            .iter()
            .filter(|(n, _)| **n != nid)
            .map(|(&n, a)| (n, a.clone()))
            .collect();

        let node = ShoalNode::new(
            ShoalNodeConfig {
                node_id: nid,
                chunk_size: self.chunk_size,
                erasure_k: self.k,
                erasure_m: self.m,
                vnodes_per_node: 128,
                shard_replication: self.shard_replication,
                cache_max_bytes: u64::MAX,
            },
            store.clone(),
            meta.clone(),
            self.cluster.clone(),
        )
        .with_transport(transport)
        .with_address_book(Arc::new(RwLock::new(book)));

        self.nodes.push(node);
        self.node_ids.push(nid);
        self.addrs.push(addr);
        self.stores.push(store);
        self.metas.push(meta);

        idx
    }
}

/// Generate deterministic, non-repeating test data.
pub fn test_data(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let mut state: u32 = 0xDEAD_BEEF;
    for _ in 0..size {
        state = state.wrapping_mul(1103515245).wrapping_add(12345);
        data.push((state >> 16) as u8);
    }
    data
}

/// Generate test data with a specific seed (for unique objects).
pub fn test_data_seeded(size: usize, seed: u32) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let mut state: u32 = seed;
    for _ in 0..size {
        state = state.wrapping_mul(1103515245).wrapping_add(12345);
        data.push((state >> 16) as u8);
    }
    data
}
