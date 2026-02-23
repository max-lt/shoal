//! Tests for cross-node read scenarios using a mock transport.
//!
//! These tests verify that the read path correctly handles the case where
//! a non-writer node has a manifest (from broadcast) but no shard owner
//! metadata, and must compute owners from the placement ring.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bytes::Bytes;
use shoal_cluster::ClusterState;
use shoal_meta::MetaStore;
use shoal_net::{NetError, ShoalMessage, Transport};
use shoal_store::{MemoryStore, ShardStore};
use shoal_types::*;
use tokio::sync::RwLock;

use crate::node::{ShoalNode, ShoalNodeConfig};

use super::helpers::{TEST_MAX_BYTES, test_data};

/// A mock transport that routes shard pull requests directly to an
/// in-memory store, without any network.
///
/// This allows testing the engine's read path (including remote shard
/// resolution) in a sandboxed environment where iroh endpoints are not
/// available.
struct MockTransport {
    /// Maps NodeId → store. When a pull_shard request arrives for a given
    /// EndpointAddr, we look up the store by NodeId.
    stores: HashMap<NodeId, Arc<dyn ShardStore>>,
}

impl MockTransport {
    fn new(stores: HashMap<NodeId, Arc<dyn ShardStore>>) -> Self {
        Self { stores }
    }

    fn store_for_addr(&self, addr: &iroh::EndpointAddr) -> Option<&Arc<dyn ShardStore>> {
        let node_id = NodeId::from(*addr.id.as_bytes());
        self.stores.get(&node_id)
    }
}

#[async_trait::async_trait]
impl Transport for MockTransport {
    async fn push_shard(
        &self,
        addr: iroh::EndpointAddr,
        shard_id: ShardId,
        data: Bytes,
    ) -> Result<(), NetError> {
        if let Some(store) = self.store_for_addr(&addr) {
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
        if let Some(store) = self.store_for_addr(&addr) {
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

/// Set up 2 nodes sharing a cluster state, connected via mock transport.
///
/// Returns (writer_node, reader_node). The reader can pull shards from
/// the writer's store via the mock transport.
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

    // Mock transport for node A: can pull from B.
    let transport_a: Arc<dyn Transport> = Arc::new(MockTransport::new(HashMap::from([(
        nid_b,
        store_b.clone(),
    )])));

    // Mock transport for node B: can pull from A.
    let transport_b: Arc<dyn Transport> = Arc::new(MockTransport::new(HashMap::from([(
        nid_a,
        store_a.clone(),
    )])));

    // Address books so nodes can resolve each other.
    let book_a: Arc<RwLock<HashMap<NodeId, iroh::EndpointAddr>>> =
        Arc::new(RwLock::new(HashMap::from([(nid_b, addr_b)])));
    let book_b: Arc<RwLock<HashMap<NodeId, iroh::EndpointAddr>>> =
        Arc::new(RwLock::new(HashMap::from([(nid_a, addr_a)])));

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

// -----------------------------------------------------------------------
// Regression: non-writer read with mock transport
// -----------------------------------------------------------------------

/// Regression test for the "shard not found locally or remotely" bug.
///
/// Scenario (from real 3-node cluster testing):
/// 1. Node A writes an object (stores shards + shard owners in its meta).
/// 2. Node B receives the manifest via ManifestPut broadcast — the handler
///    stores manifest + key, but NOT shard owners.
/// 3. Node B tries to read → before the fix, it couldn't resolve remote
///    shard owners (meta returned None), so the entire remote pull was
///    skipped, logging "shard not found locally or remotely" for every
///    shard not in its local store.
///
/// The fix: when `meta.get_shard_owners()` returns `None`, compute owners
/// from the placement ring (which is deterministic).
#[tokio::test]
async fn test_non_writer_read_after_manifest_broadcast() {
    let (node_a, node_b) = writer_reader_pair_with_transport(1024, 2, 1).await;
    let data = test_data(5000);

    // Node A writes the object.
    node_a
        .put_object("b", "k", &data, BTreeMap::new())
        .await
        .unwrap();

    // Simulate ManifestPut broadcast to node B (no shard owners stored).
    simulate_manifest_broadcast(&node_a, &node_b, "b", "k");

    // Node B reads: it has the manifest but no shard owners in meta.
    // It should compute owners from the ring and pull missing shards
    // from node A via the mock transport.
    let (got, _) = node_b.get_object("b", "k").await.unwrap();
    assert_eq!(got, data, "non-writer node should reconstruct the object");
}

/// Same scenario but with k=4, m=2 — more shards, verifies it works
/// with larger erasure configs.
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

/// Verify the writer correctly stores shard owners in meta.
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

/// Verify the ManifestPut handler does NOT store shard owners —
/// confirming the precondition of the bug.
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
