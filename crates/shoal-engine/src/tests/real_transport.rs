//! Tests using real QUIC transport (iroh-quinn).
//!
//! These tests reproduce bugs that the mock transport cannot catch:
//! - Large object reads failing over QUIC (shard fetch timeouts, stream ordering).
//! - Concurrent writes corrupting state (connection pool contention).
//! - Cross-node manifest visibility failures (broadcast drops).
//!
//! We bypass iroh's `Endpoint` (which requires netmon/netlink) and use raw
//! `iroh_quinn` endpoints with self-signed TLS certs — the same approach
//! used in `shoal-net/src/tests.rs`. The QUIC protocol layer is identical;
//! we just skip the iroh MagicSock layer.
//!
//! **Constraint**: No existing test files are modified — only this new module is added.

use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use bytes::Bytes;
use iroh_quinn::TokioRuntime;
use rustls_pki_types::PrivateKeyDer;
use shoal_cluster::ClusterState;
use shoal_meta::MetaStore;
use shoal_net::{ManifestSyncEntry, NetError, ShoalMessage, ShoalTransport, Transport};
use shoal_store::{MemoryStore, ShardStore};
use shoal_types::*;
use tokio::sync::{Mutex, RwLock};
use tracing::warn;

use crate::node::{ShoalNode, ShoalNodeConfig};

use super::helpers::{TEST_MAX_BYTES, test_data};

// =========================================================================
// QuinnTransport — Transport trait impl using raw iroh_quinn connections
// =========================================================================

/// A real QUIC transport using raw iroh-quinn endpoints.
///
/// Unlike the mock `FailableMockTransport`, this actually sends data over
/// real QUIC connections with TLS, length-prefixed postcard encoding, and
/// connection pooling — exercising the same wire protocol as production.
struct QuinnTransport {
    /// The local quinn endpoint (used for outgoing connections).
    endpoint: iroh_quinn::Endpoint,
    /// Maps EndpointId (as [u8;32] placeholder) → SocketAddr for peers.
    peer_addrs: RwLock<HashMap<[u8; 32], SocketAddr>>,
    /// Cached connections keyed by remote SocketAddr.
    ///
    /// Uses `Mutex` (not `RwLock`) to prevent the TOCTOU race where
    /// concurrent callers all see "no connection" and each establish a
    /// separate QUIC connection, overwriting each other in the cache.
    /// The overwritten connections get dropped, sending CONNECTION_CLOSE
    /// and aborting any in-flight uni-stream data.
    connections: Mutex<HashMap<SocketAddr, iroh_quinn::Connection>>,
}

impl QuinnTransport {
    fn new(endpoint: iroh_quinn::Endpoint) -> Self {
        Self {
            endpoint,
            peer_addrs: RwLock::new(HashMap::new()),
            connections: Mutex::new(HashMap::new()),
        }
    }

    /// Register a peer's socket address.
    async fn add_peer(&self, node_id_bytes: [u8; 32], addr: SocketAddr) {
        self.peer_addrs.write().await.insert(node_id_bytes, addr);
    }

    /// Resolve an EndpointAddr to a SocketAddr by looking up the peer ID bytes.
    async fn resolve(&self, addr: &iroh::EndpointAddr) -> Option<SocketAddr> {
        let id_bytes: [u8; 32] = *addr.id.as_bytes();
        self.peer_addrs.read().await.get(&id_bytes).copied()
    }

    /// Get or establish a QUIC connection to a peer.
    ///
    /// Holds the connection cache lock for the entire duration to prevent
    /// the TOCTOU race where concurrent callers each create a connection
    /// to the same peer, overwriting each other.
    async fn get_connection(&self, addr: SocketAddr) -> Result<iroh_quinn::Connection, NetError> {
        let mut cache = self.connections.lock().await;

        // Check cache.
        if let Some(conn) = cache.get(&addr)
            && conn.close_reason().is_none()
        {
            return Ok(conn.clone());
        }

        // Establish new connection while holding the lock.
        let conn = self
            .endpoint
            .connect(addr, "localhost")
            .map_err(|e| NetError::Connect(e.to_string()))?
            .await
            .map_err(|e| NetError::Connect(e.to_string()))?;

        cache.insert(addr, conn.clone());
        Ok(conn)
    }

    /// Send a postcard-encoded message on a new uni-directional stream.
    async fn send_message(
        conn: &iroh_quinn::Connection,
        message: &ShoalMessage,
    ) -> Result<(), NetError> {
        let mut send = conn
            .open_uni()
            .await
            .map_err(|e| NetError::StreamOpen(e.to_string()))?;
        // iroh_quinn::SendStream is the same type as iroh::endpoint::SendStream.
        ShoalTransport::send_on_stream(&mut send, message).await
    }

    /// Receive a message from a recv stream (same wire format as ShoalTransport).
    async fn recv_message(recv: &mut iroh_quinn::RecvStream) -> Result<ShoalMessage, NetError> {
        // The RecvStream from iroh_quinn is the same type as iroh::endpoint::RecvStream.
        ShoalTransport::recv_message(recv).await
    }

    /// Send a response on an already-open send stream (bi-directional).
    async fn send_on_stream(
        send: &mut iroh_quinn::SendStream,
        message: &ShoalMessage,
    ) -> Result<(), NetError> {
        // The SendStream from iroh_quinn is the same type as iroh::endpoint::SendStream.
        ShoalTransport::send_on_stream(send, message).await
    }
}

#[async_trait::async_trait]
impl Transport for QuinnTransport {
    async fn push_shard(
        &self,
        addr: iroh::EndpointAddr,
        shard_id: ShardId,
        data: Bytes,
    ) -> Result<(), NetError> {
        let sock = self
            .resolve(&addr)
            .await
            .ok_or_else(|| NetError::Connect("unknown peer".into()))?;
        let conn = self.get_connection(sock).await?;

        // Bi-stream: send ShardPush, wait for ShardPushAck.
        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| NetError::StreamOpen(e.to_string()))?;

        let msg = ShoalMessage::ShardPush {
            shard_id,
            data: data.to_vec(),
        };
        Self::send_on_stream(&mut send, &msg).await?;

        let response = Self::recv_message(&mut recv).await?;
        match response {
            ShoalMessage::ShardPushAck { ok, .. } => {
                if !ok {
                    return Err(NetError::Connect(format!(
                        "remote rejected shard {shard_id}"
                    )));
                }
                Ok(())
            }
            other => Err(NetError::Serialization(format!(
                "expected ShardPushAck, got: {other:?}"
            ))),
        }
    }

    async fn pull_shard(
        &self,
        addr: iroh::EndpointAddr,
        shard_id: ShardId,
    ) -> Result<Option<Bytes>, NetError> {
        let sock = self
            .resolve(&addr)
            .await
            .ok_or_else(|| NetError::Connect("unknown peer".into()))?;
        let conn = self.get_connection(sock).await?;

        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| NetError::StreamOpen(e.to_string()))?;

        let request = ShoalMessage::ShardRequest { shard_id };
        Self::send_on_stream(&mut send, &request).await?;

        let response = Self::recv_message(&mut recv).await?;

        match response {
            ShoalMessage::ShardResponse {
                shard_id: resp_id,
                data: Some(data),
            } => {
                let actual_id = ShardId::from_data(&data);
                if actual_id != shard_id {
                    return Err(NetError::IntegrityFailure {
                        expected: shard_id,
                        actual: actual_id,
                    });
                }
                if resp_id != shard_id {
                    warn!(requested = %shard_id, received = %resp_id, "response shard_id mismatch");
                }
                Ok(Some(Bytes::from(data)))
            }
            ShoalMessage::ShardResponse { data: None, .. } => Ok(None),
            other => Err(NetError::Serialization(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    async fn send_to(&self, addr: iroh::EndpointAddr, msg: &ShoalMessage) -> Result<(), NetError> {
        let sock = self
            .resolve(&addr)
            .await
            .ok_or_else(|| NetError::Connect("unknown peer".into()))?;
        let conn = self.get_connection(sock).await?;
        Self::send_message(&conn, msg).await
    }

    async fn pull_manifest(
        &self,
        addr: iroh::EndpointAddr,
        bucket: &str,
        key: &str,
    ) -> Result<Option<Vec<u8>>, NetError> {
        let sock = self
            .resolve(&addr)
            .await
            .ok_or_else(|| NetError::Connect("unknown peer".into()))?;
        let conn = self.get_connection(sock).await?;

        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| NetError::StreamOpen(e.to_string()))?;

        let request = ShoalMessage::ManifestRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
        };
        Self::send_on_stream(&mut send, &request).await?;

        let response = Self::recv_message(&mut recv).await?;

        match response {
            ShoalMessage::ManifestResponse {
                manifest_bytes: Some(bytes),
                ..
            } => Ok(Some(bytes)),
            ShoalMessage::ManifestResponse {
                manifest_bytes: None,
                ..
            } => Ok(None),
            other => Err(NetError::Serialization(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    async fn pull_all_manifests(
        &self,
        addr: iroh::EndpointAddr,
    ) -> Result<Vec<ManifestSyncEntry>, NetError> {
        let sock = self
            .resolve(&addr)
            .await
            .ok_or_else(|| NetError::Connect("unknown peer".into()))?;
        let conn = self.get_connection(sock).await?;

        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| NetError::StreamOpen(e.to_string()))?;

        let request = ShoalMessage::ManifestSyncRequest;
        Self::send_on_stream(&mut send, &request).await?;

        let response = Self::recv_message(&mut recv).await?;

        match response {
            ShoalMessage::ManifestSyncResponse { entries } => Ok(entries),
            other => Err(NetError::Serialization(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    async fn pull_log_entries(
        &self,
        _addr: iroh::EndpointAddr,
        _my_tips: &[[u8; 32]],
    ) -> Result<(Vec<Vec<u8>>, Vec<(ObjectId, Vec<u8>)>), NetError> {
        // Not needed for these tests.
        Ok((vec![], vec![]))
    }

    async fn pull_log_sync(
        &self,
        _addr: iroh::EndpointAddr,
        _entry_hashes: &[[u8; 32]],
        _my_tips: &[[u8; 32]],
    ) -> Result<(Vec<Vec<u8>>, Vec<(ObjectId, Vec<u8>)>), NetError> {
        // Not needed for these tests.
        Ok((vec![], vec![]))
    }
}

// =========================================================================
// Protocol handler — runs on each node's server endpoint
// =========================================================================

/// Spawn a protocol handler loop for incoming connections on a quinn endpoint.
///
/// Mirrors the production `ShoalProtocol` handler in `shoald/src/handler.rs`:
/// - Uni streams: `ManifestPut`
/// - Bi streams: `ShardPush`→`ShardPushAck`, `ShardRequest`→`ShardResponse`,
///   `ManifestRequest`→`ManifestResponse`, `ManifestSyncRequest`→`ManifestSyncResponse`
fn spawn_protocol_handler(
    endpoint: iroh_quinn::Endpoint,
    store: Arc<dyn ShardStore>,
    meta: Arc<MetaStore>,
) {
    tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            let conn = match incoming.await {
                Ok(c) => c,
                Err(_) => continue,
            };

            // Spawn uni-stream handler (ManifestPut only — ShardPush moved to bi-stream).
            let conn_uni = conn.clone();
            let meta_uni = meta.clone();
            tokio::spawn(async move {
                while let Ok(mut recv) = conn_uni.accept_uni().await {
                    let Ok(msg) = QuinnTransport::recv_message(&mut recv).await else {
                        break;
                    };
                    match msg {
                        ShoalMessage::ManifestPut {
                            bucket,
                            key,
                            manifest_bytes,
                        } => {
                            if let Ok(manifest) = postcard::from_bytes::<Manifest>(&manifest_bytes)
                            {
                                let _ = meta_uni.put_manifest(&manifest);
                                let _ = meta_uni.put_object_key(&bucket, &key, &manifest.object_id);
                            }
                        }
                        _ => {}
                    }
                }
            });

            // Spawn bi-stream handler.
            let store_bi = store.clone();
            let meta_bi = meta.clone();
            tokio::spawn(async move {
                while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                    let store = store_bi.clone();
                    let meta = meta_bi.clone();
                    tokio::spawn(async move {
                        let Ok(request) = QuinnTransport::recv_message(&mut recv).await else {
                            return;
                        };
                        let response = match request {
                            ShoalMessage::ShardPush { shard_id, data } => {
                                let ok = store.put(shard_id, Bytes::from(data)).await.is_ok();
                                Some(ShoalMessage::ShardPushAck { shard_id, ok })
                            }
                            ShoalMessage::ShardRequest { shard_id } => {
                                let data = store.get(shard_id).await.ok().flatten();
                                Some(ShoalMessage::ShardResponse {
                                    shard_id,
                                    data: data.map(|b| b.to_vec()),
                                })
                            }
                            ShoalMessage::ManifestRequest { bucket, key } => {
                                let manifest_bytes = meta
                                    .get_object_key(&bucket, &key)
                                    .ok()
                                    .flatten()
                                    .and_then(|oid| meta.get_manifest(&oid).ok().flatten())
                                    .and_then(|m| postcard::to_allocvec(&m).ok());
                                Some(ShoalMessage::ManifestResponse {
                                    bucket,
                                    key,
                                    manifest_bytes,
                                })
                            }
                            ShoalMessage::ManifestSyncRequest => {
                                let entries = meta
                                    .list_all_object_entries()
                                    .unwrap_or_default()
                                    .into_iter()
                                    .filter_map(|(bucket, key, oid)| {
                                        let manifest = meta.get_manifest(&oid).ok().flatten()?;
                                        let bytes = postcard::to_allocvec(&manifest).ok()?;
                                        Some(ManifestSyncEntry {
                                            bucket,
                                            key,
                                            manifest_bytes: bytes,
                                        })
                                    })
                                    .collect();
                                Some(ShoalMessage::ManifestSyncResponse { entries })
                            }
                            _ => None,
                        };
                        if let Some(resp) = response {
                            let _ = QuinnTransport::send_on_stream(&mut send, &resp).await;
                        }
                    });
                }
            });
        }
    });
}

// =========================================================================
// QuicTestCluster — real QUIC cluster setup
// =========================================================================

/// Create a shared TLS config for test endpoints.
///
/// Returns (server_config, client_config) using a self-signed certificate.
fn test_tls_config() -> (iroh_quinn::ServerConfig, iroh_quinn::ClientConfig) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let key = PrivateKeyDer::Pkcs8(cert.key_pair.serialize_der().into());

    let server_config =
        iroh_quinn::ServerConfig::with_single_cert(vec![cert.cert.der().clone()], key).unwrap();

    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let client_config = iroh_quinn::ClientConfig::with_root_certificates(Arc::new(roots)).unwrap();

    (server_config, client_config)
}

/// Derive a valid (NodeId, EndpointAddr) pair from a seed byte.
fn valid_identity(seed: u8) -> (NodeId, iroh::EndpointAddr) {
    let secret = iroh::SecretKey::from([seed; 32]);
    let public = secret.public();
    let node_id = NodeId::from(*public.as_bytes());
    let addr = iroh::EndpointAddr::new(public);
    (node_id, addr)
}

/// A test cluster using real QUIC endpoints.
struct QuicTestCluster {
    nodes: Vec<ShoalNode>,
    #[allow(dead_code)]
    node_ids: Vec<NodeId>,
    transports: Vec<Arc<QuinnTransport>>,
    /// EndpointAddrs for address book lookups (keyed by iroh identity).
    endpoint_addrs: Vec<iroh::EndpointAddr>,
    #[allow(dead_code)]
    cluster: Arc<ClusterState>,
    /// Keep server endpoints alive (their accept loops run in spawned tasks).
    #[allow(dead_code)]
    server_endpoints: Vec<iroh_quinn::Endpoint>,
}

impl QuicTestCluster {
    /// Create an N-node cluster with real QUIC endpoints.
    async fn new(n: usize, chunk_size: u32, k: usize, m: usize) -> Self {
        assert!(n >= 2, "need at least 2 nodes for QUIC tests");

        let (server_config, client_config) = test_tls_config();

        // 1. Create endpoints (each node gets a server + a client endpoint).
        let mut server_endpoints = Vec::with_capacity(n);
        let mut client_endpoints = Vec::with_capacity(n);
        let mut server_addrs = Vec::with_capacity(n);

        for _ in 0..n {
            let server = iroh_quinn::Endpoint::new(
                iroh_quinn::EndpointConfig::default(),
                Some(server_config.clone()),
                std::net::UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
                    .unwrap(),
                Arc::new(TokioRuntime),
            )
            .unwrap();
            let addr = server.local_addr().unwrap();
            server_addrs.push(addr);
            server_endpoints.push(server);

            let client = iroh_quinn::Endpoint::new(
                iroh_quinn::EndpointConfig::default(),
                None,
                std::net::UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
                    .unwrap(),
                Arc::new(TokioRuntime),
            )
            .unwrap();
            client.set_default_client_config(client_config.clone());
            client_endpoints.push(client);
        }

        // 2. Derive NodeIds using iroh keys (for consistent identity).
        let identities: Vec<_> = (1..=n as u8).map(valid_identity).collect();
        let node_ids: Vec<NodeId> = identities.iter().map(|(nid, _)| *nid).collect();
        let endpoint_addrs: Vec<iroh::EndpointAddr> =
            identities.iter().map(|(_, addr)| addr.clone()).collect();

        // 3. Build shared ClusterState.
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

        // 4. Create stores, metas, transports, and spawn protocol handlers.
        let mut stores: Vec<Arc<dyn ShardStore>> = Vec::with_capacity(n);
        let mut metas: Vec<Arc<MetaStore>> = Vec::with_capacity(n);
        let mut transports: Vec<Arc<QuinnTransport>> = Vec::with_capacity(n);

        for i in 0..n {
            let store: Arc<dyn ShardStore> = Arc::new(MemoryStore::new(TEST_MAX_BYTES));
            let meta = Arc::new(MetaStore::open_temporary().unwrap());

            // Spawn protocol handler on the server endpoint.
            spawn_protocol_handler(server_endpoints[i].clone(), store.clone(), meta.clone());

            // Create transport using the client endpoint.
            let transport = Arc::new(QuinnTransport::new(client_endpoints[i].clone()));

            // Register all peer addresses.
            for j in 0..n {
                if j != i {
                    let id_bytes: [u8; 32] = *identities[j].1.id.as_bytes();
                    transport.add_peer(id_bytes, server_addrs[j]).await;
                }
            }

            stores.push(store);
            metas.push(meta);
            transports.push(transport);
        }

        // 5. Build ShoalNodes with real transports.
        let mut nodes = Vec::with_capacity(n);
        for i in 0..n {
            let book: HashMap<NodeId, iroh::EndpointAddr> = node_ids
                .iter()
                .zip(endpoint_addrs.iter())
                .filter(|(nid, _)| **nid != node_ids[i])
                .map(|(nid, addr)| (*nid, addr.clone()))
                .collect();

            let node = ShoalNode::new(
                ShoalNodeConfig {
                    node_id: node_ids[i],
                    chunk_size,
                    erasure_k: k,
                    erasure_m: m,
                    vnodes_per_node: 128,
                    shard_replication: 1,
                    cache_max_bytes: u64::MAX,
                },
                stores[i].clone(),
                metas[i].clone(),
                cluster.clone(),
            )
            .with_transport(transports[i].clone())
            .with_address_book(Arc::new(RwLock::new(book)));

            nodes.push(node);
        }

        Self {
            nodes,
            node_ids,
            transports,
            endpoint_addrs,
            cluster,
            server_endpoints,
        }
    }

    fn node(&self, i: usize) -> &ShoalNode {
        &self.nodes[i]
    }

    /// Broadcast a manifest from one node to all others over real QUIC.
    async fn broadcast_manifest(&self, from: usize, bucket: &str, key: &str) {
        let manifest = self.nodes[from].head_object(bucket, key).await.unwrap();
        let manifest_bytes = postcard::to_allocvec(&manifest).unwrap();

        let msg = ShoalMessage::ManifestPut {
            bucket: bucket.to_string(),
            key: key.to_string(),
            manifest_bytes,
        };

        let transport = &self.transports[from];
        for (i, addr) in self.endpoint_addrs.iter().enumerate() {
            if i == from {
                continue;
            }
            if let Err(e) = transport.send_to(addr.clone(), &msg).await {
                warn!(%e, "failed to broadcast manifest to node {i}");
            }
        }

        // Wait for broadcasts to be processed.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

// =========================================================================
// Test Scenarios
// =========================================================================

/// 3a. Basic smoke test: PUT on node 0, GET from node 0 and node 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ntest::timeout(30000)]
async fn test_quic_put_get_roundtrip() {
    let c = QuicTestCluster::new(3, 1024, 2, 1).await;
    let data = test_data(5000);

    // PUT on node 0.
    c.node(0)
        .put_object("bucket", "key1", &data, BTreeMap::new())
        .await
        .unwrap();

    // GET from node 0 — shards are local, should work immediately.
    let (got, _) = c.node(0).get_object("bucket", "key1").await.unwrap();
    assert_eq!(got, data, "GET from writer node must match");

    // Broadcast manifest to other nodes.
    c.broadcast_manifest(0, "bucket", "key1").await;

    // GET from node 1 — must pull shards via real QUIC.
    let (got, _) = c.node(1).get_object("bucket", "key1").await.unwrap();
    assert_eq!(got, data, "GET from non-writer node must match");
}

/// 3b. Large object: 1 MB distributed across 4 nodes.
///
/// Uses a larger chunk size (64 KB) to keep shard count reasonable for QUIC.
/// With 1 MB / 64 KB = 16 chunks * 4 shards = 64 total shards — well within
/// what a single QUIC connection can handle without congestion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ntest::timeout(30000)]
async fn test_quic_large_object() {
    let c = QuicTestCluster::new(4, 65536, 2, 2).await;
    let data = test_data(1_000_000); // 1 MB

    // PUT on node 0. With ACK-based push, shards are confirmed stored on
    // remote nodes before put_object returns — no sleep needed.
    c.node(0)
        .put_object("bucket", "bigfile", &data, BTreeMap::new())
        .await
        .unwrap();

    // Broadcast manifest.
    c.broadcast_manifest(0, "bucket", "bigfile").await;

    // GET from all 4 nodes.
    for i in 0..4 {
        let (got, _) = c
            .node(i)
            .get_object("bucket", "bigfile")
            .await
            .unwrap_or_else(|e| panic!("GET from node {i} failed: {e}"));
        assert_eq!(got.len(), data.len(), "node {i}: size mismatch");
        assert_eq!(got, data, "node {i}: data mismatch");
    }
}

/// 3c. Cross-node manifest visibility: PUT on A, GET on B and C.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ntest::timeout(30000)]
async fn test_quic_cross_node_manifest_sync() {
    let c = QuicTestCluster::new(3, 1024, 2, 1).await;
    let data = test_data(5000);

    // PUT on node 0 — this writes shards to the cluster via QUIC.
    c.node(0)
        .put_object("bucket", "cross", &data, BTreeMap::new())
        .await
        .unwrap();

    // Broadcast manifest over real QUIC uni streams.
    c.broadcast_manifest(0, "bucket", "cross").await;

    // GET from node 1.
    let (got1, _) = c.node(1).get_object("bucket", "cross").await.unwrap();
    assert_eq!(got1, data, "node 1 GET must match");

    // GET from node 2.
    let (got2, _) = c.node(2).get_object("bucket", "cross").await.unwrap();
    assert_eq!(got2, data, "node 2 GET must match");
}

/// 3d. Concurrent writes: 20 concurrent PUT calls on node 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ntest::timeout(60000)]
async fn test_quic_concurrent_writes() {
    // Use larger chunk size to keep shard count manageable.
    let c = Arc::new(QuicTestCluster::new(3, 4096, 2, 1).await);

    // Spawn 20 concurrent writes on node 0.
    let mut handles = Vec::new();
    for i in 0..20 {
        let cluster = c.clone();
        let handle = tokio::spawn(async move {
            let key = format!("concurrent-{i}");
            let data = test_data(2000 + i * 100);
            cluster
                .node(0)
                .put_object("bucket", &key, &data, BTreeMap::new())
                .await
                .unwrap();
            (key, data)
        });
        handles.push(handle);
    }

    // Wait for all writes to complete. ACK-based push guarantees shards
    // are stored on remote nodes — no sleep needed.
    let mut results: Vec<(String, Vec<u8>)> = Vec::new();
    for h in handles {
        results.push(h.await.unwrap());
    }

    // Broadcast all manifests.
    for (key, _) in &results {
        c.broadcast_manifest(0, "bucket", key).await;
    }

    // Read all 20 keys from node 1 and verify.
    for (key, expected) in &results {
        let (got, _) = c
            .node(1)
            .get_object("bucket", key)
            .await
            .unwrap_or_else(|e| panic!("GET {key} from node 1 failed: {e}"));
        assert_eq!(&got, expected, "data mismatch for key {key}");
    }
}

/// 3e. Manifest sync from peers via bi-directional QUIC streams.
///
/// Simulates a late-joining node: node 3 loses its metadata (mimicking a
/// cold start) and must recover all manifests by pulling from peers over
/// real QUIC bi-directional streams.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ntest::timeout(30000)]
async fn test_quic_manifest_sync_from_peers() {
    let c = QuicTestCluster::new(4, 4096, 2, 2).await;

    // PUT 5 objects on node 0.
    // put_object broadcasts manifests to all peers (including node 3).
    let mut objects = Vec::new();
    for i in 0..5 {
        let key = format!("sync-{i}");
        let data = test_data(3000 + i * 500);
        c.node(0)
            .put_object("bucket", &key, &data, BTreeMap::new())
            .await
            .unwrap();
        objects.push((key, data));
    }

    // Manifest broadcasts are uni-stream (fire-and-forget), so a short
    // yield is sufficient for the async runtime to process them.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Verify all nodes received manifests via broadcast.
    for i in 0..4 {
        for (key, _) in &objects {
            assert!(
                c.node(i).head_object("bucket", key).await.is_ok(),
                "node {i} should have manifest for {key} after broadcast"
            );
        }
    }

    // Simulate node 3 losing its metadata (cold restart / data loss).
    for (key, _) in &objects {
        c.node(3).meta().delete_object_key("bucket", key).unwrap();
    }

    // Verify node 3 no longer has any object keys.
    for (key, _) in &objects {
        assert!(
            c.node(3)
                .meta()
                .get_object_key("bucket", key)
                .unwrap()
                .is_none(),
            "node 3 should have lost {key}"
        );
    }

    // Node 3 syncs manifests from peers via bi-directional QUIC streams.
    let synced = c.node(3).sync_manifests_from_peers().await.unwrap();
    assert!(
        synced >= 5,
        "expected at least 5 synced manifests, got {synced}"
    );

    // GET all 5 from node 3 — shards fetched via real QUIC.
    for (key, expected) in &objects {
        let (got, _) = c
            .node(3)
            .get_object("bucket", key)
            .await
            .unwrap_or_else(|e| panic!("GET {key} from node 3 failed: {e}"));
        assert_eq!(&got, expected, "data mismatch for key {key} on node 3");
    }
}
