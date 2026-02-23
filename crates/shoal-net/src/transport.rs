//! Network transport built on iroh QUIC.
//!
//! [`ShoalTransport`] wraps an iroh [`Endpoint`] and provides:
//! - Connection pooling (reuse connections to the same peer).
//! - Message send/receive with length-prefixed postcard encoding.
//! - Shard push/pull with blake3 integrity verification.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr, SecretKey};
use shoal_types::ShardId;
use tokio::sync::RwLock;
use tracing::{debug, error, warn};

use crate::SHOAL_ALPN;
use crate::error::NetError;
use crate::message::ShoalMessage;

/// Maximum message size: 64 MB. Shard data is typically ≤4 MB,
/// but manifests can reference many shards.
const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// Network transport for inter-node communication.
///
/// Manages an iroh QUIC endpoint, a connection pool to peer nodes,
/// and provides high-level send/receive operations with integrity checks.
pub struct ShoalTransport {
    endpoint: Endpoint,
    /// Cached connections to remote peers, keyed by their iroh endpoint ID.
    connections: Arc<RwLock<HashMap<iroh::EndpointId, Connection>>>,
    /// ALPN used for outgoing connections. Derived from the cluster secret
    /// so that nodes with different secrets cannot connect.
    alpn: Vec<u8>,
}

impl ShoalTransport {
    /// Create a new transport with the default ALPN (`shoal/0`).
    ///
    /// Use [`iroh::RelayMode::Disabled`] for tests that don't need relay servers.
    pub async fn bind(
        secret_key: SecretKey,
        relay_mode: iroh::RelayMode,
    ) -> Result<Self, NetError> {
        Self::bind_with_alpn(secret_key, relay_mode, SHOAL_ALPN.to_vec()).await
    }

    /// Create a new transport with a cluster-specific ALPN.
    ///
    /// Use [`crate::cluster_alpn`] to derive the ALPN from a shared secret.
    /// Nodes with different ALPNs cannot establish QUIC connections — the
    /// TLS handshake itself rejects the mismatch.
    pub async fn bind_with_alpn(
        secret_key: SecretKey,
        relay_mode: iroh::RelayMode,
        alpn: Vec<u8>,
    ) -> Result<Self, NetError> {
        let endpoint = Endpoint::builder()
            .secret_key(secret_key)
            .alpns(vec![alpn.clone()])
            .relay_mode(relay_mode)
            .bind()
            .await
            .map_err(|e| NetError::Endpoint(e.to_string()))?;

        Ok(Self {
            endpoint,
            connections: Arc::new(RwLock::new(HashMap::new())),
            alpn,
        })
    }

    /// Create a transport wrapping an existing iroh endpoint.
    pub fn from_endpoint(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            connections: Arc::new(RwLock::new(HashMap::new())),
            alpn: SHOAL_ALPN.to_vec(),
        }
    }

    /// Return a reference to the underlying iroh endpoint.
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Return the [`EndpointAddr`] of this transport (ID + addresses).
    pub fn addr(&self) -> EndpointAddr {
        self.endpoint.addr()
    }

    /// Return this endpoint's public identity.
    pub fn endpoint_id(&self) -> iroh::EndpointId {
        self.endpoint.id()
    }

    // -------------------------------------------------------------------
    // Connection management
    // -------------------------------------------------------------------

    /// Get or establish a QUIC connection to a remote peer.
    async fn get_connection(&self, addr: EndpointAddr) -> Result<Connection, NetError> {
        let remote_id = addr.id;

        // Check cache first.
        {
            let cache = self.connections.read().await;
            if let Some(conn) = cache.get(&remote_id) {
                // Verify the connection is still alive.
                if conn.close_reason().is_none() {
                    return Ok(conn.clone());
                }
            }
        }

        // Establish a new connection.
        debug!(remote = %remote_id.fmt_short(), "connecting to peer");
        let conn = self
            .endpoint
            .connect(addr, &self.alpn)
            .await
            .map_err(|e| NetError::Connect(e.to_string()))?;

        // Cache it.
        {
            let mut cache = self.connections.write().await;
            cache.insert(remote_id, conn.clone());
        }

        Ok(conn)
    }

    /// Remove a cached connection (e.g. after detecting it's dead).
    pub async fn remove_connection(&self, id: &iroh::EndpointId) {
        let mut cache = self.connections.write().await;
        cache.remove(id);
    }

    // -------------------------------------------------------------------
    // High-level message sending
    // -------------------------------------------------------------------

    /// Send an arbitrary message to a remote peer.
    ///
    /// Opens a new uni-directional stream on a (pooled) connection to the
    /// given address and sends the postcard-encoded message.
    pub async fn send_to(&self, addr: EndpointAddr, msg: &ShoalMessage) -> Result<(), NetError> {
        let conn = self.get_connection(addr).await?;
        Self::send_message(&conn, msg).await
    }

    // -------------------------------------------------------------------
    // Low-level message send/receive
    // -------------------------------------------------------------------

    /// Send a message over a new uni-directional stream on the given connection.
    ///
    /// The message is length-prefixed (4-byte big-endian) then postcard-encoded.
    pub async fn send_message(conn: &Connection, message: &ShoalMessage) -> Result<(), NetError> {
        let payload =
            postcard::to_allocvec(message).map_err(|e| NetError::Serialization(e.to_string()))?;

        let mut send = conn
            .open_uni()
            .await
            .map_err(|e| NetError::StreamOpen(e.to_string()))?;

        // Length prefix (4 bytes big-endian).
        send.write_all(&(payload.len() as u32).to_be_bytes())
            .await?;
        send.write_all(&payload).await?;
        send.finish()?;

        Ok(())
    }

    /// Receive a message from a uni-directional receive stream.
    ///
    /// Reads a 4-byte big-endian length prefix, then reads that many bytes
    /// and deserializes with postcard.
    pub async fn recv_message(recv: &mut RecvStream) -> Result<ShoalMessage, NetError> {
        // Read length prefix.
        let mut len_buf = [0u8; 4];
        recv.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;

        if len > MAX_MESSAGE_SIZE {
            return Err(NetError::Serialization(format!(
                "message too large: {len} bytes (max {MAX_MESSAGE_SIZE})"
            )));
        }

        let payload = recv.read_to_end(len).await?;
        let message: ShoalMessage =
            postcard::from_bytes(&payload).map_err(|e| NetError::Serialization(e.to_string()))?;

        Ok(message)
    }

    // -------------------------------------------------------------------
    // High-level shard transfer with integrity
    // -------------------------------------------------------------------

    /// Push a shard to a remote node.
    ///
    /// The receiver should verify `blake3(data) == shard_id` before accepting.
    pub async fn push_shard(
        &self,
        addr: EndpointAddr,
        shard_id: ShardId,
        data: Bytes,
    ) -> Result<(), NetError> {
        let conn = self.get_connection(addr).await?;
        let msg = ShoalMessage::ShardPush {
            shard_id,
            data: data.to_vec(),
        };
        Self::send_message(&conn, &msg).await?;
        debug!(%shard_id, size = data.len(), "pushed shard to peer");
        Ok(())
    }

    /// Pull a shard from a remote node.
    ///
    /// Opens a bidirectional stream: sends a `ShardRequest`, receives a
    /// `ShardResponse`, and verifies integrity before returning the data.
    pub async fn pull_shard(
        &self,
        addr: EndpointAddr,
        shard_id: ShardId,
    ) -> Result<Option<Bytes>, NetError> {
        let conn = self.get_connection(addr).await?;

        // Open a bi-directional stream for request/response.
        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| NetError::StreamOpen(e.to_string()))?;

        // Send request.
        let request = ShoalMessage::ShardRequest { shard_id };
        let payload =
            postcard::to_allocvec(&request).map_err(|e| NetError::Serialization(e.to_string()))?;
        send.write_all(&(payload.len() as u32).to_be_bytes())
            .await?;
        send.write_all(&payload).await?;
        send.finish()?;

        // Receive response.
        let response = Self::recv_message(&mut recv).await?;

        match response {
            ShoalMessage::ShardResponse {
                shard_id: resp_id,
                data: Some(data),
            } => {
                // End-to-end integrity: verify blake3 hash matches shard_id.
                let actual_id = ShardId::from_data(&data);
                if actual_id != shard_id {
                    error!(
                        expected = %shard_id,
                        actual = %actual_id,
                        "integrity check failed on pulled shard"
                    );
                    return Err(NetError::IntegrityFailure {
                        expected: shard_id,
                        actual: actual_id,
                    });
                }
                if resp_id != shard_id {
                    warn!(
                        requested = %shard_id,
                        received = %resp_id,
                        "response shard_id mismatch"
                    );
                }
                debug!(%shard_id, size = data.len(), "pulled shard from peer");
                Ok(Some(Bytes::from(data)))
            }
            ShoalMessage::ShardResponse { data: None, .. } => Ok(None),
            other => Err(NetError::Serialization(format!(
                "unexpected response type: {other:?}"
            ))),
        }
    }

    /// Request a manifest from a remote node by bucket/key.
    ///
    /// Opens a bidirectional stream: sends a `ManifestRequest`, receives a
    /// `ManifestResponse`. Returns the raw manifest bytes if found.
    pub async fn pull_manifest(
        &self,
        addr: EndpointAddr,
        bucket: &str,
        key: &str,
    ) -> Result<Option<Vec<u8>>, NetError> {
        let conn = self.get_connection(addr).await?;

        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| NetError::StreamOpen(e.to_string()))?;

        let request = ShoalMessage::ManifestRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
        };
        let payload =
            postcard::to_allocvec(&request).map_err(|e| NetError::Serialization(e.to_string()))?;
        send.write_all(&(payload.len() as u32).to_be_bytes())
            .await?;
        send.write_all(&payload).await?;
        send.finish()?;

        let response = Self::recv_message(&mut recv).await?;

        match response {
            ShoalMessage::ManifestResponse {
                manifest_bytes: Some(bytes),
                ..
            } => {
                debug!(%bucket, %key, "pulled manifest from peer");
                Ok(Some(bytes))
            }
            ShoalMessage::ManifestResponse {
                manifest_bytes: None,
                ..
            } => Ok(None),
            other => Err(NetError::Serialization(format!(
                "unexpected response type: {other:?}"
            ))),
        }
    }

    // -------------------------------------------------------------------
    // Incoming message handling
    // -------------------------------------------------------------------

    /// Accept a single incoming connection and return it.
    ///
    /// Returns `None` if the endpoint is shutting down.
    pub async fn accept(&self) -> Option<Connection> {
        let incoming = self.endpoint.accept().await?;
        match incoming.await {
            Ok(conn) => Some(conn),
            Err(e) => {
                warn!("failed to accept connection: {e}");
                None
            }
        }
    }

    /// Accept incoming uni-directional streams on a connection
    /// and dispatch messages to the provided handler.
    ///
    /// This runs until the connection is closed.
    pub async fn handle_connection<F, Fut>(conn: Connection, handler: F)
    where
        F: Fn(ShoalMessage, Connection) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        loop {
            match conn.accept_uni().await {
                Ok(mut recv) => match Self::recv_message(&mut recv).await {
                    Ok(msg) => handler(msg, conn.clone()).await,
                    Err(e) => {
                        warn!("failed to decode message: {e}");
                    }
                },
                Err(e) => {
                    debug!("connection closed: {e}");
                    break;
                }
            }
        }
    }

    /// Handle incoming bidirectional streams (used for request/response patterns like shard pull).
    ///
    /// For each incoming bi stream, reads a request and calls the handler which
    /// must produce a response message. The response is sent back on the same stream.
    pub async fn handle_bi_streams<F, Fut>(conn: Connection, handler: F)
    where
        F: Fn(ShoalMessage) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Option<ShoalMessage>> + Send,
    {
        loop {
            match conn.accept_bi().await {
                Ok((mut send, mut recv)) => match Self::recv_message(&mut recv).await {
                    Ok(request) => {
                        if let Some(response) = handler(request).await
                            && let Err(e) = Self::send_on_stream(&mut send, &response).await
                        {
                            warn!("failed to send response: {e}");
                        }
                    }
                    Err(e) => {
                        warn!("failed to decode bi-stream request: {e}");
                    }
                },
                Err(e) => {
                    debug!("connection closed (bi): {e}");
                    break;
                }
            }
        }
    }

    /// Send a message on an already-open send stream (for bi-directional responses).
    pub async fn send_on_stream(
        send: &mut SendStream,
        message: &ShoalMessage,
    ) -> Result<(), NetError> {
        let payload =
            postcard::to_allocvec(message).map_err(|e| NetError::Serialization(e.to_string()))?;
        send.write_all(&(payload.len() as u32).to_be_bytes())
            .await?;
        send.write_all(&payload).await?;
        send.finish()?;
        Ok(())
    }

    /// Verify that received shard data matches its expected ShardId.
    ///
    /// Returns `Ok(())` if the hash matches, or `Err(NetError::IntegrityFailure)` if not.
    pub fn verify_shard_integrity(shard_id: ShardId, data: &[u8]) -> Result<(), NetError> {
        let actual = ShardId::from_data(data);
        if actual != shard_id {
            return Err(NetError::IntegrityFailure {
                expected: shard_id,
                actual,
            });
        }
        Ok(())
    }

    /// Gracefully close the transport.
    pub async fn close(&self) {
        self.endpoint.close().await;
    }
}
