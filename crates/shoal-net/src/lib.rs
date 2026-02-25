//! Network protocol on iroh QUIC.
//!
//! This crate implements Shoal's network layer on top of [iroh] QUIC connections:
//!
//! - [`ShoalMessage`] — the wire protocol (postcard-serialized).
//! - [`ShoalTransport`] — manages an iroh [`Endpoint`], connection pooling,
//!   and message send/receive.
//! - Shard transfer with end-to-end integrity verification (blake3).
//!
//! [`Endpoint`]: iroh::Endpoint

mod error;
mod message;
#[cfg(test)]
mod tests;
mod transport;

pub use error::NetError;
pub use message::ShoalMessage;
pub use transport::ShoalTransport;

/// Trait abstracting the network transport operations used by the engine.
///
/// This allows substituting a mock transport in tests (avoiding the need
/// for real iroh QUIC endpoints and network access).
#[async_trait::async_trait]
pub trait Transport: Send + Sync {
    /// Push a shard to a remote node.
    async fn push_shard(
        &self,
        addr: iroh::EndpointAddr,
        shard_id: shoal_types::ShardId,
        data: bytes::Bytes,
    ) -> Result<(), NetError>;

    /// Pull a shard from a remote node. Returns `None` if the remote
    /// node does not have the shard.
    async fn pull_shard(
        &self,
        addr: iroh::EndpointAddr,
        shard_id: shoal_types::ShardId,
    ) -> Result<Option<bytes::Bytes>, NetError>;

    /// Send a message to a remote node (uni-directional).
    async fn send_to(&self, addr: iroh::EndpointAddr, msg: &ShoalMessage) -> Result<(), NetError>;

    /// Pull manifests by ObjectId from a remote node (batch).
    ///
    /// Returns pairs of (ObjectId, postcard-serialized Manifest) for found manifests.
    async fn pull_manifests(
        &self,
        addr: iroh::EndpointAddr,
        manifest_ids: &[shoal_types::ObjectId],
    ) -> Result<Vec<(shoal_types::ObjectId, Vec<u8>)>, NetError>;

    /// Pull missing log entries from a remote node.
    ///
    /// Sends our tip hashes, receives entries we are missing.
    /// Manifests are pulled separately via `pull_manifests`.
    async fn pull_log_entries(
        &self,
        addr: iroh::EndpointAddr,
        my_tips: &[[u8; 32]],
    ) -> Result<Vec<Vec<u8>>, NetError>;

    /// Targeted pull: request specific entries and their ancestor chain.
    ///
    /// Sends the hashes of entries we need (e.g. missing parents from the
    /// pending buffer) plus our tips. The responder walks backward from
    /// `entry_hashes` and returns the transitive closure up to our tips.
    /// Manifests are pulled separately via `pull_manifests`.
    async fn pull_log_sync(
        &self,
        addr: iroh::EndpointAddr,
        entry_hashes: &[[u8; 32]],
        my_tips: &[[u8; 32]],
    ) -> Result<Vec<Vec<u8>>, NetError>;

    /// Pull API key secrets by access_key_id from a remote node (batch).
    ///
    /// Returns pairs of (access_key_id, secret_access_key) for found keys.
    async fn pull_api_keys(
        &self,
        addr: iroh::EndpointAddr,
        access_key_ids: &[String],
    ) -> Result<Vec<(String, String)>, NetError>;
}

/// Default ALPN protocol identifier (no cluster secret).
pub const SHOAL_ALPN: &[u8] = b"shoal/0";

/// Derive a cluster-specific ALPN from a shared secret.
///
/// The ALPN is `shoal/0/<first 16 hex chars of blake3(secret)>`.
/// Nodes with different secrets get different ALPNs and cannot
/// establish QUIC connections to each other — the TLS handshake
/// itself rejects the mismatch before any application data is exchanged.
pub fn cluster_alpn(secret: &[u8]) -> Vec<u8> {
    let hash = blake3::hash(secret);
    let hex = hash.to_hex();
    format!("shoal/0/{}", &hex[..16]).into_bytes()
}
