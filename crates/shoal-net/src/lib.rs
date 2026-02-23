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
