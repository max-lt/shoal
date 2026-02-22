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

/// ALPN protocol identifier for the Shoal network protocol.
pub const SHOAL_ALPN: &[u8] = b"shoal/0";
