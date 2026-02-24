//! Protocol messages for the Shoal network layer.
//!
//! All messages are serialized with postcard over QUIC streams.

use serde::{Deserialize, Serialize};
use shoal_types::{ClusterEvent, Member, ObjectId, ShardId};

/// Protocol messages exchanged between Shoal nodes.
///
/// Each message is sent as a length-prefixed postcard-encoded payload
/// over a QUIC stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShoalMessage {
    /// Push a shard to a remote node (bi-directional, expects [`ShardPushAck`]).
    ///
    /// Sent on a bi-stream so the sender can wait for an ACK confirming the
    /// receiver has stored the shard before deleting its local copy.
    ShardPush {
        /// Content-addressed ID of the shard.
        shard_id: ShardId,
        /// Raw shard data.
        data: Vec<u8>,
    },

    /// Acknowledgement that a pushed shard was stored successfully.
    ///
    /// Sent as a response on the same bi-stream as the [`ShardPush`].
    /// The sender must not delete its local copy until this ACK is received.
    ShardPushAck {
        /// ID of the shard that was stored.
        shard_id: ShardId,
        /// Whether the store succeeded.
        ok: bool,
    },

    /// Request a shard from a remote node.
    ShardRequest {
        /// Content-addressed ID of the shard to fetch.
        shard_id: ShardId,
    },

    /// Response to a [`ShoalMessage::ShardRequest`].
    ShardResponse {
        /// ID of the requested shard.
        shard_id: ShardId,
        /// The shard data, or `None` if the node doesn't have it.
        data: Option<Vec<u8>>,
    },

    /// A cluster event broadcast via gossip.
    ClusterEvent(ClusterEvent),

    /// Membership state update (piggybacked on foca).
    MembershipUpdate(Vec<Member>),

    /// Health check ping.
    Ping {
        /// Timestamp (millis since epoch) when the ping was sent.
        timestamp: u64,
    },

    /// Health check pong (response to [`ShoalMessage::Ping`]).
    Pong {
        /// Timestamp from the original ping.
        timestamp: u64,
    },

    /// Raw SWIM protocol data routed between foca membership services.
    SwimData(Vec<u8>),

    /// Broadcast a manifest and its object key mapping to other nodes.
    ManifestPut {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Postcard-serialized [`Manifest`](shoal_types::Manifest).
        manifest_bytes: Vec<u8>,
    },

    /// Request a manifest by bucket/key (bi-directional, expects ManifestResponse).
    ManifestRequest {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
    },

    /// Response to a [`ShoalMessage::ManifestRequest`].
    ManifestResponse {
        /// Bucket name (echoed back).
        bucket: String,
        /// Object key (echoed back).
        key: String,
        /// Postcard-serialized [`Manifest`](shoal_types::Manifest), or `None` if not found.
        manifest_bytes: Option<Vec<u8>>,
    },

    /// Request all manifests from a peer (used for catch-up on join).
    ManifestSyncRequest,

    /// Response containing all manifests the peer has.
    ManifestSyncResponse {
        /// Each entry is (bucket, key, postcard-serialized Manifest).
        entries: Vec<ManifestSyncEntry>,
    },

    /// Broadcast a new log entry (+ optional manifest for Put actions).
    LogEntryBroadcast {
        /// Postcard-serialized [`LogEntry`](shoal_logtree::LogEntry).
        entry_bytes: Vec<u8>,
        /// Postcard-serialized [`Manifest`](shoal_types::Manifest) for Put actions.
        manifest_bytes: Option<Vec<u8>>,
    },

    /// Request missing log entries. Sender provides their tip hashes.
    LogSyncRequest {
        /// The sender's current DAG tip hashes.
        tips: Vec<[u8; 32]>,
    },

    /// Response with missing entries and their associated manifests.
    LogSyncResponse {
        /// Each entry is a postcard-serialized [`LogEntry`](shoal_logtree::LogEntry).
        entries: Vec<Vec<u8>>,
        /// Associated manifests: (ObjectId, postcard-serialized Manifest).
        manifests: Vec<(ObjectId, Vec<u8>)>,
    },
}

/// A single manifest entry in a [`ShoalMessage::ManifestSyncResponse`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestSyncEntry {
    /// Bucket name.
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// Postcard-serialized [`Manifest`](shoal_types::Manifest).
    pub manifest_bytes: Vec<u8>,
}
