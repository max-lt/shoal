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

    /// Batch request for manifests by ObjectId.
    ManifestRequest {
        /// The manifest ObjectIds to fetch.
        manifest_ids: Vec<ObjectId>,
    },

    /// Response to a [`ShoalMessage::ManifestRequest`].
    ManifestResponse {
        /// Pairs of (ObjectId, postcard-serialized Manifest) for found manifests.
        manifests: Vec<(ObjectId, Vec<u8>)>,
    },

    /// Broadcast a new log entry to other nodes (unicast fallback).
    ///
    /// Manifests and secrets referenced by the entry are pulled via QUIC
    /// on-demand by the receiver.
    LogEntryBroadcast {
        /// Postcard-serialized [`LogEntry`](shoal_logtree::LogEntry).
        entry_bytes: Vec<u8>,
    },

    /// Request missing log entries. Sender provides their tip hashes.
    LogSyncRequest {
        /// The sender's current DAG tip hashes.
        tips: Vec<[u8; 32]>,
    },

    /// Response with missing log entries (manifests pulled separately).
    LogSyncResponse {
        /// Each entry is a postcard-serialized [`LogEntry`](shoal_logtree::LogEntry).
        entries: Vec<Vec<u8>>,
    },

    /// Push specific log entries to a node (unicast response to gossip WantEntries).
    ///
    /// Sent as a uni-stream message — no response expected.
    /// Manifests are pulled separately via batch ManifestRequest.
    ProvideLogEntries {
        /// Postcard-serialized [`LogEntry`](shoal_logtree::LogEntry) payloads.
        entries: Vec<Vec<u8>>,
    },

    /// Targeted pull: request specific entries and their ancestor chain.
    ///
    /// The responder does BFS backward from `entry_hashes` through parents,
    /// stopping at entries in `requester_tips` (already known to the requester)
    /// or at DAG roots. Returns entries in topological order.
    ///
    /// This is LOCAL-ONLY on the responder side — no recursive peer pull.
    LogSyncPull {
        /// Hashes of entries the requester needs (typically missing parents).
        entry_hashes: Vec<[u8; 32]>,
        /// The requester's current DAG tip hashes (stop traversal here).
        requester_tips: Vec<[u8; 32]>,
    },

    /// Response to a [`ShoalMessage::LogSyncPull`].
    LogSyncPullResponse {
        /// Entries in topological order (parents before children).
        entries: Vec<Vec<u8>>,
    },

    /// Batch request for API key secrets by access_key_id.
    ApiKeyRequest {
        /// The access key IDs to look up.
        access_key_ids: Vec<String>,
    },

    /// Response with (access_key_id, secret_access_key) pairs.
    ApiKeyResponse {
        /// Found keys: (access_key_id, secret_access_key).
        keys: Vec<(String, String)>,
    },
}
