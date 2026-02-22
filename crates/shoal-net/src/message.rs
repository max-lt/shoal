//! Protocol messages for the Shoal network layer.
//!
//! All messages are serialized with postcard over QUIC streams.

use serde::{Deserialize, Serialize};
use shoal_types::{ClusterEvent, Member, ShardId};

/// Protocol messages exchanged between Shoal nodes.
///
/// Each message is sent as a length-prefixed postcard-encoded payload
/// over a QUIC stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShoalMessage {
    /// Push a shard to a remote node.
    ShardPush {
        /// Content-addressed ID of the shard.
        shard_id: ShardId,
        /// Raw shard data.
        data: Vec<u8>,
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
}
