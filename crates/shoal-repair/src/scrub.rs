//! Majority-vote deep scrub.
//!
//! For each local shard, the scrubber:
//! 1. Computes the local blake3 hash.
//! 2. Asks N peers who also hold the shard for their hash (lightweight RPC).
//! 3. If the local hash matches the majority → shard is good.
//! 4. If the local hash differs from majority → local shard is corrupt, fetch correct version.
//! 5. If no majority agrees → flag for manual inspection, do NOT auto-repair.

use std::collections::HashMap;
use std::sync::Arc;

use shoal_meta::MetaStore;
use shoal_store::ShardStore;
use shoal_types::{NodeId, ShardId};
use tracing::{debug, info, warn};

use crate::error::RepairError;

/// Result of scrubbing a single shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScrubVerdict {
    /// Local shard matches the majority hash.
    Healthy,
    /// Local shard differs from majority — it's corrupt.
    LocalCorrupt {
        /// The expected hash (majority).
        expected: ShardId,
        /// The local hash.
        local: ShardId,
    },
    /// No majority consensus — flag for manual inspection.
    NoConsensus {
        /// All hashes seen and their vote counts.
        votes: Vec<(ShardId, usize)>,
    },
    /// Not enough peers responded to form a quorum.
    InsufficientPeers {
        /// How many peers we asked.
        asked: usize,
        /// How many responded.
        responded: usize,
    },
}

/// Abstracts the hash-query RPC to peers.
#[async_trait::async_trait]
pub trait HashQuery: Send + Sync {
    /// Ask a peer for the blake3 hash of a shard they hold.
    ///
    /// Returns `None` if the peer doesn't have it or didn't respond.
    async fn query_hash(&self, node_id: NodeId, shard_id: ShardId) -> Option<ShardId>;
}

/// Deep scrubber that uses majority-vote to detect corruption.
pub struct DeepScrubber {
    local_store: Arc<dyn ShardStore>,
    meta: Arc<MetaStore>,
    hash_query: Arc<dyn HashQuery>,
    local_node_id: NodeId,
}

impl DeepScrubber {
    /// Create a new deep scrubber.
    pub fn new(
        local_store: Arc<dyn ShardStore>,
        meta: Arc<MetaStore>,
        hash_query: Arc<dyn HashQuery>,
        local_node_id: NodeId,
    ) -> Self {
        Self {
            local_store,
            meta,
            hash_query,
            local_node_id,
        }
    }

    /// Scrub a single shard using majority-vote.
    pub async fn scrub_shard(&self, shard_id: ShardId) -> Result<ScrubVerdict, RepairError> {
        // Step 1: compute local hash.
        let local_data = self.local_store.get(shard_id).await?;
        let local_hash = match local_data {
            Some(data) => ShardId::from_data(&data),
            None => {
                // We don't have it locally — can't scrub.
                return Ok(ScrubVerdict::InsufficientPeers {
                    asked: 0,
                    responded: 0,
                });
            }
        };

        // Step 2: find peers who should also hold this shard.
        let peers = self
            .meta
            .get_shard_owners(&shard_id)?
            .unwrap_or_default()
            .into_iter()
            .filter(|n| *n != self.local_node_id)
            .collect::<Vec<_>>();

        if peers.is_empty() {
            // No peers to check against.
            return Ok(ScrubVerdict::InsufficientPeers {
                asked: 0,
                responded: 0,
            });
        }

        // Step 3: query peers for their hashes.
        let mut votes: HashMap<ShardId, usize> = HashMap::new();
        votes.insert(local_hash, 1); // Count our own vote.

        let asked = peers.len();
        let mut responded = 0usize;

        for peer in &peers {
            if let Some(peer_hash) = self.hash_query.query_hash(*peer, shard_id).await {
                *votes.entry(peer_hash).or_insert(0) += 1;
                responded += 1;
            }
        }

        let total_votes = responded + 1; // +1 for local

        if responded == 0 {
            return Ok(ScrubVerdict::InsufficientPeers { asked, responded });
        }

        // Step 4: find majority.
        let majority_threshold = total_votes / 2 + 1;
        let mut vote_list: Vec<(ShardId, usize)> = votes.into_iter().collect();
        vote_list.sort_by(|a, b| b.1.cmp(&a.1)); // Highest votes first.

        let (winning_hash, winning_votes) = vote_list[0];

        if winning_votes >= majority_threshold {
            if winning_hash == local_hash {
                debug!(%shard_id, "scrub: shard is healthy (majority match)");
                Ok(ScrubVerdict::Healthy)
            } else {
                warn!(
                    %shard_id,
                    expected = %winning_hash,
                    local = %local_hash,
                    "scrub: local shard is CORRUPT (differs from majority)"
                );
                Ok(ScrubVerdict::LocalCorrupt {
                    expected: winning_hash,
                    local: local_hash,
                })
            }
        } else {
            // No majority — flag for manual inspection.
            warn!(
                %shard_id,
                votes = ?vote_list,
                "scrub: NO CONSENSUS — flagging for manual inspection"
            );
            Ok(ScrubVerdict::NoConsensus { votes: vote_list })
        }
    }

    /// Scrub all local shards. Returns a summary.
    pub async fn scrub_all(&self) -> Result<ScrubSummary, RepairError> {
        let local_shards = self.local_store.list().await?;
        let mut summary = ScrubSummary::default();

        for shard_id in local_shards {
            match self.scrub_shard(shard_id).await? {
                ScrubVerdict::Healthy => summary.healthy += 1,
                ScrubVerdict::LocalCorrupt { expected, local } => {
                    summary.corrupt += 1;
                    // Enqueue for repair with highest priority.
                    self.meta.enqueue_repair(&shard_id, 0)?;
                    info!(
                        %shard_id,
                        expected = %expected,
                        local = %local,
                        "corrupt shard enqueued for repair"
                    );
                }
                ScrubVerdict::NoConsensus { .. } => {
                    summary.no_consensus += 1;
                }
                ScrubVerdict::InsufficientPeers { .. } => {
                    summary.insufficient_peers += 1;
                }
            }
            summary.total += 1;
        }

        info!(
            total = summary.total,
            healthy = summary.healthy,
            corrupt = summary.corrupt,
            no_consensus = summary.no_consensus,
            insufficient_peers = summary.insufficient_peers,
            "deep scrub complete"
        );

        Ok(summary)
    }
}

/// Summary of a deep scrub run.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ScrubSummary {
    /// Total shards scrubbed.
    pub total: usize,
    /// Shards that matched the majority hash.
    pub healthy: usize,
    /// Shards that differed from the majority (local is corrupt).
    pub corrupt: usize,
    /// Shards where no majority consensus was reached.
    pub no_consensus: usize,
    /// Shards where not enough peers responded.
    pub insufficient_peers: usize,
}
