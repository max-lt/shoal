//! Consistent hashing ring implementation.

use std::collections::{BTreeMap, HashMap};

use shoal_types::{NodeId, ShardId};
use tracing::debug;

/// Metadata about a node on the ring.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    /// Storage capacity in bytes.
    pub capacity: u64,
    /// Number of vnodes this node owns (proportional to capacity).
    pub weight: u16,
}

/// A shard migration from one node to another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Migration {
    /// The shard that must move.
    pub shard_id: ShardId,
    /// The node that currently owns it.
    pub from: NodeId,
    /// The node that should own it after the change.
    pub to: NodeId,
}

/// Consistent hashing ring for deterministic shard placement.
///
/// Each node is mapped to multiple virtual nodes (vnodes) on a u64 ring.
/// Shard placement is determined by walking clockwise from the shard's
/// position until enough distinct physical nodes are found.
#[derive(Debug, Clone)]
pub struct Ring {
    /// Virtual node positions: ring position -> physical node.
    vnodes: BTreeMap<u64, NodeId>,
    /// Per-node metadata.
    nodes: HashMap<NodeId, NodeInfo>,
    /// Base number of vnodes per unit of weight.
    vnodes_per_node: u16,
}

impl Ring {
    /// Create a new empty ring.
    ///
    /// `vnodes_per_node` is the base vnode count for a node with weight 1.
    /// Nodes with higher capacity get proportionally more vnodes.
    pub fn new(vnodes_per_node: u16) -> Self {
        Self {
            vnodes: BTreeMap::new(),
            nodes: HashMap::new(),
            vnodes_per_node,
        }
    }

    /// Add a node to the ring with the given capacity.
    ///
    /// The node's weight (number of vnodes) is proportional to its capacity
    /// relative to the base `vnodes_per_node`. A node with `capacity` gets
    /// `vnodes_per_node` vnodes; a node with double the capacity gets double.
    pub fn add_node(&mut self, node_id: NodeId, capacity: u64) {
        self.add_node_with_weight(node_id, capacity, self.vnodes_per_node);
    }

    /// Add a node with an explicit weight (number of vnodes).
    pub fn add_node_with_weight(&mut self, node_id: NodeId, capacity: u64, weight: u16) {
        // Remove first if already present (re-add with new weight).
        self.remove_node(&node_id);

        let info = NodeInfo { capacity, weight };

        for i in 0..weight {
            let pos = vnode_position(&node_id, i);
            self.vnodes.insert(pos, node_id);
        }

        self.nodes.insert(node_id, info);
        debug!(%node_id, weight, "added node to ring");
    }

    /// Remove a node from the ring.
    pub fn remove_node(&mut self, node_id: &NodeId) {
        if let Some(info) = self.nodes.remove(node_id) {
            for i in 0..info.weight {
                let pos = vnode_position(node_id, i);
                self.vnodes.remove(&pos);
            }
            debug!(%node_id, "removed node from ring");
        }
    }

    /// Determine which nodes own a shard.
    ///
    /// Walks clockwise from the shard's position on the ring, collecting
    /// `replication_factor` distinct physical node IDs. If fewer distinct
    /// nodes exist than `replication_factor`, returns all available nodes.
    pub fn owners(&self, shard_id: &ShardId, replication_factor: usize) -> Vec<NodeId> {
        if self.vnodes.is_empty() {
            return Vec::new();
        }

        let pos = shard_position(shard_id);
        let mut owners = Vec::with_capacity(replication_factor);
        let max_distinct = replication_factor.min(self.nodes.len());

        // Walk clockwise from the shard's position.
        // BTreeMap::range gives us everything >= pos, then we wrap around.
        let after = self.vnodes.range(pos..);
        let before = self.vnodes.range(..pos);

        for (_, node_id) in after.chain(before) {
            if !owners.contains(node_id) {
                owners.push(*node_id);
                if owners.len() == max_distinct {
                    break;
                }
            }
        }

        owners
    }

    /// Compute which shards must migrate between two ring states.
    ///
    /// For each shard in `shard_ids`, computes its owners in both `old` and
    /// `new` rings. Any shard whose primary owner changed produces a migration.
    pub fn diff(
        old: &Ring,
        new: &Ring,
        shard_ids: &[ShardId],
        replication_factor: usize,
    ) -> Vec<Migration> {
        let mut migrations = Vec::new();

        for shard_id in shard_ids {
            let old_owners = old.owners(shard_id, replication_factor);
            let new_owners = new.owners(shard_id, replication_factor);

            // Find nodes that gained ownership.
            for new_owner in &new_owners {
                if !old_owners.contains(new_owner) {
                    // This shard is newly assigned to new_owner.
                    // It should come from an old owner that lost it.
                    if let Some(from) = old_owners.iter().find(|o| !new_owners.contains(o)) {
                        migrations.push(Migration {
                            shard_id: *shard_id,
                            from: *from,
                            to: *new_owner,
                        });
                    }
                }
            }
        }

        migrations
    }

    /// Return the number of physical nodes in the ring.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Return the total number of vnodes in the ring.
    pub fn vnode_count(&self) -> usize {
        self.vnodes.len()
    }

    /// Return info about a specific node, if present.
    pub fn node_info(&self, node_id: &NodeId) -> Option<&NodeInfo> {
        self.nodes.get(node_id)
    }

    /// Return all node IDs in the ring.
    pub fn node_ids(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }
}

/// Compute a vnode's position on the ring: blake3(node_id ++ vnode_index) truncated to u64.
fn vnode_position(node_id: &NodeId, vnode_index: u16) -> u64 {
    let mut input = Vec::with_capacity(34);
    input.extend_from_slice(node_id.as_ref());
    input.extend_from_slice(&vnode_index.to_le_bytes());
    let hash = blake3::hash(&input);
    let bytes: [u8; 8] = hash.as_bytes()[..8].try_into().expect("8 bytes");
    u64::from_le_bytes(bytes)
}

/// Compute a shard's position on the ring: first 8 bytes of its ID as u64.
fn shard_position(shard_id: &ShardId) -> u64 {
    let bytes: [u8; 8] = shard_id.as_ref()[..8].try_into().expect("8 bytes");
    u64::from_le_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(n: u8) -> NodeId {
        NodeId::from([n; 32])
    }

    fn shard(n: u8) -> ShardId {
        ShardId::from_data(&[n])
    }

    #[test]
    fn test_single_node_all_shards_map_to_it() {
        let mut ring = Ring::new(128);
        let n = node(1);
        ring.add_node(n, 1000);

        for i in 0..100u8 {
            let owners = ring.owners(&shard(i), 1);
            assert_eq!(owners, vec![n]);
        }
    }

    #[test]
    fn test_two_nodes_roughly_balanced() {
        let mut ring = Ring::new(128);
        let n1 = node(1);
        let n2 = node(2);
        ring.add_node(n1, 1000);
        ring.add_node(n2, 1000);

        let mut count1 = 0usize;
        let mut count2 = 0usize;
        let total = 10_000;

        for i in 0..total {
            let s = ShardId::from_data(&(i as u32).to_le_bytes());
            let owners = ring.owners(&s, 1);
            if owners[0] == n1 {
                count1 += 1;
            } else {
                count2 += 1;
            }
        }

        // Within 20% of 50/50.
        let ratio = count1 as f64 / total as f64;
        assert!(
            (0.3..=0.7).contains(&ratio),
            "distribution too skewed: {count1}/{count2} ({ratio:.2})"
        );
    }

    #[test]
    fn test_add_node_only_fraction_moves() {
        let mut ring = Ring::new(128);
        let n1 = node(1);
        let n2 = node(2);
        ring.add_node(n1, 1000);
        ring.add_node(n2, 1000);

        let total = 10_000;
        let shards: Vec<ShardId> = (0..total)
            .map(|i| ShardId::from_data(&(i as u32).to_le_bytes()))
            .collect();

        // Record owners before adding node 3.
        let before: Vec<NodeId> = shards.iter().map(|s| ring.owners(s, 1)[0]).collect();

        // Add a third node.
        let n3 = node(3);
        ring.add_node(n3, 1000);

        let after: Vec<NodeId> = shards.iter().map(|s| ring.owners(s, 1)[0]).collect();

        let moved = before
            .iter()
            .zip(after.iter())
            .filter(|(b, a)| b != a)
            .count();

        // ~1/3 should move (consistent hashing property).
        let move_ratio = moved as f64 / total as f64;
        assert!(
            (0.1..=0.6).contains(&move_ratio),
            "too many or too few shards moved: {moved}/{total} ({move_ratio:.2})"
        );
    }

    #[test]
    fn test_remove_node_only_its_shards_redistribute() {
        let mut ring = Ring::new(128);
        let n1 = node(1);
        let n2 = node(2);
        let n3 = node(3);
        ring.add_node(n1, 1000);
        ring.add_node(n2, 1000);
        ring.add_node(n3, 1000);

        let total = 10_000;
        let shards: Vec<ShardId> = (0..total)
            .map(|i| ShardId::from_data(&(i as u32).to_le_bytes()))
            .collect();

        let before: Vec<NodeId> = shards.iter().map(|s| ring.owners(s, 1)[0]).collect();

        // Remove node 2.
        ring.remove_node(&n2);

        let after: Vec<NodeId> = shards.iter().map(|s| ring.owners(s, 1)[0]).collect();

        // Only shards that were on n2 should have moved.
        for (i, (b, a)) in before.iter().zip(after.iter()).enumerate() {
            if *b != n2 {
                assert_eq!(
                    b, a,
                    "shard {i} was on {b} (not the removed node) but moved to {a}"
                );
            }
        }
    }

    #[test]
    fn test_replication_factor_3_distinct_owners() {
        let mut ring = Ring::new(128);
        let n1 = node(1);
        let n2 = node(2);
        let n3 = node(3);
        ring.add_node(n1, 1000);
        ring.add_node(n2, 1000);
        ring.add_node(n3, 1000);

        for i in 0..100u8 {
            let owners = ring.owners(&shard(i), 3);
            assert_eq!(owners.len(), 3);
            // All distinct.
            let mut unique = owners.clone();
            unique.sort();
            unique.dedup();
            assert_eq!(unique.len(), 3, "owners not distinct for shard {i}");
        }
    }

    #[test]
    fn test_replication_factor_exceeds_node_count() {
        let mut ring = Ring::new(128);
        let n1 = node(1);
        let n2 = node(2);
        ring.add_node(n1, 1000);
        ring.add_node(n2, 1000);

        let owners = ring.owners(&shard(42), 5);
        assert_eq!(owners.len(), 2, "should return all nodes, not panic");
    }

    #[test]
    fn test_weighted_nodes_proportional_shards() {
        let mut ring = Ring::new(64);
        let n1 = node(1);
        let n2 = node(2);
        // n2 gets 2x the vnodes.
        ring.add_node_with_weight(n1, 1000, 64);
        ring.add_node_with_weight(n2, 2000, 128);

        let mut count1 = 0usize;
        let mut count2 = 0usize;
        let total = 10_000;

        for i in 0..total {
            let s = ShardId::from_data(&(i as u32).to_le_bytes());
            let owners = ring.owners(&s, 1);
            if owners[0] == n1 {
                count1 += 1;
            } else {
                count2 += 1;
            }
        }

        // n2 should have ~2x the shards of n1.
        let ratio = count2 as f64 / count1 as f64;
        assert!(
            (1.3..=3.0).contains(&ratio),
            "weighted distribution off: n1={count1}, n2={count2} (ratio {ratio:.2})"
        );
    }

    #[test]
    fn test_ring_diff_identifies_migrations() {
        let mut old_ring = Ring::new(128);
        let n1 = node(1);
        let n2 = node(2);
        old_ring.add_node(n1, 1000);
        old_ring.add_node(n2, 1000);

        let shards: Vec<ShardId> = (0..1000u32)
            .map(|i| ShardId::from_data(&i.to_le_bytes()))
            .collect();

        // Add a third node.
        let mut new_ring = old_ring.clone();
        let n3 = node(3);
        new_ring.add_node(n3, 1000);

        let migrations = Ring::diff(&old_ring, &new_ring, &shards, 1);

        // Some migrations should exist (shards moving to n3).
        assert!(
            !migrations.is_empty(),
            "adding a node should cause migrations"
        );

        // All migrations should be TO the new node.
        for m in &migrations {
            assert_eq!(m.to, n3, "migration should be to the new node");
            assert!(m.from == n1 || m.from == n2);
        }
    }

    #[test]
    fn test_deterministic_placement() {
        let mut ring1 = Ring::new(128);
        let mut ring2 = Ring::new(128);
        let n1 = node(1);
        let n2 = node(2);
        ring1.add_node(n1, 1000);
        ring1.add_node(n2, 1000);
        ring2.add_node(n1, 1000);
        ring2.add_node(n2, 1000);

        for i in 0..100u8 {
            let s = shard(i);
            assert_eq!(
                ring1.owners(&s, 2),
                ring2.owners(&s, 2),
                "same input must produce same placement"
            );
        }
    }

    #[test]
    fn test_empty_ring_returns_empty_owners() {
        let ring = Ring::new(128);
        let owners = ring.owners(&shard(1), 3);
        assert!(owners.is_empty());
    }

    #[test]
    fn test_node_count_and_vnode_count() {
        let mut ring = Ring::new(64);
        assert_eq!(ring.node_count(), 0);
        assert_eq!(ring.vnode_count(), 0);

        ring.add_node(node(1), 1000);
        assert_eq!(ring.node_count(), 1);
        assert_eq!(ring.vnode_count(), 64);

        ring.add_node(node(2), 1000);
        assert_eq!(ring.node_count(), 2);
        assert_eq!(ring.vnode_count(), 128);

        ring.remove_node(&node(1));
        assert_eq!(ring.node_count(), 1);
        assert_eq!(ring.vnode_count(), 64);
    }

    #[test]
    fn test_add_same_node_twice_updates_weight() {
        let mut ring = Ring::new(64);
        ring.add_node(node(1), 1000);
        assert_eq!(ring.vnode_count(), 64);

        // Re-add with different weight.
        ring.add_node_with_weight(node(1), 2000, 128);
        assert_eq!(ring.node_count(), 1);
        assert_eq!(ring.vnode_count(), 128);
    }
}
