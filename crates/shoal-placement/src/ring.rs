//! Consistent hashing ring implementation.

use std::collections::{BTreeMap, HashMap};

use shoal_types::{NodeId, NodeTopology, ReplicationBoundary, ShardId};
use tracing::debug;

/// Metadata about a node on the ring.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    /// Storage capacity in bytes.
    pub capacity: u64,
    /// Number of vnodes this node owns (proportional to capacity).
    pub weight: u16,
    /// Physical location in the infrastructure hierarchy.
    pub topology: NodeTopology,
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
///
/// When a [`ReplicationBoundary`] is configured, [`owners`](Ring::owners) uses
/// progressive relaxation to maximize failure-domain separation:
/// - First pass tries to spread across the highest-level failure domains
///   (datacenters for `Region` boundary, machines for `Datacenter` boundary).
/// - Subsequent passes relax constraints until enough owners are found.
#[derive(Debug, Clone)]
pub struct Ring {
    /// Virtual node positions: ring position -> physical node.
    vnodes: BTreeMap<u64, NodeId>,
    /// Per-node metadata.
    nodes: HashMap<NodeId, NodeInfo>,
    /// Base number of vnodes per unit of weight.
    vnodes_per_node: u16,
    /// Optional replication boundary for topology-aware placement.
    boundary: Option<ReplicationBoundary>,
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
            boundary: None,
        }
    }

    /// Create a new empty ring with a replication boundary for topology-aware placement.
    pub fn new_with_boundary(vnodes_per_node: u16, boundary: ReplicationBoundary) -> Self {
        Self {
            vnodes: BTreeMap::new(),
            nodes: HashMap::new(),
            vnodes_per_node,
            boundary: Some(boundary),
        }
    }

    /// Add a node to the ring with the given capacity and topology.
    ///
    /// The node's weight (number of vnodes) equals `vnodes_per_node`.
    pub fn add_node(&mut self, node_id: NodeId, capacity: u64, topology: NodeTopology) {
        self.add_node_with_weight(node_id, capacity, self.vnodes_per_node, topology);
    }

    /// Add a node with an explicit weight (number of vnodes).
    pub fn add_node_with_weight(
        &mut self,
        node_id: NodeId,
        capacity: u64,
        weight: u16,
        topology: NodeTopology,
    ) {
        // Remove first if already present (re-add with new weight).
        self.remove_node(&node_id);

        let info = NodeInfo {
            capacity,
            weight,
            topology,
        };

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
    /// When a [`ReplicationBoundary`] is set, uses progressive relaxation to
    /// maximize failure-domain separation. Otherwise, walks clockwise collecting
    /// distinct physical nodes.
    ///
    /// Returns up to `replication_factor` distinct nodes. If fewer distinct
    /// nodes exist, returns all available nodes.
    pub fn owners(&self, shard_id: &ShardId, replication_factor: usize) -> Vec<NodeId> {
        if self.vnodes.is_empty() {
            return Vec::new();
        }

        let max_distinct = replication_factor.min(self.nodes.len());

        match self.boundary {
            Some(boundary) => self.owners_topology_aware(shard_id, max_distinct, boundary),
            None => self.owners_simple(shard_id, max_distinct),
        }
    }

    /// Simple clockwise walk — distinct physical nodes only (no topology awareness).
    fn owners_simple(&self, shard_id: &ShardId, max_distinct: usize) -> Vec<NodeId> {
        let pos = shard_position(shard_id);
        let mut owners = Vec::with_capacity(max_distinct);

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

    /// Topology-aware owner selection using progressive relaxation.
    ///
    /// For `ReplicationBoundary::Region` (EC spans DCs within a region):
    ///   Pass 1: skip same datacenter → collect up to n
    ///   Pass 2: relax to skip same machine → collect up to n
    ///   Pass 3: relax to skip same NodeId only → collect up to n
    ///
    /// For `ReplicationBoundary::Datacenter` (EC within one DC):
    ///   Pass 1: skip same machine → collect up to n
    ///   Pass 2: relax to skip same NodeId only → collect up to n
    fn owners_topology_aware(
        &self,
        shard_id: &ShardId,
        max_distinct: usize,
        boundary: ReplicationBoundary,
    ) -> Vec<NodeId> {
        let pos = shard_position(shard_id);

        // Build the clockwise walk order once.
        let walk: Vec<NodeId> = {
            let after = self.vnodes.range(pos..);
            let before = self.vnodes.range(..pos);
            let mut seen = Vec::with_capacity(self.nodes.len());
            let mut result = Vec::with_capacity(self.nodes.len());
            for (_, node_id) in after.chain(before) {
                if !seen.contains(node_id) {
                    seen.push(*node_id);
                    result.push(*node_id);
                }
            }
            result
        };

        match boundary {
            ReplicationBoundary::Region => {
                // Pass 1: different datacenter
                let mut owners = Vec::with_capacity(max_distinct);
                for &nid in &walk {
                    if owners.len() == max_distinct {
                        return owners;
                    }
                    let info = &self.nodes[&nid];
                    if owners.iter().all(|o: &NodeId| {
                        self.nodes[o].topology.datacenter != info.topology.datacenter
                    }) {
                        owners.push(nid);
                    }
                }
                if owners.len() == max_distinct {
                    return owners;
                }

                // Pass 2: relax to different machine
                for &nid in &walk {
                    if owners.len() == max_distinct {
                        return owners;
                    }
                    if owners.contains(&nid) {
                        continue;
                    }
                    let info = &self.nodes[&nid];
                    if owners
                        .iter()
                        .all(|o: &NodeId| self.nodes[o].topology.machine != info.topology.machine)
                    {
                        owners.push(nid);
                    }
                }
                if owners.len() == max_distinct {
                    return owners;
                }

                // Pass 3: relax to different NodeId only
                for &nid in &walk {
                    if owners.len() == max_distinct {
                        return owners;
                    }
                    if !owners.contains(&nid) {
                        owners.push(nid);
                    }
                }

                owners
            }
            ReplicationBoundary::Datacenter => {
                // Pass 1: different machine
                let mut owners = Vec::with_capacity(max_distinct);
                for &nid in &walk {
                    if owners.len() == max_distinct {
                        return owners;
                    }
                    let info = &self.nodes[&nid];
                    if owners
                        .iter()
                        .all(|o: &NodeId| self.nodes[o].topology.machine != info.topology.machine)
                    {
                        owners.push(nid);
                    }
                }
                if owners.len() == max_distinct {
                    return owners;
                }

                // Pass 2: relax to different NodeId only
                for &nid in &walk {
                    if owners.len() == max_distinct {
                        return owners;
                    }
                    if !owners.contains(&nid) {
                        owners.push(nid);
                    }
                }

                owners
            }
        }
    }

    /// Build a ring containing only nodes that belong to the given zone.
    ///
    /// What constitutes a "zone" depends on [`ReplicationBoundary`]:
    /// - `Datacenter`: zone = nodes with same `(region, datacenter)`
    /// - `Region`: zone = nodes with same `region`
    ///
    /// The `zone_key` is the value returned by [`zone_keys`](Ring::zone_keys)
    /// (either `"region"` or `"region/datacenter"`).
    pub fn for_zone(
        all_nodes: &[(NodeId, u64, NodeTopology)],
        boundary: &ReplicationBoundary,
        vnodes_per_node: u16,
        zone_key: &str,
    ) -> Self {
        let mut ring = Self::new_with_boundary(vnodes_per_node, *boundary);

        for (node_id, capacity, topology) in all_nodes {
            let key = node_zone_key(topology, boundary);
            if key == zone_key {
                ring.add_node(*node_id, *capacity, topology.clone());
            }
        }

        ring
    }

    /// Return all distinct zone keys from a set of nodes.
    ///
    /// For `ReplicationBoundary::Region`: returns distinct region names.
    /// For `ReplicationBoundary::Datacenter`: returns distinct `"region/datacenter"` strings.
    pub fn zone_keys(
        nodes: &[(NodeId, NodeTopology)],
        boundary: &ReplicationBoundary,
    ) -> Vec<String> {
        let mut keys: Vec<String> = nodes
            .iter()
            .map(|(_, topo)| node_zone_key(topo, boundary))
            .collect();
        keys.sort();
        keys.dedup();
        keys
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

/// Compute a node's zone key based on its topology and the replication boundary.
fn node_zone_key(topology: &NodeTopology, boundary: &ReplicationBoundary) -> String {
    match boundary {
        ReplicationBoundary::Region => topology.region.clone(),
        ReplicationBoundary::Datacenter => {
            format!("{}/{}", topology.region, topology.datacenter)
        }
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

    fn default_topo() -> NodeTopology {
        NodeTopology::default()
    }

    fn topo(region: &str, dc: &str, machine: &str) -> NodeTopology {
        NodeTopology {
            region: region.to_string(),
            datacenter: dc.to_string(),
            machine: machine.to_string(),
        }
    }

    // -----------------------------------------------------------------------
    // Original tests (updated for topology parameter)
    // -----------------------------------------------------------------------

    #[test]
    fn test_single_node_all_shards_map_to_it() {
        let mut ring = Ring::new(128);
        let n = node(1);
        ring.add_node(n, 1000, default_topo());

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
        ring.add_node(n1, 1000, default_topo());
        ring.add_node(n2, 1000, default_topo());

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
        ring.add_node(n1, 1000, default_topo());
        ring.add_node(n2, 1000, default_topo());

        let total = 10_000;
        let shards: Vec<ShardId> = (0..total)
            .map(|i| ShardId::from_data(&(i as u32).to_le_bytes()))
            .collect();

        // Record owners before adding node 3.
        let before: Vec<NodeId> = shards.iter().map(|s| ring.owners(s, 1)[0]).collect();

        // Add a third node.
        let n3 = node(3);
        ring.add_node(n3, 1000, default_topo());

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
        ring.add_node(n1, 1000, default_topo());
        ring.add_node(n2, 1000, default_topo());
        ring.add_node(n3, 1000, default_topo());

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
        ring.add_node(n1, 1000, default_topo());
        ring.add_node(n2, 1000, default_topo());
        ring.add_node(n3, 1000, default_topo());

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
        ring.add_node(n1, 1000, default_topo());
        ring.add_node(n2, 1000, default_topo());

        let owners = ring.owners(&shard(42), 5);
        assert_eq!(owners.len(), 2, "should return all nodes, not panic");
    }

    #[test]
    fn test_weighted_nodes_proportional_shards() {
        let mut ring = Ring::new(64);
        let n1 = node(1);
        let n2 = node(2);
        // n2 gets 2x the vnodes.
        ring.add_node_with_weight(n1, 1000, 64, default_topo());
        ring.add_node_with_weight(n2, 2000, 128, default_topo());

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
        old_ring.add_node(n1, 1000, default_topo());
        old_ring.add_node(n2, 1000, default_topo());

        let shards: Vec<ShardId> = (0..1000u32)
            .map(|i| ShardId::from_data(&i.to_le_bytes()))
            .collect();

        // Add a third node.
        let mut new_ring = old_ring.clone();
        let n3 = node(3);
        new_ring.add_node(n3, 1000, default_topo());

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
        ring1.add_node(n1, 1000, default_topo());
        ring1.add_node(n2, 1000, default_topo());
        ring2.add_node(n1, 1000, default_topo());
        ring2.add_node(n2, 1000, default_topo());

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

        ring.add_node(node(1), 1000, default_topo());
        assert_eq!(ring.node_count(), 1);
        assert_eq!(ring.vnode_count(), 64);

        ring.add_node(node(2), 1000, default_topo());
        assert_eq!(ring.node_count(), 2);
        assert_eq!(ring.vnode_count(), 128);

        ring.remove_node(&node(1));
        assert_eq!(ring.node_count(), 1);
        assert_eq!(ring.vnode_count(), 64);
    }

    #[test]
    fn test_add_same_node_twice_updates_weight() {
        let mut ring = Ring::new(64);
        ring.add_node(node(1), 1000, default_topo());
        assert_eq!(ring.vnode_count(), 64);

        // Re-add with different weight.
        ring.add_node_with_weight(node(1), 2000, 128, default_topo());
        assert_eq!(ring.node_count(), 1);
        assert_eq!(ring.vnode_count(), 128);
    }

    // -----------------------------------------------------------------------
    // Topology-aware tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_region_boundary_spreads_across_datacenters() {
        // 6 nodes across 2 DCs in the same region.
        // With ReplicationBoundary::Region, EC shards should spread across both DCs.
        let mut ring = Ring::new_with_boundary(128, ReplicationBoundary::Region);

        // DC1: nodes 1,2,3 on different machines
        ring.add_node(node(1), 1000, topo("eu", "dc1", "m1"));
        ring.add_node(node(2), 1000, topo("eu", "dc1", "m2"));
        ring.add_node(node(3), 1000, topo("eu", "dc1", "m3"));
        // DC2: nodes 4,5,6 on different machines
        ring.add_node(node(4), 1000, topo("eu", "dc2", "m4"));
        ring.add_node(node(5), 1000, topo("eu", "dc2", "m5"));
        ring.add_node(node(6), 1000, topo("eu", "dc2", "m6"));

        // With replication_factor=3, we should see owners from both DCs
        // for most shards due to DC-first spreading.
        let mut both_dcs_count = 0;
        let total = 1000;

        for i in 0..total {
            let s = ShardId::from_data(&(i as u32).to_le_bytes());
            let owners = ring.owners(&s, 3);
            assert_eq!(owners.len(), 3);

            let dc1_count = owners
                .iter()
                .filter(|o| ring.node_info(o).unwrap().topology.datacenter == "dc1")
                .count();
            let dc2_count = owners
                .iter()
                .filter(|o| ring.node_info(o).unwrap().topology.datacenter == "dc2")
                .count();

            if dc1_count > 0 && dc2_count > 0 {
                both_dcs_count += 1;
            }
        }

        // The vast majority of shards should spread across both DCs.
        let ratio = both_dcs_count as f64 / total as f64;
        assert!(
            ratio > 0.8,
            "expected >80% of shards across both DCs, got {ratio:.2} ({both_dcs_count}/{total})"
        );
    }

    #[test]
    fn test_datacenter_boundary_spreads_across_machines() {
        // 6 nodes in 1 DC, 3 machines (2 nodes per machine).
        // With ReplicationBoundary::Datacenter, shards should spread across machines.
        let mut ring = Ring::new_with_boundary(128, ReplicationBoundary::Datacenter);

        ring.add_node(node(1), 1000, topo("eu", "dc1", "m1"));
        ring.add_node(node(2), 1000, topo("eu", "dc1", "m1")); // same machine as node 1
        ring.add_node(node(3), 1000, topo("eu", "dc1", "m2"));
        ring.add_node(node(4), 1000, topo("eu", "dc1", "m2")); // same machine as node 3
        ring.add_node(node(5), 1000, topo("eu", "dc1", "m3"));
        ring.add_node(node(6), 1000, topo("eu", "dc1", "m3")); // same machine as node 5

        // With replication_factor=3, we should get 3 distinct machines.
        let mut all_different_machines = 0;
        let total = 1000;

        for i in 0..total {
            let s = ShardId::from_data(&(i as u32).to_le_bytes());
            let owners = ring.owners(&s, 3);
            assert_eq!(owners.len(), 3);

            let machines: Vec<&str> = owners
                .iter()
                .map(|o| ring.node_info(o).unwrap().topology.machine.as_str())
                .collect();

            let mut unique = machines.clone();
            unique.sort();
            unique.dedup();

            if unique.len() == 3 {
                all_different_machines += 1;
            }
        }

        // All or nearly all should have 3 distinct machines.
        let ratio = all_different_machines as f64 / total as f64;
        assert!(
            ratio > 0.9,
            "expected >90% with 3 distinct machines, got {ratio:.2}"
        );
    }

    #[test]
    fn test_graceful_relaxation_when_not_enough_domains() {
        // k+m > number of machines: should still work via graceful relaxation.
        let mut ring = Ring::new_with_boundary(128, ReplicationBoundary::Datacenter);

        // 2 machines but want 4 owners.
        ring.add_node(node(1), 1000, topo("eu", "dc1", "m1"));
        ring.add_node(node(2), 1000, topo("eu", "dc1", "m1"));
        ring.add_node(node(3), 1000, topo("eu", "dc1", "m2"));
        ring.add_node(node(4), 1000, topo("eu", "dc1", "m2"));

        let owners = ring.owners(&shard(42), 4);
        assert_eq!(owners.len(), 4, "should relax and return 4 distinct nodes");

        // First two should be on different machines.
        let m0 = &ring.node_info(&owners[0]).unwrap().topology.machine;
        let m1 = &ring.node_info(&owners[1]).unwrap().topology.machine;
        assert_ne!(m0, m1, "first two owners should be on different machines");
    }

    #[test]
    fn test_zone_filtering() {
        let all_nodes = vec![
            (node(1), 1000, topo("eu", "dc1", "m1")),
            (node(2), 1000, topo("eu", "dc2", "m2")),
            (node(3), 1000, topo("us", "dc3", "m3")),
            (node(4), 1000, topo("us", "dc4", "m4")),
            (node(5), 1000, topo("ap", "dc5", "m5")),
        ];

        // Region boundary: zone = region.
        let eu_ring = Ring::for_zone(&all_nodes, &ReplicationBoundary::Region, 128, "eu");
        assert_eq!(eu_ring.node_count(), 2);
        assert!(eu_ring.node_info(&node(1)).is_some());
        assert!(eu_ring.node_info(&node(2)).is_some());
        assert!(eu_ring.node_info(&node(3)).is_none());

        let us_ring = Ring::for_zone(&all_nodes, &ReplicationBoundary::Region, 128, "us");
        assert_eq!(us_ring.node_count(), 2);
        assert!(us_ring.node_info(&node(3)).is_some());
        assert!(us_ring.node_info(&node(4)).is_some());

        // Datacenter boundary: zone = region/datacenter.
        let dc1_ring = Ring::for_zone(&all_nodes, &ReplicationBoundary::Datacenter, 128, "eu/dc1");
        assert_eq!(dc1_ring.node_count(), 1);
        assert!(dc1_ring.node_info(&node(1)).is_some());
    }

    #[test]
    fn test_zone_keys_region_boundary() {
        let nodes = vec![
            (node(1), topo("eu", "dc1", "m1")),
            (node(2), topo("eu", "dc2", "m2")),
            (node(3), topo("us", "dc3", "m3")),
            (node(4), topo("us", "dc4", "m4")),
            (node(5), topo("ap", "dc5", "m5")),
        ];

        let keys = Ring::zone_keys(&nodes, &ReplicationBoundary::Region);
        assert_eq!(keys, vec!["ap", "eu", "us"]);
    }

    #[test]
    fn test_zone_keys_datacenter_boundary() {
        let nodes = vec![
            (node(1), topo("eu", "dc1", "m1")),
            (node(2), topo("eu", "dc2", "m2")),
            (node(3), topo("us", "dc3", "m3")),
        ];

        let keys = Ring::zone_keys(&nodes, &ReplicationBoundary::Datacenter);
        assert_eq!(keys, vec!["eu/dc1", "eu/dc2", "us/dc3"]);
    }

    #[test]
    fn test_single_zone_default_behaves_like_simple_ring() {
        // All-default topology = single zone. Should behave identically to simple ring.
        let mut simple = Ring::new(128);
        simple.add_node(node(1), 1000, default_topo());
        simple.add_node(node(2), 1000, default_topo());
        simple.add_node(node(3), 1000, default_topo());

        // Ring with boundary but all nodes have the same topology.
        let mut topo_ring = Ring::new_with_boundary(128, ReplicationBoundary::Region);
        topo_ring.add_node(node(1), 1000, default_topo());
        topo_ring.add_node(node(2), 1000, default_topo());
        topo_ring.add_node(node(3), 1000, default_topo());

        // Same placement results for all shards.
        for i in 0..100u8 {
            let s = shard(i);
            let simple_owners = simple.owners(&s, 3);
            let topo_owners = topo_ring.owners(&s, 3);
            // Both should return 3 owners (same set, though topology-aware may reorder).
            assert_eq!(simple_owners.len(), topo_owners.len());
            // Same set of nodes, just potentially different order due to relaxation passes.
            let mut simple_sorted = simple_owners.clone();
            simple_sorted.sort();
            let mut topo_sorted = topo_owners.clone();
            topo_sorted.sort();
            assert_eq!(
                simple_sorted, topo_sorted,
                "single-zone topology ring should pick same nodes as simple ring for shard {i}"
            );
        }
    }
}
