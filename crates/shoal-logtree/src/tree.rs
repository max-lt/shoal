//! Core LogTree implementation — append-only, hash-chained, signed DAG.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

use ed25519_dalek::SigningKey;
use shoal_types::{HybridClock, Manifest, NodeId, ObjectId, ObjectInfo};
use tracing::{debug, warn};

use crate::entry::{Action, LogEntry, Version};
use crate::error::LogTreeError;
use crate::store::LogTreeStore;

type Result<T> = std::result::Result<T, LogTreeError>;

/// Append-only, hash-chained, signed DAG of cluster mutations.
///
/// The `LogTree` is the source of truth for all object metadata. Each mutation
/// (put, delete) is recorded as a [`LogEntry`] that references its parent(s),
/// forming a DAG. Entries are signed with ed25519 for authenticity.
///
/// **Conflict resolution**: Last-Writer-Wins (LWW) using hybrid logical clock
/// timestamps. Equal HLCs are broken by highest `NodeId`.
pub struct LogTree {
    store: LogTreeStore,
    clock: HybridClock,
    node_id: NodeId,
    signing_key: SigningKey,
    mutation_count: AtomicU64,
}

impl LogTree {
    /// Create a new LogTree.
    pub fn new(store: LogTreeStore, node_id: NodeId, signing_key: SigningKey) -> Self {
        Self {
            store,
            clock: HybridClock::new(),
            node_id,
            signing_key,
            mutation_count: AtomicU64::new(0),
        }
    }

    /// Append a Put action. Returns the new LogEntry.
    ///
    /// Ticks HLC, signs, updates tips, caches manifest, updates materialized state.
    pub fn append_put(
        &self,
        bucket: &str,
        key: &str,
        manifest_id: ObjectId,
        manifest: &Manifest,
    ) -> Result<LogEntry> {
        let action = Action::Put {
            bucket: bucket.to_string(),
            key: key.to_string(),
            manifest_id,
        };

        let entry = self.append(action)?;
        self.store.put_manifest(manifest)?;
        Ok(entry)
    }

    /// Append a CreateApiKey action. Returns the new LogEntry.
    ///
    /// Only the access key ID is recorded in the DAG. The secret is
    /// persisted in MetaStore locally and pulled via QUIC by peers.
    pub fn append_create_api_key(&self, access_key_id: &str) -> Result<LogEntry> {
        let action = Action::CreateApiKey {
            access_key_id: access_key_id.to_string(),
        };

        self.append(action)
    }

    /// Append a DeleteApiKey action. Returns the new LogEntry.
    pub fn append_delete_api_key(&self, access_key_id: &str) -> Result<LogEntry> {
        let action = Action::DeleteApiKey {
            access_key_id: access_key_id.to_string(),
        };

        self.append(action)
    }

    /// Append a SetTags action. Returns the new LogEntry.
    pub fn append_set_tags(
        &self,
        bucket: &str,
        key: &str,
        tags: std::collections::BTreeMap<String, String>,
    ) -> Result<LogEntry> {
        let action = Action::SetTags {
            bucket: bucket.to_string(),
            key: key.to_string(),
            tags,
        };

        self.append(action)
    }

    /// Append a DeleteTags action. Returns the new LogEntry.
    pub fn append_delete_tags(&self, bucket: &str, key: &str) -> Result<LogEntry> {
        let action = Action::DeleteTags {
            bucket: bucket.to_string(),
            key: key.to_string(),
        };

        self.append(action)
    }

    /// Append a Delete action. Returns the new LogEntry.
    pub fn append_delete(&self, bucket: &str, key: &str) -> Result<LogEntry> {
        let action = Action::Delete {
            bucket: bucket.to_string(),
            key: key.to_string(),
        };

        self.append(action)
    }

    /// Internal: create, sign, store, and apply an entry.
    fn append(&self, action: Action) -> Result<LogEntry> {
        let hlc = self.clock.tick();
        let parents = self.store.get_tips()?;

        let entry = LogEntry::new_signed(hlc, self.node_id, action, parents, &self.signing_key);

        // Remove parents from tips, add new entry as tip.
        for parent in &entry.parents {
            self.store.remove_tip(parent)?;
        }
        self.store.put_entry(&entry)?;
        self.store.add_tip(entry.hash)?;

        // Update materialized state.
        self.apply_to_state(&entry)?;

        self.mutation_count.fetch_add(1, Ordering::Relaxed);

        debug!(
            hash = hex::encode_to_string(entry.hash),
            hlc = entry.hlc,
            "appended log entry"
        );

        Ok(entry)
    }

    /// Receive a remote entry. Verifies hash + signature, stores, updates state.
    ///
    /// Returns `true` if the entry was new (not already known).
    /// Returns `Err(MissingParents)` if parents are unknown.
    pub fn receive_entry(&self, entry: &LogEntry, manifest: Option<&Manifest>) -> Result<bool> {
        // Already known?
        if self.store.has_entry(&entry.hash)? {
            return Ok(false);
        }

        // Verify integrity.
        if !entry.verify_hash() {
            return Err(LogTreeError::InvalidHash);
        }

        if !entry.verify_signature() {
            return Err(LogTreeError::InvalidSignature);
        }

        // Check parents exist.
        let mut missing = Vec::new();

        for parent in &entry.parents {
            if !self.store.has_entry(parent)? {
                missing.push(*parent);
            }
        }

        if !missing.is_empty() {
            return Err(LogTreeError::MissingParents(missing));
        }

        // Witness the remote HLC.
        self.clock.witness(entry.hlc);

        // Store manifest if provided.
        if let Some(m) = manifest {
            self.store.put_manifest(m)?;
        }

        // Remove parents from tips (they now have a child), add this entry as tip.
        for parent in &entry.parents {
            self.store.remove_tip(parent)?;
        }
        self.store.put_entry(entry)?;
        self.store.add_tip(entry.hash)?;

        // Update materialized state.
        self.apply_to_state(entry)?;

        // Auto-merge if multiple tips.
        let tips = self.store.get_tips()?;

        if tips.len() > 1 {
            let _ = self.maybe_merge();
        }

        Ok(true)
    }

    /// Create a merge entry if there are multiple tips. Returns `None` if already converged.
    pub fn maybe_merge(&self) -> Result<Option<LogEntry>> {
        let tips = self.store.get_tips()?;

        if tips.len() <= 1 {
            return Ok(None);
        }

        let hlc = self.clock.tick();
        let entry = LogEntry::new_signed(hlc, self.node_id, Action::Merge, tips, &self.signing_key);

        for parent in &entry.parents {
            self.store.remove_tip(parent)?;
        }
        self.store.put_entry(&entry)?;
        self.store.add_tip(entry.hash)?;

        debug!(
            hash = hex::encode_to_string(entry.hash),
            parents = entry.parents.len(),
            "created merge entry"
        );

        Ok(Some(entry))
    }

    /// Get the current DAG tip hashes.
    pub fn tips(&self) -> Result<Vec<[u8; 32]>> {
        self.store.get_tips()
    }

    /// Resolve a key: returns the latest non-deleted version's manifest_id.
    pub fn resolve(&self, bucket: &str, key: &str) -> Result<Option<ObjectId>> {
        let versions = match self.store.get_versions(bucket, key)? {
            Some(v) => v,
            None => return Ok(None),
        };

        // Versions are sorted by HLC desc. First entry determines current state.
        if let Some(v) = versions.first()
            && !v.deleted
        {
            return Ok(Some(v.manifest_id));
        }

        Ok(None)
    }

    /// Get all versions for a key (for S3 versioning).
    pub fn versions(&self, bucket: &str, key: &str) -> Result<Vec<Version>> {
        Ok(self.store.get_versions(bucket, key)?.unwrap_or_default())
    }

    /// Get a specific version by HLC.
    pub fn resolve_version(&self, bucket: &str, key: &str, hlc: u64) -> Result<Option<ObjectId>> {
        let versions = self.store.get_versions(bucket, key)?.unwrap_or_default();

        for v in &versions {
            if v.hlc == hlc && !v.deleted {
                return Ok(Some(v.manifest_id));
            }
        }

        Ok(None)
    }

    /// List keys in a bucket with prefix, returning rich [`ObjectInfo`].
    pub fn list_keys(&self, bucket: &str, prefix: &str) -> Result<Vec<ObjectInfo>> {
        let all_keys = self.store.list_keys(bucket, prefix)?;

        // Filter out keys whose latest version is deleted, resolve manifests.
        let mut result = Vec::new();

        for key in all_keys {
            if let Some(versions) = self.store.get_versions(bucket, &key)?
                && let Some(latest) = versions.first()
                && !latest.deleted
            {
                let object_id = latest.manifest_id;
                let (size, last_modified) = self
                    .store
                    .get_manifest(&object_id)?
                    .map(|m| (m.total_size, m.created_at))
                    .unwrap_or((0, 0));

                result.push(ObjectInfo {
                    key,
                    size,
                    last_modified,
                    etag: object_id.to_string(),
                    object_id,
                });
            }
        }

        Ok(result)
    }

    /// Get a cached manifest.
    pub fn get_manifest(&self, id: &ObjectId) -> Result<Option<Manifest>> {
        self.store.get_manifest(id)
    }

    /// Create a snapshot entry recording the current materialized state hash.
    pub fn create_snapshot(&self) -> Result<LogEntry> {
        let state_hash = self.state_hash()?;

        // Serialize the full state and store it.
        let all_state = self.store.all_state()?;
        let state_bytes = postcard::to_allocvec(&all_state)?;
        self.store.put_snapshot(&state_hash, &state_bytes)?;

        let action = Action::Snapshot { state_hash };
        self.append(action)
    }

    /// Prune entries that are ancestors of the given snapshot.
    ///
    /// Walks backward from the snapshot entry, removing all entries that
    /// are reachable from it (they are now superseded by the snapshot).
    /// Returns the number of pruned entries.
    pub fn prune_before(&self, snapshot_hash: &[u8; 32]) -> Result<usize> {
        // Find the snapshot entry.
        let snapshot_entry = self
            .store
            .get_entry(snapshot_hash)?
            .ok_or_else(|| LogTreeError::SnapshotNotFound(hex::encode_to_string(snapshot_hash)))?;

        // BFS backward through parents, removing entries.
        let mut queue: VecDeque<[u8; 32]> = snapshot_entry.parents.iter().copied().collect();
        let mut visited = HashSet::new();
        let mut pruned = 0;

        while let Some(hash) = queue.pop_front() {
            if !visited.insert(hash) {
                continue;
            }

            if let Some(entry) = self.store.get_entry(&hash)? {
                for parent in &entry.parents {
                    queue.push_back(*parent);
                }

                self.store.remove_entry(&hash)?;
                pruned += 1;
            }
        }

        debug!(pruned, "pruned entries before snapshot");
        Ok(pruned)
    }

    /// Compute entries a peer is missing given their tips.
    ///
    /// BFS backward from our tips to discover the set of entries, then
    /// Kahn's algorithm to return them in correct topological order
    /// (parents before children). A simple reverse-BFS is wrong for DAGs
    /// with diamond patterns (e.g. A→B→C, A→B→D→E where B and D are at
    /// the same BFS depth but D depends on B).
    pub fn compute_delta(&self, peer_tips: &[[u8; 32]]) -> Result<Vec<LogEntry>> {
        use std::collections::HashMap;

        let peer_tip_set: HashSet<[u8; 32]> = peer_tips.iter().copied().collect();
        let our_tips = self.store.get_tips()?;

        // Phase 1: BFS backward to discover entries the peer is missing.
        let mut queue: VecDeque<[u8; 32]> = our_tips.into_iter().collect();
        let mut entries: HashMap<[u8; 32], LogEntry> = HashMap::new();

        while let Some(hash) = queue.pop_front() {
            if entries.contains_key(&hash) {
                continue;
            }

            // Stop traversal at entries the peer already has.
            if peer_tip_set.contains(&hash) {
                continue;
            }

            if let Some(entry) = self.store.get_entry(&hash)? {
                for parent in &entry.parents {
                    if !peer_tip_set.contains(parent) && !entries.contains_key(parent) {
                        queue.push_back(*parent);
                    }
                }

                entries.insert(hash, entry);
            }
        }

        if entries.is_empty() {
            return Ok(Vec::new());
        }

        // Phase 2: Kahn's topological sort (parents before children).
        // In-degree = number of parents that are ALSO in the delta set.
        let delta_set: HashSet<[u8; 32]> = entries.keys().copied().collect();
        let mut in_degree: HashMap<[u8; 32], usize> = HashMap::new();
        // child → parents-in-delta (for decrementing in-degree).
        let mut children: HashMap<[u8; 32], Vec<[u8; 32]>> = HashMap::new();

        for (hash, entry) in &entries {
            let deg = entry
                .parents
                .iter()
                .filter(|p| delta_set.contains(*p))
                .count();
            in_degree.insert(*hash, deg);

            for parent in &entry.parents {
                if delta_set.contains(parent) {
                    children.entry(*parent).or_default().push(*hash);
                }
            }
        }

        let mut ready: VecDeque<[u8; 32]> = in_degree
            .iter()
            .filter(|(_, deg)| **deg == 0)
            .map(|(h, _)| *h)
            .collect();

        let mut result = Vec::with_capacity(entries.len());

        while let Some(hash) = ready.pop_front() {
            if let Some(entry) = entries.remove(&hash) {
                result.push(entry);
            }

            if let Some(kids) = children.get(&hash) {
                for kid in kids {
                    if let Some(deg) = in_degree.get_mut(kid) {
                        *deg -= 1;

                        if *deg == 0 {
                            ready.push_back(*kid);
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    /// Compute entries needed by a requester given specific entry hashes.
    ///
    /// BFS backward from `entry_hashes` through parents, stopping at
    /// entries in `requester_tips` (already known to the requester) or at
    /// DAG roots. Returns entries in topological order (parents before
    /// children) using Kahn's algorithm.
    ///
    /// This is the responder side of a targeted pull — it is LOCAL-ONLY
    /// and never triggers recursive peer pulls.
    pub fn compute_pull_delta(
        &self,
        entry_hashes: &[[u8; 32]],
        requester_tips: &[[u8; 32]],
    ) -> Result<Vec<LogEntry>> {
        use std::collections::HashMap;

        let tip_set: HashSet<[u8; 32]> = requester_tips.iter().copied().collect();

        // Phase 1: BFS backward from requested hashes to discover entries.
        let mut queue: VecDeque<[u8; 32]> = entry_hashes.iter().copied().collect();
        let mut entries: HashMap<[u8; 32], LogEntry> = HashMap::new();

        while let Some(hash) = queue.pop_front() {
            if entries.contains_key(&hash) {
                continue;
            }

            // Stop at entries the requester already has.
            if tip_set.contains(&hash) {
                continue;
            }

            if let Some(entry) = self.store.get_entry(&hash)? {
                for parent in &entry.parents {
                    if !tip_set.contains(parent) && !entries.contains_key(parent) {
                        queue.push_back(*parent);
                    }
                }

                entries.insert(hash, entry);
            }
        }

        if entries.is_empty() {
            return Ok(Vec::new());
        }

        // Phase 2: Kahn's topological sort (parents before children).
        let delta_set: HashSet<[u8; 32]> = entries.keys().copied().collect();
        let mut in_degree: HashMap<[u8; 32], usize> = HashMap::new();
        let mut children: HashMap<[u8; 32], Vec<[u8; 32]>> = HashMap::new();

        for (hash, entry) in &entries {
            let deg = entry
                .parents
                .iter()
                .filter(|p| delta_set.contains(*p))
                .count();
            in_degree.insert(*hash, deg);

            for parent in &entry.parents {
                if delta_set.contains(parent) {
                    children.entry(*parent).or_default().push(*hash);
                }
            }
        }

        let mut ready: VecDeque<[u8; 32]> = in_degree
            .iter()
            .filter(|(_, deg)| **deg == 0)
            .map(|(h, _)| *h)
            .collect();

        let mut result = Vec::with_capacity(entries.len());

        while let Some(hash) = ready.pop_front() {
            if let Some(entry) = entries.remove(&hash) {
                result.push(entry);
            }

            if let Some(kids) = children.get(&hash) {
                for kid in kids {
                    if let Some(deg) = in_degree.get_mut(kid) {
                        *deg -= 1;

                        if *deg == 0 {
                            ready.push_back(*kid);
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    /// Apply received entries (from sync). Verifies each, updates state.
    ///
    /// Manifests are pulled separately via QUIC — this method only applies
    /// the DAG entries themselves.
    ///
    /// Returns the number of new entries applied.
    pub fn apply_sync_entries(&self, entries: &[LogEntry]) -> Result<usize> {
        // Set of hashes in this sync batch — used for parent validation.
        let delta_hashes: HashSet<[u8; 32]> = entries.iter().map(|e| e.hash).collect();

        let mut applied = 0;

        for entry in entries {
            // Skip already known.
            if self.store.has_entry(&entry.hash)? {
                continue;
            }

            // Verify integrity.
            if !entry.verify_hash() {
                warn!(
                    hash = hex::encode_to_string(entry.hash),
                    "skipping entry with invalid hash during sync"
                );
                continue;
            }

            if !entry.verify_signature() {
                warn!(
                    hash = hex::encode_to_string(entry.hash),
                    "skipping entry with invalid signature during sync"
                );
                continue;
            }

            // Verify in-delta parents exist. Parents within this sync batch
            // should have been applied first (topological order from Kahn's
            // algorithm). Parents OUTSIDE the delta are boundary entries the
            // peer already has, or were intentionally pruned on the sender
            // side — we don't validate those.
            let mut parents_ok = true;

            for parent in &entry.parents {
                if delta_hashes.contains(parent) && !self.store.has_entry(parent)? {
                    warn!(
                        hash = hex::encode_to_string(entry.hash),
                        missing_parent = hex::encode_to_string(parent),
                        "skipping sync entry with unapplied in-delta parent"
                    );
                    parents_ok = false;
                    break;
                }
            }

            if !parents_ok {
                continue;
            }

            // Witness the HLC.
            self.clock.witness(entry.hlc);

            // Update tips: remove parents from tips, add this entry.
            for parent in &entry.parents {
                self.store.remove_tip(parent)?;
            }
            self.store.put_entry(entry)?;
            self.store.add_tip(entry.hash)?;

            // Update materialized state.
            self.apply_to_state(entry)?;

            applied += 1;
        }

        if applied > 0 {
            debug!(applied, "applied sync entries");

            // Merge multiple tips to keep the DAG converged.
            let tips = self.store.get_tips()?;

            if tips.len() > 1 {
                let _ = self.maybe_merge();
            }
        }

        Ok(applied)
    }

    /// Witness a remote HLC (advance local clock if needed).
    pub fn witness(&self, remote_hlc: u64) {
        self.clock.witness(remote_hlc);
    }

    /// Return this node's ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Return a reference to the underlying store.
    pub fn store(&self) -> &LogTreeStore {
        &self.store
    }

    // ----- Internal -----

    /// Compute the materialized state hash (blake3 of serialized state).
    fn state_hash(&self) -> Result<[u8; 32]> {
        let all_state = self.store.all_state()?;
        let bytes = postcard::to_allocvec(&all_state)?;
        Ok(blake3::hash(&bytes).into())
    }

    /// Apply a log entry's action to the materialized state.
    fn apply_to_state(&self, entry: &LogEntry) -> Result<()> {
        match &entry.action {
            Action::Put {
                bucket,
                key,
                manifest_id,
            } => {
                let version = Version {
                    hlc: entry.hlc,
                    manifest_id: *manifest_id,
                    deleted: false,
                    node_id: entry.node_id,
                };
                self.insert_version(bucket, key, version)?;
            }
            Action::Delete { bucket, key } => {
                let version = Version {
                    hlc: entry.hlc,
                    manifest_id: ObjectId::from([0u8; 32]),
                    deleted: true,
                    node_id: entry.node_id,
                };
                self.insert_version(bucket, key, version)?;
            }
            Action::Merge
            | Action::Snapshot { .. }
            | Action::CreateApiKey { .. }
            | Action::DeleteApiKey { .. }
            | Action::SetTags { .. }
            | Action::DeleteTags { .. } => {
                // No LogTree-internal state change. API key and tag actions are
                // applied to MetaStore externally by the engine / gossip receiver.
            }
        }
        Ok(())
    }

    /// Insert a version into the sorted version list for `(bucket, key)`.
    ///
    /// Versions are sorted by (HLC desc, NodeId desc) for LWW semantics.
    fn insert_version(&self, bucket: &str, key: &str, version: Version) -> Result<()> {
        let mut versions = self.store.get_versions(bucket, key)?.unwrap_or_default();

        // Check for duplicate (same hlc + same node_id).
        if versions
            .iter()
            .any(|v| v.hlc == version.hlc && v.node_id == version.node_id)
        {
            return Ok(());
        }

        versions.push(version);

        // Sort by HLC descending, then NodeId descending as tiebreak.
        versions.sort_by(|a, b| b.hlc.cmp(&a.hlc).then_with(|| b.node_id.cmp(&a.node_id)));

        self.store.put_versions(bucket, key, &versions)?;
        Ok(())
    }
}

/// Encode bytes as hex string.
mod hex {
    pub fn encode_to_string(bytes: impl AsRef<[u8]>) -> String {
        bytes.as_ref().iter().map(|b| format!("{b:02x}")).collect()
    }
}
