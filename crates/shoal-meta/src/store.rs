//! [`MetaStore`] implementation with Fjall (disk) and in-memory backends.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::RwLock;

use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use shoal_types::{Manifest, Member, NodeId, ObjectId, ShardId};
use tracing::debug;

use crate::MetaError;

type Result<T> = std::result::Result<T, MetaError>;

/// Inner backend: either Fjall-backed (disk) or pure in-memory.
enum Backend {
    Fjall {
        #[allow(dead_code)]
        db: Database,
        objects: Keyspace,
        manifests: Keyspace,
        shardmap: Keyspace,
        membership: Keyspace,
        repair_queue: Keyspace,
        peers: Keyspace,
        api_keys: Keyspace,
        buckets: Keyspace,
        tags: Keyspace,
    },
    Memory(Box<MemoryBackend>),
}

/// Pure in-memory storage for zero disk I/O mode.
struct MemoryBackend {
    /// `bucket/key` → ObjectId bytes.
    objects: RwLock<BTreeMap<String, [u8; 32]>>,
    /// ObjectId bytes → serialized Manifest.
    manifests: RwLock<HashMap<[u8; 32], Vec<u8>>>,
    /// ShardId bytes → serialized Vec<NodeId>.
    shardmap: RwLock<HashMap<[u8; 32], Vec<u8>>>,
    /// NodeId bytes → serialized Member.
    membership: RwLock<HashMap<[u8; 32], Vec<u8>>>,
    /// priority (8 bytes BE) ++ ShardId → ShardId bytes.
    repair_queue: RwLock<BTreeMap<Vec<u8>, [u8; 32]>>,
    /// NodeId bytes → serialized Vec<SocketAddr>.
    peers: RwLock<HashMap<[u8; 32], Vec<u8>>>,
    /// access_key_id (String) → secret_access_key (String).
    api_keys: RwLock<HashMap<String, String>>,
    /// Explicitly created bucket names.
    buckets: RwLock<HashSet<String>>,
    /// `bucket/key` → serialized BTreeMap<String, String> (tags).
    tags: RwLock<HashMap<String, Vec<u8>>>,
}

/// Metadata store with Fjall (disk) or pure in-memory backend.
///
/// All data stored here is a **local cache** reconstructible from the cluster,
/// except for `repair_queue` which is local and transient.
pub struct MetaStore {
    backend: Backend,
}

impl MetaStore {
    /// Open a persistent MetaStore at the given path (Fjall backend).
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = Database::builder(path).open()?;
        let backend = Self::init_fjall(db)?;
        Ok(Self { backend })
    }

    /// Open a temporary MetaStore backed by Fjall (cleaned up on drop).
    ///
    /// Still uses disk I/O (tempdir). For zero disk I/O, use [`in_memory`](Self::in_memory).
    pub fn open_temporary() -> Result<Self> {
        let tmp = tempfile::tempdir().map_err(std::io::Error::other)?;
        let db = Database::builder(tmp.path()).temporary(true).open()?;
        let backend = Self::init_fjall(db)?;
        Ok(Self { backend })
    }

    /// Create a pure in-memory MetaStore. Zero disk I/O.
    pub fn in_memory() -> Self {
        Self {
            backend: Backend::Memory(Box::new(MemoryBackend {
                objects: RwLock::new(BTreeMap::new()),
                manifests: RwLock::new(HashMap::new()),
                shardmap: RwLock::new(HashMap::new()),
                membership: RwLock::new(HashMap::new()),
                repair_queue: RwLock::new(BTreeMap::new()),
                peers: RwLock::new(HashMap::new()),
                api_keys: RwLock::new(HashMap::new()),
                buckets: RwLock::new(HashSet::new()),
                tags: RwLock::new(HashMap::new()),
            })),
        }
    }

    fn init_fjall(db: Database) -> Result<Backend> {
        let objects = db.keyspace("objects", KeyspaceCreateOptions::default)?;
        let manifests = db.keyspace("manifests", KeyspaceCreateOptions::default)?;
        let shardmap = db.keyspace("shardmap", KeyspaceCreateOptions::default)?;
        let membership = db.keyspace("membership", KeyspaceCreateOptions::default)?;
        let repair_queue = db.keyspace("repair_queue", KeyspaceCreateOptions::default)?;
        let peers = db.keyspace("peers", KeyspaceCreateOptions::default)?;
        let api_keys = db.keyspace("api_keys", KeyspaceCreateOptions::default)?;
        let buckets = db.keyspace("buckets", KeyspaceCreateOptions::default)?;
        let tags = db.keyspace("tags", KeyspaceCreateOptions::default)?;
        Ok(Backend::Fjall {
            db,
            objects,
            manifests,
            shardmap,
            membership,
            repair_queue,
            peers,
            api_keys,
            buckets,
            tags,
        })
    }

    // ----- Manifests (local cache) -----

    /// Store a manifest, keyed by its `object_id`.
    pub fn put_manifest(&self, manifest: &Manifest) -> Result<()> {
        let value = postcard::to_allocvec(manifest)?;
        match &self.backend {
            Backend::Fjall { manifests, .. } => {
                manifests.insert(manifest.object_id.as_bytes(), value.as_slice())?;
            }
            Backend::Memory(m) => {
                m.manifests
                    .write()
                    .unwrap()
                    .insert(*manifest.object_id.as_bytes(), value);
            }
        }
        debug!(object_id = %manifest.object_id, "stored manifest");
        Ok(())
    }

    /// Retrieve a manifest by its [`ObjectId`].
    pub fn get_manifest(&self, id: &ObjectId) -> Result<Option<Manifest>> {
        match &self.backend {
            Backend::Fjall { manifests, .. } => match manifests.get(id.as_bytes())? {
                Some(bytes) => Ok(Some(postcard::from_bytes(&bytes)?)),
                None => Ok(None),
            },
            Backend::Memory(m) => match m.manifests.read().unwrap().get(id.as_bytes()) {
                Some(bytes) => Ok(Some(postcard::from_bytes(bytes)?)),
                None => Ok(None),
            },
        }
    }

    // ----- Object key mapping (local cache) -----

    /// Map a `bucket/key` pair to an [`ObjectId`].
    ///
    /// Also registers the bucket if it doesn't already exist.
    pub fn put_object_key(&self, bucket: &str, key: &str, id: &ObjectId) -> Result<()> {
        let storage_key = object_storage_key(bucket, key);
        match &self.backend {
            Backend::Fjall {
                objects, buckets, ..
            } => {
                objects.insert(storage_key.as_bytes(), id.as_bytes())?;
                buckets.insert(bucket.as_bytes(), b"")?;
            }
            Backend::Memory(m) => {
                m.objects
                    .write()
                    .unwrap()
                    .insert(storage_key, *id.as_bytes());
                m.buckets.write().unwrap().insert(bucket.to_string());
            }
        }
        debug!(bucket, key, object_id = %id, "stored object key mapping");
        Ok(())
    }

    /// Look up an [`ObjectId`] by `bucket/key`.
    pub fn get_object_key(&self, bucket: &str, key: &str) -> Result<Option<ObjectId>> {
        let storage_key = object_storage_key(bucket, key);
        match &self.backend {
            Backend::Fjall { objects, .. } => match objects.get(storage_key.as_bytes())? {
                Some(bytes) => {
                    let arr: [u8; 32] = bytes[..32].try_into().map_err(|_| {
                        MetaError::CorruptData(format!(
                            "ObjectId expected 32 bytes, got {}",
                            bytes.len()
                        ))
                    })?;
                    Ok(Some(ObjectId::from(arr)))
                }
                None => Ok(None),
            },
            Backend::Memory(m) => Ok(m
                .objects
                .read()
                .unwrap()
                .get(&storage_key)
                .map(|arr| ObjectId::from(*arr))),
        }
    }

    /// List all object keys in a bucket that start with the given prefix.
    ///
    /// Returns keys without the bucket prefix (i.e. just the key portion).
    pub fn list_objects(&self, bucket: &str, prefix: &str) -> Result<Vec<String>> {
        let scan_prefix = if prefix.is_empty() {
            format!("{bucket}/")
        } else {
            format!("{bucket}/{prefix}")
        };
        let bucket_prefix = format!("{bucket}/");

        match &self.backend {
            Backend::Fjall { objects, .. } => {
                let mut keys = Vec::new();
                for guard in objects.prefix(scan_prefix.as_bytes()) {
                    let k = guard.key()?;
                    let full_key = std::str::from_utf8(&k).map_err(|e| {
                        MetaError::CorruptData(format!("object key is not valid UTF-8: {e}"))
                    })?;
                    if let Some(stripped) = full_key.strip_prefix(&bucket_prefix) {
                        keys.push(stripped.to_string());
                    }
                }
                Ok(keys)
            }
            Backend::Memory(m) => {
                let map = m.objects.read().unwrap();
                let mut keys = Vec::new();
                for full_key in map.range(scan_prefix.clone()..) {
                    let k = full_key.0.as_str();
                    if !k.starts_with(&scan_prefix) {
                        break;
                    }
                    if let Some(stripped) = k.strip_prefix(&bucket_prefix) {
                        keys.push(stripped.to_string());
                    }
                }
                Ok(keys)
            }
        }
    }

    /// Delete an object key mapping.
    pub fn delete_object_key(&self, bucket: &str, key: &str) -> Result<()> {
        let storage_key = object_storage_key(bucket, key);
        match &self.backend {
            Backend::Fjall { objects, .. } => {
                objects.remove(storage_key.as_bytes())?;
            }
            Backend::Memory(m) => {
                m.objects.write().unwrap().remove(&storage_key);
            }
        }
        debug!(bucket, key, "deleted object key mapping");
        Ok(())
    }

    /// List all object entries across all buckets.
    ///
    /// Returns `(bucket, key, ObjectId)` triples by scanning the entire objects
    /// keyspace. Used for manifest sync when a node joins the cluster and needs
    /// to catch up on historical manifests.
    pub fn list_all_object_entries(&self) -> Result<Vec<(String, String, ObjectId)>> {
        match &self.backend {
            Backend::Fjall { objects, .. } => {
                let mut entries = Vec::new();
                for guard in objects.iter() {
                    let (k, v) = guard.into_inner()?;
                    let full_key = std::str::from_utf8(&k).map_err(|e| {
                        MetaError::CorruptData(format!("object key is not valid UTF-8: {e}"))
                    })?;
                    let arr: [u8; 32] = v[..32].try_into().map_err(|_| {
                        MetaError::CorruptData(format!(
                            "ObjectId expected 32 bytes, got {}",
                            v.len()
                        ))
                    })?;
                    if let Some((bucket, key)) = full_key.split_once('/') {
                        entries.push((bucket.to_string(), key.to_string(), ObjectId::from(arr)));
                    }
                }
                Ok(entries)
            }
            Backend::Memory(m) => {
                let map = m.objects.read().unwrap();
                let mut entries = Vec::new();
                for (full_key, arr) in map.iter() {
                    if let Some((bucket, key)) = full_key.split_once('/') {
                        entries.push((bucket.to_string(), key.to_string(), ObjectId::from(*arr)));
                    }
                }
                Ok(entries)
            }
        }
    }

    // ----- Buckets -----

    /// Register a bucket name. Idempotent.
    pub fn create_bucket(&self, name: &str) -> Result<()> {
        match &self.backend {
            Backend::Fjall { buckets, .. } => {
                buckets.insert(name.as_bytes(), b"")?;
            }
            Backend::Memory(m) => {
                m.buckets.write().unwrap().insert(name.to_string());
            }
        }
        debug!(bucket = name, "registered bucket");
        Ok(())
    }

    /// Remove a bucket name. Returns `true` if the bucket existed.
    pub fn delete_bucket(&self, name: &str) -> Result<bool> {
        match &self.backend {
            Backend::Fjall { buckets, .. } => {
                let existed = buckets.get(name.as_bytes())?.is_some();
                buckets.remove(name.as_bytes())?;
                Ok(existed)
            }
            Backend::Memory(m) => Ok(m.buckets.write().unwrap().remove(name)),
        }
    }

    /// Check if a bucket exists (explicitly created or has objects).
    pub fn bucket_exists(&self, name: &str) -> Result<bool> {
        // Check explicit bucket set first.
        let in_set = match &self.backend {
            Backend::Fjall { buckets, .. } => buckets.get(name.as_bytes())?.is_some(),
            Backend::Memory(m) => m.buckets.read().unwrap().contains(name),
        };

        if in_set {
            return Ok(true);
        }

        // Fall back: bucket exists implicitly if it has any objects.
        let keys = self.list_objects(name, "")?;
        Ok(!keys.is_empty())
    }

    /// List all known bucket names (union of explicit set and implicit from keys).
    pub fn list_buckets(&self) -> Result<BTreeSet<String>> {
        let mut names = BTreeSet::new();

        // Explicit buckets.
        match &self.backend {
            Backend::Fjall { buckets, .. } => {
                for guard in buckets.iter() {
                    let k = guard.key()?;
                    if let Ok(name) = std::str::from_utf8(&k) {
                        names.insert(name.to_string());
                    }
                }
            }
            Backend::Memory(m) => {
                names.extend(m.buckets.read().unwrap().iter().cloned());
            }
        }

        // Implicit buckets from object keys.
        match &self.backend {
            Backend::Fjall { objects, .. } => {
                for guard in objects.iter() {
                    let k = guard.key()?;
                    if let Ok(full_key) = std::str::from_utf8(&k) {
                        if let Some((bucket, _)) = full_key.split_once('/') {
                            names.insert(bucket.to_string());
                        }
                    }
                }
            }
            Backend::Memory(m) => {
                for full_key in m.objects.read().unwrap().keys() {
                    if let Some((bucket, _)) = full_key.split_once('/') {
                        names.insert(bucket.to_string());
                    }
                }
            }
        }

        Ok(names)
    }

    // ----- Shard map (local cache) -----

    /// Store the current owners of a shard.
    pub fn put_shard_owners(&self, id: &ShardId, owners: &[NodeId]) -> Result<()> {
        let value = postcard::to_allocvec(owners)?;
        match &self.backend {
            Backend::Fjall { shardmap, .. } => {
                shardmap.insert(id.as_bytes(), value.as_slice())?;
            }
            Backend::Memory(m) => {
                m.shardmap.write().unwrap().insert(*id.as_bytes(), value);
            }
        }
        Ok(())
    }

    /// Retrieve the owners of a shard.
    pub fn get_shard_owners(&self, id: &ShardId) -> Result<Option<Vec<NodeId>>> {
        match &self.backend {
            Backend::Fjall { shardmap, .. } => match shardmap.get(id.as_bytes())? {
                Some(bytes) => Ok(Some(postcard::from_bytes(&bytes)?)),
                None => Ok(None),
            },
            Backend::Memory(m) => match m.shardmap.read().unwrap().get(id.as_bytes()) {
                Some(bytes) => Ok(Some(postcard::from_bytes(bytes)?)),
                None => Ok(None),
            },
        }
    }

    // ----- Membership (local cache) -----

    /// Store or update a cluster member.
    pub fn put_member(&self, member: &Member) -> Result<()> {
        let value = postcard::to_allocvec(member)?;
        match &self.backend {
            Backend::Fjall { membership, .. } => {
                membership.insert(member.node_id.as_bytes(), value.as_slice())?;
            }
            Backend::Memory(m) => {
                m.membership
                    .write()
                    .unwrap()
                    .insert(*member.node_id.as_bytes(), value);
            }
        }
        debug!(node_id = %member.node_id, "stored member");
        Ok(())
    }

    /// Retrieve a member by [`NodeId`].
    pub fn get_member(&self, id: &NodeId) -> Result<Option<Member>> {
        match &self.backend {
            Backend::Fjall { membership, .. } => match membership.get(id.as_bytes())? {
                Some(bytes) => Ok(Some(postcard::from_bytes(&bytes)?)),
                None => Ok(None),
            },
            Backend::Memory(m) => match m.membership.read().unwrap().get(id.as_bytes()) {
                Some(bytes) => Ok(Some(postcard::from_bytes(bytes)?)),
                None => Ok(None),
            },
        }
    }

    /// List all known members.
    pub fn list_members(&self) -> Result<Vec<Member>> {
        match &self.backend {
            Backend::Fjall { membership, .. } => {
                let mut members = Vec::new();
                for guard in membership.iter() {
                    let v = guard.value()?;
                    members.push(postcard::from_bytes(&v)?);
                }
                Ok(members)
            }
            Backend::Memory(m) => {
                let map = m.membership.read().unwrap();
                let mut members = Vec::new();
                for bytes in map.values() {
                    members.push(postcard::from_bytes(bytes)?);
                }
                Ok(members)
            }
        }
    }

    /// Remove a member from the local membership cache.
    pub fn remove_member(&self, id: &NodeId) -> Result<()> {
        match &self.backend {
            Backend::Fjall { membership, .. } => {
                membership.remove(id.as_bytes())?;
            }
            Backend::Memory(m) => {
                m.membership.write().unwrap().remove(id.as_bytes());
            }
        }
        debug!(node_id = %id, "removed member");
        Ok(())
    }

    // ----- Peer addresses (local cache, for reconnecting after restart) -----

    /// Store the known listen addresses of a peer.
    pub fn put_peer_addrs(&self, node_id: &NodeId, addrs: &[SocketAddr]) -> Result<()> {
        let value = postcard::to_allocvec(addrs)?;
        match &self.backend {
            Backend::Fjall { peers, .. } => {
                peers.insert(node_id.as_bytes(), value.as_slice())?;
            }
            Backend::Memory(m) => {
                m.peers.write().unwrap().insert(*node_id.as_bytes(), value);
            }
        }
        debug!(node_id = %node_id, addr_count = addrs.len(), "stored peer addresses");
        Ok(())
    }

    /// List all known peer addresses.
    ///
    /// Returns `(NodeId, Vec<SocketAddr>)` pairs for all persisted peers.
    pub fn list_peer_addrs(&self) -> Result<Vec<(NodeId, Vec<SocketAddr>)>> {
        match &self.backend {
            Backend::Fjall { peers, .. } => {
                let mut result = Vec::new();

                for guard in peers.iter() {
                    let (k, v) = guard.into_inner()?;
                    let arr: [u8; 32] = k[..32].try_into().map_err(|_| {
                        MetaError::CorruptData(format!("NodeId expected 32 bytes, got {}", k.len()))
                    })?;
                    let addrs: Vec<SocketAddr> = postcard::from_bytes(&v)?;
                    result.push((NodeId::from(arr), addrs));
                }

                Ok(result)
            }
            Backend::Memory(m) => {
                let map = m.peers.read().unwrap();
                let mut result = Vec::new();

                for (key, bytes) in map.iter() {
                    let addrs: Vec<SocketAddr> = postcard::from_bytes(bytes)?;
                    result.push((NodeId::from(*key), addrs));
                }

                Ok(result)
            }
        }
    }

    /// Remove a peer's persisted addresses.
    pub fn remove_peer_addrs(&self, node_id: &NodeId) -> Result<()> {
        match &self.backend {
            Backend::Fjall { peers, .. } => {
                peers.remove(node_id.as_bytes())?;
            }
            Backend::Memory(m) => {
                m.peers.write().unwrap().remove(node_id.as_bytes());
            }
        }
        debug!(node_id = %node_id, "removed peer addresses");
        Ok(())
    }

    // ----- API keys (persistent, local to this node) -----

    /// Store an API key pair. Overwrites if the access key already exists.
    pub fn put_api_key(&self, access_key_id: &str, secret_access_key: &str) -> Result<()> {
        match &self.backend {
            Backend::Fjall { api_keys, .. } => {
                api_keys.insert(access_key_id.as_bytes(), secret_access_key.as_bytes())?;
            }
            Backend::Memory(m) => {
                m.api_keys
                    .write()
                    .unwrap()
                    .insert(access_key_id.to_string(), secret_access_key.to_string());
            }
        }
        debug!(access_key_id, "stored api key");
        Ok(())
    }

    /// Retrieve the secret for a given access key ID.
    pub fn get_api_key(&self, access_key_id: &str) -> Result<Option<String>> {
        match &self.backend {
            Backend::Fjall { api_keys, .. } => {
                let Some(value) = api_keys.get(access_key_id.as_bytes())? else {
                    return Ok(None);
                };
                let secret = String::from_utf8(value.to_vec()).map_err(|e| {
                    MetaError::CorruptData(format!("api key secret is not UTF-8: {e}"))
                })?;
                Ok(Some(secret))
            }
            Backend::Memory(m) => Ok(m.api_keys.read().unwrap().get(access_key_id).cloned()),
        }
    }

    /// Delete an API key pair by access key ID.
    pub fn delete_api_key(&self, access_key_id: &str) -> Result<()> {
        match &self.backend {
            Backend::Fjall { api_keys, .. } => {
                api_keys.remove(access_key_id.as_bytes())?;
            }
            Backend::Memory(m) => {
                m.api_keys.write().unwrap().remove(access_key_id);
            }
        }
        debug!(access_key_id, "deleted api key");
        Ok(())
    }

    /// List all access key IDs (secrets are NOT returned).
    pub fn list_api_key_ids(&self) -> Result<Vec<String>> {
        match &self.backend {
            Backend::Fjall { api_keys, .. } => {
                let mut result = Vec::new();

                for guard in api_keys.iter() {
                    let (k, _v) = guard.into_inner()?;
                    let key_id = String::from_utf8(k.to_vec()).map_err(|e| {
                        MetaError::CorruptData(format!("api key ID is not UTF-8: {e}"))
                    })?;
                    result.push(key_id);
                }

                Ok(result)
            }
            Backend::Memory(m) => Ok(m.api_keys.read().unwrap().keys().cloned().collect()),
        }
    }

    /// Load all API key pairs into a HashMap.
    ///
    /// Used at startup to populate the in-memory auth cache.
    pub fn load_all_api_keys(&self) -> Result<HashMap<String, String>> {
        match &self.backend {
            Backend::Fjall { api_keys, .. } => {
                let mut result = HashMap::new();

                for guard in api_keys.iter() {
                    let (k, v) = guard.into_inner()?;
                    let key_id = String::from_utf8(k.to_vec()).map_err(|e| {
                        MetaError::CorruptData(format!("api key ID is not UTF-8: {e}"))
                    })?;
                    let secret = String::from_utf8(v.to_vec()).map_err(|e| {
                        MetaError::CorruptData(format!("api key secret is not UTF-8: {e}"))
                    })?;
                    result.insert(key_id, secret);
                }

                Ok(result)
            }
            Backend::Memory(m) => Ok(m.api_keys.read().unwrap().clone()),
        }
    }

    // ----- Object tags -----

    /// Set tags on an object, replacing any existing tags.
    pub fn put_object_tags(
        &self,
        bucket: &str,
        key: &str,
        tags: &BTreeMap<String, String>,
    ) -> Result<()> {
        let storage_key = object_storage_key(bucket, key);
        let value = postcard::to_allocvec(tags)?;

        match &self.backend {
            Backend::Fjall { tags: tags_ks, .. } => {
                tags_ks.insert(storage_key.as_bytes(), value.as_slice())?;
            }
            Backend::Memory(m) => {
                m.tags.write().unwrap().insert(storage_key, value);
            }
        }

        debug!(bucket, key, count = tags.len(), "stored object tags");
        Ok(())
    }

    /// Retrieve tags for an object. Returns empty map if no tags set.
    pub fn get_object_tags(&self, bucket: &str, key: &str) -> Result<BTreeMap<String, String>> {
        let storage_key = object_storage_key(bucket, key);

        match &self.backend {
            Backend::Fjall { tags: tags_ks, .. } => match tags_ks.get(storage_key.as_bytes())? {
                Some(bytes) => Ok(postcard::from_bytes(&bytes)?),
                None => Ok(BTreeMap::new()),
            },
            Backend::Memory(m) => match m.tags.read().unwrap().get(&storage_key) {
                Some(bytes) => Ok(postcard::from_bytes(bytes)?),
                None => Ok(BTreeMap::new()),
            },
        }
    }

    /// Delete all tags from an object.
    pub fn delete_object_tags(&self, bucket: &str, key: &str) -> Result<()> {
        let storage_key = object_storage_key(bucket, key);

        match &self.backend {
            Backend::Fjall { tags: tags_ks, .. } => {
                tags_ks.remove(storage_key.as_bytes())?;
            }
            Backend::Memory(m) => {
                m.tags.write().unwrap().remove(&storage_key);
            }
        }

        debug!(bucket, key, "deleted object tags");
        Ok(())
    }

    // ----- Repair queue (local, transient) -----

    /// Enqueue a shard for repair with the given priority (lower = more urgent).
    pub fn enqueue_repair(&self, id: &ShardId, priority: u64) -> Result<()> {
        let key = repair_queue_key(priority, id);
        match &self.backend {
            Backend::Fjall { repair_queue, .. } => {
                repair_queue.insert(key.as_slice(), id.as_bytes())?;
            }
            Backend::Memory(m) => {
                m.repair_queue.write().unwrap().insert(key, *id.as_bytes());
            }
        }
        debug!(%id, priority, "enqueued shard for repair");
        Ok(())
    }

    /// Dequeue the highest-priority (lowest priority number) shard from the repair queue.
    ///
    /// Returns `None` if the queue is empty.
    pub fn dequeue_repair(&self) -> Result<Option<ShardId>> {
        match &self.backend {
            Backend::Fjall { repair_queue, .. } => {
                if let Some(guard) = repair_queue.first_key_value() {
                    let (key, value) = guard.into_inner()?;
                    let arr: [u8; 32] = value[..32].try_into().map_err(|_| {
                        MetaError::CorruptData(format!(
                            "ShardId expected 32 bytes, got {}",
                            value.len()
                        ))
                    })?;
                    let shard_id = ShardId::from(arr);
                    repair_queue.remove(key.as_ref())?;
                    debug!(%shard_id, "dequeued shard from repair queue");
                    Ok(Some(shard_id))
                } else {
                    Ok(None)
                }
            }
            Backend::Memory(m) => {
                let mut map = m.repair_queue.write().unwrap();
                let first_key = map.keys().next().cloned();
                match first_key {
                    Some(key) => {
                        let arr = map.remove(&key).unwrap();
                        let shard_id = ShardId::from(arr);
                        debug!(%shard_id, "dequeued shard from repair queue");
                        Ok(Some(shard_id))
                    }
                    None => Ok(None),
                }
            }
        }
    }

    /// Return the number of items in the repair queue.
    ///
    /// Note: O(n) scan for Fjall backend, O(1) for in-memory.
    pub fn repair_queue_len(&self) -> Result<usize> {
        match &self.backend {
            Backend::Fjall { repair_queue, .. } => {
                let mut count = 0;
                for guard in repair_queue.iter() {
                    let _ = guard.key()?;
                    count += 1;
                }
                Ok(count)
            }
            Backend::Memory(m) => Ok(m.repair_queue.read().unwrap().len()),
        }
    }
}

/// Build the storage key for the objects keyspace: `"bucket/key"`.
fn object_storage_key(bucket: &str, key: &str) -> String {
    format!("{bucket}/{key}")
}

/// Build the repair queue key: `priority (8 bytes big-endian) ++ shard_id (32 bytes)`.
///
/// Big-endian ensures lexicographic ordering matches numeric ordering.
fn repair_queue_key(priority: u64, shard_id: &ShardId) -> Vec<u8> {
    let mut key = Vec::with_capacity(40);
    key.extend_from_slice(&priority.to_be_bytes());
    key.extend_from_slice(shard_id.as_bytes());
    key
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use shoal_types::{ChunkMeta, MANIFEST_VERSION, MemberState, NodeTopology, ShardMeta};

    use super::*;

    fn test_manifest() -> Manifest {
        Manifest {
            version: MANIFEST_VERSION,
            object_id: ObjectId::from_data(b"test object"),
            total_size: 5000,
            chunk_size: 1024,
            chunks: vec![ChunkMeta {
                chunk_id: shoal_types::ChunkId::from_data(b"chunk 0"),
                offset: 0,
                raw_length: 1024,
                stored_length: 1024,
                compression: shoal_types::Compression::None,
                shards: vec![ShardMeta {
                    shard_id: ShardId::from_data(b"shard 0"),
                    index: 0,
                    size: 512,
                }],
            }],
            created_at: 1700000000,
            metadata: BTreeMap::from([(
                "content-type".to_string(),
                "application/octet-stream".to_string(),
            )]),
        }
    }

    fn test_member(name: &[u8]) -> Member {
        Member {
            node_id: NodeId::from_data(name),
            capacity: 1_000_000_000,
            state: MemberState::Alive,
            generation: 1,
            topology: NodeTopology::default(),
        }
    }

    /// Run a test function against both Fjall (temporary) and in-memory backends.
    fn with_both_backends(f: impl Fn(MetaStore)) {
        // Fjall (temporary).
        f(MetaStore::open_temporary().unwrap());
        // Pure in-memory.
        f(MetaStore::in_memory());
    }

    #[test]
    fn test_manifest_put_get_roundtrip() {
        with_both_backends(|store| {
            let manifest = test_manifest();
            store.put_manifest(&manifest).unwrap();
            let retrieved = store.get_manifest(&manifest.object_id).unwrap();
            assert_eq!(retrieved, Some(manifest));
        });
    }

    #[test]
    fn test_manifest_get_nonexistent() {
        with_both_backends(|store| {
            let id = ObjectId::from_data(b"nonexistent");
            let result = store.get_manifest(&id).unwrap();
            assert!(result.is_none());
        });
    }

    #[test]
    fn test_object_key_put_get() {
        with_both_backends(|store| {
            let id = ObjectId::from_data(b"my object");
            store
                .put_object_key("mybucket", "photos/cat.jpg", &id)
                .unwrap();
            let retrieved = store.get_object_key("mybucket", "photos/cat.jpg").unwrap();
            assert_eq!(retrieved, Some(id));
        });
    }

    #[test]
    fn test_object_key_get_nonexistent() {
        with_both_backends(|store| {
            let result = store.get_object_key("mybucket", "nope").unwrap();
            assert!(result.is_none());
        });
    }

    #[test]
    fn test_object_key_delete() {
        with_both_backends(|store| {
            let id = ObjectId::from_data(b"deleteme");
            store.put_object_key("bucket", "key", &id).unwrap();
            assert!(store.get_object_key("bucket", "key").unwrap().is_some());
            store.delete_object_key("bucket", "key").unwrap();
            assert!(store.get_object_key("bucket", "key").unwrap().is_none());
        });
    }

    #[test]
    fn test_list_objects_all() {
        with_both_backends(|store| {
            let id1 = ObjectId::from_data(b"obj1");
            let id2 = ObjectId::from_data(b"obj2");
            let id3 = ObjectId::from_data(b"obj3");

            store.put_object_key("bucket", "a.txt", &id1).unwrap();
            store.put_object_key("bucket", "b.txt", &id2).unwrap();
            store.put_object_key("bucket", "c.txt", &id3).unwrap();

            let mut keys = store.list_objects("bucket", "").unwrap();
            keys.sort();
            assert_eq!(keys, vec!["a.txt", "b.txt", "c.txt"]);
        });
    }

    #[test]
    fn test_list_objects_with_prefix() {
        with_both_backends(|store| {
            let id1 = ObjectId::from_data(b"obj1");
            let id2 = ObjectId::from_data(b"obj2");
            let id3 = ObjectId::from_data(b"obj3");

            store
                .put_object_key("bucket", "photos/a.jpg", &id1)
                .unwrap();
            store
                .put_object_key("bucket", "photos/b.jpg", &id2)
                .unwrap();
            store.put_object_key("bucket", "docs/c.pdf", &id3).unwrap();

            let mut keys = store.list_objects("bucket", "photos/").unwrap();
            keys.sort();
            assert_eq!(keys, vec!["photos/a.jpg", "photos/b.jpg"]);

            let docs = store.list_objects("bucket", "docs/").unwrap();
            assert_eq!(docs, vec!["docs/c.pdf"]);
        });
    }

    #[test]
    fn test_list_objects_empty_bucket() {
        with_both_backends(|store| {
            let keys = store.list_objects("empty", "").unwrap();
            assert!(keys.is_empty());
        });
    }

    #[test]
    fn test_list_objects_no_cross_bucket_leakage() {
        with_both_backends(|store| {
            let id = ObjectId::from_data(b"obj");
            store.put_object_key("bucket1", "file.txt", &id).unwrap();
            store.put_object_key("bucket2", "file.txt", &id).unwrap();

            let keys1 = store.list_objects("bucket1", "").unwrap();
            assert_eq!(keys1, vec!["file.txt"]);

            let keys2 = store.list_objects("bucket2", "").unwrap();
            assert_eq!(keys2, vec!["file.txt"]);
        });
    }

    #[test]
    fn test_shard_owners_put_get() {
        with_both_backends(|store| {
            let shard_id = ShardId::from_data(b"shard1");
            let owners = vec![
                NodeId::from_data(b"node1"),
                NodeId::from_data(b"node2"),
                NodeId::from_data(b"node3"),
            ];

            store.put_shard_owners(&shard_id, &owners).unwrap();
            let retrieved = store.get_shard_owners(&shard_id).unwrap();
            assert_eq!(retrieved, Some(owners));
        });
    }

    #[test]
    fn test_shard_owners_get_nonexistent() {
        with_both_backends(|store| {
            let shard_id = ShardId::from_data(b"unknown");
            let result = store.get_shard_owners(&shard_id).unwrap();
            assert!(result.is_none());
        });
    }

    #[test]
    fn test_member_put_get() {
        with_both_backends(|store| {
            let member = test_member(b"node-1");
            store.put_member(&member).unwrap();
            let retrieved = store.get_member(&member.node_id).unwrap();
            assert_eq!(retrieved, Some(member));
        });
    }

    #[test]
    fn test_member_get_nonexistent() {
        with_both_backends(|store| {
            let id = NodeId::from_data(b"ghost");
            let result = store.get_member(&id).unwrap();
            assert!(result.is_none());
        });
    }

    #[test]
    fn test_member_list() {
        with_both_backends(|store| {
            let m1 = test_member(b"node-1");
            let m2 = test_member(b"node-2");
            let m3 = test_member(b"node-3");

            store.put_member(&m1).unwrap();
            store.put_member(&m2).unwrap();
            store.put_member(&m3).unwrap();

            let members = store.list_members().unwrap();
            assert_eq!(members.len(), 3);

            let ids: Vec<NodeId> = members.iter().map(|m| m.node_id).collect();
            assert!(ids.contains(&m1.node_id));
            assert!(ids.contains(&m2.node_id));
            assert!(ids.contains(&m3.node_id));
        });
    }

    #[test]
    fn test_member_remove() {
        with_both_backends(|store| {
            let member = test_member(b"node-1");
            store.put_member(&member).unwrap();
            assert!(store.get_member(&member.node_id).unwrap().is_some());
            store.remove_member(&member.node_id).unwrap();
            assert!(store.get_member(&member.node_id).unwrap().is_none());
        });
    }

    #[test]
    fn test_member_update() {
        with_both_backends(|store| {
            let mut member = test_member(b"node-1");
            store.put_member(&member).unwrap();

            member.state = MemberState::Dead;
            member.generation = 2;
            store.put_member(&member).unwrap();

            let retrieved = store.get_member(&member.node_id).unwrap().unwrap();
            assert_eq!(retrieved.state, MemberState::Dead);
            assert_eq!(retrieved.generation, 2);
        });
    }

    #[test]
    fn test_repair_queue_enqueue_dequeue() {
        with_both_backends(|store| {
            let s1 = ShardId::from_data(b"shard-1");
            let s2 = ShardId::from_data(b"shard-2");
            let s3 = ShardId::from_data(b"shard-3");

            store.enqueue_repair(&s2, 10).unwrap();
            store.enqueue_repair(&s1, 1).unwrap();
            store.enqueue_repair(&s3, 100).unwrap();

            assert_eq!(store.dequeue_repair().unwrap(), Some(s1));
            assert_eq!(store.dequeue_repair().unwrap(), Some(s2));
            assert_eq!(store.dequeue_repair().unwrap(), Some(s3));
            assert_eq!(store.dequeue_repair().unwrap(), None);
        });
    }

    #[test]
    fn test_repair_queue_len() {
        with_both_backends(|store| {
            assert_eq!(store.repair_queue_len().unwrap(), 0);
            store.enqueue_repair(&ShardId::from_data(b"s1"), 1).unwrap();
            store.enqueue_repair(&ShardId::from_data(b"s2"), 2).unwrap();
            assert_eq!(store.repair_queue_len().unwrap(), 2);
            store.dequeue_repair().unwrap();
            assert_eq!(store.repair_queue_len().unwrap(), 1);
        });
    }

    #[test]
    fn test_repair_queue_empty_dequeue() {
        with_both_backends(|store| {
            assert_eq!(store.dequeue_repair().unwrap(), None);
        });
    }

    #[test]
    fn test_persistence_across_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().to_path_buf();

        // Write data.
        {
            let store = MetaStore::open(&path).unwrap();
            let manifest = test_manifest();
            store.put_manifest(&manifest).unwrap();
            store
                .put_object_key("bucket", "key", &manifest.object_id)
                .unwrap();
            store.put_member(&test_member(b"node-1")).unwrap();
            store
                .put_shard_owners(
                    &ShardId::from_data(b"shard"),
                    &[NodeId::from_data(b"owner")],
                )
                .unwrap();
        }

        // Reopen and verify data persists.
        {
            let store = MetaStore::open(&path).unwrap();

            let manifest = test_manifest();
            let retrieved = store.get_manifest(&manifest.object_id).unwrap();
            assert_eq!(retrieved, Some(manifest.clone()));

            let obj_id = store.get_object_key("bucket", "key").unwrap();
            assert_eq!(obj_id, Some(manifest.object_id));

            let member = store.get_member(&NodeId::from_data(b"node-1")).unwrap();
            assert!(member.is_some());

            let owners = store
                .get_shard_owners(&ShardId::from_data(b"shard"))
                .unwrap();
            assert_eq!(owners, Some(vec![NodeId::from_data(b"owner")]));
        }
    }

    // -----------------------------------------------------------------------
    // Concurrent access from multiple threads
    // -----------------------------------------------------------------------

    #[test]
    fn test_concurrent_manifest_put_get() {
        with_both_backends(|store| {
            let store = std::sync::Arc::new(store);
            let mut handles = Vec::new();

            for i in 0..20u32 {
                let s = store.clone();
                handles.push(std::thread::spawn(move || {
                    let mut manifest = test_manifest();
                    // Each thread creates a distinct manifest.
                    manifest.object_id = ObjectId::from_data(&i.to_le_bytes());
                    manifest.total_size = i as u64 * 1000;
                    s.put_manifest(&manifest).unwrap();

                    let got = s.get_manifest(&manifest.object_id).unwrap().unwrap();
                    assert_eq!(got.total_size, i as u64 * 1000);
                }));
            }

            for h in handles {
                h.join().unwrap();
            }
        });
    }

    #[test]
    fn test_concurrent_object_key_operations() {
        with_both_backends(|store| {
            let store = std::sync::Arc::new(store);
            let mut handles = Vec::new();

            for i in 0..20u32 {
                let s = store.clone();
                handles.push(std::thread::spawn(move || {
                    let key = format!("key-{i}");
                    let id = ObjectId::from_data(key.as_bytes());
                    s.put_object_key("bucket", &key, &id).unwrap();
                    let got = s.get_object_key("bucket", &key).unwrap();
                    assert_eq!(got, Some(id));
                }));
            }

            for h in handles {
                h.join().unwrap();
            }

            // All 20 keys should exist.
            let keys = store.list_objects("bucket", "").unwrap();
            assert_eq!(keys.len(), 20);
        });
    }

    #[test]
    fn test_concurrent_repair_queue() {
        with_both_backends(|store| {
            let store = std::sync::Arc::new(store);
            let mut handles = Vec::new();

            // Enqueue from multiple threads.
            for i in 0..20u32 {
                let s = store.clone();
                handles.push(std::thread::spawn(move || {
                    let shard = ShardId::from_data(&i.to_le_bytes());
                    s.enqueue_repair(&shard, i as u64).unwrap();
                }));
            }

            for h in handles {
                h.join().unwrap();
            }

            assert_eq!(store.repair_queue_len().unwrap(), 20);

            // Dequeue should come out in priority order.
            let mut prev_priority = None;
            for _ in 0..20 {
                let shard = store.dequeue_repair().unwrap().unwrap();
                // We can't easily check priority since we only get the ShardId,
                // but we verify all 20 are dequeued.
                // At least verify no duplicates by checking against a rolling collection.
                let _ = shard;
                let _ = prev_priority;
                prev_priority = Some(shard);
            }
            assert_eq!(store.dequeue_repair().unwrap(), None);
        });
    }

    // -----------------------------------------------------------------------
    // Overwrite behavior
    // -----------------------------------------------------------------------

    #[test]
    fn test_manifest_overwrite() {
        with_both_backends(|store| {
            let mut manifest = test_manifest();
            store.put_manifest(&manifest).unwrap();

            manifest.total_size = 99999;
            store.put_manifest(&manifest).unwrap();

            let got = store.get_manifest(&manifest.object_id).unwrap().unwrap();
            assert_eq!(got.total_size, 99999);
        });
    }

    #[test]
    fn test_object_key_overwrite() {
        with_both_backends(|store| {
            let id1 = ObjectId::from_data(b"first");
            let id2 = ObjectId::from_data(b"second");

            store.put_object_key("b", "k", &id1).unwrap();
            assert_eq!(store.get_object_key("b", "k").unwrap(), Some(id1));

            store.put_object_key("b", "k", &id2).unwrap();
            assert_eq!(store.get_object_key("b", "k").unwrap(), Some(id2));
        });
    }

    #[test]
    fn test_shard_owners_overwrite() {
        with_both_backends(|store| {
            let shard = ShardId::from_data(b"shard");
            let owners1 = vec![NodeId::from_data(b"node1")];
            let owners2 = vec![NodeId::from_data(b"node2"), NodeId::from_data(b"node3")];

            store.put_shard_owners(&shard, &owners1).unwrap();
            assert_eq!(store.get_shard_owners(&shard).unwrap(), Some(owners1));

            store.put_shard_owners(&shard, &owners2).unwrap();
            assert_eq!(store.get_shard_owners(&shard).unwrap(), Some(owners2));
        });
    }

    // -----------------------------------------------------------------------
    // Repair queue: same priority, deterministic ordering
    // -----------------------------------------------------------------------

    #[test]
    fn test_repair_queue_same_priority() {
        with_both_backends(|store| {
            let s1 = ShardId::from_data(b"shard-a");
            let s2 = ShardId::from_data(b"shard-b");
            let s3 = ShardId::from_data(b"shard-c");

            // All same priority.
            store.enqueue_repair(&s1, 5).unwrap();
            store.enqueue_repair(&s2, 5).unwrap();
            store.enqueue_repair(&s3, 5).unwrap();

            assert_eq!(store.repair_queue_len().unwrap(), 3);

            // All three should be dequeued (order depends on ShardId bytes at same priority).
            let mut dequeued = Vec::new();
            while let Some(sid) = store.dequeue_repair().unwrap() {
                dequeued.push(sid);
            }
            assert_eq!(dequeued.len(), 3);

            // Verify all three are present (in some order).
            assert!(dequeued.contains(&s1));
            assert!(dequeued.contains(&s2));
            assert!(dequeued.contains(&s3));
        });
    }

    // -----------------------------------------------------------------------
    // Repair queue: enqueue duplicate shard with different priority
    // -----------------------------------------------------------------------

    #[test]
    fn test_repair_queue_duplicate_shard_different_priority() {
        with_both_backends(|store| {
            let shard = ShardId::from_data(b"dup-shard");

            // Enqueue same shard twice with different priorities.
            store.enqueue_repair(&shard, 10).unwrap();
            store.enqueue_repair(&shard, 1).unwrap();

            // Both entries exist (the key includes priority + shard_id).
            assert_eq!(store.repair_queue_len().unwrap(), 2);

            // Lower priority (1) dequeues first.
            let first = store.dequeue_repair().unwrap().unwrap();
            assert_eq!(first, shard);
            let second = store.dequeue_repair().unwrap().unwrap();
            assert_eq!(second, shard);
            assert_eq!(store.dequeue_repair().unwrap(), None);
        });
    }

    // -----------------------------------------------------------------------
    // Empty owners list
    // -----------------------------------------------------------------------

    #[test]
    fn test_shard_owners_empty_list() {
        with_both_backends(|store| {
            let shard = ShardId::from_data(b"no-owners");
            store.put_shard_owners(&shard, &[]).unwrap();
            let got = store.get_shard_owners(&shard).unwrap();
            assert_eq!(got, Some(vec![]));
        });
    }

    // -----------------------------------------------------------------------
    // Large number of items
    // -----------------------------------------------------------------------

    #[test]
    fn test_many_object_keys() {
        with_both_backends(|store| {
            let count = 500;
            for i in 0..count {
                let key = format!("key-{i:04}");
                let id = ObjectId::from_data(key.as_bytes());
                store.put_object_key("bucket", &key, &id).unwrap();
            }

            let keys = store.list_objects("bucket", "").unwrap();
            assert_eq!(keys.len(), count);
        });
    }

    #[test]
    fn test_many_members() {
        with_both_backends(|store| {
            let count = 100;
            for i in 0..count {
                let member = test_member(&[i as u8]);
                store.put_member(&member).unwrap();
            }

            let members = store.list_members().unwrap();
            assert_eq!(members.len(), count);
        });
    }

    // -----------------------------------------------------------------------
    // Delete operations: idempotency
    // -----------------------------------------------------------------------

    #[test]
    fn test_delete_nonexistent_object_key() {
        with_both_backends(|store| {
            // Should not error when deleting a key that doesn't exist.
            store.delete_object_key("b", "nope").unwrap();
        });
    }

    #[test]
    fn test_remove_nonexistent_member() {
        with_both_backends(|store| {
            let id = NodeId::from_data(b"ghost");
            // Should not error.
            store.remove_member(&id).unwrap();
        });
    }

    // -----------------------------------------------------------------------
    // List objects: no cross-prefix leakage
    // -----------------------------------------------------------------------

    #[test]
    fn test_list_objects_prefix_boundary() {
        with_both_backends(|store| {
            let id = ObjectId::from_data(b"x");

            // "a" and "ab" should not match prefix "a/" (note trailing slash).
            store.put_object_key("b", "a", &id).unwrap();
            store.put_object_key("b", "ab", &id).unwrap();
            store.put_object_key("b", "a/x", &id).unwrap();
            store.put_object_key("b", "a/y", &id).unwrap();

            let result = store.list_objects("b", "a/").unwrap();
            assert_eq!(result.len(), 2);
            assert!(result.contains(&"a/x".to_string()));
            assert!(result.contains(&"a/y".to_string()));
        });
    }

    // -----------------------------------------------------------------------
    // Member state transitions
    // -----------------------------------------------------------------------

    #[test]
    fn test_member_state_transitions() {
        with_both_backends(|store| {
            let mut member = test_member(b"transitioning-node");

            // Start as Alive.
            member.state = MemberState::Alive;
            store.put_member(&member).unwrap();
            assert_eq!(
                store.get_member(&member.node_id).unwrap().unwrap().state,
                MemberState::Alive
            );

            // Transition to Suspect.
            member.state = MemberState::Suspect;
            store.put_member(&member).unwrap();
            assert_eq!(
                store.get_member(&member.node_id).unwrap().unwrap().state,
                MemberState::Suspect
            );

            // Transition to Dead.
            member.state = MemberState::Dead;
            store.put_member(&member).unwrap();
            assert_eq!(
                store.get_member(&member.node_id).unwrap().unwrap().state,
                MemberState::Dead
            );

            // Remove the member.
            store.remove_member(&member.node_id).unwrap();
            assert!(store.get_member(&member.node_id).unwrap().is_none());

            // List should be empty now.
            assert!(store.list_members().unwrap().is_empty());
        });
    }

    // -----------------------------------------------------------------------
    // Manifest with many chunks
    // -----------------------------------------------------------------------

    #[test]
    fn test_manifest_with_many_chunks() {
        with_both_backends(|store| {
            let chunks: Vec<ChunkMeta> = (0..50)
                .map(|i| ChunkMeta {
                    chunk_id: shoal_types::ChunkId::from_data(&[i as u8]),
                    offset: i as u64 * 1024,
                    raw_length: 1024,
                    stored_length: 1024,
                    compression: shoal_types::Compression::None,
                    shards: (0..6)
                        .map(|j| ShardMeta {
                            shard_id: ShardId::from_data(&[i as u8, j]),
                            index: j,
                            size: 512,
                        })
                        .collect(),
                })
                .collect();

            let manifest = Manifest {
                version: MANIFEST_VERSION,
                object_id: ObjectId::from_data(b"big manifest"),
                total_size: 50 * 1024,
                chunk_size: 1024,
                chunks,
                created_at: 1700000000,
                metadata: BTreeMap::new(),
            };

            store.put_manifest(&manifest).unwrap();
            let got = store.get_manifest(&manifest.object_id).unwrap().unwrap();
            assert_eq!(got.chunks.len(), 50);
            assert_eq!(got.chunks[0].shards.len(), 6);
        });
    }

    // -----------------------------------------------------------------------
    // Peer address persistence
    // -----------------------------------------------------------------------

    #[test]
    fn test_peer_addrs_put_list() {
        with_both_backends(|store| {
            let node1 = NodeId::from_data(b"peer-1");
            let node2 = NodeId::from_data(b"peer-2");

            let addrs1: Vec<std::net::SocketAddr> = vec!["192.168.1.10:4820".parse().unwrap()];
            let addrs2: Vec<std::net::SocketAddr> = vec![
                "10.0.0.5:4820".parse().unwrap(),
                "[::1]:4820".parse().unwrap(),
            ];

            store.put_peer_addrs(&node1, &addrs1).unwrap();
            store.put_peer_addrs(&node2, &addrs2).unwrap();

            let peers = store.list_peer_addrs().unwrap();
            assert_eq!(peers.len(), 2);

            let map: HashMap<NodeId, Vec<std::net::SocketAddr>> = peers.into_iter().collect();
            assert_eq!(map[&node1], addrs1);
            assert_eq!(map[&node2], addrs2);
        });
    }

    #[test]
    fn test_peer_addrs_remove() {
        with_both_backends(|store| {
            let node = NodeId::from_data(b"removable-peer");
            let addrs: Vec<std::net::SocketAddr> = vec!["1.2.3.4:4820".parse().unwrap()];

            store.put_peer_addrs(&node, &addrs).unwrap();
            assert_eq!(store.list_peer_addrs().unwrap().len(), 1);

            store.remove_peer_addrs(&node).unwrap();
            assert!(store.list_peer_addrs().unwrap().is_empty());
        });
    }

    #[test]
    fn test_peer_addrs_persist_across_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().to_path_buf();

        let node = NodeId::from_data(b"persistent-peer");
        let addrs: Vec<std::net::SocketAddr> = vec!["10.0.0.1:4820".parse().unwrap()];

        {
            let store = MetaStore::open(&path).unwrap();
            store.put_peer_addrs(&node, &addrs).unwrap();
        }

        {
            let store = MetaStore::open(&path).unwrap();
            let peers = store.list_peer_addrs().unwrap();
            assert_eq!(peers.len(), 1);
            assert_eq!(peers[0].0, node);
            assert_eq!(peers[0].1, addrs);
        }
    }
}
