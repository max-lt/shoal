//! [`MetaStore`] implementation with Fjall (disk) and in-memory backends.

use std::collections::{BTreeMap, HashMap};
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
            })),
        }
    }

    fn init_fjall(db: Database) -> Result<Backend> {
        let objects = db.keyspace("objects", KeyspaceCreateOptions::default)?;
        let manifests = db.keyspace("manifests", KeyspaceCreateOptions::default)?;
        let shardmap = db.keyspace("shardmap", KeyspaceCreateOptions::default)?;
        let membership = db.keyspace("membership", KeyspaceCreateOptions::default)?;
        let repair_queue = db.keyspace("repair_queue", KeyspaceCreateOptions::default)?;
        Ok(Backend::Fjall {
            db,
            objects,
            manifests,
            shardmap,
            membership,
            repair_queue,
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
    pub fn put_object_key(&self, bucket: &str, key: &str, id: &ObjectId) -> Result<()> {
        let storage_key = object_storage_key(bucket, key);
        match &self.backend {
            Backend::Fjall { objects, .. } => {
                objects.insert(storage_key.as_bytes(), id.as_bytes())?;
            }
            Backend::Memory(m) => {
                m.objects
                    .write()
                    .unwrap()
                    .insert(storage_key, *id.as_bytes());
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
                size: 1024,
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
}
