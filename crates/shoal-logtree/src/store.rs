//! Storage backend for the LogTree (Fjall disk or pure in-memory).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::RwLock;

use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use shoal_types::{Manifest, ObjectId};

use crate::entry::{LogEntry, Version};
use crate::error::LogTreeError;

type Result<T> = std::result::Result<T, LogTreeError>;

/// Inner backend: either Fjall-backed (disk) or pure in-memory.
enum Backend {
    Fjall {
        #[allow(dead_code)]
        db: Database,
        entries: Keyspace,
        tips: Keyspace,
        manifests: Keyspace,
        state: Keyspace,
        snapshots: Keyspace,
    },
    Memory(Box<MemoryBackend>),
}

/// Pure in-memory storage.
struct MemoryBackend {
    /// hash → serialized LogEntry.
    entries: RwLock<HashMap<[u8; 32], Vec<u8>>>,
    /// Set of current DAG tip hashes.
    tips: RwLock<HashSet<[u8; 32]>>,
    /// ObjectId → serialized Manifest.
    manifests: RwLock<HashMap<[u8; 32], Vec<u8>>>,
    /// "bucket/key" → serialized Vec<Version>.
    state: RwLock<BTreeMap<String, Vec<u8>>>,
    /// state_hash → serialized snapshot blob.
    snapshots: RwLock<HashMap<[u8; 32], Vec<u8>>>,
}

/// Storage backend for the LogTree DAG.
pub struct LogTreeStore {
    backend: Backend,
}

fn storage_err(e: impl std::fmt::Display) -> LogTreeError {
    LogTreeError::Storage(e.to_string())
}

impl LogTreeStore {
    /// Open a persistent store at the given path (Fjall backend).
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = Database::builder(path).open().map_err(storage_err)?;
        let backend = Self::init_fjall(db)?;
        Ok(Self { backend })
    }

    /// Open a temporary store backed by Fjall (cleaned up on drop).
    pub fn open_temporary() -> Result<Self> {
        let tmp = tempfile::tempdir().map_err(storage_err)?;
        let db = Database::builder(tmp.path())
            .temporary(true)
            .open()
            .map_err(storage_err)?;
        let backend = Self::init_fjall(db)?;
        Ok(Self { backend })
    }

    /// Create a pure in-memory store.
    pub fn in_memory() -> Self {
        Self {
            backend: Backend::Memory(Box::new(MemoryBackend {
                entries: RwLock::new(HashMap::new()),
                tips: RwLock::new(HashSet::new()),
                manifests: RwLock::new(HashMap::new()),
                state: RwLock::new(BTreeMap::new()),
                snapshots: RwLock::new(HashMap::new()),
            })),
        }
    }

    fn init_fjall(db: Database) -> Result<Backend> {
        let entries = db
            .keyspace("lt_entries", KeyspaceCreateOptions::default)
            .map_err(storage_err)?;
        let tips = db
            .keyspace("lt_tips", KeyspaceCreateOptions::default)
            .map_err(storage_err)?;
        let manifests = db
            .keyspace("lt_manifests", KeyspaceCreateOptions::default)
            .map_err(storage_err)?;
        let state = db
            .keyspace("lt_state", KeyspaceCreateOptions::default)
            .map_err(storage_err)?;
        let snapshots = db
            .keyspace("lt_snapshots", KeyspaceCreateOptions::default)
            .map_err(storage_err)?;
        Ok(Backend::Fjall {
            db,
            entries,
            tips,
            manifests,
            state,
            snapshots,
        })
    }

    // ----- Entries -----

    /// Store a log entry by its hash.
    pub fn put_entry(&self, entry: &LogEntry) -> Result<()> {
        let bytes = postcard::to_allocvec(entry)?;

        match &self.backend {
            Backend::Fjall { entries, .. } => {
                entries.insert(entry.hash, bytes).map_err(storage_err)?;
            }
            Backend::Memory(m) => {
                m.entries.write().unwrap().insert(entry.hash, bytes);
            }
        }
        Ok(())
    }

    /// Retrieve a log entry by hash.
    pub fn get_entry(&self, hash: &[u8; 32]) -> Result<Option<LogEntry>> {
        let bytes = match &self.backend {
            Backend::Fjall { entries, .. } => {
                entries.get(hash).map_err(storage_err)?.map(|v| v.to_vec())
            }
            Backend::Memory(m) => m.entries.read().unwrap().get(hash).cloned(),
        };

        match bytes {
            Some(b) => Ok(Some(postcard::from_bytes(&b)?)),
            None => Ok(None),
        }
    }

    /// Check if an entry exists.
    pub fn has_entry(&self, hash: &[u8; 32]) -> Result<bool> {
        match &self.backend {
            Backend::Fjall { entries, .. } => Ok(entries.get(hash).map_err(storage_err)?.is_some()),
            Backend::Memory(m) => Ok(m.entries.read().unwrap().contains_key(hash)),
        }
    }

    /// Remove an entry (for pruning).
    pub fn remove_entry(&self, hash: &[u8; 32]) -> Result<()> {
        match &self.backend {
            Backend::Fjall { entries, .. } => {
                entries.remove(hash).map_err(storage_err)?;
            }
            Backend::Memory(m) => {
                m.entries.write().unwrap().remove(hash);
            }
        }
        Ok(())
    }

    // ----- Tips -----

    /// Add a hash to the set of DAG tips.
    pub fn add_tip(&self, hash: [u8; 32]) -> Result<()> {
        match &self.backend {
            Backend::Fjall { tips, .. } => {
                tips.insert(hash, []).map_err(storage_err)?;
            }
            Backend::Memory(m) => {
                m.tips.write().unwrap().insert(hash);
            }
        }
        Ok(())
    }

    /// Remove a hash from the set of DAG tips.
    pub fn remove_tip(&self, hash: &[u8; 32]) -> Result<()> {
        match &self.backend {
            Backend::Fjall { tips, .. } => {
                tips.remove(hash).map_err(storage_err)?;
            }
            Backend::Memory(m) => {
                m.tips.write().unwrap().remove(hash);
            }
        }
        Ok(())
    }

    /// Get all current DAG tip hashes.
    pub fn get_tips(&self) -> Result<Vec<[u8; 32]>> {
        match &self.backend {
            Backend::Fjall { tips, .. } => {
                let mut result = Vec::new();

                for guard in tips.iter() {
                    let k = guard.key().map_err(storage_err)?;
                    let mut hash = [0u8; 32];
                    hash.copy_from_slice(&k);
                    result.push(hash);
                }

                Ok(result)
            }
            Backend::Memory(m) => Ok(m.tips.read().unwrap().iter().copied().collect()),
        }
    }

    // ----- Manifests -----

    /// Cache a manifest by its ObjectId.
    pub fn put_manifest(&self, manifest: &Manifest) -> Result<()> {
        let bytes = postcard::to_allocvec(manifest)?;
        let key = *manifest.object_id.as_bytes();

        match &self.backend {
            Backend::Fjall { manifests, .. } => {
                manifests.insert(key, bytes).map_err(storage_err)?;
            }
            Backend::Memory(m) => {
                m.manifests.write().unwrap().insert(key, bytes);
            }
        }
        Ok(())
    }

    /// Retrieve a cached manifest by ObjectId.
    pub fn get_manifest(&self, id: &ObjectId) -> Result<Option<Manifest>> {
        let key = id.as_bytes();
        let bytes = match &self.backend {
            Backend::Fjall { manifests, .. } => {
                manifests.get(key).map_err(storage_err)?.map(|v| v.to_vec())
            }
            Backend::Memory(m) => m.manifests.read().unwrap().get(key).cloned(),
        };

        match bytes {
            Some(b) => Ok(Some(postcard::from_bytes(&b)?)),
            None => Ok(None),
        }
    }

    // ----- Materialized state -----

    /// Store versions for a `(bucket, key)` pair.
    pub fn put_versions(&self, bucket: &str, key: &str, versions: &[Version]) -> Result<()> {
        let compound_key = format!("{bucket}/{key}");
        let bytes = postcard::to_allocvec(versions)?;

        match &self.backend {
            Backend::Fjall { state, .. } => {
                state
                    .insert(compound_key.as_bytes(), bytes)
                    .map_err(storage_err)?;
            }
            Backend::Memory(m) => {
                m.state.write().unwrap().insert(compound_key, bytes);
            }
        }
        Ok(())
    }

    /// Retrieve versions for a `(bucket, key)` pair.
    pub fn get_versions(&self, bucket: &str, key: &str) -> Result<Option<Vec<Version>>> {
        let compound_key = format!("{bucket}/{key}");

        let bytes = match &self.backend {
            Backend::Fjall { state, .. } => state
                .get(compound_key.as_bytes())
                .map_err(storage_err)?
                .map(|v| v.to_vec()),
            Backend::Memory(m) => m.state.read().unwrap().get(&compound_key).cloned(),
        };

        match bytes {
            Some(b) => Ok(Some(postcard::from_bytes(&b)?)),
            None => Ok(None),
        }
    }

    /// Delete versions for a `(bucket, key)` pair.
    pub fn delete_versions(&self, bucket: &str, key: &str) -> Result<()> {
        let compound_key = format!("{bucket}/{key}");

        match &self.backend {
            Backend::Fjall { state, .. } => {
                state.remove(compound_key.as_bytes()).map_err(storage_err)?;
            }
            Backend::Memory(m) => {
                m.state.write().unwrap().remove(&compound_key);
            }
        }
        Ok(())
    }

    /// List all keys in a bucket matching a prefix.
    pub fn list_keys(&self, bucket: &str, prefix: &str) -> Result<Vec<String>> {
        let scan_prefix = if prefix.is_empty() {
            format!("{bucket}/")
        } else {
            format!("{bucket}/{prefix}")
        };
        let bucket_prefix = format!("{bucket}/");

        match &self.backend {
            Backend::Fjall { state, .. } => {
                let mut keys = Vec::new();

                for guard in state.prefix(scan_prefix.as_bytes()) {
                    let k = guard.key().map_err(storage_err)?;
                    let full_key = std::str::from_utf8(&k).map_err(storage_err)?.to_string();

                    if let Some(stripped) = full_key.strip_prefix(&bucket_prefix) {
                        keys.push(stripped.to_string());
                    }
                }

                Ok(keys)
            }
            Backend::Memory(m) => {
                let state = m.state.read().unwrap();
                let keys: Vec<String> = state
                    .range(scan_prefix.clone()..)
                    .take_while(|(k, _)| k.starts_with(&scan_prefix))
                    .filter_map(|(k, _)| k.strip_prefix(&bucket_prefix).map(|s| s.to_string()))
                    .collect();
                Ok(keys)
            }
        }
    }

    /// Get all state entries (for snapshot serialization).
    pub fn all_state(&self) -> Result<BTreeMap<String, Vec<Version>>> {
        match &self.backend {
            Backend::Fjall { state, .. } => {
                let mut result = BTreeMap::new();

                for guard in state.iter() {
                    let (key_bytes, val_bytes) = guard.into_inner().map_err(storage_err)?;
                    let key = std::str::from_utf8(&key_bytes)
                        .map_err(storage_err)?
                        .to_string();
                    let versions: Vec<Version> = postcard::from_bytes(&val_bytes)?;
                    result.insert(key, versions);
                }

                Ok(result)
            }
            Backend::Memory(m) => {
                let state = m.state.read().unwrap();
                let mut result = BTreeMap::new();

                for (key, bytes) in state.iter() {
                    let versions: Vec<Version> = postcard::from_bytes(bytes)?;
                    result.insert(key.clone(), versions);
                }

                Ok(result)
            }
        }
    }

    // ----- Snapshots -----

    /// Store a snapshot blob by its state hash.
    pub fn put_snapshot(&self, state_hash: &[u8; 32], data: &[u8]) -> Result<()> {
        match &self.backend {
            Backend::Fjall { snapshots, .. } => {
                snapshots.insert(state_hash, data).map_err(storage_err)?;
            }
            Backend::Memory(m) => {
                m.snapshots
                    .write()
                    .unwrap()
                    .insert(*state_hash, data.to_vec());
            }
        }
        Ok(())
    }

    /// Retrieve a snapshot blob by its state hash.
    pub fn get_snapshot(&self, state_hash: &[u8; 32]) -> Result<Option<Vec<u8>>> {
        match &self.backend {
            Backend::Fjall { snapshots, .. } => Ok(snapshots
                .get(state_hash)
                .map_err(storage_err)?
                .map(|v| v.to_vec())),
            Backend::Memory(m) => Ok(m.snapshots.read().unwrap().get(state_hash).cloned()),
        }
    }
}
