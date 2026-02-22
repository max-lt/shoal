//! File-based shard storage backend.
//!
//! Stores one file per shard with a 2-level fan-out directory structure:
//! `{base_dir}/{hex[0..2]}/{hex[2..4]}/{hex}`.

use std::path::{Path, PathBuf};

use shoal_types::ShardId;
use tracing::debug;

use crate::error::StoreError;
use crate::traits::{ShardStore, StorageCapacity};

/// File-based shard store with 2-level fan-out directory layout.
///
/// Each shard is stored as a file at:
/// `{base_dir}/{hex(id)[0..2]}/{hex(id)[2..4]}/{hex(id)}`.
pub struct FileStore {
    base_dir: PathBuf,
}

impl FileStore {
    /// Create a new file store rooted at the given directory.
    ///
    /// The directory is created if it does not exist.
    pub fn new(base_dir: impl AsRef<Path>) -> Result<Self, StoreError> {
        let base_dir = base_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&base_dir)?;
        Ok(Self { base_dir })
    }

    /// Compute the full file path for a shard ID.
    fn shard_path(&self, id: &ShardId) -> PathBuf {
        let hex = id.to_string();
        self.base_dir.join(&hex[0..2]).join(&hex[2..4]).join(&hex)
    }
}

#[async_trait::async_trait]
impl ShardStore for FileStore {
    async fn put(&self, id: ShardId, data: &[u8]) -> Result<(), StoreError> {
        let path = self.shard_path(&id);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        debug!(%id, path = %path.display(), size = data.len(), "storing shard to file");
        tokio::fs::write(&path, data).await?;
        Ok(())
    }

    async fn get(&self, id: ShardId) -> Result<Option<Vec<u8>>, StoreError> {
        let path = self.shard_path(&id);
        match tokio::fs::read(&path).await {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StoreError::Io(e)),
        }
    }

    async fn delete(&self, id: ShardId) -> Result<(), StoreError> {
        let path = self.shard_path(&id);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {
                debug!(%id, "deleted shard file");
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(StoreError::Io(e)),
        }
    }

    async fn contains(&self, id: ShardId) -> Result<bool, StoreError> {
        let path = self.shard_path(&id);
        Ok(path.exists())
    }

    async fn list(&self) -> Result<Vec<ShardId>, StoreError> {
        let mut ids = Vec::new();
        let base = self.base_dir.clone();

        // Walk the 2-level fan-out: base/XX/YY/<hex>
        let mut level0 = tokio::fs::read_dir(&base).await?;
        while let Some(d0) = level0.next_entry().await? {
            if !d0.file_type().await?.is_dir() {
                continue;
            }
            let mut level1 = tokio::fs::read_dir(d0.path()).await?;
            while let Some(d1) = level1.next_entry().await? {
                if !d1.file_type().await?.is_dir() {
                    continue;
                }
                let mut files = tokio::fs::read_dir(d1.path()).await?;
                while let Some(entry) = files.next_entry().await? {
                    if !entry.file_type().await?.is_file() {
                        continue;
                    }
                    if let Some(name) = entry.file_name().to_str()
                        && name.len() == 64
                        && let Ok(bytes) = hex_to_bytes(name)
                    {
                        ids.push(ShardId::from(bytes));
                    }
                }
            }
        }
        Ok(ids)
    }

    async fn capacity(&self) -> Result<StorageCapacity, StoreError> {
        let stat = statvfs(&self.base_dir)?;
        Ok(stat)
    }

    async fn verify(&self, id: ShardId) -> Result<bool, StoreError> {
        let path = self.shard_path(&id);
        match tokio::fs::read(&path).await {
            Ok(data) => {
                let computed = ShardId::from_data(&data);
                Ok(computed == id)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(StoreError::NotFound(id)),
            Err(e) => Err(StoreError::Io(e)),
        }
    }
}

/// Decode a 64-character hex string into a `[u8; 32]`.
fn hex_to_bytes(hex: &str) -> Result<[u8; 32], ()> {
    if hex.len() != 64 {
        return Err(());
    }
    let mut bytes = [0u8; 32];
    for (i, byte) in bytes.iter_mut().enumerate() {
        let hi = hex.as_bytes()[i * 2];
        let lo = hex.as_bytes()[i * 2 + 1];
        *byte = (hex_nibble(hi)? << 4) | hex_nibble(lo)?;
    }
    Ok(bytes)
}

fn hex_nibble(c: u8) -> Result<u8, ()> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(()),
    }
}

/// Get filesystem capacity information using `libc::statvfs`.
#[cfg(unix)]
fn statvfs(path: &Path) -> Result<StorageCapacity, StoreError> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c_path.as_ptr(), &mut stat) != 0 {
            return Err(StoreError::Io(std::io::Error::last_os_error()));
        }

        let block_size = stat.f_frsize;
        let total = stat.f_blocks * block_size;
        let available = stat.f_bavail * block_size;
        // used = total - free (f_bfree includes reserved blocks)
        let free = stat.f_bfree * block_size;
        let used = total.saturating_sub(free);

        Ok(StorageCapacity {
            total_bytes: total,
            used_bytes: used,
            available_bytes: available,
        })
    }
}

#[cfg(not(unix))]
fn statvfs(_path: &Path) -> Result<StorageCapacity, StoreError> {
    // Fallback for non-Unix platforms: report unknown capacity.
    Ok(StorageCapacity {
        total_bytes: 0,
        used_bytes: 0,
        available_bytes: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn make_store() -> (FileStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = FileStore::new(dir.path()).unwrap();
        (store, dir)
    }

    #[tokio::test]
    async fn test_put_get_roundtrip() {
        let (store, _dir) = make_store().await;
        let data = b"hello file shard";
        let id = ShardId::from_data(data);

        store.put(id, data).await.unwrap();
        let result = store.get(id).await.unwrap();
        assert_eq!(result, Some(data.to_vec()));
    }

    #[tokio::test]
    async fn test_get_nonexistent_returns_none() {
        let (store, _dir) = make_store().await;
        let id = ShardId::from_data(b"not stored");
        assert_eq!(store.get(id).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_delete_then_get_returns_none() {
        let (store, _dir) = make_store().await;
        let data = b"to delete";
        let id = ShardId::from_data(data);

        store.put(id, data).await.unwrap();
        store.delete(id).await.unwrap();
        assert_eq!(store.get(id).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_contains_true_false() {
        let (store, _dir) = make_store().await;
        let data = b"exists on disk";
        let id = ShardId::from_data(data);

        assert!(!store.contains(id).await.unwrap());
        store.put(id, data).await.unwrap();
        assert!(store.contains(id).await.unwrap());
    }

    #[tokio::test]
    async fn test_list_returns_all_stored_ids() {
        let (store, _dir) = make_store().await;
        let data1 = b"file shard one";
        let data2 = b"file shard two";
        let data3 = b"file shard three";
        let id1 = ShardId::from_data(data1);
        let id2 = ShardId::from_data(data2);
        let id3 = ShardId::from_data(data3);

        store.put(id1, data1).await.unwrap();
        store.put(id2, data2).await.unwrap();
        store.put(id3, data3).await.unwrap();

        let mut listed = store.list().await.unwrap();
        listed.sort();
        let mut expected = vec![id1, id2, id3];
        expected.sort();
        assert_eq!(listed, expected);
    }

    #[tokio::test]
    async fn test_verify_valid_shard() {
        let (store, _dir) = make_store().await;
        let data = b"valid file data";
        let id = ShardId::from_data(data);

        store.put(id, data).await.unwrap();
        assert!(store.verify(id).await.unwrap());
    }

    #[tokio::test]
    async fn test_verify_corrupted_shard() {
        let (store, _dir) = make_store().await;
        let data = b"original file data";
        let id = ShardId::from_data(data);

        store.put(id, data).await.unwrap();

        // Corrupt the file on disk.
        let path = store.shard_path(&id);
        tokio::fs::write(&path, b"corrupted!").await.unwrap();

        assert!(!store.verify(id).await.unwrap());
    }

    #[tokio::test]
    async fn test_verify_nonexistent_returns_error() {
        let (store, _dir) = make_store().await;
        let id = ShardId::from_data(b"missing");
        assert!(store.verify(id).await.is_err());
    }

    #[tokio::test]
    async fn test_capacity_reports_filesystem_info() {
        let (store, _dir) = make_store().await;
        let cap = store.capacity().await.unwrap();
        // On a real filesystem, total_bytes should be > 0.
        assert!(cap.total_bytes > 0);
        assert!(cap.available_bytes > 0);
    }

    #[tokio::test]
    async fn test_fanout_directory_structure() {
        let (store, dir) = make_store().await;
        let data = b"fanout test data";
        let id = ShardId::from_data(data);

        store.put(id, data).await.unwrap();

        let hex = id.to_string();
        let expected_path = dir.path().join(&hex[0..2]).join(&hex[2..4]).join(&hex);

        assert!(
            expected_path.exists(),
            "shard file should exist at fan-out path: {}",
            expected_path.display()
        );

        // Verify the file content matches.
        let stored = std::fs::read(&expected_path).unwrap();
        assert_eq!(stored, data);
    }

    #[tokio::test]
    async fn test_delete_nonexistent_is_ok() {
        let (store, _dir) = make_store().await;
        let id = ShardId::from_data(b"never stored");
        // Should not error.
        store.delete(id).await.unwrap();
    }
}
