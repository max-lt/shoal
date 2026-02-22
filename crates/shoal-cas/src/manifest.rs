//! Manifest building and serialization.
//!
//! A [`Manifest`] describes how an object was chunked and erasure-coded.
//! The manifest itself is stored as a regular erasure-coded object:
//! serialized with postcard, then chunked and distributed like user data.
//! Its [`ObjectId`] is `blake3(postcard_bytes)`.

use std::collections::BTreeMap;

use shoal_types::{ChunkMeta, MANIFEST_VERSION, Manifest, ObjectId};

use crate::error::CasError;

/// Intermediate manifest content used to derive the ObjectId.
///
/// This contains everything except the `object_id` and `version` fields,
/// so we can serialize it deterministically, hash it, and then set `object_id`.
/// The version is excluded so that bumping the version doesn't change existing
/// ObjectIds (the content hasn't changed, only the envelope format).
#[derive(serde::Serialize, serde::Deserialize)]
struct ManifestContent {
    total_size: u64,
    chunk_size: u32,
    chunks: Vec<ChunkMeta>,
    created_at: u64,
    metadata: BTreeMap<String, String>,
}

/// Build a [`Manifest`] from chunk metadata.
///
/// The `ObjectId` is computed as `blake3(postcard(content))` where `content`
/// contains all fields except `object_id` itself. This ensures the ID is
/// deterministic and content-addressed.
///
/// The resulting manifest is meant to be serialized with postcard and then
/// stored as a regular erasure-coded object in the cluster.
pub fn build_manifest(
    chunks: &[ChunkMeta],
    total_size: u64,
    chunk_size: u32,
    metadata: BTreeMap<String, String>,
) -> Result<Manifest, CasError> {
    build_manifest_with_timestamp(chunks, total_size, chunk_size, metadata, now_secs())
}

/// Build a manifest with an explicit timestamp (for deterministic testing).
pub fn build_manifest_with_timestamp(
    chunks: &[ChunkMeta],
    total_size: u64,
    chunk_size: u32,
    metadata: BTreeMap<String, String>,
    created_at: u64,
) -> Result<Manifest, CasError> {
    let content = ManifestContent {
        total_size,
        chunk_size,
        chunks: chunks.to_vec(),
        created_at,
        metadata: metadata.clone(),
    };

    let serialized =
        postcard::to_allocvec(&content).map_err(|e| CasError::Serialization(e.to_string()))?;

    let object_id = ObjectId::from_data(&serialized);

    Ok(Manifest {
        version: MANIFEST_VERSION,
        object_id,
        total_size,
        chunk_size,
        chunks: chunks.to_vec(),
        created_at,
        metadata,
    })
}

/// Serialize a manifest to postcard bytes.
pub fn serialize_manifest(manifest: &Manifest) -> Result<Vec<u8>, CasError> {
    postcard::to_allocvec(manifest).map_err(|e| CasError::Serialization(e.to_string()))
}

/// Deserialize a manifest from postcard bytes.
///
/// Rejects manifests with unknown version numbers to prevent silent
/// misinterpretation of data across format changes.
pub fn deserialize_manifest(bytes: &[u8]) -> Result<Manifest, CasError> {
    let manifest: Manifest =
        postcard::from_bytes(bytes).map_err(|e| CasError::Serialization(e.to_string()))?;
    if manifest.version != MANIFEST_VERSION {
        return Err(CasError::UnsupportedVersion {
            found: manifest.version,
            supported: MANIFEST_VERSION,
        });
    }
    Ok(manifest)
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use shoal_types::{ChunkId, ShardId, ShardMeta};

    fn sample_chunks() -> Vec<ChunkMeta> {
        vec![
            ChunkMeta {
                chunk_id: ChunkId::from_data(b"chunk-0"),
                offset: 0,
                size: 1024,
                shards: vec![
                    ShardMeta {
                        shard_id: ShardId::from_data(b"shard-0-0"),
                        index: 0,
                        size: 512,
                    },
                    ShardMeta {
                        shard_id: ShardId::from_data(b"shard-0-1"),
                        index: 1,
                        size: 512,
                    },
                ],
            },
            ChunkMeta {
                chunk_id: ChunkId::from_data(b"chunk-1"),
                offset: 1024,
                size: 500,
                shards: vec![ShardMeta {
                    shard_id: ShardId::from_data(b"shard-1-0"),
                    index: 0,
                    size: 500,
                }],
            },
        ]
    }

    #[test]
    fn test_manifest_roundtrip() {
        let chunks = sample_chunks();
        let metadata = BTreeMap::from([("content-type".to_string(), "text/plain".to_string())]);

        let manifest =
            build_manifest_with_timestamp(&chunks, 1524, 1024, metadata.clone(), 1700000000)
                .unwrap();

        let bytes = serialize_manifest(&manifest).unwrap();
        let decoded = deserialize_manifest(&bytes).unwrap();

        assert_eq!(manifest, decoded);
    }

    #[test]
    fn test_object_id_deterministic() {
        let chunks = sample_chunks();
        let metadata = BTreeMap::new();

        let m1 = build_manifest_with_timestamp(&chunks, 1524, 1024, metadata.clone(), 1700000000)
            .unwrap();
        let m2 = build_manifest_with_timestamp(&chunks, 1524, 1024, metadata, 1700000000).unwrap();

        assert_eq!(
            m1.object_id, m2.object_id,
            "same content must produce same ObjectId"
        );
    }

    #[test]
    fn test_object_id_changes_with_content() {
        let chunks = sample_chunks();
        let m1 = build_manifest_with_timestamp(&chunks, 1524, 1024, BTreeMap::new(), 1700000000)
            .unwrap();

        // Different total_size → different ObjectId.
        let m2 = build_manifest_with_timestamp(&chunks, 9999, 1024, BTreeMap::new(), 1700000000)
            .unwrap();

        assert_ne!(m1.object_id, m2.object_id);
    }

    #[test]
    fn test_empty_chunks_manifest() {
        let manifest =
            build_manifest_with_timestamp(&[], 0, 1024, BTreeMap::new(), 1700000000).unwrap();
        assert_eq!(manifest.total_size, 0);
        assert!(manifest.chunks.is_empty());

        // Still round-trips.
        let bytes = serialize_manifest(&manifest).unwrap();
        let decoded = deserialize_manifest(&bytes).unwrap();
        assert_eq!(manifest, decoded);
    }

    #[test]
    fn test_manifest_version_is_set() {
        let manifest =
            build_manifest_with_timestamp(&sample_chunks(), 1524, 1024, BTreeMap::new(), 0)
                .unwrap();
        assert_eq!(manifest.version, MANIFEST_VERSION);
    }

    #[test]
    fn test_deserialize_rejects_unknown_version() {
        let mut manifest =
            build_manifest_with_timestamp(&sample_chunks(), 1524, 1024, BTreeMap::new(), 0)
                .unwrap();
        manifest.version = 99;
        let bytes = serialize_manifest(&manifest).unwrap();
        let err = deserialize_manifest(&bytes).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unsupported manifest version 99"),
            "error should mention version: {msg}"
        );
    }

    #[test]
    fn test_manifest_with_metadata() {
        let metadata = BTreeMap::from([
            ("content-type".to_string(), "image/png".to_string()),
            ("x-custom".to_string(), "value".to_string()),
        ]);
        let manifest =
            build_manifest_with_timestamp(&sample_chunks(), 1524, 1024, metadata.clone(), 0)
                .unwrap();
        assert_eq!(manifest.metadata, metadata);
    }
}
