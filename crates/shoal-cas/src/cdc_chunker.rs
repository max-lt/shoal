//! Content-Defined Chunking (CDC) using FastCDC.
//!
//! Replaces fixed-size chunking with variable-size chunks whose boundaries
//! are determined by content fingerprints. This enables inter-version
//! deduplication: when a file is partially modified, only the changed
//! chunks produce new shards — unchanged regions keep the same `ChunkId`.
//!
//! **Parameters are fixed at deployment and must never change**, otherwise
//! deduplication breaks (same data would produce different chunk boundaries).

use bytes::Bytes;
use shoal_types::ChunkId;

use crate::chunker::Chunk;

/// Default CDC minimum chunk size (16 KB).
pub const CDC_MIN_SIZE: u32 = 16_384;

/// Default CDC average chunk size (64 KB).
pub const CDC_AVG_SIZE: u32 = 65_536;

/// Default CDC maximum chunk size (256 KB).
pub const CDC_MAX_SIZE: u32 = 262_144;

/// Content-defined chunker using the FastCDC algorithm.
///
/// Chunk boundaries are determined by a rolling hash over the data content,
/// producing chunks between `min_size` and `max_size` bytes
/// with an average of `avg_size`.
///
/// Parameters are derived from `chunk_size` using a fixed 1:4:16 ratio:
/// - `min_size` = `chunk_size / 16`
/// - `avg_size` = `chunk_size / 4`
/// - `max_size` = `chunk_size`
///
/// The minimum valid `chunk_size` is 1024 (FastCDC constraint).
pub struct CdcChunker {
    min_size: u32,
    avg_size: u32,
    max_size: u32,
}

impl CdcChunker {
    /// Create a new CDC chunker with the default parameters (256 KB max).
    pub fn new() -> Self {
        Self::from_chunk_size(CDC_MAX_SIZE)
    }

    /// Create a CDC chunker derived from the configured chunk size.
    ///
    /// The chunk size maps to `max_size`; `avg_size` and `min_size` are
    /// derived as `chunk_size / 4` and `chunk_size / 16` respectively.
    ///
    /// # Panics
    ///
    /// Panics if `chunk_size < 1024` (FastCDC minimum).
    pub fn from_chunk_size(chunk_size: u32) -> Self {
        assert!(
            chunk_size >= 1024,
            "chunk_size must be >= 1024 (got {chunk_size})"
        );

        Self {
            min_size: chunk_size / 16,
            avg_size: chunk_size / 4,
            max_size: chunk_size,
        }
    }

    /// Create a CDC chunker with custom parameters (for testing).
    pub fn with_sizes(min_size: u32, avg_size: u32, max_size: u32) -> Self {
        Self {
            min_size,
            avg_size,
            max_size,
        }
    }

    /// Return the average chunk size (used as `chunk_size` in manifests).
    pub fn avg_size(&self) -> u32 {
        self.avg_size
    }

    /// Split data into content-defined chunks.
    ///
    /// Each chunk's ID is the BLAKE3 hash of its raw data.
    /// Returns an empty vec for empty input.
    pub fn chunk(&self, data: &[u8]) -> Vec<Chunk> {
        if data.is_empty() {
            return Vec::new();
        }

        let chunker =
            fastcdc::v2020::FastCDC::new(data, self.min_size, self.avg_size, self.max_size);
        let mut chunks = Vec::new();

        for entry in chunker {
            let chunk_data = &data[entry.offset..entry.offset + entry.length];
            let id = ChunkId::from_data(chunk_data);
            chunks.push(Chunk {
                id,
                offset: entry.offset as u64,
                data: Bytes::copy_from_slice(chunk_data),
            });
        }

        chunks
    }

    /// Split data from an async reader into content-defined chunks.
    pub async fn chunk_stream(
        &self,
        reader: impl tokio::io::AsyncRead + Unpin,
    ) -> Result<Vec<Chunk>, crate::error::CasError> {
        use tokio::io::AsyncReadExt;

        // Read all data first, then CDC it.
        // FastCDC needs the full buffer for deterministic boundary detection.
        let mut buf = Vec::new();
        let mut reader = reader;
        reader.read_to_end(&mut buf).await?;

        Ok(self.chunk(&buf))
    }
}

impl Default for CdcChunker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cdc_empty_data() {
        let chunker = CdcChunker::new();
        let chunks = chunker.chunk(b"");
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_cdc_small_file_single_chunk() {
        // A file smaller than min_size should produce exactly one chunk.
        let chunker = CdcChunker::new();
        let data = vec![0xABu8; 1000]; // 1KB < 16KB min
        let chunks = chunker.chunk(&data);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].data.as_ref(), data.as_slice());
        assert_eq!(chunks[0].offset, 0);
    }

    #[test]
    fn test_cdc_chunk_sizes_within_bounds() {
        let chunker = CdcChunker::new();
        // 1MB of pseudo-random data
        let data: Vec<u8> = (0..1_048_576u32)
            .map(|i| (i.wrapping_mul(2654435761) >> 24) as u8)
            .collect();
        let chunks = chunker.chunk(&data);

        assert!(chunks.len() > 1, "1MB should produce multiple chunks");

        for (i, chunk) in chunks.iter().enumerate() {
            let len = chunk.data.len() as u32;
            if i < chunks.len() - 1 {
                // Non-last chunks must be >= min_size
                assert!(
                    len >= CDC_MIN_SIZE,
                    "chunk {i} size {len} < min {CDC_MIN_SIZE}"
                );
            }
            // All chunks must be <= max_size
            assert!(
                len <= CDC_MAX_SIZE,
                "chunk {i} size {len} > max {CDC_MAX_SIZE}"
            );
        }
    }

    #[test]
    fn test_cdc_deterministic() {
        let chunker = CdcChunker::new();
        let data: Vec<u8> = (0..200_000u32).map(|i| (i % 256) as u8).collect();

        let chunks1 = chunker.chunk(&data);
        let chunks2 = chunker.chunk(&data);

        assert_eq!(chunks1.len(), chunks2.len());
        for (c1, c2) in chunks1.iter().zip(chunks2.iter()) {
            assert_eq!(c1.id, c2.id);
            assert_eq!(c1.offset, c2.offset);
            assert_eq!(c1.data, c2.data);
        }
    }

    #[test]
    fn test_cdc_dedup_partial_modification() {
        let chunker = CdcChunker::new();

        // Create two versions of a large file: v2 has 5% modified at the end.
        let size = 1_048_576; // 1MB
        let v1: Vec<u8> = (0..size)
            .map(|i| ((i as u32).wrapping_mul(2654435761) >> 24) as u8)
            .collect();
        let mut v2 = v1.clone();

        // Modify the last 5%.
        let modify_start = size - size / 20;
        for b in &mut v2[modify_start..] {
            *b = b.wrapping_add(1);
        }

        let chunks_v1 = chunker.chunk(&v1);
        let chunks_v2 = chunker.chunk(&v2);

        // Count shared chunk IDs.
        let ids_v1: std::collections::HashSet<_> = chunks_v1.iter().map(|c| c.id).collect();
        let ids_v2: std::collections::HashSet<_> = chunks_v2.iter().map(|c| c.id).collect();
        let shared = ids_v1.intersection(&ids_v2).count();

        let max_chunks = chunks_v1.len().max(chunks_v2.len());
        let shared_ratio = shared as f64 / max_chunks as f64;

        assert!(
            shared_ratio > 0.80,
            "expected >80% chunk reuse, got {:.1}% ({shared}/{max_chunks})",
            shared_ratio * 100.0
        );
    }

    #[test]
    fn test_cdc_offsets_contiguous() {
        let chunker = CdcChunker::new();
        let data: Vec<u8> = (0..500_000u32).map(|i| (i % 256) as u8).collect();
        let chunks = chunker.chunk(&data);

        // Verify chunks are contiguous and cover all data.
        let mut expected_offset = 0u64;
        for chunk in &chunks {
            assert_eq!(chunk.offset, expected_offset);
            expected_offset += chunk.data.len() as u64;
        }
        assert_eq!(expected_offset, data.len() as u64);
    }

    #[tokio::test]
    async fn test_cdc_stream_matches_sync() {
        let chunker = CdcChunker::new();
        let data: Vec<u8> = (0..200_000u32).map(|i| (i % 256) as u8).collect();

        let sync_chunks = chunker.chunk(&data);
        let stream_chunks = chunker
            .chunk_stream(std::io::Cursor::new(&data))
            .await
            .unwrap();

        assert_eq!(sync_chunks.len(), stream_chunks.len());
        for (s, a) in sync_chunks.iter().zip(stream_chunks.iter()) {
            assert_eq!(s.id, a.id);
            assert_eq!(s.offset, a.offset);
            assert_eq!(s.data, a.data);
        }
    }
}
