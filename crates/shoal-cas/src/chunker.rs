//! Fixed-size chunker for splitting data into content-addressed chunks.

use shoal_types::ChunkId;
use tokio::io::AsyncRead;

use crate::error::CasError;

/// A single chunk of data with its content-addressed ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    /// Content-addressed identifier: `blake3(data)`.
    pub id: ChunkId,
    /// Byte offset within the original object.
    pub offset: u64,
    /// The raw chunk data.
    pub data: Vec<u8>,
}

/// Fixed-size chunker that splits data into chunks of a configured size.
///
/// The last chunk may be smaller than `chunk_size`.
/// Empty data produces zero chunks.
pub struct Chunker {
    chunk_size: u32,
}

impl Chunker {
    /// Create a new chunker with the given chunk size in bytes.
    pub fn new(chunk_size: u32) -> Self {
        Self { chunk_size }
    }

    /// Split data into fixed-size chunks.
    ///
    /// Each chunk's ID is the BLAKE3 hash of its data.
    /// Returns an empty vec for empty input.
    pub fn chunk(&self, data: &[u8]) -> Vec<Chunk> {
        if data.is_empty() {
            return Vec::new();
        }

        let chunk_size = self.chunk_size as usize;
        let mut chunks = Vec::new();
        let mut offset = 0u64;

        for slice in data.chunks(chunk_size) {
            let id = ChunkId::from_data(slice);
            chunks.push(Chunk {
                id,
                offset,
                data: slice.to_vec(),
            });
            offset += slice.len() as u64;
        }

        chunks
    }

    /// Split data from an async reader into fixed-size chunks.
    ///
    /// Reads the entire stream, producing chunks as it goes.
    pub async fn chunk_stream(
        &self,
        mut reader: impl AsyncRead + Unpin,
    ) -> Result<Vec<Chunk>, CasError> {
        use tokio::io::AsyncReadExt;

        let chunk_size = self.chunk_size as usize;
        let mut chunks = Vec::new();
        let mut offset = 0u64;

        loop {
            let mut buf = vec![0u8; chunk_size];
            let mut filled = 0;

            // Read exactly chunk_size bytes, or until EOF.
            while filled < chunk_size {
                let n = reader.read(&mut buf[filled..]).await?;
                if n == 0 {
                    break;
                }
                filled += n;
            }

            if filled == 0 {
                break;
            }

            buf.truncate(filled);
            let id = ChunkId::from_data(&buf);
            chunks.push(Chunk {
                id,
                offset,
                data: buf,
            });
            offset += filled as u64;
        }

        Ok(chunks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_empty_data() {
        let chunker = Chunker::new(1024);
        let chunks = chunker.chunk(b"");
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_chunk_exactly_chunk_size() {
        let chunker = Chunker::new(16);
        let data = vec![0xABu8; 16];
        let chunks = chunker.chunk(&data);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].data, data);
        assert_eq!(chunks[0].offset, 0);
    }

    #[test]
    fn test_chunk_size_plus_one() {
        let chunker = Chunker::new(16);
        let data = vec![0xCDu8; 17];
        let chunks = chunker.chunk(&data);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].data.len(), 16);
        assert_eq!(chunks[0].offset, 0);
        assert_eq!(chunks[1].data.len(), 1);
        assert_eq!(chunks[1].offset, 16);
    }

    #[test]
    fn test_chunk_three_and_half() {
        let chunk_size = 100u32;
        let chunker = Chunker::new(chunk_size);
        // 3.5 * 100 = 350 bytes
        let data = vec![0xFFu8; 350];
        let chunks = chunker.chunk(&data);
        assert_eq!(chunks.len(), 4);
        assert_eq!(chunks[0].data.len(), 100);
        assert_eq!(chunks[1].data.len(), 100);
        assert_eq!(chunks[2].data.len(), 100);
        assert_eq!(chunks[3].data.len(), 50);
        assert_eq!(chunks[0].offset, 0);
        assert_eq!(chunks[1].offset, 100);
        assert_eq!(chunks[2].offset, 200);
        assert_eq!(chunks[3].offset, 300);
    }

    #[test]
    fn test_chunk_id_deterministic() {
        let chunker = Chunker::new(1024);
        let data = b"deterministic chunk content";
        let chunks1 = chunker.chunk(data);
        let chunks2 = chunker.chunk(data);
        assert_eq!(chunks1.len(), chunks2.len());
        for (c1, c2) in chunks1.iter().zip(chunks2.iter()) {
            assert_eq!(c1.id, c2.id);
        }
    }

    #[test]
    fn test_deduplication_identical_chunks() {
        let chunker = Chunker::new(4);
        // "AAAAAAAA" → two chunks of "AAAA", which are identical
        let data = vec![b'A'; 8];
        let chunks = chunker.chunk(&data);
        assert_eq!(chunks.len(), 2);
        assert_eq!(
            chunks[0].id, chunks[1].id,
            "identical chunks must have same ChunkId"
        );
    }

    #[tokio::test]
    async fn test_chunk_stream_matches_sync() {
        let chunker = Chunker::new(10);
        let data = b"hello world, this is streaming chunker test data!";

        let sync_chunks = chunker.chunk(data);
        let stream_chunks = chunker
            .chunk_stream(std::io::Cursor::new(data))
            .await
            .unwrap();

        assert_eq!(sync_chunks.len(), stream_chunks.len());
        for (s, a) in sync_chunks.iter().zip(stream_chunks.iter()) {
            assert_eq!(s.id, a.id);
            assert_eq!(s.offset, a.offset);
            assert_eq!(s.data, a.data);
        }
    }

    #[tokio::test]
    async fn test_chunk_stream_empty() {
        let chunker = Chunker::new(1024);
        let chunks = chunker
            .chunk_stream(std::io::Cursor::new(b""))
            .await
            .unwrap();
        assert!(chunks.is_empty());
    }
}
