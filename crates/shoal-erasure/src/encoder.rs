//! Reed-Solomon erasure encoder.
//!
//! Splits a chunk into `k` data shards, generates `m` parity shards,
//! and returns all `k + m` shards with their content-addressed IDs.

use bytes::Bytes;
use shoal_types::ShardId;
use tracing::debug;

use crate::error::ErasureError;

/// An erasure-coded shard with its content-addressed ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shard {
    /// Content-addressed identifier: `blake3(data)`.
    pub id: ShardId,
    /// Position in the RS coding scheme (0..k for data, k..k+m for parity).
    pub index: u8,
    /// The raw shard data.
    pub data: Bytes,
}

/// Reed-Solomon erasure encoder.
///
/// Splits chunk data into `k` data shards and generates `m` parity shards.
/// All shards are the same size. If the chunk isn't evenly divisible by `k`,
/// it is padded with zeros. Shard sizes are also padded to be even (required
/// by `reed-solomon-simd`).
pub struct ErasureEncoder {
    /// Number of data shards.
    k: usize,
    /// Number of parity shards.
    m: usize,
}

impl ErasureEncoder {
    /// Create a new encoder with the given data/parity shard counts.
    pub fn new(k: usize, m: usize) -> Self {
        Self { k, m }
    }

    /// Encode a chunk into `k + m` shards.
    ///
    /// Returns all shards (data shards first at indices 0..k, then parity
    /// shards at indices k..k+m). The `original_size` of the unpadded chunk
    /// is needed later for decoding.
    pub fn encode(&self, chunk: &[u8]) -> Result<(Vec<Shard>, usize), ErasureError> {
        if chunk.is_empty() {
            return Err(ErasureError::EmptyChunk);
        }

        let original_size = chunk.len();

        // Compute shard size: ceil(chunk.len() / k), then round up to even.
        let shard_size = round_up_even(chunk.len().div_ceil(self.k));

        // Pad the chunk to exactly k * shard_size.
        let padded_len = self.k * shard_size;
        let mut padded = Vec::with_capacity(padded_len);
        padded.extend_from_slice(chunk);
        padded.resize(padded_len, 0);

        // Split into k original shards.
        let originals: Vec<&[u8]> = padded.chunks_exact(shard_size).collect();
        debug_assert_eq!(originals.len(), self.k);

        // Fast path: m=0 means no parity shards needed (passthrough).
        // reed-solomon-simd doesn't support recovery_count=0.
        if self.m == 0 {
            let mut shards = Vec::with_capacity(self.k);
            for (i, original) in originals.iter().enumerate() {
                let data = Bytes::copy_from_slice(original);
                let id = ShardId::from_data(&data);
                shards.push(Shard {
                    id,
                    index: i as u8,
                    data,
                });
            }
            debug!(
                k = self.k,
                m = self.m,
                original_size,
                shard_size,
                "encoded chunk into shards (no parity)"
            );
            return Ok((shards, original_size));
        }

        // Generate m parity shards.
        let recovery = reed_solomon_simd::encode(self.k, self.m, &originals)?;

        // Build output: data shards (0..k) then parity shards (k..k+m).
        let mut shards = Vec::with_capacity(self.k + self.m);

        for (i, original) in originals.iter().enumerate() {
            let data = Bytes::copy_from_slice(original);
            let id = ShardId::from_data(&data);
            shards.push(Shard {
                id,
                index: i as u8,
                data,
            });
        }

        for (i, rec) in recovery.iter().enumerate() {
            let data = Bytes::copy_from_slice(rec);
            let id = ShardId::from_data(&data);
            shards.push(Shard {
                id,
                index: (self.k + i) as u8,
                data,
            });
        }

        debug!(
            k = self.k,
            m = self.m,
            original_size,
            shard_size,
            "encoded chunk into shards"
        );

        Ok((shards, original_size))
    }
}

/// Round up to the nearest even number.
fn round_up_even(n: usize) -> usize {
    if n.is_multiple_of(2) { n } else { n + 1 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_basic() {
        let encoder = ErasureEncoder::new(2, 1);
        let data = vec![0xABu8; 100];
        let (shards, original_size) = encoder.encode(&data).unwrap();
        assert_eq!(original_size, 100);
        assert_eq!(shards.len(), 3); // 2 data + 1 parity
        // All shards same size.
        let shard_size = shards[0].data.len();
        for s in &shards {
            assert_eq!(s.data.len(), shard_size);
        }
    }

    #[test]
    fn test_encode_indices() {
        let encoder = ErasureEncoder::new(3, 2);
        let data = vec![0xCDu8; 300];
        let (shards, _) = encoder.encode(&data).unwrap();
        assert_eq!(shards.len(), 5);
        for (i, s) in shards.iter().enumerate() {
            assert_eq!(s.index, i as u8);
        }
    }

    #[test]
    fn test_encode_shard_ids_deterministic() {
        let encoder = ErasureEncoder::new(2, 1);
        let data = vec![0xFFu8; 64];
        let (shards1, _) = encoder.encode(&data).unwrap();
        let (shards2, _) = encoder.encode(&data).unwrap();
        for (s1, s2) in shards1.iter().zip(shards2.iter()) {
            assert_eq!(s1.id, s2.id);
        }
    }

    #[test]
    fn test_encode_empty_chunk_errors() {
        let encoder = ErasureEncoder::new(2, 1);
        assert!(encoder.encode(b"").is_err());
    }

    #[test]
    fn test_encode_non_divisible_chunk() {
        // 7 bytes / k=3 → shard_size = ceil(7/3) = 3, rounded to 4 (even).
        let encoder = ErasureEncoder::new(3, 1);
        let data = vec![0x42u8; 7];
        let (shards, original_size) = encoder.encode(&data).unwrap();
        assert_eq!(original_size, 7);
        assert_eq!(shards.len(), 4); // 3 data + 1 parity
        // All shards must be even-sized.
        for s in &shards {
            assert_eq!(s.data.len() % 2, 0);
        }
    }

    #[test]
    fn test_round_up_even() {
        assert_eq!(round_up_even(1), 2);
        assert_eq!(round_up_even(2), 2);
        assert_eq!(round_up_even(3), 4);
        assert_eq!(round_up_even(4), 4);
    }

    #[test]
    fn test_encode_single_byte() {
        let encoder = ErasureEncoder::new(2, 1);
        let data = vec![42u8];
        let (shards, original_size) = encoder.encode(&data).unwrap();
        assert_eq!(original_size, 1);
        assert_eq!(shards.len(), 3);
    }

    #[test]
    fn test_encode_k1_m0_passthrough() {
        // k=1, m=0: data goes into 1 shard, no parity.
        let encoder = ErasureEncoder::new(1, 0);
        let data = vec![0xAA; 100];
        let (shards, original_size) = encoder.encode(&data).unwrap();
        assert_eq!(original_size, 100);
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].index, 0);
        // The shard should contain the original data (possibly padded to even).
        assert!(shards[0].data.len() >= 100);
    }

    #[test]
    fn test_encode_k1_m1_mirror() {
        let encoder = ErasureEncoder::new(1, 1);
        let data = vec![0xBB; 50];
        let (shards, _) = encoder.encode(&data).unwrap();
        assert_eq!(shards.len(), 2);
        // Data shard and parity shard.
        assert_eq!(shards[0].index, 0);
        assert_eq!(shards[1].index, 1);
    }

    #[test]
    fn test_encode_k8_m4_production() {
        let encoder = ErasureEncoder::new(8, 4);
        let data = vec![0xCC; 8192];
        let (shards, original_size) = encoder.encode(&data).unwrap();
        assert_eq!(original_size, 8192);
        assert_eq!(shards.len(), 12);
        for (i, s) in shards.iter().enumerate() {
            assert_eq!(s.index, i as u8);
        }
    }

    #[test]
    fn test_encode_different_data_different_shard_ids() {
        let encoder = ErasureEncoder::new(2, 1);
        let (shards1, _) = encoder.encode(&[1u8; 64]).unwrap();
        let (shards2, _) = encoder.encode(&[2u8; 64]).unwrap();
        // At least some shard IDs should differ.
        let different = shards1
            .iter()
            .zip(shards2.iter())
            .any(|(s1, s2)| s1.id != s2.id);
        assert!(
            different,
            "different data should produce different shard IDs"
        );
    }

    #[test]
    fn test_encode_shard_size_even() {
        // Verify all shard sizes are even (reed-solomon-simd requirement).
        for k in 1..=8 {
            for data_len in [1, 2, 3, 7, 15, 31, 64, 127, 255, 1000] {
                let encoder = ErasureEncoder::new(k, 1);
                let data = vec![0xAA; data_len];
                let (shards, _) = encoder.encode(&data).unwrap();
                for s in &shards {
                    assert_eq!(
                        s.data.len() % 2,
                        0,
                        "shard size must be even: k={k}, data_len={data_len}, shard_size={}",
                        s.data.len()
                    );
                }
            }
        }
    }

    #[test]
    fn test_encode_all_shards_same_size() {
        let encoder = ErasureEncoder::new(4, 2);
        for data_len in [1, 7, 100, 1000, 8191, 8192, 8193] {
            let data = vec![0xDD; data_len];
            let (shards, _) = encoder.encode(&data).unwrap();
            let expected_size = shards[0].data.len();
            for s in &shards {
                assert_eq!(
                    s.data.len(),
                    expected_size,
                    "all shards must be same size for data_len={data_len}"
                );
            }
        }
    }
}
