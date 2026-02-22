//! Reed-Solomon erasure decoder.
//!
//! Reconstructs the original chunk data from any `k` (or more) of the
//! `k + m` shards produced by [`ErasureEncoder`](crate::ErasureEncoder).

use tracing::debug;

use crate::error::ErasureError;

/// Decode original chunk data from a subset of shards.
///
/// # Arguments
///
/// * `k` — number of data shards used during encoding
/// * `m` — number of parity shards used during encoding
/// * `shards` — at least `k` shards as `(index, data)` pairs, where index
///   is 0..k for data shards and k..k+m for parity shards
/// * `original_size` — the original unpadded chunk size in bytes
///
/// # Errors
///
/// Returns [`ErasureError::NotEnoughShards`] if fewer than `k` shards are
/// provided, or [`ErasureError::ReedSolomon`] if the RS library fails.
pub fn decode(
    k: usize,
    m: usize,
    shards: &[(u8, Vec<u8>)],
    original_size: usize,
) -> Result<Vec<u8>, ErasureError> {
    if shards.len() < k {
        return Err(ErasureError::NotEnoughShards {
            needed: k,
            got: shards.len(),
        });
    }

    // Separate shards into originals (index < k) and recovery (index >= k).
    let mut originals: Vec<(usize, &[u8])> = Vec::new();
    let mut recovery: Vec<(usize, &[u8])> = Vec::new();

    for (index, data) in shards {
        let idx = *index as usize;
        if idx < k {
            originals.push((idx, data.as_slice()));
        } else {
            recovery.push((idx - k, data.as_slice()));
        }
    }

    debug!(
        k,
        m,
        originals = originals.len(),
        recovery = recovery.len(),
        original_size,
        "decoding chunk from shards"
    );

    // If we have all k originals, just concatenate — no RS decode needed.
    if originals.len() == k {
        let shard_size = originals[0].1.len();
        let mut result = vec![0u8; k * shard_size];
        for (idx, data) in &originals {
            let start = idx * shard_size;
            result[start..start + shard_size].copy_from_slice(data);
        }
        result.truncate(original_size);
        return Ok(result);
    }

    // Need RS decode to recover missing originals.
    let restored = reed_solomon_simd::decode(k, m, originals, recovery)?;

    // Reconstruct full padded data by placing all k original shards.
    let shard_size = shards[0].1.len();
    let mut result = vec![0u8; k * shard_size];

    // Place shards we already had.
    for (index, data) in shards {
        let idx = *index as usize;
        if idx < k {
            let start = idx * shard_size;
            result[start..start + shard_size].copy_from_slice(data);
        }
    }

    // Place restored shards.
    for (idx, data) in &restored {
        let start = idx * shard_size;
        result[start..start + shard_size].copy_from_slice(data);
    }

    result.truncate(original_size);
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErasureEncoder;

    fn encode_helper(k: usize, m: usize, data: &[u8]) -> (Vec<(u8, Vec<u8>)>, usize) {
        let encoder = ErasureEncoder::new(k, m);
        let (shards, original_size) = encoder.encode(data).unwrap();
        let indexed: Vec<(u8, Vec<u8>)> = shards
            .into_iter()
            .map(|s| (s.index, s.data.to_vec()))
            .collect();
        (indexed, original_size)
    }

    #[test]
    fn test_decode_all_shards() {
        let data = b"hello erasure coding world!!";
        let (shards, original_size) = encode_helper(2, 1, data);
        let result = decode(2, 1, &shards, original_size).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_decode_only_data_shards() {
        let data = vec![0xABu8; 200];
        let (shards, original_size) = encode_helper(3, 2, &data);
        // Keep only data shards (index 0, 1, 2).
        let data_only: Vec<_> = shards
            .into_iter()
            .filter(|(i, _)| (*i as usize) < 3)
            .collect();
        assert_eq!(data_only.len(), 3);
        let result = decode(3, 2, &data_only, original_size).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_decode_drop_parity_shards() {
        let data = vec![0xCDu8; 100];
        let (shards, original_size) = encode_helper(2, 2, &data);
        // Drop all parity shards, keep only data.
        let data_only: Vec<_> = shards
            .into_iter()
            .filter(|(i, _)| (*i as usize) < 2)
            .collect();
        let result = decode(2, 2, &data_only, original_size).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_decode_with_mixed_shards() {
        let data = vec![0xEFu8; 150];
        let (shards, original_size) = encode_helper(3, 2, &data);
        // Keep shard 0 (data), drop shard 1 (data), keep shard 2 (data),
        // keep shard 3 (parity), drop shard 4 (parity).
        let mixed: Vec<_> = shards
            .into_iter()
            .filter(|(i, _)| *i != 1 && *i != 4)
            .collect();
        assert_eq!(mixed.len(), 3); // exactly k=3
        let result = decode(3, 2, &mixed, original_size).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_decode_drop_different_combinations() {
        let data = vec![0x42u8; 256];
        let (shards, original_size) = encode_helper(2, 2, &data);
        // k=2, m=2 → total 4 shards. Any 2 should work.
        let combos: Vec<Vec<usize>> = vec![
            vec![0, 1],
            vec![0, 2],
            vec![0, 3],
            vec![1, 2],
            vec![1, 3],
            vec![2, 3],
        ];
        for keep in &combos {
            let subset: Vec<_> = shards
                .iter()
                .filter(|(i, _)| keep.contains(&(*i as usize)))
                .cloned()
                .collect();
            let result = decode(2, 2, &subset, original_size).unwrap();
            assert_eq!(result, data, "failed with shards {:?}", keep);
        }
    }

    #[test]
    fn test_decode_fewer_than_k_shards_errors() {
        let data = vec![0xAAu8; 100];
        let (shards, original_size) = encode_helper(3, 2, &data);
        // Keep only 2 shards (need 3).
        let too_few: Vec<_> = shards.into_iter().take(2).collect();
        let result = decode(3, 2, &too_few, original_size);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_non_divisible_padding() {
        // 7 bytes, k=3 → needs padding.
        let data = b"1234567";
        let (shards, original_size) = encode_helper(3, 1, data);
        assert_eq!(original_size, 7);

        // Drop one shard, should still decode.
        let subset: Vec<_> = shards.into_iter().skip(1).take(3).collect();
        let result = decode(3, 1, &subset, original_size).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_decode_large_chunk() {
        let data = vec![0xBBu8; 1024 * 1024]; // 1MB
        let (shards, original_size) = encode_helper(4, 2, &data);
        // Drop 2 shards.
        let subset: Vec<_> = shards.into_iter().skip(2).collect();
        assert_eq!(subset.len(), 4); // exactly k=4
        let result = decode(4, 2, &subset, original_size).unwrap();
        assert_eq!(result, data);
    }
}
