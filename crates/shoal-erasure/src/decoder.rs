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

    // -----------------------------------------------------------------------
    // k=1, m=0: passthrough decode
    // -----------------------------------------------------------------------

    #[test]
    fn test_decode_k1_m0() {
        let data = vec![0xAA; 100];
        let (shards, original_size) = encode_helper(1, 0, &data);
        assert_eq!(shards.len(), 1);
        let result = decode(1, 0, &shards, original_size).unwrap();
        assert_eq!(result, data);
    }

    // -----------------------------------------------------------------------
    // k=1, m=1: mirror decode from either shard
    // -----------------------------------------------------------------------

    #[test]
    fn test_decode_k1_m1_data_shard_only() {
        let data = vec![0xBB; 50];
        let (shards, original_size) = encode_helper(1, 1, &data);
        // Keep only data shard (index 0).
        let data_only: Vec<_> = shards.into_iter().filter(|(i, _)| *i == 0).collect();
        let result = decode(1, 1, &data_only, original_size).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_decode_k1_m1_parity_shard_only() {
        let data = vec![0xCC; 50];
        let (shards, original_size) = encode_helper(1, 1, &data);
        // Keep only parity shard (index 1).
        let parity_only: Vec<_> = shards.into_iter().filter(|(i, _)| *i == 1).collect();
        let result = decode(1, 1, &parity_only, original_size).unwrap();
        assert_eq!(result, data);
    }

    // -----------------------------------------------------------------------
    // k=3, m=3: all C(6,3) = 20 combinations
    // -----------------------------------------------------------------------

    #[test]
    fn test_decode_k3_m3_all_combinations() {
        let data = vec![0xDD; 300];
        let (shards, original_size) = encode_helper(3, 3, &data);
        assert_eq!(shards.len(), 6);

        // Generate all C(6,3) = 20 combinations.
        for a in 0..6 {
            for b in (a + 1)..6 {
                for c in (b + 1)..6 {
                    let subset: Vec<_> = shards
                        .iter()
                        .filter(|(i, _)| *i as usize == a || *i as usize == b || *i as usize == c)
                        .cloned()
                        .collect();
                    let result = decode(3, 3, &subset, original_size)
                        .unwrap_or_else(|e| panic!("failed for combo [{a},{b},{c}]: {e}"));
                    assert_eq!(result, data, "mismatch for shard combination [{a},{b},{c}]");
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Shard order shouldn't matter
    // -----------------------------------------------------------------------

    #[test]
    fn test_decode_shard_order_independent() {
        let data = vec![0xEE; 200];
        let (shards, original_size) = encode_helper(3, 2, &data);

        // Reverse order.
        let mut reversed = shards.clone();
        reversed.reverse();
        let result = decode(3, 2, &reversed, original_size).unwrap();
        assert_eq!(result, data);

        // Random-ish shuffle: take indices 2, 0, 4, 1, 3.
        let shuffled = vec![
            shards[2].clone(),
            shards[0].clone(),
            shards[4].clone(),
            shards[1].clone(),
            shards[3].clone(),
        ];
        let result = decode(3, 2, &shuffled, original_size).unwrap();
        assert_eq!(result, data);
    }

    // -----------------------------------------------------------------------
    // Single byte data
    // -----------------------------------------------------------------------

    #[test]
    fn test_decode_single_byte() {
        let data = vec![42u8];
        let (shards, original_size) = encode_helper(2, 1, &data);
        let result = decode(2, 1, &shards, original_size).unwrap();
        assert_eq!(result, data);
    }

    // -----------------------------------------------------------------------
    // k=8, m=4 production config
    // -----------------------------------------------------------------------

    #[test]
    fn test_decode_k8_m4_drop_4() {
        let data = vec![0xFF; 4096];
        let (shards, original_size) = encode_helper(8, 4, &data);
        assert_eq!(shards.len(), 12);

        // Drop 4 shards (the maximum tolerable).
        let subset: Vec<_> = shards.into_iter().take(8).collect();
        let result = decode(8, 4, &subset, original_size).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_decode_k8_m4_drop_5_fails() {
        let data = vec![0xFF; 4096];
        let (shards, original_size) = encode_helper(8, 4, &data);

        // Drop 5 shards → only 7 left < k=8.
        let subset: Vec<_> = shards.into_iter().take(7).collect();
        let result = decode(8, 4, &subset, original_size);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Exactly k shards present: should use fast-path (no RS decode)
    // -----------------------------------------------------------------------

    #[test]
    fn test_decode_exactly_k_data_shards_fast_path() {
        let data = vec![0x11; 500];
        let (shards, original_size) = encode_helper(4, 2, &data);

        // Keep exactly the k=4 data shards (indices 0-3).
        let data_shards: Vec<_> = shards
            .into_iter()
            .filter(|(i, _)| (*i as usize) < 4)
            .collect();
        assert_eq!(data_shards.len(), 4);

        let result = decode(4, 2, &data_shards, original_size).unwrap();
        assert_eq!(result, data);
    }

    // -----------------------------------------------------------------------
    // More than k shards provided (extra are ignored)
    // -----------------------------------------------------------------------

    #[test]
    fn test_decode_more_than_k_shards() {
        let data = vec![0x22; 200];
        let (shards, original_size) = encode_helper(2, 2, &data);

        // Provide all 4 shards (k=2, so 2 extra).
        let result = decode(2, 2, &shards, original_size).unwrap();
        assert_eq!(result, data);
    }

    // -----------------------------------------------------------------------
    // Zero shards → error
    // -----------------------------------------------------------------------

    #[test]
    fn test_decode_zero_shards_errors() {
        let result = decode(2, 1, &[], 100);
        assert!(result.is_err());
        match result.unwrap_err() {
            ErasureError::NotEnoughShards { needed, got } => {
                assert_eq!(needed, 2);
                assert_eq!(got, 0);
            }
            other => panic!("expected NotEnoughShards, got: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Varying data sizes with same k,m
    // -----------------------------------------------------------------------

    #[test]
    fn test_decode_various_sizes() {
        let k = 3;
        let m = 2;
        for size in [1, 2, 3, 5, 7, 13, 64, 100, 255, 1000, 4096] {
            let data = vec![size as u8; size];
            let (shards, original_size) = encode_helper(k, m, &data);
            let result = decode(k, m, &shards, original_size)
                .unwrap_or_else(|e| panic!("failed for size={size}: {e}"));
            assert_eq!(result, data, "mismatch for size={size}");
        }
    }
}
