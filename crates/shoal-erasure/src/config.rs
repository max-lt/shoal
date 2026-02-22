//! Adaptive erasure coding configuration.
//!
//! Suggests Reed-Solomon parameters (k, m) based on cluster size,
//! balancing storage overhead against fault tolerance.

/// Suggest erasure coding parameters based on the number of nodes in the cluster.
///
/// Returns `(k, m)` where `k` is the number of data shards and `m` is the
/// number of parity shards. The total shard count `k + m` never exceeds the
/// node count, so every shard can be placed on a distinct node.
///
/// | Nodes | k | m | Overhead | Tolerance |
/// |-------|---|---|----------|-----------|
/// | 1     | 1 | 0 | 1.0x     | 0 failures |
/// | 2     | 1 | 1 | 2.0x     | 1 failure  |
/// | 3     | 2 | 1 | 1.5x     | 1 failure  |
/// | 4     | 2 | 2 | 2.0x     | 2 failures |
/// | 5     | 3 | 2 | 1.67x   | 2 failures |
/// | 6-11  | 4 | 2 | 1.5x    | 2 failures |
/// | 12+   | 8 | 4 | 1.5x    | 4 failures |
pub fn suggest_config(node_count: usize) -> (usize, usize) {
    match node_count {
        0 | 1 => (1, 0),
        2 => (1, 1),
        3 => (2, 1),
        4 => (2, 2),
        5 => (3, 2),
        6..=11 => (4, 2),
        _ => (8, 4),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_suggest_config_single_node() {
        let (k, m) = suggest_config(1);
        assert_eq!((k, m), (1, 0));
    }

    #[test]
    fn test_suggest_config_two_nodes() {
        let (k, m) = suggest_config(2);
        assert_eq!(k + m, 2);
        assert!(m >= 1);
    }

    #[test]
    fn test_suggest_config_three_nodes() {
        let (k, m) = suggest_config(3);
        assert_eq!((k, m), (2, 1));
        assert_eq!(k + m, 3);
    }

    #[test]
    fn test_suggest_config_five_nodes() {
        let (k, m) = suggest_config(5);
        assert_eq!((k, m), (3, 2));
        assert_eq!(k + m, 5);
    }

    #[test]
    fn test_suggest_config_large_cluster() {
        let (k, m) = suggest_config(20);
        assert_eq!((k, m), (8, 4));
    }

    #[test]
    fn test_suggest_config_never_exceeds_nodes() {
        for n in 0..=50 {
            let (k, m) = suggest_config(n);
            assert!(k + m <= n.max(1), "k+m={} exceeds node_count={n}", k + m);
        }
    }

    #[test]
    fn test_suggest_config_zero_nodes() {
        let (k, m) = suggest_config(0);
        assert_eq!((k, m), (1, 0));
    }
}
