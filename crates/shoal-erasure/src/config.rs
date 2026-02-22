//! Adaptive erasure coding configuration.
//!
//! Suggests Reed-Solomon parameters (k, m) based on cluster size,
//! balancing storage overhead against fault tolerance.

/// Suggested erasure coding configuration with optional safety warning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErasureSuggestion {
    /// Number of data shards.
    pub k: usize,
    /// Number of parity shards.
    pub m: usize,
    /// Safety warning, if any (e.g. m=1 provides minimal redundancy).
    pub warning: Option<String>,
}

/// Suggest erasure coding parameters based on the number of nodes in the cluster.
///
/// Returns an [`ErasureSuggestion`] containing `k` (data shards), `m` (parity
/// shards), and an optional warning when the configuration provides minimal
/// redundancy. The total shard count `k + m` never exceeds the node count,
/// so every shard can be placed on a distinct node.
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
pub fn suggest_config(node_count: usize) -> ErasureSuggestion {
    let (k, m) = match node_count {
        0 | 1 => (1, 0),
        2 => (1, 1),
        3 => (2, 1),
        4 => (2, 2),
        5 => (3, 2),
        6..=11 => (4, 2),
        _ => (8, 4),
    };

    let warning = if m == 0 {
        Some("m=0 provides no redundancy. Any single failure will cause data loss.".to_string())
    } else if m == 1 {
        Some(
            "m=1 provides minimal redundancy. During repair of a failed shard, \
             a second failure will cause data loss. Use m>=2 for production workloads."
                .to_string(),
        )
    } else {
        None
    };

    ErasureSuggestion { k, m, warning }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_suggest_config_single_node() {
        let s = suggest_config(1);
        assert_eq!((s.k, s.m), (1, 0));
        assert!(s.warning.is_some(), "m=0 should produce a warning");
    }

    #[test]
    fn test_suggest_config_two_nodes() {
        let s = suggest_config(2);
        assert_eq!(s.k + s.m, 2);
        assert!(s.m >= 1);
        assert!(s.warning.is_some(), "m=1 should produce a warning");
    }

    #[test]
    fn test_suggest_config_three_nodes() {
        let s = suggest_config(3);
        assert_eq!((s.k, s.m), (2, 1));
        assert_eq!(s.k + s.m, 3);
        assert!(s.warning.is_some(), "m=1 should produce a warning");
    }

    #[test]
    fn test_suggest_config_five_nodes() {
        let s = suggest_config(5);
        assert_eq!((s.k, s.m), (3, 2));
        assert_eq!(s.k + s.m, 5);
        assert!(s.warning.is_none(), "m=2 should not produce a warning");
    }

    #[test]
    fn test_suggest_config_large_cluster() {
        let s = suggest_config(20);
        assert_eq!((s.k, s.m), (8, 4));
        assert!(s.warning.is_none());
    }

    #[test]
    fn test_suggest_config_never_exceeds_nodes() {
        for n in 0..=50 {
            let s = suggest_config(n);
            assert!(
                s.k + s.m <= n.max(1),
                "k+m={} exceeds node_count={n}",
                s.k + s.m
            );
        }
    }

    #[test]
    fn test_suggest_config_zero_nodes() {
        let s = suggest_config(0);
        assert_eq!((s.k, s.m), (1, 0));
    }

    #[test]
    fn test_m0_warning_text() {
        let s = suggest_config(1);
        assert!(s.warning.as_ref().unwrap().contains("m=0"));
    }

    #[test]
    fn test_m1_warning_text() {
        let s = suggest_config(3);
        assert!(s.warning.as_ref().unwrap().contains("m=1"));
        assert!(s.warning.as_ref().unwrap().contains("m>=2"));
    }

    #[test]
    fn test_m2_no_warning() {
        let s = suggest_config(4);
        assert_eq!(s.m, 2);
        assert!(s.warning.is_none());
    }
}
