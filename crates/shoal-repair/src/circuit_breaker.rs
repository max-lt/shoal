//! Repair circuit breaker logic.
//!
//! Prevents cascade meltdowns by suspending repair when the cluster is in a
//! degraded state. Inspired by Ceph's 2018 rebalancing storm.

use std::collections::HashMap;
use std::sync::Arc;

use shoal_types::{NodeId, RepairCircuitBreaker as CircuitBreakerConfig};
use tokio::sync::RwLock;
use tokio::time::Instant;
use tracing::{info, warn};

/// Live circuit breaker state that gates repair/rebalancing activity.
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    /// Tracks when each node was last seen coming back from dead/suspect.
    cooldowns: Arc<RwLock<HashMap<NodeId, Instant>>>,
}

/// The result of checking the circuit breaker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CircuitBreakerStatus {
    /// Repair may proceed normally.
    Closed,
    /// Repair is suspended — too many nodes are down.
    OpenTooManyDown {
        /// Fraction of dead nodes.
        down_fraction: u64,
    },
    /// Repair is throttled — the queue is too large.
    ThrottledQueuePressure {
        /// Current queue length.
        queue_len: usize,
    },
}

impl CircuitBreaker {
    /// Create a new circuit breaker with the given configuration.
    pub fn new(config: RepairCircuitBreaker) -> Self {
        Self {
            config,
            cooldowns: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Check whether repair should proceed given the current cluster state.
    ///
    /// - `total_members`: total known cluster members.
    /// - `alive_members`: number of members currently alive.
    /// - `queue_len`: current repair queue length.
    pub fn check(
        &self,
        total_members: usize,
        alive_members: usize,
        queue_len: usize,
    ) -> CircuitBreakerStatus {
        if total_members == 0 {
            return CircuitBreakerStatus::Closed;
        }

        let dead = total_members.saturating_sub(alive_members);
        let down_fraction = dead as f64 / total_members as f64;

        if down_fraction >= self.config.max_down_fraction {
            warn!(
                down_fraction = format!("{down_fraction:.2}"),
                threshold = format!("{:.2}", self.config.max_down_fraction),
                dead,
                total_members,
                "CRITICAL: circuit breaker OPEN — too many nodes down, suspending all repair"
            );
            // Encode as fixed-point (fraction * 100) for the enum.
            return CircuitBreakerStatus::OpenTooManyDown {
                down_fraction: (down_fraction * 100.0) as u64,
            };
        }

        if queue_len > self.config.queue_pressure_threshold {
            info!(
                queue_len,
                threshold = self.config.queue_pressure_threshold,
                "circuit breaker: queue pressure — throttling repair"
            );
            return CircuitBreakerStatus::ThrottledQueuePressure { queue_len };
        }

        CircuitBreakerStatus::Closed
    }

    /// Record that a node has come back online. Starts the cooldown timer.
    pub async fn record_node_recovery(&self, node_id: NodeId) {
        let mut cooldowns = self.cooldowns.write().await;
        cooldowns.insert(node_id, Instant::now());
        info!(%node_id, cooldown_secs = self.config.rebalance_cooldown_secs,
              "node recovered — rebalance cooldown started");
    }

    /// Check whether a node is still in its rebalance cooldown period.
    ///
    /// During cooldown, we don't rebalance shards away from this node
    /// (it's probably just rebooting).
    pub async fn is_in_cooldown(&self, node_id: &NodeId) -> bool {
        let cooldowns = self.cooldowns.read().await;
        if let Some(recovery_time) = cooldowns.get(node_id) {
            let elapsed = recovery_time.elapsed();
            elapsed.as_secs() < self.config.rebalance_cooldown_secs
        } else {
            false
        }
    }

    /// Remove expired cooldowns. Call periodically to avoid unbounded growth.
    pub async fn cleanup_expired_cooldowns(&self) {
        let mut cooldowns = self.cooldowns.write().await;
        let threshold = self.config.rebalance_cooldown_secs;
        cooldowns.retain(|_, recovery_time| recovery_time.elapsed().as_secs() < threshold);
    }

    /// Return the underlying configuration.
    pub fn config(&self) -> &CircuitBreakerConfig {
        &self.config
    }
}

use shoal_types::RepairCircuitBreaker;

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> CircuitBreakerConfig {
        CircuitBreakerConfig::default()
    }

    #[test]
    fn test_circuit_breaker_closed_normal() {
        let cb = CircuitBreaker::new(default_config());
        let status = cb.check(10, 8, 100);
        assert_eq!(status, CircuitBreakerStatus::Closed);
    }

    #[test]
    fn test_circuit_breaker_open_too_many_down() {
        let cb = CircuitBreaker::new(default_config());
        // 5 out of 10 down = 50% >= max_down_fraction (0.5)
        let status = cb.check(10, 5, 100);
        assert!(matches!(
            status,
            CircuitBreakerStatus::OpenTooManyDown { .. }
        ));
    }

    #[test]
    fn test_circuit_breaker_open_more_than_half_down() {
        let cb = CircuitBreaker::new(default_config());
        // 7 out of 10 down = 70%
        let status = cb.check(10, 3, 0);
        assert!(matches!(
            status,
            CircuitBreakerStatus::OpenTooManyDown { .. }
        ));
    }

    #[test]
    fn test_circuit_breaker_queue_pressure() {
        let cb = CircuitBreaker::new(default_config());
        let status = cb.check(10, 9, 15_000);
        assert!(matches!(
            status,
            CircuitBreakerStatus::ThrottledQueuePressure { .. }
        ));
    }

    #[test]
    fn test_circuit_breaker_empty_cluster() {
        let cb = CircuitBreaker::new(default_config());
        let status = cb.check(0, 0, 0);
        assert_eq!(status, CircuitBreakerStatus::Closed);
    }

    #[tokio::test]
    async fn test_cooldown_tracking() {
        let config = CircuitBreakerConfig {
            rebalance_cooldown_secs: 1,
            ..default_config()
        };
        let cb = CircuitBreaker::new(config);
        let node = NodeId::from_data(b"recovering-node");

        assert!(!cb.is_in_cooldown(&node).await);

        cb.record_node_recovery(node).await;
        assert!(cb.is_in_cooldown(&node).await);

        // Wait for cooldown to expire.
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        assert!(!cb.is_in_cooldown(&node).await);
    }
}
