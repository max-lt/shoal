//! Repair scheduler: dequeues shards from the repair queue and dispatches
//! them to the executor, respecting rate limits and the circuit breaker.

use std::sync::Arc;

use shoal_cluster::ClusterState;
use shoal_meta::MetaStore;
use tokio::sync::Semaphore;
use tokio::time::{Duration, interval};
use tracing::{debug, error, info, warn};

use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerStatus};
use crate::executor::RepairExecutor;
use crate::throttle::Throttle;

/// Repair scheduler that dequeues work and dispatches it to the executor.
///
/// Respects:
/// - Circuit breaker (stops repair when too many nodes down).
/// - Concurrent transfer limit (semaphore).
/// - Bandwidth throttle (token bucket).
pub struct RepairScheduler {
    meta: Arc<MetaStore>,
    cluster: Arc<ClusterState>,
    executor: Arc<RepairExecutor>,
    circuit_breaker: Arc<CircuitBreaker>,
    throttle: Arc<Throttle>,
    /// Semaphore limiting concurrent repair transfers.
    concurrency: Arc<Semaphore>,
    /// How often to check the repair queue (milliseconds).
    poll_interval_ms: u64,
}

impl RepairScheduler {
    /// Create a new scheduler.
    pub fn new(
        meta: Arc<MetaStore>,
        cluster: Arc<ClusterState>,
        executor: Arc<RepairExecutor>,
        circuit_breaker: Arc<CircuitBreaker>,
        throttle: Arc<Throttle>,
        max_concurrent_transfers: u16,
        poll_interval_ms: u64,
    ) -> Self {
        Self {
            meta,
            cluster,
            executor,
            circuit_breaker,
            throttle,
            concurrency: Arc::new(Semaphore::new(max_concurrent_transfers as usize)),
            poll_interval_ms,
        }
    }

    /// Run the scheduler loop. Should be spawned as a background task.
    pub async fn run(&self) {
        info!("repair scheduler started");
        let mut tick = interval(Duration::from_millis(self.poll_interval_ms));

        loop {
            tick.tick().await;

            // Check circuit breaker.
            let total = self.cluster.member_count().await;
            let alive = self.cluster.alive_count().await;
            let queue_len = match self.meta.repair_queue_len() {
                Ok(n) => n,
                Err(e) => {
                    error!(error = %e, "failed to read repair queue length");
                    continue;
                }
            };

            match self.circuit_breaker.check(total, alive, queue_len) {
                CircuitBreakerStatus::Closed => {}
                CircuitBreakerStatus::OpenTooManyDown { .. } => {
                    debug!("repair suspended — circuit breaker open");
                    continue;
                }
                CircuitBreakerStatus::ThrottledQueuePressure { queue_len } => {
                    // Under pressure: slow down by processing only one item per tick.
                    debug!(queue_len, "repair throttled — queue pressure");
                    self.process_one().await;
                    continue;
                }
            }

            // Normal mode: drain up to concurrency limit.
            self.process_batch().await;
        }
    }

    /// Dequeue and dispatch one repair task.
    async fn process_one(&self) {
        let shard_id = match self.meta.dequeue_repair() {
            Ok(Some(id)) => id,
            Ok(None) => return,
            Err(e) => {
                error!(error = %e, "failed to dequeue from repair queue");
                return;
            }
        };

        let permit = match self.concurrency.clone().acquire_owned().await {
            Ok(p) => p,
            Err(_) => {
                warn!("concurrency semaphore closed");
                return;
            }
        };

        let executor = self.executor.clone();
        let throttle = self.throttle.clone();

        tokio::spawn(async move {
            let _permit = permit;
            match executor.repair_shard(shard_id, &throttle).await {
                Ok(()) => {
                    info!(%shard_id, "shard repair completed");
                }
                Err(e) => {
                    error!(%shard_id, error = %e, "shard repair failed");
                }
            }
        });
    }

    /// Drain up to the concurrency limit of items from the queue.
    async fn process_batch(&self) {
        let available = self.concurrency.available_permits();
        if available == 0 {
            return;
        }

        for _ in 0..available {
            let shard_id = match self.meta.dequeue_repair() {
                Ok(Some(id)) => id,
                Ok(None) => break,
                Err(e) => {
                    error!(error = %e, "failed to dequeue from repair queue");
                    break;
                }
            };

            let permit = match self.concurrency.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => break,
            };

            let executor = self.executor.clone();
            let throttle = self.throttle.clone();

            tokio::spawn(async move {
                let _permit = permit;
                match executor.repair_shard(shard_id, &throttle).await {
                    Ok(()) => {
                        info!(%shard_id, "shard repair completed");
                    }
                    Err(e) => {
                        error!(%shard_id, error = %e, "shard repair failed");
                    }
                }
            });
        }
    }

    /// Return the current queue length.
    pub fn queue_len(&self) -> Result<usize, crate::RepairError> {
        Ok(self.meta.repair_queue_len()?)
    }
}
