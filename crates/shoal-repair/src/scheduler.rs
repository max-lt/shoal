//! Repair scheduler: dequeues shards from the repair queue and dispatches
//! them to the executor, respecting rate limits and the circuit breaker.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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
    /// Estimated total shards to repair in the current repair wave.
    /// Set once when a non-empty queue is first observed.
    total_estimate: Arc<AtomicU64>,
    /// Total shards successfully repaired since the scheduler started.
    completed: Arc<AtomicU64>,
    /// Total shards that failed repair since the scheduler started.
    failed: Arc<AtomicU64>,
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
            total_estimate: Arc::new(AtomicU64::new(0)),
            completed: Arc::new(AtomicU64::new(0)),
            failed: Arc::new(AtomicU64::new(0)),
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

            if queue_len == 0 {
                // Reset counters when queue is drained so the next wave starts fresh.
                if self.total_estimate.load(Ordering::Relaxed) > 0 {
                    let done = self.completed.load(Ordering::Relaxed);
                    let err = self.failed.load(Ordering::Relaxed);

                    if done + err > 0 {
                        info!(
                            completed = done,
                            failed = err,
                            "repair wave finished — queue empty"
                        );
                    }

                    self.total_estimate.store(0, Ordering::Relaxed);
                    self.completed.store(0, Ordering::Relaxed);
                    self.failed.store(0, Ordering::Relaxed);
                }

                continue;
            }

            match self.circuit_breaker.check(total, alive, queue_len) {
                CircuitBreakerStatus::Closed => {}
                CircuitBreakerStatus::OpenTooManyDown { .. } => {
                    debug!("repair suspended — circuit breaker open");
                    continue;
                }
                CircuitBreakerStatus::ThrottledQueuePressure { queue_len } => {
                    // Under pressure: slow down by processing only one item per tick.
                    debug!(queue_len, "repair throttled — queue pressure");
                    self.dispatch_one().await;
                    continue;
                }
            }

            // Set total estimate on first sight of a non-empty queue.
            let current_estimate = self.total_estimate.load(Ordering::Relaxed);

            if current_estimate == 0 {
                let done =
                    self.completed.load(Ordering::Relaxed) + self.failed.load(Ordering::Relaxed);
                self.total_estimate
                    .store(done + queue_len as u64, Ordering::Relaxed);
                info!(queue_len, "repair wave started — draining queue");
            } else {
                // Queue may have grown (new enqueues). Adjust total upwards only.
                let done =
                    self.completed.load(Ordering::Relaxed) + self.failed.load(Ordering::Relaxed);
                let new_total = done + queue_len as u64;

                if new_total > current_estimate {
                    self.total_estimate.store(new_total, Ordering::Relaxed);
                }
            }

            // Drain the entire queue, dispatching in batches of concurrency limit.
            self.drain_queue().await;
        }
    }

    /// Drain the queue completely, dispatching concurrency-limited batches.
    async fn drain_queue(&self) {
        loop {
            let available = self.concurrency.available_permits();

            if available == 0 {
                // All permits in use — wait for one to come back.
                let _permit = match self.concurrency.clone().acquire_owned().await {
                    Ok(p) => {
                        // Got a permit, release it immediately and retry the loop.
                        drop(p);
                        continue;
                    }
                    Err(_) => return,
                };
            }

            let mut dispatched_any = false;

            for _ in 0..available {
                let shard_id = match self.meta.dequeue_repair() {
                    Ok(Some(id)) => id,
                    Ok(None) => return, // Queue empty — done.
                    Err(e) => {
                        error!(error = %e, "failed to dequeue from repair queue");
                        return;
                    }
                };

                let permit = match self.concurrency.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => return,
                };

                dispatched_any = true;
                let executor = self.executor.clone();
                let throttle = self.throttle.clone();
                let completed = self.completed.clone();
                let failed = self.failed.clone();
                let total_estimate = self.total_estimate.clone();

                tokio::spawn(async move {
                    let _permit = permit;

                    match executor.repair_shard(shard_id, &throttle).await {
                        Ok(()) => {
                            let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                            let err = failed.load(Ordering::Relaxed);
                            let total = total_estimate.load(Ordering::Relaxed);
                            info!(%shard_id, progress = %format!("{}/{total}", done + err), failed = err, "shard repair completed");
                        }
                        Err(e) => {
                            let err = failed.fetch_add(1, Ordering::Relaxed) + 1;
                            let done = completed.load(Ordering::Relaxed);
                            let total = total_estimate.load(Ordering::Relaxed);
                            error!(%shard_id, progress = %format!("{}/{total}", done + err), failed = err, error = %e, "shard repair failed");
                        }
                    }
                });
            }

            if !dispatched_any {
                return;
            }
        }
    }

    /// Dequeue and dispatch one repair task (used under throttled mode).
    async fn dispatch_one(&self) {
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
        let completed = self.completed.clone();
        let failed = self.failed.clone();
        let total_estimate = self.total_estimate.clone();

        tokio::spawn(async move {
            let _permit = permit;

            match executor.repair_shard(shard_id, &throttle).await {
                Ok(()) => {
                    let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                    let err = failed.load(Ordering::Relaxed);
                    let total = total_estimate.load(Ordering::Relaxed);
                    info!(%shard_id, progress = %format!("{}/{total}", done + err), failed = err, "shard repair completed");
                }
                Err(e) => {
                    let err = failed.fetch_add(1, Ordering::Relaxed) + 1;
                    let done = completed.load(Ordering::Relaxed);
                    let total = total_estimate.load(Ordering::Relaxed);
                    error!(%shard_id, progress = %format!("{}/{total}", done + err), failed = err, error = %e, "shard repair failed");
                }
            }
        });
    }

    /// Return the current queue length.
    pub fn queue_len(&self) -> Result<usize, crate::RepairError> {
        Ok(self.meta.repair_queue_len()?)
    }
}
