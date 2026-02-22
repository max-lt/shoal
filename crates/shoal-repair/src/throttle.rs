//! Token bucket rate limiter for repair bandwidth.
//!
//! [`Throttle`] limits the rate of repair traffic to avoid starving client I/O.
//! It uses a token bucket algorithm where tokens represent bytes.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::time::{Duration, Instant};
use tracing::debug;

/// Token bucket rate limiter for repair bandwidth.
///
/// Each token represents one byte. The bucket refills at `bytes_per_sec` rate,
/// up to `bytes_per_sec` capacity (one second's worth of burst).
pub struct Throttle {
    /// Maximum bytes per second.
    bytes_per_sec: AtomicU64,
    /// Available tokens (bytes).
    tokens: tokio::sync::Mutex<ThrottleState>,
}

struct ThrottleState {
    available: f64,
    last_refill: Instant,
    capacity: f64,
    rate: f64,
}

impl Throttle {
    /// Create a new throttle with the given bandwidth limit.
    pub fn new(bytes_per_sec: u64) -> Arc<Self> {
        let rate = bytes_per_sec as f64;
        Arc::new(Self {
            bytes_per_sec: AtomicU64::new(bytes_per_sec),
            tokens: tokio::sync::Mutex::new(ThrottleState {
                available: rate,
                last_refill: Instant::now(),
                capacity: rate,
                rate,
            }),
        })
    }

    /// Wait until `bytes` tokens are available, then consume them.
    ///
    /// If `bytes` exceeds the capacity (one second's burst), it is clamped
    /// and the caller may need to call this multiple times for large transfers.
    pub async fn acquire(&self, bytes: u64) {
        let bytes = bytes as f64;

        loop {
            let wait_time = {
                let mut state = self.tokens.lock().await;
                self.refill(&mut state);

                if state.available >= bytes {
                    state.available -= bytes;
                    return;
                }

                // Compute how long to wait for enough tokens.
                let deficit = bytes - state.available;
                if state.rate > 0.0 {
                    Duration::from_secs_f64(deficit / state.rate)
                } else {
                    // Rate is 0 — throttle completely blocks. Wait a bit and retry.
                    Duration::from_millis(100)
                }
            };

            debug!(
                wait_ms = wait_time.as_millis(),
                bytes, "throttle: waiting for tokens"
            );
            tokio::time::sleep(wait_time).await;
        }
    }

    /// Try to consume tokens without waiting. Returns `true` if acquired.
    pub async fn try_acquire(&self, bytes: u64) -> bool {
        let bytes = bytes as f64;
        let mut state = self.tokens.lock().await;
        self.refill(&mut state);

        if state.available >= bytes {
            state.available -= bytes;
            true
        } else {
            false
        }
    }

    /// Update the bandwidth limit at runtime.
    pub async fn set_rate(&self, bytes_per_sec: u64) {
        self.bytes_per_sec.store(bytes_per_sec, Ordering::Relaxed);
        let mut state = self.tokens.lock().await;
        state.rate = bytes_per_sec as f64;
        state.capacity = bytes_per_sec as f64;
        if state.available > state.capacity {
            state.available = state.capacity;
        }
        debug!(bytes_per_sec, "throttle rate updated");
    }

    /// Return the current configured rate.
    pub fn rate(&self) -> u64 {
        self.bytes_per_sec.load(Ordering::Relaxed)
    }

    /// Refill tokens based on elapsed time since last refill.
    fn refill(&self, state: &mut ThrottleState) {
        let now = Instant::now();
        let elapsed = now.duration_since(state.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            state.available = (state.available + elapsed * state.rate).min(state.capacity);
            state.last_refill = now;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_throttle_basic_acquire() {
        let throttle = Throttle::new(10_000);
        // Should acquire immediately — bucket starts full.
        throttle.acquire(5_000).await;
        // Should still have 5000 tokens.
        assert!(throttle.try_acquire(5_000).await);
        // Now empty.
        assert!(!throttle.try_acquire(1).await);
    }

    #[tokio::test]
    async fn test_throttle_refill() {
        let throttle = Throttle::new(100_000);
        // Drain.
        throttle.acquire(100_000).await;
        assert!(!throttle.try_acquire(1).await);
        // Wait for refill.
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Should have ~5000 tokens now (100k/s * 0.05s).
        assert!(throttle.try_acquire(1).await);
    }

    #[tokio::test]
    async fn test_throttle_set_rate() {
        let throttle = Throttle::new(1_000);
        assert_eq!(throttle.rate(), 1_000);
        throttle.set_rate(2_000).await;
        assert_eq!(throttle.rate(), 2_000);
    }
}
