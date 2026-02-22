//! Auto-repair, rebalancing, and deep scrub for Shoal.
//!
//! This crate provides:
//!
//! - [`RepairDetector`] — watches cluster events and enqueues shards for repair.
//! - [`RepairScheduler`] — dequeues shards and dispatches repairs, respecting
//!   rate limits and the circuit breaker.
//! - [`RepairExecutor`] — fetches missing shards (direct copy or RS reconstruction)
//!   and pushes them to their new owners.
//! - [`CircuitBreaker`] — prevents cascade meltdowns when too many nodes are down.
//! - [`Throttle`] — token bucket rate limiter for repair bandwidth.
//! - [`DeepScrubber`] — majority-vote integrity verification across peers.

pub mod circuit_breaker;
pub mod detector;
pub mod error;
pub mod executor;
pub mod scheduler;
pub mod scrub;
pub mod throttle;

pub use circuit_breaker::{CircuitBreaker, CircuitBreakerStatus};
pub use detector::{RepairDetector, ScanResult};
pub use error::RepairError;
pub use executor::{RepairExecutor, ShardTransfer};
pub use scheduler::RepairScheduler;
pub use scrub::{DeepScrubber, HashQuery, ScrubSummary, ScrubVerdict};
pub use throttle::Throttle;

#[cfg(test)]
mod tests;
