//! Node orchestrator tying all Shoal components together.
//!
//! The [`ShoalNode`] owns all subsystems and exposes the full
//! write / read / delete pipeline for objects.

pub mod cache;
pub mod error;
pub mod node;
pub mod pending;

pub use cache::ShardCache;
pub use error::EngineError;
pub use node::{ShoalNode, ShoalNodeConfig};
pub use pending::{PendingBuffer, PendingEntry, drain_pending};

#[cfg(test)]
mod tests;
