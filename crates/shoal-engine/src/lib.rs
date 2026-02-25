//! Node orchestrator tying all Shoal components together.
//!
//! The [`ShoalNode`] owns all subsystems and exposes the full
//! write / read / delete pipeline for objects.
//!
//! Protocol adapters (S3, SFTP, Admin API) depend on the [`ShoalEngine`]
//! trait rather than the concrete `ShoalNode` struct. This makes adapters
//! interchangeable — they all call the same five data-plane methods.

pub mod cache;
pub mod engine;
pub mod error;
pub mod node;
pub mod pending;

pub use cache::ShardCache;
pub use engine::ShoalEngine;
pub use error::EngineError;
pub use node::{ShoalNode, ShoalNodeConfig};
pub use pending::{PendingBuffer, PendingEntry, drain_pending};

#[cfg(test)]
mod tests;
