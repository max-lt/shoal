//! Append-only, hash-chained, signed DAG of cluster mutations.
//!
//! The `LogTree` is the source of truth for all cluster mutations (object puts,
//! deletes). Each mutation is recorded as a [`LogEntry`] in a DAG structure
//! (like Git's commit graph). Entries are hash-chained and ed25519-signed,
//! providing an auditable, tamper-evident history.
//!
//! Conflict resolution uses Last-Writer-Wins (LWW) with hybrid logical clock
//! timestamps: the entry with the highest HLC wins, with `NodeId` as tiebreak.

mod entry;
mod error;
mod store;
mod tree;

#[cfg(test)]
mod tests;

pub use entry::{Action, LogEntry, Version};
pub use error::LogTreeError;
pub use store::LogTreeStore;
pub use tree::LogTree;
