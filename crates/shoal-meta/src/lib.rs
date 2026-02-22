//! Metadata persistence layer wrapping Fjall.
//!
//! [`MetaStore`] provides typed accessors over five Fjall keyspaces that serve
//! as a **local cache and index** — never the source of truth for user data.
//! Everything stored here is reconstructible from the cluster:
//!
//! - `objects` — user key → [`ObjectId`] mapping (cache, from manifests)
//! - `manifests` — [`ObjectId`] → serialized [`Manifest`] (cache, manifest is also EC-stored)
//! - `shardmap` — [`ShardId`] → owners list (cache, derived from placement ring)
//! - `membership` — [`NodeId`] → serialized [`Member`] (cache, from foca/gossip)
//! - `repair_queue` — [`ShardId`] → priority (local, transient, rebuilt on restart)

mod error;
mod store;

pub use error::MetaError;
pub use store::MetaStore;
