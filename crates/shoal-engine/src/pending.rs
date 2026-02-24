//! Pending entry buffer for log entries awaiting missing parents.
//!
//! When a log entry arrives (via gossip or unicast) but its parent entries
//! haven't been received yet, it's buffered here. The buffer is shared
//! between the protocol handler (which adds entries) and the engine
//! (which queries and drains it).

use std::sync::Mutex;

use shoal_logtree::{Action, LogEntry, LogTree, LogTreeError};
use shoal_types::{Manifest, NodeId};
use tracing::{debug, trace, warn};

/// An entry waiting for its parents to arrive.
pub struct PendingEntry {
    /// The log entry itself.
    pub entry: LogEntry,
    /// Postcard-serialized manifest bytes (for Put actions).
    pub manifest_bytes: Option<Vec<u8>>,
}

impl PendingEntry {
    /// Create a new pending entry.
    pub fn new(entry: LogEntry, manifest_bytes: Option<Vec<u8>>) -> Self {
        Self {
            entry,
            manifest_bytes,
        }
    }
}

/// Shared pending entry buffer type.
pub type PendingBuffer = std::sync::Arc<Mutex<Vec<PendingEntry>>>;

/// Check if any pending entry references a specific `(bucket, key)`.
///
/// Returns the `NodeId` of the first matching entry's author, which can
/// be used to do a targeted pull from that peer.
pub fn pending_author_for_key(
    pending: &Mutex<Vec<PendingEntry>>,
    bucket: &str,
    key: &str,
) -> Option<NodeId> {
    let buf = pending.lock().expect("pending lock poisoned");
    for pe in buf.iter() {
        match &pe.entry.action {
            Action::Put {
                bucket: b, key: k, ..
            }
            | Action::Delete { bucket: b, key: k } => {
                if b == bucket && k == key {
                    return Some(pe.entry.node_id);
                }
            }
            _ => {}
        }
    }
    None
}

/// Check if any pending entries exist at all.
pub fn has_any_pending(pending: &Mutex<Vec<PendingEntry>>) -> bool {
    let buf = pending.lock().expect("pending lock poisoned");
    !buf.is_empty()
}

/// Collect missing parent hashes from pending entries for a given `(bucket, key)`.
///
/// Checks each parent hash against the LogTree store to find the ones that
/// are actually missing. These hashes can be sent in a `LogSyncPull` request.
pub fn missing_parents_for_key(
    pending: &Mutex<Vec<PendingEntry>>,
    log_tree: &LogTree,
    bucket: &str,
    key: &str,
) -> Vec<[u8; 32]> {
    let buf = pending.lock().expect("pending lock poisoned");
    let mut missing = Vec::new();

    for pe in buf.iter() {
        let matches = match &pe.entry.action {
            Action::Put {
                bucket: b, key: k, ..
            }
            | Action::Delete { bucket: b, key: k } => b == bucket && k == key,
            _ => false,
        };

        if matches {
            for parent in &pe.entry.parents {
                if !log_tree.store().has_entry(parent).unwrap_or(true) && !missing.contains(parent)
                {
                    missing.push(*parent);
                }
            }
        }
    }

    missing
}

/// Drain pending entries, applying those whose parents are now available.
///
/// Returns the number of entries successfully applied.
pub fn drain_pending(log_tree: &LogTree, pending: &Mutex<Vec<PendingEntry>>) -> usize {
    let mut buf = pending.lock().expect("pending lock poisoned");

    if buf.is_empty() {
        return 0;
    }

    let mut applied = 0;
    let mut i = 0;

    while i < buf.len() {
        let manifest = buf[i]
            .manifest_bytes
            .as_ref()
            .and_then(|b| postcard::from_bytes::<Manifest>(b).ok());

        match log_tree.receive_entry(&buf[i].entry, manifest.as_ref()) {
            Ok(true) => {
                trace!("resolved pending log entry");
                buf.swap_remove(i);
                applied += 1;
                // Don't increment i — swap_remove moved the last element here.
            }
            Ok(false) => {
                // Already known — drop from buffer.
                buf.swap_remove(i);
            }
            Err(LogTreeError::MissingParents(_)) => {
                // Still missing parents — keep in buffer.
                i += 1;
            }
            Err(e) => {
                warn!(%e, "dropping invalid pending log entry");
                buf.swap_remove(i);
            }
        }
    }

    if applied > 0 {
        debug!(
            applied,
            remaining = buf.len(),
            "drained pending log entries"
        );
    }

    applied
}
