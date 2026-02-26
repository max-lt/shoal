//! Type-safe event bus for intra-node pub/sub.
//!
//! The [`EventBus`] allows any component to emit typed events and any other
//! component to subscribe to specific event types without direct coupling.
//!
//! # Design
//!
//! Each event type is a distinct struct implementing the [`Event`] marker trait.
//! Internally, the bus maintains a `HashMap<TypeId, Box<dyn Any>>` where each
//! value is a `tokio::sync::broadcast::Sender<E>`. Channels are created lazily
//! on the first `subscribe()` or `emit()` for a given type.
//!
//! # Example
//!
//! ```rust
//! use shoal_types::events::{EventBus, Event, EventOrigin, MembershipReady};
//! use shoal_types::NodeId;
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! let bus = EventBus::new();
//! let mut rx = bus.subscribe::<MembershipReady>();
//!
//! bus.emit(MembershipReady {
//!     node_id: NodeId::from([1u8; 32]),
//!     origin: EventOrigin::Local,
//! });
//!
//! let event = rx.recv().await.unwrap();
//! assert_eq!(event.node_id, NodeId::from([1u8; 32]));
//! # });
//! ```

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;
use tracing::warn;

use crate::{NodeId, ObjectId, ShardId};

// ---------------------------------------------------------------------------
// Event trait
// ---------------------------------------------------------------------------

/// Marker trait for all events that can travel through the [`EventBus`].
///
/// Every event is a distinct struct with its own fields. Subscribers receive
/// only events of the type they subscribed to — type safety is guaranteed at
/// compile time.
pub trait Event: Any + Send + Sync + Clone + std::fmt::Debug + 'static {}

// ---------------------------------------------------------------------------
// Event types
// ---------------------------------------------------------------------------

/// Whether an event originated locally or was received from a peer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventOrigin {
    /// The event was produced by this node.
    Local,
    /// The event was received via gossip from a peer.
    Gossip,
}

/// A node has been declared active in the cluster.
#[derive(Clone, Debug)]
pub struct MembershipReady {
    /// The node that became ready.
    pub node_id: NodeId,
    /// Whether this event originated locally or from gossip.
    pub origin: EventOrigin,
}
impl Event for MembershipReady {}

/// A node has been declared dead by the failure detector.
#[derive(Clone, Debug)]
pub struct MembershipDead {
    /// The node that was declared dead.
    pub node_id: NodeId,
    /// Whether this event originated locally or from gossip.
    pub origin: EventOrigin,
}
impl Event for MembershipDead {}

/// A node has gracefully left the cluster.
#[derive(Clone, Debug)]
pub struct MembershipLeft {
    /// The node that departed.
    pub node_id: NodeId,
    /// Whether this event originated locally or from gossip.
    pub origin: EventOrigin,
}
impl Event for MembershipLeft {}

/// A manifest has been received (from a put or via gossip).
#[derive(Clone, Debug)]
pub struct ManifestReceived {
    /// Target bucket.
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// The object identifier.
    pub object_id: ObjectId,
}
impl Event for ManifestReceived {}

/// A log entry has been successfully applied to the DAG.
#[derive(Clone, Debug)]
pub struct LogEntryApplied {
    /// BLAKE3 hash of the log entry.
    pub hash: [u8; 32],
    /// Bucket the entry belongs to.
    pub bucket: String,
    /// Key the entry belongs to.
    pub key: String,
}
impl Event for LogEntryApplied {}

/// A log entry is pending because its parent entries are missing.
#[derive(Clone, Debug)]
pub struct LogEntryPending {
    /// BLAKE3 hash of the pending entry.
    pub hash: [u8; 32],
    /// Hashes of the missing parent entries.
    pub missing_parents: Vec<[u8; 32]>,
}
impl Event for LogEntryPending {}

/// How a shard was obtained.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShardSource {
    /// Written locally (during a put_object).
    Local,
    /// Pushed by a peer node.
    PeerPush,
    /// Pulled from a peer node (during a read).
    PeerPull,
    /// Fetched or reconstructed during repair.
    Repair,
}

/// A shard has been stored locally.
#[derive(Clone, Debug)]
pub struct ShardStored {
    /// The shard that was stored.
    pub shard_id: ShardId,
    /// How the shard was obtained.
    pub source: ShardSource,
}
impl Event for ShardStored {}

/// A local shard has been found to be corrupted.
#[derive(Clone, Debug)]
pub struct ShardCorrupted {
    /// The corrupted shard.
    pub shard_id: ShardId,
}
impl Event for ShardCorrupted {}

/// An object write has been fully completed (all shards distributed, manifest stored).
#[derive(Clone, Debug)]
pub struct ObjectComplete {
    /// Bucket.
    pub bucket: String,
    /// Key.
    pub key: String,
    /// The object identifier.
    pub object_id: ObjectId,
}
impl Event for ObjectComplete {}

/// Repair has started for a shard.
#[derive(Clone, Debug)]
pub struct RepairStarted {
    /// The shard being repaired.
    pub shard_id: ShardId,
}
impl Event for RepairStarted {}

/// Repair has completed for a shard.
#[derive(Clone, Debug)]
pub struct RepairCompleted {
    /// The shard that was repaired.
    pub shard_id: ShardId,
}
impl Event for RepairCompleted {}

/// A shard needs repair (under-replicated or corrupted).
#[derive(Clone, Debug)]
pub struct RepairNeeded {
    /// The shard that needs repair.
    pub shard_id: ShardId,
}
impl Event for RepairNeeded {}

/// Log sync with a peer has completed.
#[derive(Clone, Debug)]
pub struct SyncComplete {
    /// The peer we synced with.
    pub peer: NodeId,
    /// Number of log entries applied during sync.
    pub entries_applied: usize,
}
impl Event for SyncComplete {}

// ---------------------------------------------------------------------------
// EventBus
// ---------------------------------------------------------------------------

/// Default broadcast channel capacity.
const DEFAULT_CHANNEL_CAPACITY: usize = 256;

/// Inner state of the event bus, protected by a mutex.
///
/// Each entry maps a `TypeId` to a type-erased `broadcast::Sender<E>`.
/// Channels are created lazily on first `subscribe()` or `emit()`.
struct EventBusInner {
    channels: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

/// A type-safe event bus for intra-node communication.
///
/// Clonable (`Arc` inside). Thread-safe. Non-blocking emits.
///
/// Each event type gets its own independent `tokio::broadcast` channel,
/// so subscribers of `MembershipDead` never receive `ShardStored` events.
#[derive(Clone)]
pub struct EventBus {
    inner: Arc<Mutex<EventBusInner>>,
}

impl EventBus {
    /// Create a new empty event bus.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(EventBusInner {
                channels: HashMap::new(),
            })),
        }
    }

    /// Emit an event to all current subscribers of type `E`.
    ///
    /// Non-blocking: if no subscribers exist, or if a subscriber's buffer
    /// is full, the event is dropped (with a tracing warning for overflow).
    pub fn emit<E: Event>(&self, event: E) {
        let type_id = TypeId::of::<E>();
        let inner = self.inner.lock().expect("event bus lock poisoned");

        if let Some(boxed) = inner.channels.get(&type_id) {
            let sender = boxed
                .downcast_ref::<broadcast::Sender<E>>()
                .expect("type mismatch in event bus");

            match sender.send(event) {
                Ok(_) => {}
                Err(_) => {
                    // No active receivers — event is dropped silently.
                    // This is expected when no component has subscribed yet.
                }
            }
        }
        // If no channel exists, nobody is listening — drop silently.
    }

    /// Subscribe to events of type `E`.
    ///
    /// Returns an [`EventReceiver`] that yields only events of the requested
    /// type. The channel is created lazily if it doesn't exist yet.
    pub fn subscribe<E: Event>(&self) -> EventReceiver<E> {
        let type_id = TypeId::of::<E>();
        let mut inner = self.inner.lock().expect("event bus lock poisoned");

        let sender = inner
            .channels
            .entry(type_id)
            .or_insert_with(|| {
                let (tx, _) = broadcast::channel::<E>(DEFAULT_CHANNEL_CAPACITY);
                Box::new(tx)
            })
            .downcast_ref::<broadcast::Sender<E>>()
            .expect("type mismatch in event bus");

        EventReceiver {
            rx: sender.subscribe(),
        }
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for EventBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock().expect("event bus lock poisoned");
        f.debug_struct("EventBus")
            .field("channel_count", &inner.channels.len())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// EventReceiver
// ---------------------------------------------------------------------------

/// Typed receiver for a specific event type.
///
/// Wraps a `tokio::sync::broadcast::Receiver<E>`.
pub struct EventReceiver<E: Event> {
    rx: broadcast::Receiver<E>,
}

impl<E: Event> EventReceiver<E> {
    /// Wait for the next event.
    ///
    /// Returns `Some(event)` when an event is available, or `None` if the
    /// bus has been dropped (all senders gone). Skips over lagged events
    /// with a warning.
    pub async fn recv(&mut self) -> Option<E> {
        loop {
            match self.rx.recv().await {
                Ok(event) => return Some(event),
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(
                        skipped = n,
                        event_type = std::any::type_name::<E>(),
                        "event receiver lagged"
                    );
                    // Continue — try to receive the next available event.
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeId;

    #[tokio::test]
    async fn test_emit_subscribe_basic() {
        let bus = EventBus::new();
        let mut rx = bus.subscribe::<MembershipReady>();

        bus.emit(MembershipReady {
            node_id: NodeId::from([1u8; 32]),
            origin: EventOrigin::Local,
        });

        let event = rx.recv().await.unwrap();
        assert_eq!(event.node_id, NodeId::from([1u8; 32]));
        assert_eq!(event.origin, EventOrigin::Local);
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let bus = EventBus::new();
        let mut rx1 = bus.subscribe::<MembershipDead>();
        let mut rx2 = bus.subscribe::<MembershipDead>();

        bus.emit(MembershipDead {
            node_id: NodeId::from([2u8; 32]),
            origin: EventOrigin::Gossip,
        });

        let e1 = rx1.recv().await.unwrap();
        let e2 = rx2.recv().await.unwrap();
        assert_eq!(e1.node_id, NodeId::from([2u8; 32]));
        assert_eq!(e2.node_id, NodeId::from([2u8; 32]));
    }

    #[tokio::test]
    async fn test_subscribe_before_emit() {
        let bus = EventBus::new();
        let mut rx = bus.subscribe::<ShardStored>();

        // Emit after subscribe — subscriber should receive it.
        bus.emit(ShardStored {
            shard_id: ShardId::from_data(b"test-shard"),
            source: ShardSource::Local,
        });

        let event = rx.recv().await.unwrap();
        assert_eq!(event.shard_id, ShardId::from_data(b"test-shard"));
        assert_eq!(event.source, ShardSource::Local);
    }

    #[tokio::test]
    async fn test_subscribe_after_emit_misses_past() {
        let bus = EventBus::new();

        // Emit before any subscriber — events are lost.
        bus.emit(MembershipReady {
            node_id: NodeId::from([1u8; 32]),
            origin: EventOrigin::Local,
        });

        // Subscribe after the emit.
        let mut rx = bus.subscribe::<MembershipReady>();

        // Emit a new event.
        bus.emit(MembershipReady {
            node_id: NodeId::from([2u8; 32]),
            origin: EventOrigin::Gossip,
        });

        // Should only receive the second event.
        let event = rx.recv().await.unwrap();
        assert_eq!(event.node_id, NodeId::from([2u8; 32]));
    }

    #[tokio::test]
    async fn test_multiple_event_types_independent() {
        let bus = EventBus::new();
        let mut rx_ready = bus.subscribe::<MembershipReady>();
        let mut rx_dead = bus.subscribe::<MembershipDead>();

        bus.emit(MembershipReady {
            node_id: NodeId::from([1u8; 32]),
            origin: EventOrigin::Local,
        });
        bus.emit(MembershipDead {
            node_id: NodeId::from([2u8; 32]),
            origin: EventOrigin::Gossip,
        });

        let ready = rx_ready.recv().await.unwrap();
        let dead = rx_dead.recv().await.unwrap();

        assert_eq!(ready.node_id, NodeId::from([1u8; 32]));
        assert_eq!(dead.node_id, NodeId::from([2u8; 32]));
    }

    #[tokio::test]
    async fn test_emit_without_subscriber_does_not_block() {
        let bus = EventBus::new();

        // No subscribers — should not panic or block.
        bus.emit(ShardCorrupted {
            shard_id: ShardId::from_data(b"orphan"),
        });

        // Also emit to a type that has a channel but no active receivers.
        let _rx = bus.subscribe::<RepairStarted>();
        drop(_rx);
        bus.emit(RepairStarted {
            shard_id: ShardId::from_data(b"repair-test"),
        });
    }

    #[tokio::test]
    async fn test_event_bus_is_clonable() {
        let bus1 = EventBus::new();
        let bus2 = bus1.clone();

        let mut rx = bus1.subscribe::<MembershipReady>();

        // Emit via the clone — subscriber on the original should see it.
        bus2.emit(MembershipReady {
            node_id: NodeId::from([3u8; 32]),
            origin: EventOrigin::Local,
        });

        let event = rx.recv().await.unwrap();
        assert_eq!(event.node_id, NodeId::from([3u8; 32]));
    }

    #[tokio::test]
    async fn test_all_event_types_emit_subscribe() {
        let bus = EventBus::new();

        // MembershipReady
        let mut rx = bus.subscribe::<MembershipReady>();
        bus.emit(MembershipReady {
            node_id: NodeId::from([1; 32]),
            origin: EventOrigin::Local,
        });
        assert!(rx.recv().await.is_some());

        // MembershipDead
        let mut rx = bus.subscribe::<MembershipDead>();
        bus.emit(MembershipDead {
            node_id: NodeId::from([1; 32]),
            origin: EventOrigin::Local,
        });
        assert!(rx.recv().await.is_some());

        // MembershipLeft
        let mut rx = bus.subscribe::<MembershipLeft>();
        bus.emit(MembershipLeft {
            node_id: NodeId::from([1; 32]),
            origin: EventOrigin::Local,
        });
        assert!(rx.recv().await.is_some());

        // ManifestReceived
        let mut rx = bus.subscribe::<ManifestReceived>();
        bus.emit(ManifestReceived {
            bucket: "b".into(),
            key: "k".into(),
            object_id: ObjectId::from_data(b"obj"),
        });
        assert!(rx.recv().await.is_some());

        // LogEntryApplied
        let mut rx = bus.subscribe::<LogEntryApplied>();
        bus.emit(LogEntryApplied {
            hash: [0; 32],
            bucket: "b".into(),
            key: "k".into(),
        });
        assert!(rx.recv().await.is_some());

        // LogEntryPending
        let mut rx = bus.subscribe::<LogEntryPending>();
        bus.emit(LogEntryPending {
            hash: [0; 32],
            missing_parents: vec![[1; 32]],
        });
        assert!(rx.recv().await.is_some());

        // ShardStored
        let mut rx = bus.subscribe::<ShardStored>();
        bus.emit(ShardStored {
            shard_id: ShardId::from_data(b"s"),
            source: ShardSource::PeerPush,
        });
        assert!(rx.recv().await.is_some());

        // ShardCorrupted
        let mut rx = bus.subscribe::<ShardCorrupted>();
        bus.emit(ShardCorrupted {
            shard_id: ShardId::from_data(b"s"),
        });
        assert!(rx.recv().await.is_some());

        // ObjectComplete
        let mut rx = bus.subscribe::<ObjectComplete>();
        bus.emit(ObjectComplete {
            bucket: "b".into(),
            key: "k".into(),
            object_id: ObjectId::from_data(b"obj"),
        });
        assert!(rx.recv().await.is_some());

        // RepairStarted
        let mut rx = bus.subscribe::<RepairStarted>();
        bus.emit(RepairStarted {
            shard_id: ShardId::from_data(b"r"),
        });
        assert!(rx.recv().await.is_some());

        // RepairCompleted
        let mut rx = bus.subscribe::<RepairCompleted>();
        bus.emit(RepairCompleted {
            shard_id: ShardId::from_data(b"r"),
        });
        assert!(rx.recv().await.is_some());

        // RepairNeeded
        let mut rx = bus.subscribe::<RepairNeeded>();
        bus.emit(RepairNeeded {
            shard_id: ShardId::from_data(b"r"),
        });
        assert!(rx.recv().await.is_some());

        // SyncComplete
        let mut rx = bus.subscribe::<SyncComplete>();
        bus.emit(SyncComplete {
            peer: NodeId::from([1; 32]),
            entries_applied: 42,
        });
        let e = rx.recv().await.unwrap();
        assert_eq!(e.entries_applied, 42);
    }

    #[tokio::test]
    async fn test_receiver_closed_when_bus_dropped() {
        let bus = EventBus::new();
        let mut rx = bus.subscribe::<MembershipReady>();

        // Drop the bus — all senders are dropped.
        drop(bus);

        // recv should return None.
        let result = rx.recv().await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_concurrent_emit_subscribe() {
        let bus = EventBus::new();
        let mut rx = bus.subscribe::<ShardStored>();

        let bus_clone = bus.clone();
        let handle = tokio::spawn(async move {
            for i in 0..100u8 {
                bus_clone.emit(ShardStored {
                    shard_id: ShardId::from([i; 32]),
                    source: ShardSource::Local,
                });
            }
        });

        let mut count = 0;
        let timeout = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if rx.recv().await.is_some() {
                    count += 1;
                    if count >= 100 {
                        break;
                    }
                } else {
                    break;
                }
            }
        });

        handle.await.unwrap();
        timeout.await.unwrap();
        assert_eq!(count, 100);
    }
}
