//! SWIM-based membership service powered by foca.
//!
//! The [`MembershipService`] wraps a foca SWIM instance in a tokio task
//! that continuously:
//!
//! - Receives incoming SWIM protocol data and feeds it to foca.
//! - Processes timer events (probe intervals, suspect timeouts, etc.).
//! - Drains foca's accumulated runtime events:
//!   - **send**: outgoing SWIM messages routed to other nodes.
//!   - **schedule**: timer events queued via `tokio::time::sleep`.
//!   - **notify**: membership changes applied to [`ClusterState`].

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use foca::{AccumulatingRuntime, Config, OwnedNotification, PostcardCodec, Timer};
use rand::SeedableRng;
use rand::rngs::SmallRng;
use shoal_meta::MetaStore;
use shoal_types::{Member, MemberState, NodeId};
use tokio::sync::{Mutex, RwLock, mpsc};
use tracing::{debug, error, info, warn};

use crate::error::ClusterError;
use crate::identity::ClusterIdentity;
use crate::state::ClusterState;

/// Commands sent to the membership service event loop.
#[derive(Debug)]
pub enum MembershipCommand {
    /// Announce ourselves to a seed node to join the cluster.
    Join(ClusterIdentity),
    /// Gracefully leave the cluster.
    Leave,
}

/// Handle to a running membership service.
///
/// Provides channels to feed incoming SWIM data, send commands,
/// and read outgoing SWIM data destined for other nodes.
pub struct MembershipHandle {
    /// Send incoming SWIM protocol bytes (received from the network) here.
    incoming_tx: mpsc::UnboundedSender<Vec<u8>>,
    /// Receive outgoing SWIM protocol bytes to send to other nodes.
    outgoing_rx: Mutex<mpsc::UnboundedReceiver<(ClusterIdentity, Vec<u8>)>>,
    /// Send commands (join, leave) to the service.
    command_tx: mpsc::UnboundedSender<MembershipCommand>,
    /// Shared cluster state maintained by the service.
    state: Arc<ClusterState>,
    /// Background task handle.
    task: tokio::task::JoinHandle<()>,
}

/// Shared address book type: maps NodeId → EndpointAddr.
pub type AddressBook = Arc<RwLock<HashMap<NodeId, iroh::EndpointAddr>>>;

impl MembershipHandle {
    /// Feed incoming SWIM protocol data from the network.
    pub fn feed_data(&self, data: Vec<u8>) -> Result<(), ClusterError> {
        self.incoming_tx
            .send(data)
            .map_err(|_| ClusterError::ServiceStopped)
    }

    /// Send a command to the membership service.
    pub fn send_command(&self, cmd: MembershipCommand) -> Result<(), ClusterError> {
        self.command_tx
            .send(cmd)
            .map_err(|_| ClusterError::ServiceStopped)
    }

    /// Join the cluster by announcing to a seed node.
    pub fn join(&self, seed: ClusterIdentity) -> Result<(), ClusterError> {
        self.send_command(MembershipCommand::Join(seed))
    }

    /// Gracefully leave the cluster.
    pub fn leave(&self) -> Result<(), ClusterError> {
        self.send_command(MembershipCommand::Leave)
    }

    /// Take the next outgoing message (target identity, SWIM data).
    ///
    /// Returns `None` if the service has stopped.
    pub async fn next_outgoing(&self) -> Option<(ClusterIdentity, Vec<u8>)> {
        self.outgoing_rx.lock().await.recv().await
    }

    /// Return a reference to the shared cluster state.
    pub fn state(&self) -> &Arc<ClusterState> {
        &self.state
    }

    /// Abort the background task.
    pub fn abort(&self) {
        self.task.abort();
    }

    /// Check whether the background task is still running.
    pub fn is_running(&self) -> bool {
        !self.task.is_finished()
    }
}

/// Start the membership service and return a handle.
///
/// The service runs as a background tokio task that drives the foca SWIM
/// protocol. The caller is responsible for routing outgoing messages to
/// the network (read from [`MembershipHandle::next_outgoing`]) and feeding
/// incoming network data (via [`MembershipHandle::feed_data`]).
///
/// When `address_book` is provided, the service updates it whenever a
/// new member is discovered via foca (using the addresses carried in
/// [`ClusterIdentity::listen_addrs`]).
pub fn start(
    identity: ClusterIdentity,
    foca_config: Config,
    state: Arc<ClusterState>,
    meta: Option<Arc<MetaStore>>,
) -> MembershipHandle {
    start_with_address_book(identity, foca_config, state, meta, None)
}

/// Start the membership service with an address book for peer discovery.
///
/// Same as [`start`], but also updates the given address book when new
/// members are discovered.
pub fn start_with_address_book(
    identity: ClusterIdentity,
    foca_config: Config,
    state: Arc<ClusterState>,
    meta: Option<Arc<MetaStore>>,
    address_book: Option<AddressBook>,
) -> MembershipHandle {
    let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
    let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel();
    let (command_tx, command_rx) = mpsc::unbounded_channel();

    let task = tokio::spawn(membership_loop(
        identity,
        foca_config,
        state.clone(),
        meta,
        address_book,
        incoming_rx,
        outgoing_tx,
        command_rx,
    ));

    MembershipHandle {
        incoming_tx,
        outgoing_rx: Mutex::new(outgoing_rx),
        command_tx,
        state,
        task,
    }
}

/// Create a foca config suitable for fast test execution.
pub fn test_config() -> Config {
    let mut config = Config::new_lan(NonZeroU32::new(20).expect("non-zero"));
    config.probe_period = Duration::from_millis(100);
    config.probe_rtt = Duration::from_millis(50);
    config.suspect_to_down_after = Duration::from_millis(500);
    config.remove_down_after = Duration::from_secs(5);

    // Enable periodic announce so nodes discover each other quickly.
    config.periodic_announce = Some(foca::PeriodicParams {
        frequency: Duration::from_millis(200),
        num_members: std::num::NonZeroUsize::new(3).expect("non-zero"),
    });

    // Enable periodic gossip for faster state convergence.
    config.periodic_gossip = Some(foca::PeriodicParams {
        frequency: Duration::from_millis(150),
        num_members: std::num::NonZeroUsize::new(3).expect("non-zero"),
    });

    config
}

/// Create a default foca config for production use.
pub fn default_config(max_members: u32) -> Config {
    Config::new_lan(NonZeroU32::new(max_members.max(2)).expect("non-zero"))
}

// ---------------------------------------------------------------------------
// Internal event loop
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn membership_loop(
    identity: ClusterIdentity,
    config: Config,
    state: Arc<ClusterState>,
    meta: Option<Arc<MetaStore>>,
    address_book: Option<AddressBook>,
    mut incoming_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    outgoing_tx: mpsc::UnboundedSender<(ClusterIdentity, Vec<u8>)>,
    mut command_rx: mpsc::UnboundedReceiver<MembershipCommand>,
) {
    let rng = SmallRng::from_os_rng();
    let codec = PostcardCodec;
    let mut foca = foca::Foca::new(identity, config, rng, codec);
    let mut runtime = AccumulatingRuntime::new();

    // Channel for delivering delayed timer events back to this loop.
    let (timer_tx, mut timer_rx) = mpsc::unbounded_channel::<Timer<ClusterIdentity>>();

    info!("membership service started");

    loop {
        tokio::select! {
            // --- Incoming SWIM protocol data from the network ---
            Some(data) = incoming_rx.recv() => {
                if let Err(e) = foca.handle_data(&data, &mut runtime) {
                    debug!("foca handle_data error: {e}");
                }
                drain_runtime(&mut runtime, &outgoing_tx, &timer_tx, &state, &meta, &address_book).await;
            }

            // --- Scheduled timer events ---
            Some(timer) = timer_rx.recv() => {
                if let Err(e) = foca.handle_timer(timer, &mut runtime) {
                    debug!("foca handle_timer error: {e}");
                }
                drain_runtime(&mut runtime, &outgoing_tx, &timer_tx, &state, &meta, &address_book).await;
            }

            // --- Commands (join, leave) ---
            Some(cmd) = command_rx.recv() => {
                match cmd {
                    MembershipCommand::Join(seed) => {
                        info!(seed = %seed.node_id, "announcing to seed node");
                        if let Err(e) = foca.announce(seed, &mut runtime) {
                            warn!("foca announce error: {e}");
                        }
                        drain_runtime(&mut runtime, &outgoing_tx, &timer_tx, &state, &meta, &address_book).await;
                    }
                    MembershipCommand::Leave => {
                        info!("leaving cluster gracefully");
                        if let Err(e) = foca.leave_cluster(&mut runtime) {
                            warn!("foca leave error: {e}");
                        }
                        drain_runtime(&mut runtime, &outgoing_tx, &timer_tx, &state, &meta, &address_book).await;
                        break;
                    }
                }
            }

            // All channels closed — shut down.
            else => {
                debug!("all channels closed, membership loop exiting");
                break;
            }
        }
    }

    info!("membership service stopped");
}

/// Drain all accumulated events from the foca runtime and dispatch them.
async fn drain_runtime(
    runtime: &mut AccumulatingRuntime<ClusterIdentity>,
    outgoing_tx: &mpsc::UnboundedSender<(ClusterIdentity, Vec<u8>)>,
    timer_tx: &mpsc::UnboundedSender<Timer<ClusterIdentity>>,
    state: &Arc<ClusterState>,
    meta: &Option<Arc<MetaStore>>,
    address_book: &Option<AddressBook>,
) {
    // Send outgoing SWIM messages.
    while let Some((target, data)) = runtime.to_send() {
        if outgoing_tx.send((target, data.to_vec())).is_err() {
            warn!("outgoing channel closed");
            return;
        }
    }

    // Schedule timer events.
    while let Some((delay, timer)) = runtime.to_schedule() {
        let tx = timer_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = tx.send(timer);
        });
    }

    // Process membership notifications.
    while let Some(notification) = runtime.to_notify() {
        handle_notification(notification, state, meta, address_book).await;
    }
}

/// Process a single foca membership notification.
async fn handle_notification(
    notification: OwnedNotification<ClusterIdentity>,
    state: &Arc<ClusterState>,
    meta: &Option<Arc<MetaStore>>,
    address_book: &Option<AddressBook>,
) {
    match notification {
        OwnedNotification::MemberUp(identity) => {
            let member = Member::from(&identity);
            info!(
                node_id = %member.node_id,
                addrs = ?identity.listen_addrs,
                "foca: member up"
            );

            // Update address book with the member's listen addresses.
            if let Some(book) = address_book
                && !identity.listen_addrs.is_empty()
                && let Ok(eid) = iroh::EndpointId::from_bytes(identity.node_id.as_bytes())
            {
                let mut addr = iroh::EndpointAddr::new(eid);

                for socket_addr in &identity.listen_addrs {
                    addr = addr.with_ip_addr(*socket_addr);
                }

                book.write().await.insert(identity.node_id, addr);
                debug!(
                    node_id = %identity.node_id,
                    addrs = ?identity.listen_addrs,
                    "updated address book from foca membership"
                );
            }

            // Persist member and peer addresses to meta store.
            if let Some(meta) = meta {
                if let Err(e) = meta.put_member(&member) {
                    error!("failed to persist member: {e}");
                }

                if !identity.listen_addrs.is_empty()
                    && let Err(e) = meta.put_peer_addrs(&identity.node_id, &identity.listen_addrs)
                {
                    error!("failed to persist peer addresses: {e}");
                }
            }

            state.add_member(member).await;
        }

        OwnedNotification::MemberDown(identity) => {
            info!(node_id = %identity.node_id, "foca: member down");

            // Update meta store.
            if let Some(meta) = meta {
                let mut member = Member::from(&identity);
                member.state = MemberState::Dead;
                if let Err(e) = meta.put_member(&member) {
                    error!("failed to persist dead member state: {e}");
                }
            }

            state.mark_dead(&identity.node_id).await;
        }

        OwnedNotification::Rename(old, new) => {
            info!(
                old_node = %old.node_id,
                new_gen = new.generation,
                "foca: member identity renewed"
            );
            // Remove old, add new.
            state.remove_member(&old.node_id).await;
            let member = Member::from(&new);
            state.add_member(member).await;
        }

        OwnedNotification::Active => {
            info!("foca: this node is now active in the cluster");
            let local_id = state.local_node_id();
            state
                .event_bus()
                .emit(shoal_types::events::MembershipReady {
                    node_id: local_id,
                    origin: shoal_types::events::EventOrigin::Local,
                });
        }

        OwnedNotification::Idle => {
            debug!("foca: this node is now idle (no known peers)");
        }

        OwnedNotification::Defunct => {
            warn!("foca: this node has been declared defunct");
        }

        OwnedNotification::Rejoin(new_identity) => {
            info!(
                gen = new_identity.generation,
                "foca: auto-rejoined with new generation"
            );
        }
    }
}
