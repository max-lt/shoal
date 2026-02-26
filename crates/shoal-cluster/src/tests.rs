//! Tests for the shoal-cluster crate.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use shoal_types::events::{MembershipDead, MembershipReady};
    use shoal_types::{MemberState, NodeId, NodeTopology};
    use tokio::sync::RwLock;
    use tokio::time;

    use crate::state::ClusterState;

    // -----------------------------------------------------------------------
    // ClusterState tests (independent of membership implementation)
    // -----------------------------------------------------------------------

    /// Create a test member.
    fn test_member(n: u8) -> shoal_types::Member {
        shoal_types::Member {
            node_id: NodeId::from([n; 32]),
            capacity: 1_000_000_000,
            state: MemberState::Alive,
            generation: 1,
            topology: NodeTopology::default(),
        }
    }

    #[tokio::test]
    async fn test_cluster_state_add_member() {
        let state = ClusterState::new(NodeId::from([0; 32]), 64);
        let member = test_member(1);

        state.add_member(member.clone()).await;

        assert_eq!(state.member_count().await, 1);
        let retrieved = state.get_member(&member.node_id).await;
        assert_eq!(retrieved, Some(member));
    }

    #[tokio::test]
    async fn test_cluster_state_remove_member() {
        let state = ClusterState::new(NodeId::from([0; 32]), 64);
        let member = test_member(1);

        state.add_member(member.clone()).await;
        assert_eq!(state.member_count().await, 1);

        state.remove_member(&member.node_id).await;
        assert_eq!(state.member_count().await, 0);
        assert_eq!(state.get_member(&member.node_id).await, None);
    }

    #[tokio::test]
    async fn test_cluster_state_mark_dead() {
        let state = ClusterState::new(NodeId::from([0; 32]), 64);
        let member = test_member(1);

        state.add_member(member.clone()).await;
        state.mark_dead(&member.node_id).await;

        let retrieved = state
            .get_member(&member.node_id)
            .await
            .expect("should exist");
        assert_eq!(retrieved.state, MemberState::Dead);

        // Ring should no longer contain the dead node.
        let ring = state.ring().await;
        assert_eq!(ring.node_count(), 0);
    }

    #[tokio::test]
    async fn test_cluster_state_ring_recomputed_on_add() {
        let state = ClusterState::new(NodeId::from([0; 32]), 64);

        let m1 = test_member(1);
        let m2 = test_member(2);

        state.add_member(m1).await;
        assert_eq!(state.ring().await.node_count(), 1);

        state.add_member(m2).await;
        assert_eq!(state.ring().await.node_count(), 2);
    }

    #[tokio::test]
    async fn test_cluster_state_ring_recomputed_on_remove() {
        let state = ClusterState::new(NodeId::from([0; 32]), 64);

        let m1 = test_member(1);
        let m2 = test_member(2);

        state.add_member(m1.clone()).await;
        state.add_member(m2).await;
        assert_eq!(state.ring().await.node_count(), 2);

        state.remove_member(&m1.node_id).await;
        assert_eq!(state.ring().await.node_count(), 1);
    }

    #[tokio::test]
    async fn test_cluster_state_event_subscription() {
        let state = ClusterState::new(NodeId::from([0; 32]), 64);
        let mut rx = state.event_bus().subscribe::<MembershipReady>();

        let member = test_member(1);
        state.add_member(member.clone()).await;

        let event = rx.recv().await.expect("should receive event");
        assert_eq!(event.node_id, member.node_id);
    }

    #[tokio::test]
    async fn test_cluster_state_event_on_dead() {
        let state = ClusterState::new(NodeId::from([0; 32]), 64);
        let mut rx_ready = state.event_bus().subscribe::<MembershipReady>();
        let mut rx_dead = state.event_bus().subscribe::<MembershipDead>();

        let member = test_member(1);
        state.add_member(member.clone()).await;
        let _ = rx_ready.recv().await; // consume MembershipReady

        state.mark_dead(&member.node_id).await;

        let event = rx_dead.recv().await.expect("should receive event");
        assert_eq!(event.node_id, member.node_id);
    }

    // -----------------------------------------------------------------------
    // PeerHandle unit tests (mock transport)
    // -----------------------------------------------------------------------

    use async_trait::async_trait;
    use bytes::Bytes;
    use shoal_net::{NetError, ShoalMessage, Transport};
    use shoal_types::ShardId;

    use crate::membership::{self, AddressBook, PeerManagerConfig};

    /// A mock transport that responds to pings with pongs and records sends.
    struct MockTransport {
        /// If true, pings succeed (return Pong). If false, they fail.
        pings_succeed: Arc<std::sync::atomic::AtomicBool>,
    }

    impl MockTransport {
        fn new(succeed: bool) -> Self {
            Self {
                pings_succeed: Arc::new(std::sync::atomic::AtomicBool::new(succeed)),
            }
        }

        fn set_pings_succeed(&self, succeed: bool) {
            self.pings_succeed
                .store(succeed, std::sync::atomic::Ordering::Relaxed);
        }
    }

    #[async_trait]
    impl Transport for MockTransport {
        async fn push_shard(
            &self,
            _addr: iroh::EndpointAddr,
            _shard_id: ShardId,
            _data: Bytes,
        ) -> Result<(), NetError> {
            Ok(())
        }
        async fn pull_shard(
            &self,
            _addr: iroh::EndpointAddr,
            _shard_id: ShardId,
        ) -> Result<Option<Bytes>, NetError> {
            Ok(None)
        }
        async fn send_to(
            &self,
            _addr: iroh::EndpointAddr,
            _msg: &ShoalMessage,
        ) -> Result<(), NetError> {
            Ok(())
        }
        async fn pull_manifests(
            &self,
            _addr: iroh::EndpointAddr,
            _manifest_ids: &[shoal_types::ObjectId],
        ) -> Result<Vec<(shoal_types::ObjectId, Vec<u8>)>, NetError> {
            Ok(vec![])
        }
        async fn pull_log_entries(
            &self,
            _addr: iroh::EndpointAddr,
            _my_tips: &[[u8; 32]],
        ) -> Result<Vec<Vec<u8>>, NetError> {
            Ok(vec![])
        }
        async fn pull_log_sync(
            &self,
            _addr: iroh::EndpointAddr,
            _entry_hashes: &[[u8; 32]],
            _my_tips: &[[u8; 32]],
        ) -> Result<Vec<Vec<u8>>, NetError> {
            Ok(vec![])
        }
        async fn pull_api_keys(
            &self,
            _addr: iroh::EndpointAddr,
            _access_key_ids: &[String],
        ) -> Result<Vec<(String, String)>, NetError> {
            Ok(vec![])
        }
        async fn lookup_key(
            &self,
            _addr: iroh::EndpointAddr,
            _bucket: &str,
            _key: &str,
        ) -> Result<Option<Vec<u8>>, NetError> {
            Ok(None)
        }
        async fn request_response(
            &self,
            _addr: iroh::EndpointAddr,
            msg: &ShoalMessage,
        ) -> Result<ShoalMessage, NetError> {
            if self
                .pings_succeed
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                match msg {
                    ShoalMessage::Ping { timestamp } => Ok(ShoalMessage::Pong {
                        timestamp: *timestamp,
                    }),
                    ShoalMessage::JoinRequest { .. } => {
                        Ok(ShoalMessage::JoinResponse { members: vec![] })
                    }
                    _ => Err(NetError::Serialization("unexpected message".into())),
                }
            } else {
                Err(NetError::Connect("mock: ping failed".into()))
            }
        }
    }

    /// Create a test EndpointAddr from a node number using a valid keypair.
    fn test_addr(n: u8) -> iroh::EndpointAddr {
        let secret = iroh::SecretKey::from([n; 32]);
        let public = secret.public();
        iroh::EndpointAddr::new(public)
    }

    #[tokio::test]
    async fn test_ping_pong_alive() {
        let node_id = NodeId::from([0u8; 32]);
        let cluster = ClusterState::new(node_id, 64);
        let transport = Arc::new(MockTransport::new(true));
        let address_book: AddressBook = Arc::new(RwLock::new(HashMap::new()));

        let handle = membership::start(
            node_id,
            PeerManagerConfig::test_config(),
            cluster.clone(),
            transport,
            None,
            address_book,
        );

        // Add a peer.
        let peer_id = NodeId::from([1u8; 32]);
        handle
            .add_peer(peer_id, test_addr(1), 1, 1_000_000, NodeTopology::default())
            .await;

        // Wait for a few ping rounds.
        time::sleep(Duration::from_millis(500)).await;

        // Peer should still be alive in cluster state.
        let member = cluster.get_member(&peer_id).await;
        assert!(member.is_some(), "peer should be in cluster state");
        assert_eq!(member.unwrap().state, MemberState::Alive);

        handle.leave();
        time::sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn test_no_pong_marks_suspect_then_dead() {
        let node_id = NodeId::from([0u8; 32]);
        let cluster = ClusterState::new(node_id, 64);
        let transport = Arc::new(MockTransport::new(false)); // Pings fail
        let address_book: AddressBook = Arc::new(RwLock::new(HashMap::new()));

        let config = PeerManagerConfig {
            ping_interval: Duration::from_millis(50),
            suspect_timeout: Duration::from_millis(200),
            dead_timeout: Duration::from_millis(500),
            max_failures_before_suspect: 2,
        };

        let handle = membership::start(
            node_id,
            config,
            cluster.clone(),
            transport,
            None,
            address_book,
        );

        let peer_id = NodeId::from([1u8; 32]);
        handle
            .add_peer(peer_id, test_addr(1), 1, 1_000_000, NodeTopology::default())
            .await;

        // Wait for enough failures to trigger suspect, then dead.
        let mut rx_dead = cluster.event_bus().subscribe::<MembershipDead>();

        let deadline = time::Instant::now() + Duration::from_secs(3);
        let mut got_dead = false;
        loop {
            tokio::select! {
                Some(event) = rx_dead.recv() => {
                    if event.node_id == peer_id {
                        got_dead = true;
                        break;
                    }
                }
                _ = time::sleep(Duration::from_millis(50)) => {
                    if time::Instant::now() >= deadline {
                        break;
                    }
                }
            }
        }

        assert!(got_dead, "peer should have been declared dead");

        let member = cluster.get_member(&peer_id).await.expect("should exist");
        assert_eq!(member.state, MemberState::Dead);

        handle.leave();
        time::sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn test_recovery_after_suspect() {
        let node_id = NodeId::from([0u8; 32]);
        let cluster = ClusterState::new(node_id, 64);
        let transport = Arc::new(MockTransport::new(false)); // Start with failures
        let address_book: AddressBook = Arc::new(RwLock::new(HashMap::new()));

        let config = PeerManagerConfig {
            ping_interval: Duration::from_millis(50),
            suspect_timeout: Duration::from_millis(200),
            dead_timeout: Duration::from_secs(10), // Long dead timeout
            max_failures_before_suspect: 2,
        };

        let handle = membership::start(
            node_id,
            config,
            cluster.clone(),
            transport.clone(),
            None,
            address_book,
        );

        let peer_id = NodeId::from([1u8; 32]);
        handle
            .add_peer(peer_id, test_addr(1), 1, 1_000_000, NodeTopology::default())
            .await;

        // Wait for suspect.
        time::sleep(Duration::from_millis(400)).await;

        // Now make pings succeed — peer should recover.
        transport.set_pings_succeed(true);
        time::sleep(Duration::from_millis(300)).await;

        // Peer should be alive again.
        let member = cluster.get_member(&peer_id).await.expect("should exist");
        assert_eq!(
            member.state,
            MemberState::Alive,
            "peer should have recovered to alive"
        );

        handle.leave();
        time::sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn test_handle_join_request() {
        let node_id = NodeId::from([0u8; 32]);
        let cluster = ClusterState::new(node_id, 64);
        let transport = Arc::new(MockTransport::new(true));
        let address_book: AddressBook = Arc::new(RwLock::new(HashMap::new()));

        let handle = membership::start(
            node_id,
            PeerManagerConfig::test_config(),
            cluster.clone(),
            transport,
            None,
            address_book,
        );

        // Add an existing peer.
        let existing_id = NodeId::from([1u8; 32]);
        handle
            .add_peer(
                existing_id,
                test_addr(1),
                1,
                1_000_000,
                NodeTopology::default(),
            )
            .await;

        // Handle a join request from a new node.
        let new_id = NodeId::from([2u8; 32]);
        let response = handle
            .handle_join_request(new_id, 1, 2_000_000, NodeTopology::default(), test_addr(2))
            .await;

        // Response should contain both the local node and the existing peer.
        match response {
            ShoalMessage::JoinResponse { members } => {
                assert!(
                    members.len() >= 2,
                    "response should contain at least 2 members, got {}",
                    members.len()
                );
                let node_ids: Vec<NodeId> = members.iter().map(|m| m.node_id).collect();
                assert!(node_ids.contains(&node_id), "should include local node");
                assert!(
                    node_ids.contains(&existing_id),
                    "should include existing peer"
                );
            }
            other => panic!("expected JoinResponse, got: {other:?}"),
        }

        // The new node should now be in the cluster state.
        let member = cluster.get_member(&new_id).await;
        assert!(member.is_some(), "new node should be in cluster state");

        handle.leave();
        time::sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn test_peer_manager_with_multiple_nodes() {
        let node_id = NodeId::from([0u8; 32]);
        let cluster = ClusterState::new(node_id, 64);
        let transport = Arc::new(MockTransport::new(true));
        let address_book: AddressBook = Arc::new(RwLock::new(HashMap::new()));

        let handle = membership::start(
            node_id,
            PeerManagerConfig::test_config(),
            cluster.clone(),
            transport,
            None,
            address_book,
        );

        // Add 5 peers.
        for i in 1..=5u8 {
            handle
                .add_peer(
                    NodeId::from([i; 32]),
                    test_addr(i),
                    1,
                    1_000_000_000,
                    NodeTopology::default(),
                )
                .await;
        }

        // Wait for some ping rounds.
        time::sleep(Duration::from_millis(500)).await;

        // All peers should be alive.
        assert_eq!(cluster.member_count().await, 5);
        assert_eq!(cluster.alive_count().await, 5);

        // Ring should have 5 nodes.
        let ring = cluster.ring().await;
        assert_eq!(ring.node_count(), 5);

        handle.leave();
        time::sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn test_leave_gracefully() {
        let node_id = NodeId::from([0u8; 32]);
        let cluster = ClusterState::new(node_id, 64);
        let transport = Arc::new(MockTransport::new(true));
        let address_book: AddressBook = Arc::new(RwLock::new(HashMap::new()));

        let handle = membership::start(
            node_id,
            PeerManagerConfig::test_config(),
            cluster,
            transport,
            None,
            address_book,
        );

        // Add a peer.
        handle
            .add_peer(
                NodeId::from([1u8; 32]),
                test_addr(1),
                1,
                1_000_000,
                NodeTopology::default(),
            )
            .await;

        assert!(handle.is_running(), "should be running");

        handle.leave();

        // Wait for it to shut down.
        let deadline = time::Instant::now() + Duration::from_secs(3);
        loop {
            if !handle.is_running() {
                break;
            }
            if time::Instant::now() >= deadline {
                break;
            }
            time::sleep(Duration::from_millis(50)).await;
        }

        assert!(!handle.is_running(), "should have stopped after leave");
    }
}
