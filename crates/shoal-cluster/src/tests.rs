//! Tests for the shoal-cluster crate.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use shoal_types::{ClusterEvent, MemberState, NodeId, NodeTopology};
    use tokio::sync::RwLock;
    use tokio::time;

    use crate::identity::ClusterIdentity;
    use crate::membership;
    use crate::membership::MembershipHandle;
    use crate::state::ClusterState;

    // -----------------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------------

    /// Create a test identity for node `n`.
    fn test_identity(n: u8) -> ClusterIdentity {
        ClusterIdentity::new(
            NodeId::from([n; 32]),
            1,
            1_000_000_000,
            NodeTopology::default(),
        )
    }

    /// Shared routing table used by the test network.
    type RoutingTable = Arc<RwLock<HashMap<NodeId, Arc<MembershipHandle>>>>;

    /// A simulated network that routes foca messages between nodes.
    ///
    /// Uses a shared routing table so that new nodes added later are
    /// visible to all existing routing tasks.
    struct TestNetwork {
        handles: RoutingTable,
        router_tasks: Arc<tokio::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    }

    impl TestNetwork {
        /// Create a test cluster with `n` nodes and wire them together.
        async fn create(n: usize) -> (Self, Vec<ClusterIdentity>) {
            let identities: Vec<ClusterIdentity> = (1..=n as u8).map(test_identity).collect();
            let routing_table: RoutingTable = Arc::new(RwLock::new(HashMap::new()));

            // Start membership services.
            for id in &identities {
                let state = ClusterState::new(id.node_id, 64);
                let handle = membership::start(id.clone(), membership::test_config(), state, None);
                routing_table
                    .write()
                    .await
                    .insert(id.node_id, Arc::new(handle));
            }

            // Create routing tasks for each node.
            let mut router_tasks = Vec::new();
            for id in &identities {
                let task = spawn_router(id.node_id, routing_table.clone());
                router_tasks.push(task);
            }

            let network = TestNetwork {
                handles: routing_table,
                router_tasks: Arc::new(tokio::sync::Mutex::new(router_tasks)),
            };

            (network, identities)
        }

        /// Get the handle for a specific node.
        async fn handle(&self, node_id: &NodeId) -> Arc<MembershipHandle> {
            self.handles.read().await[node_id].clone()
        }

        /// Get the cluster state for a specific node.
        async fn state(&self, node_id: &NodeId) -> Arc<ClusterState> {
            self.handles.read().await[node_id].state().clone()
        }

        /// Add a new node to the network.
        async fn add_node(&self, identity: ClusterIdentity) {
            let state = ClusterState::new(identity.node_id, 64);
            let handle =
                membership::start(identity.clone(), membership::test_config(), state, None);
            self.handles
                .write()
                .await
                .insert(identity.node_id, Arc::new(handle));

            let task = spawn_router(identity.node_id, self.handles.clone());
            self.router_tasks.lock().await.push(task);
        }

        /// Shut down the network (abort all routing tasks).
        async fn shutdown(&self) {
            for task in self.router_tasks.lock().await.iter() {
                task.abort();
            }
            for handle in self.handles.read().await.values() {
                handle.abort();
            }
        }
    }

    /// Spawn a routing task for a given node that reads outgoing messages
    /// and delivers them to the target via the shared routing table.
    fn spawn_router(node_id: NodeId, routing_table: RoutingTable) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let handle = {
                    let table = routing_table.read().await;
                    match table.get(&node_id) {
                        Some(h) => h.clone(),
                        None => break,
                    }
                };

                match handle.next_outgoing().await {
                    Some((target, data)) => {
                        let table = routing_table.read().await;
                        if let Some(target_handle) = table.get(&target.node_id) {
                            let _ = target_handle.feed_data(data);
                        }
                    }
                    None => break,
                }
            }
        })
    }

    /// Wait for a condition to become true within a timeout.
    async fn wait_for<F, Fut>(timeout: Duration, poll_interval: Duration, condition: F)
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let deadline = time::Instant::now() + timeout;
        loop {
            if condition().await {
                return;
            }
            if time::Instant::now() >= deadline {
                panic!("condition not met within {timeout:?}");
            }
            time::sleep(poll_interval).await;
        }
    }

    // -----------------------------------------------------------------------
    // Identity tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_cluster_identity_foca_traits() {
        use foca::Identity;

        let id = test_identity(1);
        assert_eq!(id.addr(), NodeId::from([1; 32]));

        // Renewal bumps generation.
        let renewed = id.renew().expect("should renew");
        assert_eq!(renewed.generation, 2);
        assert_eq!(renewed.node_id, id.node_id);

        // Renewed identity wins conflict.
        assert!(renewed.win_addr_conflict(&id));
        assert!(!id.win_addr_conflict(&renewed));
    }

    #[test]
    fn test_cluster_identity_to_member_conversion() {
        use shoal_types::Member;

        let id = test_identity(1);
        let member: Member = id.clone().into();

        assert_eq!(member.node_id, id.node_id);
        assert_eq!(member.generation, id.generation);
        assert_eq!(member.capacity, id.capacity);
        assert_eq!(member.state, MemberState::Alive);
    }

    #[test]
    fn test_member_to_cluster_identity_conversion() {
        use shoal_types::Member;

        let member = Member {
            node_id: NodeId::from([5; 32]),
            capacity: 999,
            state: MemberState::Dead,
            generation: 42,
            topology: NodeTopology::default(),
        };

        let id: ClusterIdentity = member.into();
        assert_eq!(id.node_id, NodeId::from([5; 32]));
        assert_eq!(id.generation, 42);
        assert_eq!(id.capacity, 999);
    }

    #[test]
    fn test_cluster_identity_postcard_roundtrip() {
        let id = test_identity(1);
        let encoded = postcard::to_allocvec(&id).expect("encode");
        let decoded: ClusterIdentity = postcard::from_bytes(&encoded).expect("decode");
        assert_eq!(id, decoded);
    }

    // -----------------------------------------------------------------------
    // ClusterState tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_cluster_state_add_member() {
        let state = ClusterState::new(NodeId::from([0; 32]), 64);
        let member: shoal_types::Member = test_identity(1).into();

        state.add_member(member.clone()).await;

        assert_eq!(state.member_count().await, 1);
        let retrieved = state.get_member(&member.node_id).await;
        assert_eq!(retrieved, Some(member));
    }

    #[tokio::test]
    async fn test_cluster_state_remove_member() {
        let state = ClusterState::new(NodeId::from([0; 32]), 64);
        let member: shoal_types::Member = test_identity(1).into();

        state.add_member(member.clone()).await;
        assert_eq!(state.member_count().await, 1);

        state.remove_member(&member.node_id).await;
        assert_eq!(state.member_count().await, 0);
        assert_eq!(state.get_member(&member.node_id).await, None);
    }

    #[tokio::test]
    async fn test_cluster_state_mark_dead() {
        let state = ClusterState::new(NodeId::from([0; 32]), 64);
        let member: shoal_types::Member = test_identity(1).into();

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

        let m1: shoal_types::Member = test_identity(1).into();
        let m2: shoal_types::Member = test_identity(2).into();

        state.add_member(m1).await;
        assert_eq!(state.ring().await.node_count(), 1);

        state.add_member(m2).await;
        assert_eq!(state.ring().await.node_count(), 2);
    }

    #[tokio::test]
    async fn test_cluster_state_ring_recomputed_on_remove() {
        let state = ClusterState::new(NodeId::from([0; 32]), 64);

        let m1: shoal_types::Member = test_identity(1).into();
        let m2: shoal_types::Member = test_identity(2).into();

        state.add_member(m1.clone()).await;
        state.add_member(m2).await;
        assert_eq!(state.ring().await.node_count(), 2);

        state.remove_member(&m1.node_id).await;
        assert_eq!(state.ring().await.node_count(), 1);
    }

    #[tokio::test]
    async fn test_cluster_state_event_subscription() {
        let state = ClusterState::new(NodeId::from([0; 32]), 64);
        let mut rx = state.subscribe();

        let member: shoal_types::Member = test_identity(1).into();
        state.add_member(member.clone()).await;

        let event = rx.recv().await.expect("should receive event");
        match event {
            ClusterEvent::NodeJoined(m) => assert_eq!(m.node_id, member.node_id),
            other => panic!("expected NodeJoined, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_cluster_state_event_on_dead() {
        let state = ClusterState::new(NodeId::from([0; 32]), 64);
        let mut rx = state.subscribe();

        let member: shoal_types::Member = test_identity(1).into();
        state.add_member(member.clone()).await;
        let _ = rx.recv().await; // consume NodeJoined

        state.mark_dead(&member.node_id).await;

        let event = rx.recv().await.expect("should receive event");
        match event {
            ClusterEvent::NodeDead(id) => assert_eq!(id, member.node_id),
            other => panic!("expected NodeDead, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // MembershipService tests — cluster via simulated network
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_three_nodes_discover_each_other() {
        let (network, identities) = TestNetwork::create(3).await;

        let seed = identities[0].clone();
        network
            .handle(&identities[1].node_id)
            .await
            .join(seed.clone())
            .expect("join");
        network
            .handle(&identities[2].node_id)
            .await
            .join(seed)
            .expect("join");

        let net = &network;
        let ids = &identities;
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(50),
            || async {
                let mut ok = true;
                for id in ids {
                    if net.state(&id.node_id).await.member_count().await < 2 {
                        ok = false;
                    }
                }
                ok
            },
        )
        .await;

        for id in &identities {
            let members = network.state(&id.node_id).await.members().await;
            assert_eq!(
                members.len(),
                2,
                "node {} should see 2 other members, sees {}",
                id.node_id,
                members.len()
            );
        }

        network.shutdown().await;
    }

    #[tokio::test]
    async fn test_kill_node_others_detect_dead() {
        let (network, identities) = TestNetwork::create(3).await;

        let seed = identities[0].clone();
        network
            .handle(&identities[1].node_id)
            .await
            .join(seed.clone())
            .expect("join");
        network
            .handle(&identities[2].node_id)
            .await
            .join(seed)
            .expect("join");

        let net = &network;
        let ids = &identities;
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(50),
            || async {
                for id in ids {
                    if net.state(&id.node_id).await.member_count().await < 2 {
                        return false;
                    }
                }
                true
            },
        )
        .await;

        let killed_id = identities[2].node_id;
        network.handle(&killed_id).await.abort();

        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(50),
            || async {
                let s1 = net
                    .state(&identities[0].node_id)
                    .await
                    .get_member(&killed_id)
                    .await;
                let s2 = net
                    .state(&identities[1].node_id)
                    .await
                    .get_member(&killed_id)
                    .await;
                let d1 = s1.as_ref().is_some_and(|m| m.state == MemberState::Dead);
                let d2 = s2.as_ref().is_some_and(|m| m.state == MemberState::Dead);
                d1 && d2
            },
        )
        .await;

        network.shutdown().await;
    }

    #[tokio::test]
    async fn test_new_node_joins_existing_cluster() {
        let (network, identities) = TestNetwork::create(2).await;

        let seed = identities[0].clone();
        network
            .handle(&identities[1].node_id)
            .await
            .join(seed.clone())
            .expect("join");

        // Wait for initial convergence.
        let net = &network;
        let ids = &identities;
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(50),
            || async {
                let c1 = net.state(&ids[0].node_id).await.member_count().await;
                let c2 = net.state(&ids[1].node_id).await.member_count().await;
                c1 >= 1 && c2 >= 1
            },
        )
        .await;

        // Add node 3.
        let new_id = test_identity(3);
        network.add_node(new_id.clone()).await;
        network
            .handle(&new_id.node_id)
            .await
            .join(seed)
            .expect("join");

        // Wait for all 3 to converge.
        let new_node_id = new_id.node_id;
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(50),
            || async {
                let c1 = net.state(&ids[0].node_id).await.member_count().await;
                let c2 = net.state(&ids[1].node_id).await.member_count().await;
                let c3 = net.state(&new_node_id).await.member_count().await;
                c1 >= 2 && c2 >= 2 && c3 >= 2
            },
        )
        .await;

        for id in &identities {
            let has_new = network
                .state(&id.node_id)
                .await
                .get_member(&new_id.node_id)
                .await;
            assert!(
                has_new.is_some(),
                "node {} should know about new node {}",
                id.node_id,
                new_id.node_id
            );
        }

        network.shutdown().await;
    }

    #[tokio::test]
    async fn test_ring_recomputed_on_membership_change() {
        let (network, identities) = TestNetwork::create(3).await;

        let seed = identities[0].clone();
        network
            .handle(&identities[1].node_id)
            .await
            .join(seed.clone())
            .expect("join");
        network
            .handle(&identities[2].node_id)
            .await
            .join(seed)
            .expect("join");

        let net = &network;
        let ids = &identities;
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(50),
            || async { net.state(&ids[0].node_id).await.member_count().await >= 2 },
        )
        .await;

        let ring = network.state(&identities[0].node_id).await.ring().await;
        assert_eq!(ring.node_count(), 2);

        network.shutdown().await;
    }

    #[tokio::test]
    async fn test_events_received_by_subscribers() {
        let (network, identities) = TestNetwork::create(2).await;
        let mut rx = network.state(&identities[0].node_id).await.subscribe();

        network
            .handle(&identities[1].node_id)
            .await
            .join(identities[0].clone())
            .expect("join");

        let deadline = time::Instant::now() + Duration::from_secs(5);
        let mut received_join = false;
        loop {
            tokio::select! {
                Ok(event) = rx.recv() => {
                    if let ClusterEvent::NodeJoined(member) = event {
                        if member.node_id == identities[1].node_id {
                            received_join = true;
                            break;
                        }
                    }
                }
                _ = time::sleep(Duration::from_millis(50)) => {
                    if time::Instant::now() >= deadline {
                        break;
                    }
                }
            }
        }

        assert!(received_join, "should have received NodeJoined event");

        network.shutdown().await;
    }

    // -----------------------------------------------------------------------
    // Foca member_buf panic reproduction (foca 0.17.2 bug)
    // -----------------------------------------------------------------------

    /// Encode a foca SWIM message using PostcardCodec.
    ///
    /// This crafts raw bytes that can be fed to a foca instance via handle_data.
    fn encode_swim_message(
        header: foca::Header<ClusterIdentity>,
        updates: &[foca::Member<ClusterIdentity>],
    ) -> Vec<u8> {
        use bytes::BufMut;
        use foca::Codec;

        let mut codec = foca::PostcardCodec;
        let mut buf = bytes::BytesMut::new();

        codec
            .encode_header(&header, &mut buf)
            .expect("encode header");

        if !updates.is_empty() {
            buf.put_u16(updates.len() as u16);
            for member in updates {
                codec
                    .encode_member(member, &mut buf)
                    .expect("encode member");
            }
        }

        buf.to_vec()
    }

    /// Verify that foca 1.0 handles suspect-self correctly without panicking.
    ///
    /// foca 0.17.2 had a bug where receiving a Suspect update about the local
    /// node triggered a debug_assert panic (member_buf modified while taken).
    /// foca 1.0 fixes this — handle_data should succeed.
    #[tokio::test]
    async fn test_foca_handles_suspect_self_without_panic() {
        use foca::{AccumulatingRuntime, PostcardCodec};
        use rand::SeedableRng;
        use rand::rngs::SmallRng;

        let id_a = test_identity(1);
        let id_b = test_identity(2);
        let id_c = test_identity(3);

        // Create a foca instance for Node B.
        let rng = SmallRng::from_os_rng();
        let codec = PostcardCodec;
        let config = membership::test_config();
        let mut foca = foca::Foca::new(id_b.clone(), config, rng, codec);
        let mut runtime = AccumulatingRuntime::new();

        // Add two peers so gossip() has targets to send to.
        foca.apply_many(
            [
                foca::Member::alive(id_a.clone()),
                foca::Member::alive(id_c.clone()),
            ]
            .into_iter(),
            true,
            &mut runtime,
        )
        .expect("apply");
        // Drain runtime to clear accumulated events.
        while runtime.to_send().is_some() {}
        while runtime.to_schedule().is_some() {}
        while runtime.to_notify().is_some() {}

        // Craft a SWIM message from Node A to Node B that contains
        // a piggybacked Suspect update about Node B itself.
        let msg = encode_swim_message(
            foca::Header {
                src: id_a.clone(),
                src_incarnation: 0,
                dst: id_b.clone(),
                message: foca::Message::Gossip,
            },
            &[foca::Member::new(
                id_b.clone(),
                0, // incarnation
                foca::State::Suspect,
            )],
        );

        // In foca 1.0, this should succeed without panic.
        foca.handle_data(&msg, &mut runtime)
            .expect("handle_data should succeed for suspect-self in foca 1.0");
    }

    /// Verify the membership service handles suspect-self messages correctly.
    ///
    /// In foca 1.0, this is handled cleanly (no panic). The service should
    /// remain running and functional after processing the message.
    #[tokio::test]
    async fn test_membership_handles_suspect_self() {
        // Set up a 3-node network.
        let (network, identities) = TestNetwork::create(3).await;

        let seed = identities[0].clone();
        network
            .handle(&identities[1].node_id)
            .await
            .join(seed.clone())
            .expect("join");
        network
            .handle(&identities[2].node_id)
            .await
            .join(seed)
            .expect("join");

        // Wait for convergence.
        let net = &network;
        let ids = &identities;
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(50),
            || async {
                for id in ids {
                    if net.state(&id.node_id).await.member_count().await < 2 {
                        return false;
                    }
                }
                true
            },
        )
        .await;

        // Craft a Suspect message about Node 1 and feed it to Node 1.
        // This triggers the foca member_buf bug, but our workaround
        // should keep the service alive.
        let suspect_msg = encode_swim_message(
            foca::Header {
                src: identities[1].clone(),
                src_incarnation: 0,
                dst: identities[0].clone(),
                message: foca::Message::Gossip,
            },
            &[foca::Member::new(
                identities[0].clone(),
                0,
                foca::State::Suspect,
            )],
        );

        let handle = network.handle(&identities[0].node_id).await;
        // Feed the suspect message — foca 1.0 handles this cleanly.
        handle.feed_data(suspect_msg).expect("feed_data");

        // Give it time to process.
        time::sleep(Duration::from_millis(200)).await;

        // The service should still be running.
        assert!(
            handle.is_running(),
            "membership service should survive the foca bug"
        );

        // The node should still respond to SWIM messages (cluster is still functional).
        let count = network
            .state(&identities[0].node_id)
            .await
            .member_count()
            .await;
        assert!(
            count >= 1,
            "node should still have members after processing suspect-self"
        );

        network.shutdown().await;
    }

    #[tokio::test]
    async fn test_membership_service_leave() {
        let (network, identities) = TestNetwork::create(2).await;

        let seed = identities[0].clone();
        network
            .handle(&identities[1].node_id)
            .await
            .join(seed)
            .expect("join");

        let net = &network;
        let ids = &identities;
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(50),
            || async { net.state(&ids[0].node_id).await.member_count().await >= 1 },
        )
        .await;

        network
            .handle(&identities[1].node_id)
            .await
            .leave()
            .expect("leave");

        time::sleep(Duration::from_millis(200)).await;

        let h = network.handle(&identities[1].node_id).await;
        let deadline = time::Instant::now() + Duration::from_secs(3);
        loop {
            if !h.is_running() {
                break;
            }
            if time::Instant::now() >= deadline {
                break;
            }
            time::sleep(Duration::from_millis(50)).await;
        }

        network.shutdown().await;
    }
}
