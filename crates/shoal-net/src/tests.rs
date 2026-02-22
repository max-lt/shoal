//! Tests for the shoal-net crate.

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;

    use bytes::Bytes;
    use iroh_quinn::TokioRuntime;
    use rustls_pki_types::PrivateKeyDer;
    use shoal_types::ShardId;

    use crate::message::ShoalMessage;
    use crate::transport::ShoalTransport;

    /// Helper: create a pair of iroh-quinn endpoints for testing.
    ///
    /// Returns (server_endpoint, client_endpoint, server_addr).
    /// These bypass iroh's Endpoint (and its netmon requirement) by using
    /// raw iroh-quinn with self-signed TLS certificates.
    fn test_quinn_endpoints() -> (iroh_quinn::Endpoint, iroh_quinn::Endpoint, SocketAddr) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let key = PrivateKeyDer::Pkcs8(cert.key_pair.serialize_der().into());

        let server_config =
            iroh_quinn::ServerConfig::with_single_cert(vec![cert.cert.der().clone()], key).unwrap();

        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();

        let server = iroh_quinn::Endpoint::new(
            iroh_quinn::EndpointConfig::default(),
            Some(server_config),
            std::net::UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap(),
            Arc::new(TokioRuntime),
        )
        .unwrap();
        let server_addr = server.local_addr().unwrap();

        let client_config =
            iroh_quinn::ClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let client = iroh_quinn::Endpoint::new(
            iroh_quinn::EndpointConfig::default(),
            None,
            std::net::UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap(),
            Arc::new(TokioRuntime),
        )
        .unwrap();
        client.set_default_client_config(client_config);

        (server, client, server_addr)
    }

    #[tokio::test]
    async fn test_message_roundtrip_postcard() {
        // Verify all message variants serialize/deserialize correctly.
        let messages = vec![
            ShoalMessage::ShardPush {
                shard_id: ShardId::from_data(b"test shard"),
                data: b"hello world".to_vec(),
            },
            ShoalMessage::ShardRequest {
                shard_id: ShardId::from_data(b"test shard"),
            },
            ShoalMessage::ShardResponse {
                shard_id: ShardId::from_data(b"test shard"),
                data: Some(b"hello world".to_vec()),
            },
            ShoalMessage::ShardResponse {
                shard_id: ShardId::from_data(b"test shard"),
                data: None,
            },
            ShoalMessage::Ping {
                timestamp: 1234567890,
            },
            ShoalMessage::Pong {
                timestamp: 1234567890,
            },
        ];

        for msg in &messages {
            let encoded = postcard::to_allocvec(msg).unwrap();
            let decoded: ShoalMessage = postcard::from_bytes(&encoded).unwrap();
            assert_eq!(msg, &decoded);
        }
    }

    #[tokio::test]
    async fn test_send_shard_between_two_endpoints() {
        // Spin up two endpoints in-process. Send a shard from A to B.
        let (server, client, server_addr) = test_quinn_endpoints();

        let shard_data = b"hello shard data for testing";
        let shard_id = ShardId::from_data(shard_data);

        // Spawn an accept loop on B that receives a ShardPush.
        let (tx, rx) = tokio::sync::oneshot::channel::<ShoalMessage>();

        tokio::spawn({
            let server = server.clone();
            async move {
                let incoming = server.accept().await.expect("no incoming");
                let conn = incoming.await.expect("accept failed");
                let mut recv = conn.accept_uni().await.expect("accept_uni failed");
                let msg = ShoalTransport::recv_message(&mut recv)
                    .await
                    .expect("recv failed");
                let _ = tx.send(msg);
            }
        });

        // Connect from A to B and send a shard.
        let conn = client
            .connect(server_addr, "localhost")
            .unwrap()
            .await
            .expect("connect failed");

        let msg = ShoalMessage::ShardPush {
            shard_id,
            data: shard_data.to_vec(),
        };
        let mut send = conn.open_uni().await.expect("open_uni failed");
        ShoalTransport::send_on_stream(&mut send, &msg)
            .await
            .expect("send failed");

        // Verify B received the shard intact.
        let received_msg = rx.await.expect("receiver dropped");
        match received_msg {
            ShoalMessage::ShardPush {
                shard_id: recv_id,
                data,
            } => {
                assert_eq!(recv_id, shard_id);
                assert_eq!(data, shard_data);
            }
            other => panic!("expected ShardPush, got: {other:?}"),
        }

        server.close(0u32.into(), b"done");
        client.close(0u32.into(), b"done");
    }

    #[tokio::test]
    async fn test_pull_shard_between_two_endpoints() {
        // B holds a shard. A pulls it via request/response on a bi-directional stream.
        let shard_data = b"data to be pulled from B";
        let shard_id = ShardId::from_data(shard_data);

        let (server, client, server_addr) = test_quinn_endpoints();

        // Signal to let the server know the client is done reading.
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();

        // Spawn B: accept bi-stream, read request, send response, wait for client.
        tokio::spawn({
            let server = server.clone();
            let shard_data = shard_data.to_vec();
            let shard_id = shard_id;
            async move {
                let incoming = server.accept().await.expect("no incoming");
                let conn = incoming.await.expect("accept failed");
                let (mut send, mut recv) = conn.accept_bi().await.expect("accept_bi failed");

                let request = ShoalTransport::recv_message(&mut recv)
                    .await
                    .expect("recv failed");
                match request {
                    ShoalMessage::ShardRequest { shard_id: req_id } => {
                        assert_eq!(req_id, shard_id);
                        let response = ShoalMessage::ShardResponse {
                            shard_id,
                            data: Some(shard_data),
                        };
                        ShoalTransport::send_on_stream(&mut send, &response)
                            .await
                            .expect("send response failed");
                    }
                    other => panic!("expected ShardRequest, got: {other:?}"),
                }
                // Keep connection alive until client is done reading.
                let _ = done_rx.await;
            }
        });

        // A: open bi-stream and pull the shard.
        let conn = client
            .connect(server_addr, "localhost")
            .unwrap()
            .await
            .expect("connect failed");

        let (mut send, mut recv) = conn.open_bi().await.expect("open_bi failed");

        // Send request.
        let request = ShoalMessage::ShardRequest { shard_id };
        ShoalTransport::send_on_stream(&mut send, &request)
            .await
            .expect("send request failed");

        // Receive response.
        let response = ShoalTransport::recv_message(&mut recv)
            .await
            .expect("recv response failed");

        // Signal server we're done reading.
        let _ = done_tx.send(());

        match response {
            ShoalMessage::ShardResponse {
                shard_id: resp_id,
                data: Some(data),
            } => {
                assert_eq!(resp_id, shard_id);
                assert_eq!(data, shard_data);
                // Verify integrity.
                ShoalTransport::verify_shard_integrity(shard_id, &data)
                    .expect("integrity check failed");
            }
            other => panic!("expected ShardResponse with data, got: {other:?}"),
        }

        server.close(0u32.into(), b"done");
        client.close(0u32.into(), b"done");
    }

    #[tokio::test]
    async fn test_corrupted_shard_rejected_on_pull() {
        // B sends back data that doesn't match the shard_id.
        let real_data = b"real shard data";
        let shard_id = ShardId::from_data(real_data);

        let (server, client, server_addr) = test_quinn_endpoints();

        // Signal to let the server know the client is done reading.
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();

        // Spawn B: respond with WRONG data for the shard_id.
        tokio::spawn({
            let server = server.clone();
            let shard_id = shard_id;
            async move {
                let incoming = server.accept().await.expect("no incoming");
                let conn = incoming.await.expect("accept failed");
                let (mut send, mut recv) = conn.accept_bi().await.expect("accept_bi failed");

                let _request = ShoalTransport::recv_message(&mut recv)
                    .await
                    .expect("recv failed");

                // Send corrupt data: different content but same shard_id in header.
                let response = ShoalMessage::ShardResponse {
                    shard_id,
                    data: Some(b"CORRUPTED DATA".to_vec()),
                };
                ShoalTransport::send_on_stream(&mut send, &response)
                    .await
                    .expect("send response failed");

                // Keep connection alive until client is done reading.
                let _ = done_rx.await;
            }
        });

        // A: open bi-stream and pull the shard, then verify integrity manually.
        let conn = client
            .connect(server_addr, "localhost")
            .unwrap()
            .await
            .expect("connect failed");

        let (mut send, mut recv) = conn.open_bi().await.expect("open_bi failed");

        let request = ShoalMessage::ShardRequest { shard_id };
        ShoalTransport::send_on_stream(&mut send, &request)
            .await
            .expect("send request failed");

        let response = ShoalTransport::recv_message(&mut recv)
            .await
            .expect("recv response failed");

        // Signal server we're done reading.
        let _ = done_tx.send(());

        match response {
            ShoalMessage::ShardResponse {
                data: Some(data), ..
            } => {
                // Integrity check should FAIL because the data is corrupted.
                let result = ShoalTransport::verify_shard_integrity(shard_id, &data);
                assert!(
                    result.is_err(),
                    "expected integrity failure, got: {result:?}"
                );
                let err = result.unwrap_err();
                assert!(
                    err.to_string().contains("integrity check failed"),
                    "expected integrity error message, got: {err}"
                );
            }
            other => panic!("expected ShardResponse with data, got: {other:?}"),
        }

        server.close(0u32.into(), b"done");
        client.close(0u32.into(), b"done");
    }

    #[tokio::test]
    async fn test_verify_shard_integrity_valid() {
        let data = b"valid data";
        let id = ShardId::from_data(data);
        assert!(ShoalTransport::verify_shard_integrity(id, data).is_ok());
    }

    #[tokio::test]
    async fn test_verify_shard_integrity_invalid() {
        let data = b"original data";
        let id = ShardId::from_data(data);
        let result = ShoalTransport::verify_shard_integrity(id, b"tampered data");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_push_shard_via_transport() {
        // Test sending a shard via uni-directional stream and verifying on receiver side.
        let shard_data = Bytes::from_static(b"push via transport test");
        let shard_id = ShardId::from_data(&shard_data);

        let (server, client, server_addr) = test_quinn_endpoints();

        let (tx, rx) = tokio::sync::oneshot::channel::<ShoalMessage>();

        tokio::spawn({
            let server = server.clone();
            async move {
                let incoming = server.accept().await.expect("no incoming");
                let conn = incoming.await.expect("accept failed");
                let mut recv = conn.accept_uni().await.expect("accept_uni failed");
                let msg = ShoalTransport::recv_message(&mut recv)
                    .await
                    .expect("recv failed");
                let _ = tx.send(msg);
            }
        });

        let conn = client
            .connect(server_addr, "localhost")
            .unwrap()
            .await
            .expect("connect failed");

        let msg = ShoalMessage::ShardPush {
            shard_id,
            data: shard_data.to_vec(),
        };
        let mut send = conn.open_uni().await.expect("open_uni failed");
        ShoalTransport::send_on_stream(&mut send, &msg)
            .await
            .expect("send failed");

        let received_msg = rx.await.expect("receiver dropped");
        match received_msg {
            ShoalMessage::ShardPush { data, .. } => {
                // Verify integrity on receiver side.
                let actual_id = ShardId::from_data(&data);
                assert_eq!(actual_id, shard_id, "received shard data integrity check");
            }
            other => panic!("expected ShardPush, got: {other:?}"),
        }

        server.close(0u32.into(), b"done");
        client.close(0u32.into(), b"done");
    }
}
