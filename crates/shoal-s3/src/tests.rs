//! Tests for the shoal-s3 crate.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use shoal_cluster::ClusterState;
use shoal_meta::MetaStore;
use shoal_store::MemoryStore;
use shoal_types::{Member, MemberState, NodeId, NodeTopology};
use tower::ServiceExt;

use crate::{S3Server, S3ServerConfig};

const TEST_MAX_BYTES: u64 = 1_000_000_000;

/// Create a test S3 router backed by a single-node engine.
async fn test_router() -> axum::Router {
    let node_id = NodeId::from([1u8; 32]);
    let store = Arc::new(MemoryStore::new(TEST_MAX_BYTES));
    let meta = Arc::new(MetaStore::open_temporary().unwrap());
    let cluster = ClusterState::new(node_id, 128);

    cluster
        .add_member(Member {
            node_id,
            capacity: TEST_MAX_BYTES,
            state: MemberState::Alive,
            generation: 1,
            topology: NodeTopology::default(),
        })
        .await;

    let engine: Arc<dyn shoal_engine::ShoalEngine> = Arc::new(shoal_engine::ShoalNode::new(
        shoal_engine::ShoalNodeConfig {
            node_id,
            chunk_size: 1024,
            erasure_k: 2,
            erasure_m: 1,
            vnodes_per_node: 128,
            shard_replication: 1,
            cache_max_bytes: u64::MAX,
        },
        store,
        meta,
        cluster,
    ));

    S3Server::new(S3ServerConfig {
        engine,
        auth_secret: None,
    })
    .into_router()
}

/// Create a test S3 router with auth enabled.
async fn test_router_with_auth(secret: &str) -> axum::Router {
    let node_id = NodeId::from([1u8; 32]);
    let store = Arc::new(MemoryStore::new(TEST_MAX_BYTES));
    let meta = Arc::new(MetaStore::open_temporary().unwrap());
    let cluster = ClusterState::new(node_id, 128);

    cluster
        .add_member(Member {
            node_id,
            capacity: TEST_MAX_BYTES,
            state: MemberState::Alive,
            generation: 1,
            topology: NodeTopology::default(),
        })
        .await;

    let engine: Arc<dyn shoal_engine::ShoalEngine> = Arc::new(shoal_engine::ShoalNode::new(
        shoal_engine::ShoalNodeConfig {
            node_id,
            chunk_size: 1024,
            erasure_k: 2,
            erasure_m: 1,
            vnodes_per_node: 128,
            shard_replication: 1,
            cache_max_bytes: u64::MAX,
        },
        store,
        meta,
        cluster,
    ));

    S3Server::new(S3ServerConfig {
        engine,
        auth_secret: Some(secret.to_string()),
    })
    .into_router()
}

/// Read the full response body as bytes.
async fn body_bytes(response: axum::response::Response) -> Vec<u8> {
    response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec()
}

/// Read the full response body as a UTF-8 string.
async fn body_string(response: axum::response::Response) -> String {
    String::from_utf8(body_bytes(response).await).unwrap()
}

// -----------------------------------------------------------------------
// PutObject + GetObject round-trip
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_put_get_object() {
    let app = test_router().await;
    let data = b"hello world, this is shoal!";

    // PUT object.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/mybucket/hello.txt")
                .header("content-type", "text/plain")
                .body(Body::from(data.as_slice()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let etag = response
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(etag.starts_with('"'));
    assert!(etag.ends_with('"'));

    // GET object.
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/mybucket/hello.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("etag").unwrap().to_str().unwrap(),
        &etag
    );
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "text/plain"
    );

    let body = body_bytes(response).await;
    assert_eq!(body, data);
}

// -----------------------------------------------------------------------
// HeadObject
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_head_object() {
    let app = test_router().await;
    let data = vec![42u8; 3000];

    // PUT.
    let _ = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/mybucket/headtest")
                .header("content-type", "application/octet-stream")
                .header("x-amz-meta-custom", "value42")
                .body(Body::from(data))
                .unwrap(),
        )
        .await
        .unwrap();

    // HEAD.
    let response = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/mybucket/headtest")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap(),
        "3000"
    );
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "application/octet-stream"
    );
    assert_eq!(
        response
            .headers()
            .get("x-amz-meta-custom")
            .unwrap()
            .to_str()
            .unwrap(),
        "value42"
    );
    assert!(response.headers().get("etag").is_some());

    // Body should be empty for HEAD.
    let body = body_bytes(response).await;
    assert!(body.is_empty());
}

// -----------------------------------------------------------------------
// ListObjectsV2 with prefix filtering
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_list_objects_with_prefix() {
    let app = test_router().await;

    // PUT several objects.
    for key in ["photos/a.jpg", "photos/b.jpg", "docs/c.txt"] {
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/mybucket/{key}"))
                    .body(Body::from("data"))
                    .unwrap(),
            )
            .await
            .unwrap();
    }

    // List with prefix=photos/.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/mybucket?list-type=2&prefix=photos/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "application/xml"
    );

    let body = body_string(response).await;
    assert!(body.contains("<KeyCount>2</KeyCount>"));
    assert!(body.contains("<Key>photos/a.jpg</Key>"));
    assert!(body.contains("<Key>photos/b.jpg</Key>"));
    assert!(!body.contains("docs/c.txt"));

    // List all (no prefix).
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/mybucket?list-type=2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = body_string(response).await;
    assert!(body.contains("<KeyCount>3</KeyCount>"));
}

// -----------------------------------------------------------------------
// DeleteObject then GetObject → 404
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_delete_then_get_404() {
    let app = test_router().await;

    // PUT.
    let _ = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/mybucket/delme")
                .body(Body::from("byebye"))
                .unwrap(),
        )
        .await
        .unwrap();

    // DELETE.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/mybucket/delme")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // GET → 404.
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/mybucket/delme")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = body_string(response).await;
    assert!(body.contains("<Code>NoSuchKey</Code>"));
}

// -----------------------------------------------------------------------
// Multipart upload: 3 parts
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_multipart_upload_3_parts() {
    let app = test_router().await;
    let part1: Vec<u8> = vec![0xAA; 1024];
    let part2: Vec<u8> = vec![0xBB; 1024];
    let part3: Vec<u8> = vec![0xCC; 512];

    // Initiate multipart upload.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mybucket/multipart.bin?uploads")
                .header("content-type", "application/octet-stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_string(response).await;
    assert!(body.contains("<UploadId>"));

    // Extract upload ID.
    let upload_id = body
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap();

    // Upload 3 parts.
    for (num, data) in [(1u16, &part1), (2, &part2), (3, &part3)] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!(
                        "/mybucket/multipart.bin?uploadId={upload_id}&partNumber={num}"
                    ))
                    .body(Body::from(data.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get("etag").is_some());
    }

    // Complete multipart upload.
    let complete_body = "\
        <CompleteMultipartUpload>\
        <Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part>\
        <Part><PartNumber>2</PartNumber><ETag>\"b\"</ETag></Part>\
        <Part><PartNumber>3</PartNumber><ETag>\"c\"</ETag></Part>\
        </CompleteMultipartUpload>";

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/mybucket/multipart.bin?uploadId={upload_id}"))
                .body(Body::from(complete_body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_string(response).await;
    assert!(body.contains("<ETag>"));

    // GET the completed object.
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/mybucket/multipart.bin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let got = body_bytes(response).await;

    let mut expected = Vec::new();
    expected.extend_from_slice(&part1);
    expected.extend_from_slice(&part2);
    expected.extend_from_slice(&part3);
    assert_eq!(got, expected);
}

// -----------------------------------------------------------------------
// CreateBucket
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_create_bucket() {
    let app = test_router().await;

    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/newbucket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

// -----------------------------------------------------------------------
// Auth: Bearer token
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_auth_required_no_header() {
    let app = test_router_with_auth("mysecret").await;

    // No auth header → 403.
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/mybucket/key")
                .body(Body::from("data"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let body = body_string(response).await;
    assert!(body.contains("AccessDenied"));
}

#[tokio::test]
async fn test_auth_required_wrong_secret() {
    let app = test_router_with_auth("mysecret").await;

    // Wrong secret → 403.
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/mybucket/key")
                .header("authorization", "Bearer wrong")
                .body(Body::from("data"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_auth_correct_secret() {
    let app = test_router_with_auth("mysecret").await;

    // Correct secret → 200.
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/mybucket/key")
                .header("authorization", "Bearer mysecret")
                .body(Body::from("data"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

// -----------------------------------------------------------------------
// GET nonexistent → 404
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_get_nonexistent_returns_404() {
    let app = test_router().await;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/mybucket/doesnotexist")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = body_string(response).await;
    assert!(body.contains("NoSuchKey"));
}

// -----------------------------------------------------------------------
// ETag is quoted blake3 hex (64 chars)
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_etag_is_blake3_hex() {
    let app = test_router().await;

    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/mybucket/etagtest")
                .body(Body::from("test data"))
                .unwrap(),
        )
        .await
        .unwrap();

    let etag = response.headers().get("etag").unwrap().to_str().unwrap();

    // ETag should be quoted with 64 hex characters inside.
    assert!(etag.starts_with('"'));
    assert!(etag.ends_with('"'));
    let hex = &etag[1..etag.len() - 1];
    assert_eq!(hex.len(), 64);
    assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
}

// -----------------------------------------------------------------------
// User metadata pass-through via x-amz-meta-*
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_user_metadata_passthrough() {
    let app = test_router().await;

    // PUT with metadata.
    let _ = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/mybucket/metadata-test")
                .header("x-amz-meta-author", "alice")
                .header("x-amz-meta-version", "2")
                .body(Body::from("content"))
                .unwrap(),
        )
        .await
        .unwrap();

    // GET and verify metadata headers.
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/mybucket/metadata-test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("x-amz-meta-author")
            .unwrap()
            .to_str()
            .unwrap(),
        "alice"
    );
    assert_eq!(
        response
            .headers()
            .get("x-amz-meta-version")
            .unwrap()
            .to_str()
            .unwrap(),
        "2"
    );
}

// -----------------------------------------------------------------------
// S3 error XML format
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_error_response_is_xml() {
    let app = test_router().await;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/mybucket/nonexistent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "application/xml"
    );

    let body = body_string(response).await;
    assert!(body.contains("<?xml version=\"1.0\""));
    assert!(body.contains("<Error>"));
    assert!(body.contains("<Code>NoSuchKey</Code>"));
    assert!(body.contains("<Message>"));
}

// -----------------------------------------------------------------------
// XML helper unit tests
// -----------------------------------------------------------------------

#[test]
fn test_xml_escape() {
    assert_eq!(
        crate::xml::xml_escape("a<b>c&d\"e'f"),
        "a&lt;b&gt;c&amp;d&quot;e&apos;f"
    );
}

// -----------------------------------------------------------------------
// Bug 3 repro: maxKeys parameter ignored in ListObjectsV2
// -----------------------------------------------------------------------

/// Reproduction of torture test Bug 3: when `max-keys=2` is passed as a
/// query parameter in ListObjectsV2, the response should only contain 2
/// keys and `IsTruncated` should be `true`. Currently the server ignores
/// `max-keys` entirely and always returns all matching objects.
#[tokio::test]
async fn test_bug3_list_objects_max_keys_ignored() {
    let app = test_router().await;

    // Write 5 objects with a shared prefix.
    for i in 0..5 {
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/mybucket/page/item-{i}"))
                    .body(Body::from(format!("value-{i}")))
                    .unwrap(),
            )
            .await
            .unwrap();
    }

    // List with max-keys=2.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/mybucket?list-type=2&prefix=page/&max-keys=2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_string(response).await;

    // BUG 3: maxKeys is ignored — all 5 objects are returned instead of 2.
    // The XML response hardcodes <MaxKeys>1000</MaxKeys> and
    // <IsTruncated>false</IsTruncated> regardless of query params.
    assert!(
        body.contains("<KeyCount>2</KeyCount>"),
        "BUG 3: expected max-keys=2 to limit results to 2, but got: {body}"
    );
    assert!(
        body.contains("<IsTruncated>true</IsTruncated>"),
        "BUG 3: response should be truncated when max-keys < total keys: {body}"
    );
    assert!(
        body.contains("<MaxKeys>2</MaxKeys>"),
        "BUG 3: MaxKeys should echo the requested value (2), not 1000: {body}"
    );
}

#[test]
fn test_parse_complete_multipart_request() {
    let body = "\
        <CompleteMultipartUpload>\
        <Part><PartNumber>3</PartNumber><ETag>\"c\"</ETag></Part>\
        <Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part>\
        <Part><PartNumber>2</PartNumber><ETag>\"b\"</ETag></Part>\
        </CompleteMultipartUpload>";

    let parts = crate::xml::parse_complete_multipart_request(body);
    assert_eq!(parts, vec![1, 2, 3]);
}

#[test]
fn test_parse_complete_multipart_request_multiline() {
    let body = r#"
<CompleteMultipartUpload>
  <Part>
    <PartNumber>1</PartNumber>
    <ETag>"abc"</ETag>
  </Part>
  <Part>
    <PartNumber>2</PartNumber>
    <ETag>"def"</ETag>
  </Part>
</CompleteMultipartUpload>
"#;

    let parts = crate::xml::parse_complete_multipart_request(body);
    assert_eq!(parts, vec![1, 2]);
}
