//! S3 API request handlers.
//!
//! Each public function handles one (or dispatches between) S3 operation(s).
//! Query parameters determine whether a PUT is `PutObject` vs `UploadPart`,
//! and whether a POST is `InitiateMultipartUpload` vs `CompleteMultipartUpload`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, Response, StatusCode};
use tracing::info;

use crate::AppState;
use crate::error::S3Error;
use crate::xml;

/// Atomic counter for generating unique upload IDs.
static UPLOAD_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Generate a unique multipart upload ID using an atomic counter + blake3.
fn generate_upload_id() -> String {
    let count = UPLOAD_COUNTER.fetch_add(1, Ordering::Relaxed);
    let hash = blake3::hash(&count.to_le_bytes());
    format!("{hash}")
}

/// Convert an `EngineError` to an `S3Error`, mapping `ObjectNotFound` to `NoSuchKey`.
fn engine_to_s3(e: shoal_engine::EngineError, bucket: &str, key: &str) -> S3Error {
    match e {
        shoal_engine::EngineError::ObjectNotFound { .. } => S3Error::NoSuchKey {
            bucket: bucket.to_string(),
            key: key.to_string(),
        },
        other => S3Error::Engine(other),
    }
}

// -----------------------------------------------------------------------
// PUT /{bucket} — CreateBucket
// -----------------------------------------------------------------------

/// Create a bucket. Buckets are namespaces in the key mapping — this is a no-op.
pub(crate) async fn create_bucket(
    Path(bucket): Path<String>,
) -> Result<axum::response::Response, S3Error> {
    info!(bucket = %bucket, "create_bucket");
    Ok(Response::builder()
        .status(StatusCode::OK)
        .body(Body::empty())
        .unwrap())
}

// -----------------------------------------------------------------------
// GET /{bucket}?list-type=2&prefix=... — ListObjectsV2
// -----------------------------------------------------------------------

/// List objects in a bucket, optionally filtered by prefix.
pub(crate) async fn list_objects(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    Query(params): Query<BTreeMap<String, String>>,
) -> Result<axum::response::Response, S3Error> {
    let prefix = params.get("prefix").map(|s| s.as_str()).unwrap_or("");
    tracing::debug!(bucket = %bucket, prefix, "list_objects");

    let keys = state.engine.list_objects(&bucket, prefix)?;
    let body = xml::list_objects_v2(&bucket, prefix, &keys);

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(Body::from(body))
        .unwrap())
}

// -----------------------------------------------------------------------
// PUT /{bucket}/{*key} — PutObject or UploadPart
// -----------------------------------------------------------------------

/// Handle PUT for objects — dispatches between PutObject and UploadPart
/// based on query parameters.
pub(crate) async fn put_object_handler(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(params): Query<BTreeMap<String, String>>,
    headers: HeaderMap,
    body: bytes::Bytes,
) -> Result<axum::response::Response, S3Error> {
    // If uploadId and partNumber are present, this is an UploadPart request.
    if let (Some(upload_id), Some(part_number_str)) =
        (params.get("uploadId"), params.get("partNumber"))
    {
        return upload_part(&state, &bucket, &key, upload_id, part_number_str, body).await;
    }

    // Regular PutObject.
    let mut metadata = BTreeMap::new();

    // Capture Content-Type.
    if let Some(ct) = headers.get("content-type")
        && let Ok(v) = ct.to_str()
    {
        metadata.insert("content-type".to_string(), v.to_string());
    }

    // Capture x-amz-meta-* headers as user metadata.
    for (name, value) in &headers {
        if let Some(meta_key) = name.as_str().strip_prefix("x-amz-meta-")
            && let Ok(v) = value.to_str()
        {
            metadata.insert(meta_key.to_string(), v.to_string());
        }
    }

    let object_id = state
        .engine
        .put_object(&bucket, &key, &body, metadata)
        .await?;
    let etag = format!("\"{object_id}\"");

    info!(bucket = %bucket, key = %key, %object_id, "put_object");

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("etag", etag)
        .body(Body::empty())
        .unwrap())
}

/// Handle an UploadPart request within a multipart upload.
async fn upload_part(
    state: &AppState,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number_str: &str,
    body: bytes::Bytes,
) -> Result<axum::response::Response, S3Error> {
    let part_number: u16 = part_number_str
        .parse()
        .map_err(|_| S3Error::InvalidPartNumber {
            part_number: part_number_str.to_string(),
        })?;

    let etag = format!("\"{}\"", blake3::hash(&body));

    {
        let mut uploads = state.uploads.write().await;
        let upload = uploads
            .get_mut(upload_id)
            .ok_or_else(|| S3Error::NoSuchUpload {
                upload_id: upload_id.to_string(),
            })?;

        if upload.bucket != bucket || upload.key != key {
            return Err(S3Error::NoSuchUpload {
                upload_id: upload_id.to_string(),
            });
        }

        upload.parts.insert(part_number, body.to_vec());
    }

    tracing::debug!(upload_id, part_number, "upload_part");

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("etag", etag)
        .body(Body::empty())
        .unwrap())
}

// -----------------------------------------------------------------------
// GET /{bucket}/{*key} — GetObject
// -----------------------------------------------------------------------

/// Retrieve an object and return it in the response body.
pub(crate) async fn get_object_handler(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<axum::response::Response, S3Error> {
    let (data, manifest) = state
        .engine
        .get_object(&bucket, &key)
        .await
        .map_err(|e| engine_to_s3(e, &bucket, &key))?;

    let etag = format!("\"{0}\"", manifest.object_id);

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header("etag", &etag)
        .header("content-length", data.len().to_string());

    if let Some(ct) = manifest.metadata.get("content-type") {
        builder = builder.header("content-type", ct);
    }

    for (k, v) in &manifest.metadata {
        if k != "content-type" {
            builder = builder.header(format!("x-amz-meta-{k}"), v);
        }
    }

    Ok(builder.body(Body::from(data)).unwrap())
}

// -----------------------------------------------------------------------
// DELETE /{bucket}/{*key} — DeleteObject
// -----------------------------------------------------------------------

/// Delete an object.
pub(crate) async fn delete_object_handler(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<axum::response::Response, S3Error> {
    state
        .engine
        .delete_object(&bucket, &key)
        .await
        .map_err(|e| engine_to_s3(e, &bucket, &key))?;

    info!(bucket = %bucket, key = %key, "delete_object");

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap())
}

// -----------------------------------------------------------------------
// HEAD /{bucket}/{*key} — HeadObject
// -----------------------------------------------------------------------

/// Return object metadata without fetching the body.
pub(crate) async fn head_object_handler(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<axum::response::Response, S3Error> {
    let manifest = state
        .engine
        .head_object(&bucket, &key)
        .map_err(|e| engine_to_s3(e, &bucket, &key))?;

    let etag = format!("\"{0}\"", manifest.object_id);

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header("etag", &etag)
        .header("content-length", manifest.total_size.to_string());

    if let Some(ct) = manifest.metadata.get("content-type") {
        builder = builder.header("content-type", ct);
    }

    for (k, v) in &manifest.metadata {
        if k != "content-type" {
            builder = builder.header(format!("x-amz-meta-{k}"), v);
        }
    }

    Ok(builder.body(Body::empty()).unwrap())
}

// -----------------------------------------------------------------------
// POST /{bucket}/{*key} — InitiateMultipartUpload or CompleteMultipartUpload
// -----------------------------------------------------------------------

/// Handle POST for objects — dispatches between InitiateMultipartUpload
/// and CompleteMultipartUpload based on query parameters.
pub(crate) async fn post_object_handler(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(params): Query<BTreeMap<String, String>>,
    headers: HeaderMap,
    body: bytes::Bytes,
) -> Result<axum::response::Response, S3Error> {
    if params.contains_key("uploads") {
        return initiate_multipart(&state, &bucket, &key, &headers).await;
    }

    if let Some(upload_id) = params.get("uploadId") {
        return complete_multipart(&state, &bucket, &key, upload_id, &body).await;
    }

    Err(S3Error::InvalidRequest {
        message: "unsupported POST operation".to_string(),
    })
}

/// Initiate a new multipart upload.
async fn initiate_multipart(
    state: &AppState,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
) -> Result<axum::response::Response, S3Error> {
    let upload_id = generate_upload_id();

    let mut metadata = BTreeMap::new();
    if let Some(ct) = headers.get("content-type")
        && let Ok(v) = ct.to_str()
    {
        metadata.insert("content-type".to_string(), v.to_string());
    }
    for (name, value) in headers {
        if let Some(meta_key) = name.as_str().strip_prefix("x-amz-meta-")
            && let Ok(v) = value.to_str()
        {
            metadata.insert(meta_key.to_string(), v.to_string());
        }
    }

    {
        let mut uploads = state.uploads.write().await;
        uploads.insert(
            upload_id.clone(),
            crate::MultipartUpload {
                bucket: bucket.to_string(),
                key: key.to_string(),
                parts: BTreeMap::new(),
                metadata,
            },
        );
    }

    info!(bucket, key, upload_id = %upload_id, "initiate_multipart_upload");

    let body = xml::initiate_multipart_upload(bucket, key, &upload_id);
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(Body::from(body))
        .unwrap())
}

/// Complete a multipart upload by assembling parts and storing the object.
async fn complete_multipart(
    state: &AppState,
    bucket: &str,
    key: &str,
    upload_id: &str,
    body: &[u8],
) -> Result<axum::response::Response, S3Error> {
    let body_str = String::from_utf8_lossy(body);
    let requested_parts = xml::parse_complete_multipart_request(&body_str);

    // Retrieve and remove the upload.
    let upload = {
        let mut uploads = state.uploads.write().await;
        uploads
            .remove(upload_id)
            .ok_or_else(|| S3Error::NoSuchUpload {
                upload_id: upload_id.to_string(),
            })?
    };

    if upload.bucket != bucket || upload.key != key {
        return Err(S3Error::NoSuchUpload {
            upload_id: upload_id.to_string(),
        });
    }

    // Determine part ordering: use requested parts if specified, otherwise all parts in order.
    let part_keys: Vec<u16> = if requested_parts.is_empty() {
        upload.parts.keys().copied().collect()
    } else {
        requested_parts
    };

    // Assemble parts in order.
    let mut assembled = Vec::new();
    for part_num in &part_keys {
        let part_data = upload
            .parts
            .get(part_num)
            .ok_or_else(|| S3Error::InvalidRequest {
                message: format!("missing part {part_num}"),
            })?;
        assembled.extend_from_slice(part_data);
    }

    // Store the assembled object.
    let object_id = state
        .engine
        .put_object(bucket, key, &assembled, upload.metadata)
        .await?;
    let etag = format!("{object_id}");

    info!(
        bucket,
        key,
        upload_id,
        parts = part_keys.len(),
        total_size = assembled.len(),
        "complete_multipart_upload"
    );

    let response_body = xml::complete_multipart_upload(bucket, key, &etag);
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(Body::from(response_body))
        .unwrap())
}
