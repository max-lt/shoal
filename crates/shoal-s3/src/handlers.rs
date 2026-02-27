//! S3 API request handlers.
//!
//! Each public function handles one (or dispatches between) S3 operation(s).
//! Query parameters determine whether a PUT is `PutObject` vs `UploadPart`,
//! and whether a POST is `InitiateMultipartUpload` vs `CompleteMultipartUpload`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::Json;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, Response, StatusCode};
use serde::Serialize;
use tracing::info;

use crate::AppState;
use crate::error::S3Error;
use crate::xml;

/// Atomic counter for generating unique upload IDs.
static UPLOAD_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Standard HTTP headers that are stored in manifest metadata and returned as-is
/// (not prefixed with `x-amz-meta-`).
const PASSTHROUGH_HEADERS: &[&str] = &[
    "content-type",
    "cache-control",
    "content-encoding",
    "content-disposition",
];

// -----------------------------------------------------------------------
// POST /admin/keys — CreateApiKey (no auth required)
// -----------------------------------------------------------------------

/// Table of uppercase alphanumeric characters used for access key IDs.
const ALPHA_NUMERIC: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

/// Generate an access key ID: `"SHOAL"` prefix + 15 uppercase alphanumeric chars.
///
/// The result is safe to log and identifies the key without revealing any secret.
fn gen_access_key_id() -> String {
    use rand::RngCore;
    let mut rng = rand::rng();
    let mut out = String::with_capacity(20);
    out.push_str("SHOAL");

    for _ in 0..15 {
        let idx = (rng.next_u32() as usize) % ALPHA_NUMERIC.len();
        out.push(ALPHA_NUMERIC[idx] as char);
    }

    out
}

/// Generate a secret access key: 40 lowercase hex characters (20 random bytes).
///
/// Never log this value.
fn gen_secret_access_key() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 20];
    rand::rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Response body for `POST /admin/keys`.
#[derive(Serialize)]
pub(crate) struct CreateApiKeyResponse {
    pub access_key_id: String,
    pub secret_access_key: String,
}

/// Create a new API key pair, persist it in MetaStore, and return it.
///
/// Requires `Authorization: Bearer <admin_secret>`. The returned
/// `access_key_id` is safe to log; `secret_access_key` must be stored securely
/// and is never returned again.
///
/// Use `Authorization: Bearer <access_key_id>:<secret_access_key>` for subsequent
/// S3 requests.
pub(crate) async fn create_api_key(
    State(state): State<AppState>,
) -> Result<(StatusCode, Json<CreateApiKeyResponse>), S3Error> {
    let key_id = gen_access_key_id();
    let secret = gen_secret_access_key();

    // Persist to MetaStore + replicate via LogTree+gossip.
    state
        .engine
        .create_api_key(&key_id, &secret)
        .await
        .map_err(|e| S3Error::Internal {
            message: format!("failed to create api key: {e}"),
        })?;

    info!(access_key_id = %key_id, "api_key_created");

    Ok((
        StatusCode::CREATED,
        Json(CreateApiKeyResponse {
            access_key_id: key_id,
            secret_access_key: secret,
        }),
    ))
}

// -----------------------------------------------------------------------
// GET /admin/keys — ListApiKeys
// -----------------------------------------------------------------------

/// Response item for `GET /admin/keys`.
///
/// Only the access key ID is returned — secrets are never exposed.
#[derive(Serialize)]
pub(crate) struct ApiKeyInfo {
    pub access_key_id: String,
}

/// List all API key IDs (secrets are NOT returned).
///
/// Requires `Authorization: Bearer <admin_secret>`.
pub(crate) async fn list_api_keys(
    State(state): State<AppState>,
) -> Result<Json<Vec<ApiKeyInfo>>, S3Error> {
    let ids = state
        .engine
        .meta()
        .list_api_key_ids()
        .map_err(|e| S3Error::Internal {
            message: format!("failed to list api keys: {e}"),
        })?;

    let list: Vec<ApiKeyInfo> = ids
        .into_iter()
        .map(|id| ApiKeyInfo { access_key_id: id })
        .collect();

    Ok(Json(list))
}

// -----------------------------------------------------------------------
// DELETE /admin/keys/{access_key_id} — DeleteApiKey
// -----------------------------------------------------------------------

/// Delete an API key pair by access key ID.
///
/// Requires `Authorization: Bearer <admin_secret>`. Returns 204 on success,
/// 404 if the key does not exist.
pub(crate) async fn delete_api_key(
    State(state): State<AppState>,
    Path(access_key_id): Path<String>,
) -> Result<axum::response::Response, S3Error> {
    // Check the key exists in MetaStore.
    let exists = state
        .engine
        .meta()
        .get_api_key(&access_key_id)
        .map_err(|e| S3Error::Internal {
            message: format!("failed to check api key: {e}"),
        })?
        .is_some();

    if !exists {
        return Err(S3Error::InvalidRequest {
            message: format!("api key not found: {access_key_id}"),
        });
    }

    // Delete from MetaStore + replicate via LogTree+gossip.
    state
        .engine
        .delete_api_key(&access_key_id)
        .await
        .map_err(|e| S3Error::Internal {
            message: format!("failed to delete api key: {e}"),
        })?;

    info!(access_key_id = %access_key_id, "api_key_deleted");

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap())
}

// -----------------------------------------------------------------------
// POST /admin/buckets/{name} — CreateBucket (admin, no auth)
// -----------------------------------------------------------------------

pub(crate) async fn admin_create_bucket(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, S3Error> {
    state.engine.create_bucket(&name).await?;
    info!(bucket = %name, "admin_create_bucket");
    Ok(StatusCode::OK)
}

/// Generate a unique multipart upload ID using an atomic counter + blake3.
fn generate_upload_id() -> String {
    let count = UPLOAD_COUNTER.fetch_add(1, Ordering::Relaxed);
    let hash = blake3::hash(&count.to_le_bytes());
    format!("{hash}")
}

/// Parse `key=value&key=value` URL-encoded tag string (from `x-amz-tagging` header).
fn parse_url_tags(s: &str) -> BTreeMap<String, String> {
    form_urlencoded::parse(s.as_bytes())
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect()
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
// GET / — ListBuckets
// -----------------------------------------------------------------------

/// List all buckets.
pub(crate) async fn list_buckets_handler(
    State(state): State<AppState>,
) -> Result<axum::response::Response, S3Error> {
    let buckets = state.engine.list_buckets().await?;
    let body = xml::list_all_my_buckets("shoal", &buckets);

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(Body::from(body))
        .unwrap())
}

// -----------------------------------------------------------------------
// PUT /{bucket} — CreateBucket
// -----------------------------------------------------------------------

/// Create a bucket (or handle unsupported PUT sub-resources like `?policy`).
pub(crate) async fn create_bucket(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    Query(params): Query<BTreeMap<String, String>>,
) -> Result<axum::response::Response, S3Error> {
    // PUT /{bucket}?versioning → reject (we don't support enabling versioning)
    if params.contains_key("versioning") {
        return Err(S3Error::NotImplemented {
            message: "bucket versioning is not supported".to_string(),
        });
    }

    for &op in UNSUPPORTED_BUCKET_OPS {
        if params.contains_key(op) {
            return Err(S3Error::NotImplemented {
                message: format!("bucket operation '{op}' is not supported"),
            });
        }
    }

    state.engine.create_bucket(&bucket).await?;
    info!(bucket = %bucket, "create_bucket");
    Ok(Response::builder()
        .status(StatusCode::OK)
        .body(Body::empty())
        .unwrap())
}

// -----------------------------------------------------------------------
// DELETE /{bucket} — DeleteBucket
// -----------------------------------------------------------------------

/// Delete a bucket (or handle unsupported DELETE sub-resources like `?policy`).
pub(crate) async fn delete_bucket_handler(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    Query(params): Query<BTreeMap<String, String>>,
) -> Result<axum::response::Response, S3Error> {
    for &op in UNSUPPORTED_BUCKET_OPS {
        if params.contains_key(op) {
            return Err(S3Error::NotImplemented {
                message: format!("bucket operation '{op}' is not supported"),
            });
        }
    }

    state.engine.delete_bucket(&bucket).await?;
    info!(bucket = %bucket, "delete_bucket");
    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap())
}

// -----------------------------------------------------------------------
// HEAD /{bucket} — HeadBucket
// -----------------------------------------------------------------------

/// Check if a bucket exists. Returns 200 if it does, 404 otherwise.
pub(crate) async fn head_bucket_handler(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<axum::response::Response, S3Error> {
    if !state.engine.bucket_exists(&bucket).await? {
        return Err(S3Error::NoSuchBucket {
            bucket: bucket.clone(),
        });
    }

    Ok(Response::builder()
        .status(StatusCode::OK)
        .body(Body::empty())
        .unwrap())
}

// -----------------------------------------------------------------------
// GET /{bucket}?list-type=2&prefix=... — ListObjectsV2
// -----------------------------------------------------------------------

/// Bucket-level sub-resources that we don't implement.
const UNSUPPORTED_BUCKET_OPS: &[&str] = &[
    "policy",
    "lifecycle",
    "cors",
    "encryption",
    "replication",
    "tagging",
    "accelerate",
    "logging",
    "metrics",
    "analytics",
    "inventory",
    "notification",
    "publicAccessBlock",
    "object-lock",
    "intelligent-tiering",
    "website",
    "requestPayment",
    "acl",
];

/// Dispatch GET on a bucket: ListMultipartUploads or ListObjectsV2.
pub(crate) async fn list_objects(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    Query(params): Query<BTreeMap<String, String>>,
) -> Result<axum::response::Response, S3Error> {
    // Reject unsupported bucket sub-resources early.
    for &op in UNSUPPORTED_BUCKET_OPS {
        if params.contains_key(op) {
            return Err(S3Error::NotImplemented {
                message: format!("bucket operation '{op}' is not supported"),
            });
        }
    }

    // GET /{bucket}?versioning → GetBucketVersioning (always "not configured")
    if params.contains_key("versioning") {
        let body = xml::versioning_configuration();
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/xml")
            .body(Body::from(body))
            .unwrap());
    }

    // GET /{bucket}?uploads → ListMultipartUploads
    if params.contains_key("uploads") {
        return list_multipart_uploads(&state, &bucket).await;
    }

    let prefix = params.get("prefix").map(|s| s.as_str()).unwrap_or("");
    let max_keys: usize = params
        .get("max-keys")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    tracing::debug!(bucket = %bucket, prefix, max_keys, "list_objects");

    let objects = state.engine.list_objects(&bucket, prefix).await?;
    let truncated = objects.len() > max_keys;
    let returned = if truncated {
        &objects[..max_keys]
    } else {
        &objects
    };

    let body = xml::list_objects_v2(&bucket, prefix, returned, max_keys, truncated);

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(Body::from(body))
        .unwrap())
}

/// List in-progress multipart uploads for a bucket.
async fn list_multipart_uploads(
    state: &AppState,
    bucket: &str,
) -> Result<axum::response::Response, S3Error> {
    let uploads = state.uploads.read().await;
    let entries: Vec<(String, String)> = uploads
        .iter()
        .filter(|(_, u)| u.bucket == bucket)
        .map(|(id, u)| (u.key.clone(), id.clone()))
        .collect();

    let body = xml::list_multipart_uploads(bucket, &entries);
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
#[tracing::instrument(skip(state, params, headers, body), fields(response_status = tracing::field::Empty, etag = tracing::field::Empty))]
pub(crate) async fn put_object_handler(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(params): Query<BTreeMap<String, String>>,
    headers: HeaderMap,
    body: bytes::Bytes,
) -> Result<axum::response::Response, S3Error> {
    // PUT /{bucket}/{key}?tagging → PutObjectTagging
    if params.contains_key("tagging") {
        return put_object_tagging(&state, &bucket, &key, &body).await;
    }

    // If uploadId and partNumber are present, this is an UploadPart request.
    if let (Some(upload_id), Some(part_number_str)) =
        (params.get("uploadId"), params.get("partNumber"))
    {
        return upload_part(&state, &bucket, &key, upload_id, part_number_str, body).await;
    }

    // If x-amz-copy-source is present, this is a CopyObject request.
    if let Some(copy_source) = headers.get("x-amz-copy-source")
        && let Ok(source) = copy_source.to_str()
    {
        return copy_object(&state, source, &bucket, &key).await;
    }

    // Bucket must exist (standard S3 behavior).
    if !state.engine.bucket_exists(&bucket).await? {
        return Err(S3Error::NoSuchBucket {
            bucket: bucket.clone(),
        });
    }

    // Regular PutObject.
    let mut metadata = BTreeMap::new();

    // Capture standard HTTP headers.
    for &header in PASSTHROUGH_HEADERS {
        if let Some(val) = headers.get(header)
            && let Ok(v) = val.to_str()
        {
            metadata.insert(header.to_string(), v.to_string());
        }
    }

    // Capture x-amz-meta-* headers as user metadata.
    for (name, value) in &headers {
        if let Some(meta_key) = name.as_str().strip_prefix("x-amz-meta-")
            && let Ok(v) = value.to_str()
        {
            metadata.insert(meta_key.to_string(), v.to_string());
        }
    }

    // Parse x-amz-tagging header (key=value&key=value format).
    let inline_tags = headers
        .get("x-amz-tagging")
        .and_then(|v| v.to_str().ok())
        .map(parse_url_tags);

    let object_id = state
        .engine
        .put_object(&bucket, &key, &body, metadata)
        .await?;

    // Store inline tags if present.
    if let Some(tags) = inline_tags {
        let _ = state.engine.put_object_tags(&bucket, &key, tags).await;
    }
    let etag = format!("\"{object_id}\"");

    let span = tracing::Span::current();
    span.record("response_status", 200u16);
    span.record("etag", tracing::field::display(&etag));

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

/// Handle a CopyObject request.
///
/// `source` is the `x-amz-copy-source` header value: `"/bucket/key"` or `"bucket/key"`.
async fn copy_object(
    state: &AppState,
    source: &str,
    dst_bucket: &str,
    dst_key: &str,
) -> Result<axum::response::Response, S3Error> {
    // Parse source: strip leading slash, split into bucket/key.
    let source = source.strip_prefix('/').unwrap_or(source);
    let (src_bucket, src_key) = source
        .split_once('/')
        .ok_or_else(|| S3Error::InvalidRequest {
            message: format!("invalid x-amz-copy-source: {source}"),
        })?;

    let object_id = state
        .engine
        .copy_object(src_bucket, src_key, dst_bucket, dst_key)
        .await
        .map_err(|e| engine_to_s3(e, src_bucket, src_key))?;

    info!(
        src_bucket,
        src_key,
        dst_bucket,
        dst_key,
        %object_id,
        "copy_object"
    );

    let body = xml::copy_object_result(&object_id.to_string());
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(Body::from(body))
        .unwrap())
}

// -----------------------------------------------------------------------
// GET /{bucket}/{*key} — GetObject
// -----------------------------------------------------------------------

/// Dispatch GET on an object: GetObjectTagging, ListParts, or GetObject.
#[tracing::instrument(skip(state, params), fields(response_status = tracing::field::Empty, content_length = tracing::field::Empty))]
pub(crate) async fn get_object_handler(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(params): Query<BTreeMap<String, String>>,
) -> Result<axum::response::Response, S3Error> {
    // GET /{bucket}/{key}?tagging → GetObjectTagging
    if params.contains_key("tagging") {
        return get_object_tagging(&state, &bucket, &key).await;
    }

    // GET /{bucket}/{key}?uploadId=... (without partNumber) → ListParts
    if let Some(upload_id) = params.get("uploadId") {
        return list_parts(&state, &bucket, &key, upload_id).await;
    }

    let (data, manifest) = state
        .engine
        .get_object(&bucket, &key)
        .await
        .map_err(|e| engine_to_s3(e, &bucket, &key))?;

    let etag = format!("\"{0}\"", manifest.object_id);

    let span = tracing::Span::current();
    span.record("response_status", 200u16);
    span.record("content_length", data.len() as u64);

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header("etag", &etag)
        .header("content-length", data.len().to_string());

    // Return standard HTTP headers stored in metadata.
    for &header in PASSTHROUGH_HEADERS {
        if let Some(val) = manifest.metadata.get(header) {
            builder = builder.header(header, val);
        }
    }

    // Return user metadata as x-amz-meta-* headers.
    for (k, v) in &manifest.metadata {
        if !PASSTHROUGH_HEADERS.contains(&k.as_str()) {
            builder = builder.header(format!("x-amz-meta-{k}"), v);
        }
    }

    Ok(builder.body(Body::from(data)).unwrap())
}

// -----------------------------------------------------------------------
// DELETE /{bucket}/{*key} — DeleteObject
// -----------------------------------------------------------------------

/// Delete an object or its tags.
///
/// S3 spec: DELETE is idempotent — deleting a non-existent key returns 204.
#[tracing::instrument(skip(state, params))]
pub(crate) async fn delete_object_handler(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(params): Query<BTreeMap<String, String>>,
) -> Result<axum::response::Response, S3Error> {
    // DELETE /{bucket}/{key}?tagging → DeleteObjectTagging
    if params.contains_key("tagging") {
        return delete_object_tagging(&state, &bucket, &key).await;
    }

    // DELETE /{bucket}/{key}?uploadId=... → AbortMultipartUpload
    if let Some(upload_id) = params.get("uploadId") {
        return abort_multipart_upload(&state, &bucket, &key, upload_id).await;
    }

    match state.engine.delete_object(&bucket, &key).await {
        Ok(()) => {
            info!(bucket = %bucket, key = %key, "delete_object");
        }
        Err(shoal_engine::EngineError::ObjectNotFound { .. }) => {
            // S3 spec: DELETE is idempotent, return 204 for non-existent keys.
            tracing::debug!(bucket = %bucket, key = %key, "delete_object: key not found, returning 204");
        }
        Err(e) => return Err(engine_to_s3(e, &bucket, &key)),
    }

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap())
}

// -----------------------------------------------------------------------
// GET /{bucket}/{*key}?tagging — GetObjectTagging
// -----------------------------------------------------------------------

async fn get_object_tagging(
    state: &AppState,
    bucket: &str,
    key: &str,
) -> Result<axum::response::Response, S3Error> {
    let tags = state
        .engine
        .get_object_tags(bucket, key)
        .await
        .map_err(|e| engine_to_s3(e, bucket, key))?;

    let body = xml::tagging_xml(&tags);
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(Body::from(body))
        .unwrap())
}

// -----------------------------------------------------------------------
// PUT /{bucket}/{*key}?tagging — PutObjectTagging
// -----------------------------------------------------------------------

async fn put_object_tagging(
    state: &AppState,
    bucket: &str,
    key: &str,
    body: &[u8],
) -> Result<axum::response::Response, S3Error> {
    let body_str = String::from_utf8_lossy(body);
    let tags = xml::parse_tagging_request(&body_str);

    if tags.len() > 10 {
        return Err(S3Error::BadRequest {
            message: format!("Object tags cannot exceed 10, got {}", tags.len()),
        });
    }

    state
        .engine
        .put_object_tags(bucket, key, tags)
        .await
        .map_err(|e| engine_to_s3(e, bucket, key))?;

    Ok(Response::builder()
        .status(StatusCode::OK)
        .body(Body::empty())
        .unwrap())
}

// -----------------------------------------------------------------------
// DELETE /{bucket}/{*key}?tagging — DeleteObjectTagging
// -----------------------------------------------------------------------

async fn delete_object_tagging(
    state: &AppState,
    bucket: &str,
    key: &str,
) -> Result<axum::response::Response, S3Error> {
    state
        .engine
        .delete_object_tags(bucket, key)
        .await
        .map_err(|e| engine_to_s3(e, bucket, key))?;

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap())
}

// -----------------------------------------------------------------------
// DELETE /{bucket}/{*key}?uploadId=... — AbortMultipartUpload
// -----------------------------------------------------------------------

async fn abort_multipart_upload(
    state: &AppState,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Result<axum::response::Response, S3Error> {
    let mut uploads = state.uploads.write().await;
    let removed = uploads.remove(upload_id);

    if let Some(upload) = removed
        && (upload.bucket != bucket || upload.key != key)
    {
        // Put it back — wrong bucket/key.
        uploads.insert(upload_id.to_string(), upload);
        return Err(S3Error::NoSuchUpload {
            upload_id: upload_id.to_string(),
        });
    }
    // S3 spec: aborting a non-existent upload returns 204.

    info!(bucket, key, upload_id, "abort_multipart_upload");

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap())
}

// -----------------------------------------------------------------------
// GET /{bucket}/{*key}?uploadId=... — ListParts
// -----------------------------------------------------------------------

async fn list_parts(
    state: &AppState,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Result<axum::response::Response, S3Error> {
    let uploads = state.uploads.read().await;
    let upload = uploads
        .get(upload_id)
        .ok_or_else(|| S3Error::NoSuchUpload {
            upload_id: upload_id.to_string(),
        })?;

    if upload.bucket != bucket || upload.key != key {
        return Err(S3Error::NoSuchUpload {
            upload_id: upload_id.to_string(),
        });
    }

    let parts: Vec<(u16, usize, String)> = upload
        .parts
        .iter()
        .map(|(num, data)| {
            let etag = format!("\"{}\"", blake3::hash(data));
            (*num, data.len(), etag)
        })
        .collect();

    let body = xml::list_parts(bucket, key, upload_id, &parts);
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(Body::from(body))
        .unwrap())
}

// -----------------------------------------------------------------------
// HEAD /{bucket}/{*key} — HeadObject
// -----------------------------------------------------------------------

/// Return object metadata without fetching the body.
#[tracing::instrument(skip(state))]
pub(crate) async fn head_object_handler(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<axum::response::Response, S3Error> {
    let manifest = state
        .engine
        .head_object(&bucket, &key)
        .await
        .map_err(|e| engine_to_s3(e, &bucket, &key))?;

    let etag = format!("\"{0}\"", manifest.object_id);

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header("etag", &etag)
        .header("content-length", manifest.total_size.to_string());

    // Return standard HTTP headers stored in metadata.
    for &header in PASSTHROUGH_HEADERS {
        if let Some(val) = manifest.metadata.get(header) {
            builder = builder.header(header, val);
        }
    }

    // Return user metadata as x-amz-meta-* headers.
    for (k, v) in &manifest.metadata {
        if !PASSTHROUGH_HEADERS.contains(&k.as_str()) {
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

    for &header in PASSTHROUGH_HEADERS {
        if let Some(val) = headers.get(header)
            && let Ok(v) = val.to_str()
        {
            metadata.insert(header.to_string(), v.to_string());
        }
    }

    for (name, value) in headers {
        if let Some(meta_key) = name.as_str().strip_prefix("x-amz-meta-")
            && let Ok(v) = value.to_str()
        {
            metadata.insert(meta_key.to_string(), v.to_string());
        }
    }

    // Capture x-amz-tagging header for the completed object.
    let tags = headers
        .get("x-amz-tagging")
        .and_then(|v| v.to_str().ok())
        .map(parse_url_tags);

    {
        let mut uploads = state.uploads.write().await;
        uploads.insert(
            upload_id.clone(),
            crate::MultipartUpload {
                bucket: bucket.to_string(),
                key: key.to_string(),
                parts: BTreeMap::new(),
                metadata,
                tags,
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

    // Apply tags captured at initiation.
    if let Some(tags) = upload.tags {
        let _ = state.engine.put_object_tags(bucket, key, tags).await;
    }

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
