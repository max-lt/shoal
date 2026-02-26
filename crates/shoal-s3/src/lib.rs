//! S3-compatible HTTP API for Shoal.
//!
//! Provides an [`S3Server`] that exposes an axum-based HTTP API compatible
//! with a subset of the AWS S3 protocol. Supported operations:
//!
//! - `POST /admin/keys` — CreateApiKey (requires admin secret)
//! - `GET /admin/keys` — ListApiKeys (requires admin secret)
//! - `DELETE /admin/keys/{access_key_id}` — DeleteApiKey (requires admin secret)
//! - `GET /` — ListBuckets
//! - `PUT /{bucket}` — CreateBucket
//! - `DELETE /{bucket}` — DeleteBucket
//! - `HEAD /{bucket}` — HeadBucket
//! - `GET /{bucket}?list-type=2&prefix=...` — ListObjectsV2
//! - `PUT /{bucket}/{key}` — PutObject (or CopyObject with `x-amz-copy-source`)
//! - `GET /{bucket}/{key}` — GetObject
//! - `DELETE /{bucket}/{key}` — DeleteObject
//! - `HEAD /{bucket}/{key}` — HeadObject
//! - `POST /{bucket}/{key}?uploads` — InitiateMultipartUpload
//! - `PUT /{bucket}/{key}?partNumber=N&uploadId=X` — UploadPart
//! - `POST /{bucket}/{key}?uploadId=X` — CompleteMultipartUpload
//! - `GET /{bucket}?uploads` — ListMultipartUploads
//! - `GET /{bucket}/{key}?uploadId=X` — ListParts
//! - `GET /{bucket}/{key}?tagging` — GetObjectTagging
//! - `PUT /{bucket}/{key}?tagging` — PutObjectTagging
//! - `DELETE /{bucket}/{key}?tagging` — DeleteObjectTagging
//!
//! ## Authentication
//!
//! - **Admin endpoints** (`/admin/keys`): open, no auth required.
//! - **S3 data-plane**: all other endpoints require AWS Signature V4
//!   authentication using an API key created via the admin endpoint.
//!   Standard S3 clients (AWS CLI, boto3, Bun S3Client) work out of the box.
//!   If no API keys exist, all S3 requests are rejected (403).

mod auth;
mod error;
mod handlers;
mod xml;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use axum::Router;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{delete, get, post, put};
use shoal_engine::ShoalEngine;
use tokio::sync::RwLock;
use tracing::warn;

pub use error::S3Error;

/// In-flight multipart upload state.
pub(crate) struct MultipartUpload {
    /// Target bucket.
    pub bucket: String,
    /// Target object key.
    pub key: String,
    /// Uploaded parts keyed by part number.
    pub parts: BTreeMap<u16, Vec<u8>>,
    /// User metadata captured at initiation.
    pub metadata: BTreeMap<String, String>,
}

/// Shared application state for all S3 handlers.
#[derive(Clone)]
pub(crate) struct AppState {
    /// The storage engine (trait object — works with any [`ShoalEngine`] impl).
    pub engine: Arc<dyn ShoalEngine>,
    /// In-flight multipart uploads.
    pub uploads: Arc<RwLock<HashMap<String, MultipartUpload>>>,
}

/// Authentication middleware for S3 data-plane routes.
///
/// Verifies AWS Signature V4 (`Authorization: AWS4-HMAC-SHA256 ...`) against
/// the stored API keys. If the key is not found locally, falls back to a
/// QUIC peer pull via `engine.lookup_api_key()` before rejecting.
/// Constant-time signature comparison prevents timing attacks.
async fn auth_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, S3Error> {
    let mut keys = state.engine.meta().load_all_api_keys().unwrap_or_default();

    // If SigV4 fails, try pulling the missing key from peers before rejecting.
    if let Err(_e) = auth::verify_sigv4(&request, &keys) {
        // Extract the access_key_id from the Authorization header to try a peer pull.
        if let Some(access_key_id) = auth::extract_access_key_id(&request)
            && !keys.contains_key(&access_key_id)
            && let Ok(Some(secret)) = state.engine.lookup_api_key(&access_key_id).await
        {
            keys.insert(access_key_id, secret);

            // Retry verification with the newly fetched key.
            if auth::verify_sigv4(&request, &keys).is_ok() {
                return Ok(next.run(request).await);
            }
        }

        warn!("unauthorized S3 request");
        return Err(S3Error::AccessDenied);
    }

    Ok(next.run(request).await)
}

/// Configuration for creating an [`S3Server`].
pub struct S3ServerConfig {
    /// The storage engine to serve (any [`ShoalEngine`] implementation).
    pub engine: Arc<dyn ShoalEngine>,
}

/// S3-compatible HTTP server backed by any [`ShoalEngine`] implementation.
pub struct S3Server {
    router: Router,
}

impl S3Server {
    /// Create a new S3 server with the given configuration.
    pub fn new(config: S3ServerConfig) -> Self {
        let state = AppState {
            engine: config.engine,
            uploads: Arc::new(RwLock::new(HashMap::new())),
        };

        let router = Self::build_router(state);
        Self { router }
    }

    /// Build the axum [`Router`] for the S3 API.
    fn build_router(state: AppState) -> Router {
        // S3 data-plane routes — require a valid API key.
        let s3_routes = Router::new()
            // Service-level: list all buckets.
            .route("/", get(handlers::list_buckets_handler))
            // Bucket-level operations.
            .route(
                "/{bucket}",
                put(handlers::create_bucket)
                    .get(handlers::list_objects)
                    .delete(handlers::delete_bucket_handler)
                    .head(handlers::head_bucket_handler),
            )
            // Trailing-slash variant — S3 clients often send GET /bucket/.
            .route(
                "/{bucket}/",
                put(handlers::create_bucket)
                    .get(handlers::list_objects)
                    .delete(handlers::delete_bucket_handler)
                    .head(handlers::head_bucket_handler),
            )
            // Object-level operations (key may contain slashes).
            .route(
                "/{bucket}/{*key}",
                put(handlers::put_object_handler)
                    .get(handlers::get_object_handler)
                    .delete(handlers::delete_object_handler)
                    .head(handlers::head_object_handler)
                    .post(handlers::post_object_handler),
            )
            .route_layer(middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ));

        // Admin routes — open for now (no auth required).
        // TODO: gate behind admin_secret once we have a proper admin UI / bootstrap flow.
        let admin_routes = Router::new()
            .route(
                "/admin/keys",
                post(handlers::create_api_key).get(handlers::list_api_keys),
            )
            .route(
                "/admin/keys/{access_key_id}",
                delete(handlers::delete_api_key),
            );

        Router::new()
            .merge(s3_routes)
            .merge(admin_routes)
            // Allow uploads up to 5 GiB (S3 single-PUT max).
            .layer(DefaultBodyLimit::max(5 * 1024 * 1024 * 1024))
            .with_state(state)
    }

    /// Return the inner [`Router`] (useful for testing with `tower::ServiceExt`).
    pub fn into_router(self) -> Router {
        self.router
    }

    /// Serve the S3 API on the given TCP address.
    pub async fn serve(self, addr: &str) -> Result<(), std::io::Error> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        tracing::info!(addr, "S3 server listening");
        axum::serve(listener, self.router).await
    }

    /// Serve the S3 API with graceful shutdown triggered by the given future.
    ///
    /// When `shutdown` completes, the server stops accepting new connections
    /// and waits for in-flight requests to finish.
    pub async fn serve_with_shutdown(
        self,
        addr: &str,
        shutdown: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<(), std::io::Error> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        tracing::info!(addr, "S3 server listening");
        axum::serve(listener, self.router)
            .with_graceful_shutdown(shutdown)
            .await
    }
}
