//! S3-compatible HTTP API for Shoal.
//!
//! Provides an [`S3Server`] that exposes an axum-based HTTP API compatible
//! with a subset of the AWS S3 protocol. Supported operations:
//!
//! - `POST /admin/keys` — CreateApiKey (requires admin secret)
//! - `GET /admin/keys` — ListApiKeys (requires admin secret)
//! - `DELETE /admin/keys/{access_key_id}` — DeleteApiKey (requires admin secret)
//! - `PUT /{bucket}` — CreateBucket
//! - `GET /{bucket}?list-type=2&prefix=...` — ListObjectsV2
//! - `PUT /{bucket}/{key}` — PutObject
//! - `GET /{bucket}/{key}` — GetObject
//! - `DELETE /{bucket}/{key}` — DeleteObject
//! - `HEAD /{bucket}/{key}` — HeadObject
//! - `POST /{bucket}/{key}?uploads` — InitiateMultipartUpload
//! - `PUT /{bucket}/{key}?partNumber=N&uploadId=X` — UploadPart
//! - `POST /{bucket}/{key}?uploadId=X` — CompleteMultipartUpload
//!
//! ## Authentication
//!
//! - **Admin endpoints** (`/admin/keys`): open, no auth required.
//! - **S3 data-plane**: all other endpoints require an API key created via
//!   the admin endpoint. Supply `Authorization: Bearer <access_key_id>:<secret_access_key>`.
//!   If no API keys exist, all S3 requests are rejected (403).

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
use axum::routing::{delete, post, put};
use shoal_engine::ShoalNode;
use subtle::ConstantTimeEq;
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
    /// The storage engine.
    pub engine: Arc<ShoalNode>,
    /// In-flight multipart uploads.
    pub uploads: Arc<RwLock<HashMap<String, MultipartUpload>>>,
    /// Active API keys: `access_key_id` → `secret_access_key`.
    pub api_keys: Arc<RwLock<HashMap<String, String>>>,
}

/// Authentication middleware for S3 data-plane routes.
///
/// Every request must supply `Authorization: Bearer <access_key_id>:<secret_access_key>`
/// matching an existing API key. Secret comparison is constant-time to prevent
/// timing attacks (using the `subtle` crate, same approach as rustfs).
async fn auth_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, S3Error> {
    let keys = state.api_keys.read().await;

    let authenticated = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .and_then(|v| v.split_once(':'))
        .is_some_and(|(key_id, secret)| {
            keys.get(key_id)
                .is_some_and(|stored| stored.as_bytes().ct_eq(secret.as_bytes()).into())
        });

    if !authenticated {
        warn!("unauthorized S3 request");
        return Err(S3Error::AccessDenied);
    }

    drop(keys);
    Ok(next.run(request).await)
}

/// Configuration for creating an [`S3Server`].
pub struct S3ServerConfig {
    /// The storage engine to serve.
    pub engine: Arc<ShoalNode>,
}

/// S3-compatible HTTP server backed by a [`ShoalNode`].
pub struct S3Server {
    router: Router,
}

impl S3Server {
    /// Create a new S3 server with the given configuration.
    ///
    /// On startup, loads any previously persisted API keys from MetaStore
    /// into the in-memory auth cache.
    pub fn new(config: S3ServerConfig) -> Self {
        // Load persisted API keys from MetaStore into the in-memory cache.
        let persisted_keys = config.engine.meta().load_all_api_keys().unwrap_or_default();

        let key_count = persisted_keys.len();

        let state = AppState {
            engine: config.engine,
            uploads: Arc::new(RwLock::new(HashMap::new())),
            api_keys: Arc::new(RwLock::new(persisted_keys)),
        };

        if key_count > 0 {
            tracing::info!(key_count, "loaded persisted API keys from MetaStore");
        }

        let router = Self::build_router(state);
        Self { router }
    }

    /// Build the axum [`Router`] for the S3 API.
    fn build_router(state: AppState) -> Router {
        // S3 data-plane routes — require a valid API key.
        let s3_routes = Router::new()
            // Bucket-level operations.
            .route(
                "/{bucket}",
                put(handlers::create_bucket).get(handlers::list_objects),
            )
            // Trailing-slash variant — S3 clients often send GET /bucket/.
            .route(
                "/{bucket}/",
                put(handlers::create_bucket).get(handlers::list_objects),
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
