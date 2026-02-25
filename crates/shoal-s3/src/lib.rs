//! S3-compatible HTTP API for Shoal.
//!
//! Provides an [`S3Server`] that exposes an axum-based HTTP API compatible
//! with a subset of the AWS S3 protocol. Supported operations:
//!
//! - `PUT /{bucket}` — CreateBucket
//! - `GET /{bucket}?list-type=2&prefix=...` — ListObjectsV2
//! - `PUT /{bucket}/{key}` — PutObject
//! - `GET /{bucket}/{key}` — GetObject
//! - `DELETE /{bucket}/{key}` — DeleteObject
//! - `HEAD /{bucket}/{key}` — HeadObject
//! - `POST /{bucket}/{key}?uploads` — InitiateMultipartUpload
//! - `PUT /{bucket}/{key}?partNumber=N&uploadId=X` — UploadPart
//! - `POST /{bucket}/{key}?uploadId=X` — CompleteMultipartUpload

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
use axum::routing::put;
use shoal_engine::ShoalNode;
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
    /// Optional shared-secret for Bearer token authentication.
    pub auth_secret: Option<String>,
}

/// Authentication middleware.
///
/// When `auth_secret` is configured, requires `Authorization: Bearer <secret>`.
/// When no secret is configured, all requests pass through.
async fn auth_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, S3Error> {
    if let Some(ref secret) = state.auth_secret {
        let expected = format!("Bearer {secret}");
        let valid = request
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v == expected);

        if !valid {
            warn!("unauthorized S3 request");
            return Err(S3Error::AccessDenied);
        }
    }
    Ok(next.run(request).await)
}

/// Configuration for creating an [`S3Server`].
pub struct S3ServerConfig {
    /// The storage engine to serve.
    pub engine: Arc<ShoalNode>,
    /// Optional shared-secret for Bearer token auth. `None` disables auth.
    pub auth_secret: Option<String>,
}

/// S3-compatible HTTP server backed by a [`ShoalNode`].
pub struct S3Server {
    router: Router,
}

impl S3Server {
    /// Create a new S3 server with the given configuration.
    pub fn new(config: S3ServerConfig) -> Self {
        let state = AppState {
            engine: config.engine,
            uploads: Arc::new(RwLock::new(HashMap::new())),
            auth_secret: config.auth_secret,
        };

        let router = Self::build_router(state);
        Self { router }
    }

    /// Build the axum [`Router`] for the S3 API.
    fn build_router(state: AppState) -> Router {
        Router::new()
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
            .layer(middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
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
