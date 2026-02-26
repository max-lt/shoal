//! S3-compatible error types and XML error responses.

use axum::body::Body;
use axum::http::{Response, StatusCode};
use axum::response::IntoResponse;

use crate::xml;

/// Errors returned by S3 API handlers.
#[derive(Debug, thiserror::Error)]
pub enum S3Error {
    /// The requested bucket does not exist.
    #[error("no such bucket: {bucket}")]
    NoSuchBucket {
        /// Bucket name.
        bucket: String,
    },

    /// The bucket is not empty (cannot delete).
    #[error("bucket not empty: {bucket}")]
    BucketNotEmpty {
        /// Bucket name.
        bucket: String,
    },

    /// The requested object key does not exist.
    #[error("no such key: {bucket}/{key}")]
    NoSuchKey {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
    },

    /// The specified multipart upload does not exist.
    #[error("no such upload: {upload_id}")]
    NoSuchUpload {
        /// Upload identifier.
        upload_id: String,
    },

    /// Invalid part number in a multipart upload request.
    #[error("invalid part number: {part_number}")]
    InvalidPartNumber {
        /// The invalid part number string.
        part_number: String,
    },

    /// Malformed or unsupported request.
    #[error("invalid request: {message}")]
    InvalidRequest {
        /// Description of the problem.
        message: String,
    },

    /// An error from the storage engine.
    #[error("engine error: {0}")]
    Engine(#[from] shoal_engine::EngineError),

    /// Authentication failed.
    #[error("access denied")]
    AccessDenied,

    /// Internal server error.
    #[error("internal error: {message}")]
    Internal {
        /// Description of the internal failure.
        message: String,
    },
}

impl S3Error {
    /// Map to an HTTP status code.
    fn status_code(&self) -> StatusCode {
        match self {
            Self::NoSuchBucket { .. } => StatusCode::NOT_FOUND,
            Self::BucketNotEmpty { .. } => StatusCode::CONFLICT,
            Self::NoSuchKey { .. } => StatusCode::NOT_FOUND,
            Self::NoSuchUpload { .. } => StatusCode::NOT_FOUND,
            Self::InvalidPartNumber { .. } => StatusCode::BAD_REQUEST,
            Self::InvalidRequest { .. } => StatusCode::BAD_REQUEST,
            Self::Engine(e) => match e {
                shoal_engine::EngineError::ObjectNotFound { .. } => StatusCode::NOT_FOUND,
                shoal_engine::EngineError::BucketNotFound { .. } => StatusCode::NOT_FOUND,
                shoal_engine::EngineError::BucketNotEmpty { .. } => StatusCode::CONFLICT,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::AccessDenied => StatusCode::FORBIDDEN,
            Self::Internal { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Map to an S3 error code string.
    fn s3_code(&self) -> &str {
        match self {
            Self::NoSuchBucket { .. } => "NoSuchBucket",
            Self::BucketNotEmpty { .. } => "BucketNotEmpty",
            Self::NoSuchKey { .. } => "NoSuchKey",
            Self::NoSuchUpload { .. } => "NoSuchUpload",
            Self::InvalidPartNumber { .. } => "InvalidArgument",
            Self::InvalidRequest { .. } => "InvalidRequest",
            Self::Engine(e) => match e {
                shoal_engine::EngineError::ObjectNotFound { .. } => "NoSuchKey",
                shoal_engine::EngineError::BucketNotFound { .. } => "NoSuchBucket",
                shoal_engine::EngineError::BucketNotEmpty { .. } => "BucketNotEmpty",
                _ => "InternalError",
            },
            Self::AccessDenied => "AccessDenied",
            Self::Internal { .. } => "InternalError",
        }
    }
}

impl IntoResponse for S3Error {
    fn into_response(self) -> axum::response::Response {
        let status = self.status_code();
        let code = self.s3_code();
        let message = self.to_string();
        let body = xml::error_xml(code, &message);

        Response::builder()
            .status(status)
            .header("content-type", "application/xml")
            .body(Body::from(body))
            .unwrap()
    }
}
