//! API error type.
//!
//! All fallible handlers return [`ApiError`], which:
//! 1. maps every internal error to a stable `code` string (not the
//!    debug representation of the enum — that would leak refactors
//!    into the public contract).
//! 2. chooses an appropriate HTTP status.
//! 3. serializes as `{"error": {"code": "...", "message": "..."}}`.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use thiserror::Error;

use nebula_embed::EmbedError;
use nebula_index::IndexError;
use nebula_sql::SqlError;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("unauthorized")]
    Unauthorized,
    #[error("not found: {0}")]
    NotFound(String),
    #[error("payload too large")]
    PayloadTooLarge,
    #[error(transparent)]
    Index(#[from] IndexError),
    #[error(transparent)]
    Embed(#[from] EmbedError),
    #[error(transparent)]
    Sql(#[from] SqlError),
    #[error("internal: {0}")]
    Internal(String),
}

impl ApiError {
    fn code_and_status(&self) -> (&'static str, StatusCode) {
        match self {
            ApiError::BadRequest(_) => ("bad_request", StatusCode::BAD_REQUEST),
            ApiError::Unauthorized => ("unauthorized", StatusCode::UNAUTHORIZED),
            ApiError::NotFound(_) => ("not_found", StatusCode::NOT_FOUND),
            ApiError::PayloadTooLarge => ("payload_too_large", StatusCode::PAYLOAD_TOO_LARGE),
            ApiError::Index(IndexError::DocNotFound { .. }) => ("not_found", StatusCode::NOT_FOUND),
            ApiError::Index(IndexError::Invalid(_)) => ("bad_request", StatusCode::BAD_REQUEST),
            ApiError::Index(IndexError::BucketNotFound(_)) => ("not_found", StatusCode::NOT_FOUND),
            ApiError::Index(_) => ("internal", StatusCode::INTERNAL_SERVER_ERROR),
            ApiError::Embed(EmbedError::Provider { .. }) => ("upstream_error", StatusCode::BAD_GATEWAY),
            ApiError::Embed(_) => ("embed_error", StatusCode::BAD_GATEWAY),
            ApiError::Sql(SqlError::Parse(_)) => ("sql_parse", StatusCode::BAD_REQUEST),
            ApiError::Sql(SqlError::Unsupported(_)) => ("sql_unsupported", StatusCode::BAD_REQUEST),
            ApiError::Sql(SqlError::InvalidPlan(_)) => ("sql_invalid", StatusCode::BAD_REQUEST),
            ApiError::Sql(SqlError::TypeError(_)) => ("sql_type", StatusCode::BAD_REQUEST),
            ApiError::Sql(SqlError::Index(_)) => ("internal", StatusCode::INTERNAL_SERVER_ERROR),
            ApiError::Internal(_) => ("internal", StatusCode::INTERNAL_SERVER_ERROR),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (code, status) = self.code_and_status();
        // Only log 5xx; 4xx are caller errors and would otherwise
        // dominate logs during abuse.
        if status.is_server_error() {
            tracing::error!(code, error = %self, "server error");
        }
        let body = Json(json!({
            "error": {
                "code": code,
                "message": self.to_string(),
            }
        }));
        (status, body).into_response()
    }
}
