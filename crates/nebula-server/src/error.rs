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
use nebula_raft::{MembershipError, SubmitError};
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
    /// Raft mode: write hit a non-leader node. The optional addr lets
    /// a smart client retry directly without polling the cluster
    /// status endpoint. Returns HTTP 421 (Misdirected Request) — the
    /// same code the cross-region home guard uses, distinct from the
    /// 409 the within-region follower guard returns so clients can
    /// tell the cases apart without parsing bodies.
    #[error("not leader (current leader: {leader_id:?}, addr: {leader_addr:?})")]
    NotLeader {
        leader_id: Option<u64>,
        leader_addr: Option<String>,
    },
    /// A conflicting operation is in progress (e.g. a Raft membership
    /// reconfiguration is still committing). Carries a stable code and
    /// message; maps to HTTP 409 so the caller retries.
    #[error("conflict: {message}")]
    Conflict {
        code: &'static str,
        message: String,
    },
    #[error("internal: {0}")]
    Internal(String),
}

impl From<SubmitError> for ApiError {
    fn from(e: SubmitError) -> Self {
        match e {
            SubmitError::NotLeader {
                leader_id,
                leader_addr,
            } => ApiError::NotLeader {
                leader_id,
                leader_addr,
            },
            SubmitError::Other(msg) => ApiError::Internal(msg),
        }
    }
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
            // Embed failures reach handlers wrapped in IndexError on
            // the search/upsert paths — unwrap to the same mapping as
            // the direct Embed arm so the circuit-open fail-fast keeps
            // its distinct 503 regardless of which layer surfaced it.
            ApiError::Index(IndexError::Embed(EmbedError::CircuitOpen)) => {
                ("embedder_unavailable", StatusCode::SERVICE_UNAVAILABLE)
            }
            ApiError::Index(IndexError::Embed(EmbedError::Provider { .. })) => {
                ("upstream_error", StatusCode::BAD_GATEWAY)
            }
            ApiError::Index(IndexError::Embed(_)) => ("embed_error", StatusCode::BAD_GATEWAY),
            ApiError::Index(_) => ("internal", StatusCode::INTERNAL_SERVER_ERROR),
            ApiError::Embed(EmbedError::Provider { .. }) => {
                ("upstream_error", StatusCode::BAD_GATEWAY)
            }
            // Circuit open: the provider is degraded and calls fail
            // fast (design 0010 §5). 503 + a distinct code so clients
            // back off (or switch to embed_mode=deferred) instead of
            // treating it as a one-off upstream hiccup.
            ApiError::Embed(EmbedError::CircuitOpen) => {
                ("embedder_unavailable", StatusCode::SERVICE_UNAVAILABLE)
            }
            ApiError::Embed(_) => ("embed_error", StatusCode::BAD_GATEWAY),
            ApiError::Sql(SqlError::Parse(_)) => ("sql_parse", StatusCode::BAD_REQUEST),
            ApiError::Sql(SqlError::Unsupported(_)) => ("sql_unsupported", StatusCode::BAD_REQUEST),
            ApiError::Sql(SqlError::InvalidPlan(_)) => ("sql_invalid", StatusCode::BAD_REQUEST),
            ApiError::Sql(SqlError::TypeError(_)) => ("sql_type", StatusCode::BAD_REQUEST),
            ApiError::Sql(SqlError::Index(_)) => ("internal", StatusCode::INTERNAL_SERVER_ERROR),
            ApiError::Sql(SqlError::Llm(_)) => ("llm_error", StatusCode::BAD_GATEWAY),
            ApiError::NotLeader { .. } => ("not_leader", StatusCode::MISDIRECTED_REQUEST),
            ApiError::Conflict { code, .. } => (code, StatusCode::CONFLICT),
            ApiError::Internal(_) => ("internal", StatusCode::INTERNAL_SERVER_ERROR),
        }
    }
}

impl From<MembershipError> for ApiError {
    fn from(e: MembershipError) -> Self {
        match e {
            MembershipError::NotLeader {
                leader_id,
                leader_addr,
            } => ApiError::NotLeader {
                leader_id,
                leader_addr,
            },
            MembershipError::InProgress => ApiError::Conflict {
                code: "membership_in_progress",
                message: "a Raft membership change is already committing; retry shortly".into(),
            },
            MembershipError::LearnerNotFound(id) => ApiError::BadRequest(format!(
                "node {id} is not a known learner — add it via POST /admin/raft/learner first"
            )),
            // Idempotent formation: the caller treats this as success, but
            // if it reaches the error path we surface it as a benign 409
            // rather than a 500.
            MembershipError::AlreadyInitialized => ApiError::Conflict {
                code: "already_initialized",
                message: "raft cluster is already initialized".into(),
            },
            MembershipError::Other(msg) => ApiError::Internal(msg),
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
        // For NotLeader we attach the leader id + addr at the top level
        // (alongside `error`) so a retry-aware client can re-target
        // without parsing the human-readable message.
        let body = match &self {
            ApiError::NotLeader {
                leader_id,
                leader_addr,
            } => Json(json!({
                "error": {
                    "code": code,
                    "message": self.to_string(),
                },
                "leader_id": leader_id,
                "leader_addr": leader_addr,
            })),
            _ => Json(json!({
                "error": {
                    "code": code,
                    "message": self.to_string(),
                }
            })),
        };
        (status, body).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_leader_maps_to_421_misdirected() {
        let err = ApiError::NotLeader {
            leader_id: Some(7),
            leader_addr: Some("10.0.0.7:50052".into()),
        };
        let (code, status) = err.code_and_status();
        assert_eq!(code, "not_leader");
        assert_eq!(status, StatusCode::MISDIRECTED_REQUEST);
    }

    #[test]
    fn submit_error_not_leader_converts_to_api_error() {
        let raft_err = SubmitError::NotLeader {
            leader_id: Some(3),
            leader_addr: Some("a:1".into()),
        };
        match ApiError::from(raft_err) {
            ApiError::NotLeader {
                leader_id,
                leader_addr,
            } => {
                assert_eq!(leader_id, Some(3));
                assert_eq!(leader_addr.as_deref(), Some("a:1"));
            }
            other => panic!("expected NotLeader, got {other:?}"),
        }
    }

    #[test]
    fn submit_error_other_converts_to_internal() {
        let raft_err = SubmitError::Other("storage shrugged".into());
        assert!(matches!(ApiError::from(raft_err), ApiError::Internal(_)));
    }

    #[test]
    fn membership_not_leader_maps_to_421() {
        let err: ApiError = MembershipError::NotLeader {
            leader_id: Some(3),
            leader_addr: Some("node3:50052".into()),
        }
        .into();
        let (code, status) = err.code_and_status();
        assert_eq!(code, "not_leader");
        assert_eq!(status, StatusCode::MISDIRECTED_REQUEST);
    }

    #[test]
    fn membership_in_progress_maps_to_409() {
        let err: ApiError = MembershipError::InProgress.into();
        let (code, status) = err.code_and_status();
        assert_eq!(code, "membership_in_progress");
        assert_eq!(status, StatusCode::CONFLICT);
    }

    #[test]
    fn membership_learner_not_found_maps_to_400() {
        let err: ApiError = MembershipError::LearnerNotFound(7).into();
        let (_, status) = err.code_and_status();
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn membership_already_initialized_maps_to_409() {
        let err: ApiError = MembershipError::AlreadyInitialized.into();
        let (code, status) = err.code_and_status();
        assert_eq!(code, "already_initialized");
        assert_eq!(status, StatusCode::CONFLICT);
    }
}
