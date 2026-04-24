//! Auth + audit middleware.
//!
//!
//! Auth is layered:
//!
//! 1. If the incoming `Authorization: Bearer <token>` matches any
//!    entry in `AppConfig::api_keys` (constant-time compare), accept.
//! 2. Otherwise, if `AppConfig::jwt` is configured, verify the token
//!    as a JWT. On success, accept.
//! 3. If neither allowlist nor JWT is configured, auth is disabled —
//!    dev-mode default.
//!
//! The combined scheme makes migration painless: roll JWT alongside an
//! existing allowlist, then drop the allowlist once all callers have
//! migrated.

use axum::{
    extract::State,
    http::{header, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use subtle_eq::constant_time_eq;

use crate::jwt;
use crate::state::AppState;

mod subtle_eq {
    /// Constant-time byte-string equality. Not cryptographic
    /// hardening — it just removes the most obvious timing oracle on a
    /// naive `==`. A real deployment should pair this with an opaque
    /// token store + rate limiting on auth failures.
    pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        let mut diff = 0u8;
        for i in 0..a.len() {
            diff |= a[i] ^ b[i];
        }
        diff == 0
    }
}

/// Auth middleware. Runs on every route under `/api/v1`; `/healthz`
/// and `/metrics` are mounted outside the auth layer deliberately so
/// ops tooling can scrape without a credential.
pub async fn require_auth(
    State(state): State<AppState>,
    req: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, Response> {
    state.metrics.inc_request();

    let allowlist_enabled = !state.config.api_keys.is_empty();
    let jwt_enabled = state.config.jwt.is_some();

    if !allowlist_enabled && !jwt_enabled {
        // No auth configured at all — dev / local mode.
        return Ok(next.run(req).await);
    }

    let header = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();

    let token = header.strip_prefix("Bearer ").unwrap_or("").trim();

    // Step 1: allowlist. Cheap, constant-time, no crypto per request.
    // We attempt this first so an allowlisted caller never pays the
    // JWT verification cost.
    if allowlist_enabled
        && state
            .config
            .api_keys
            .iter()
            .any(|k| constant_time_eq(k.as_bytes(), token.as_bytes()))
    {
        return Ok(next.run(req).await);
    }

    // Step 2: JWT. We only reach here if the allowlist didn't match
    // (or was empty). A failed allowlist attempt is NOT counted as an
    // auth failure yet — it might still be a valid JWT.
    if let Some(cfg) = &state.config.jwt {
        if !token.is_empty() {
            match jwt::verify(cfg, token) {
                Ok(()) => return Ok(next.run(req).await),
                Err(reason) => {
                    tracing::debug!(reason, "jwt verification failed");
                    state.metrics.inc_jwt_failure();
                }
            }
        }
    }

    state.metrics.inc_auth_failure();
    Err((
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Bearer")],
        r#"{"error":{"code":"unauthorized","message":"missing or invalid credentials"}}"#,
    )
        .into_response())
}

/// Write-path audit middleware.
///
/// Records method + path + principal + response status after the
/// handler runs. GET requests are skipped — they're high-volume and
/// reads already show up in counters. 5xx responses are still
/// recorded because an audit log of "what was attempted" matters
/// more than "what succeeded".
///
/// Runs *outside* the auth layer so unauthenticated attempts are
/// recorded too (useful for spotting credential-stuffing).
pub async fn audit_writes(
    axum::extract::State(state): axum::extract::State<crate::state::AppState>,
    connect_info: Option<axum::extract::ConnectInfo<std::net::SocketAddr>>,
    req: Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let is_write = !matches!(method, axum::http::Method::GET | axum::http::Method::HEAD);

    // Only keep what we need; don't hold the request alive across
    // the `next.run` call.
    let principal = if is_write {
        let auth = req
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        Some(crate::audit::principal_fingerprint(
            auth.as_deref(),
            connect_info.map(|c| c.0),
        ))
    } else {
        None
    };

    let resp = next.run(req).await;

    if let Some(p) = principal {
        state.audit.record(crate::audit::AuditEntry {
            ts_ms: crate::audit::now_ms(),
            principal: p,
            method: method.to_string(),
            path,
            status: resp.status().as_u16(),
        });
    }
    resp
}
