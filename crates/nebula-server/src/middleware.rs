//! Auth middleware: bearer-token allowlist and/or JWT verification.
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
