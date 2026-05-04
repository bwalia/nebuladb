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

/// Follower-mode write guard.
///
/// When the cluster role is `follower`, every mutating HTTP method
/// short-circuits with 409 Conflict. The follower mirrors the
/// leader's WAL; accepting a local write would silently diverge
/// the two nodes and there is no merge story today. 409 is the
/// right signal — the request is well-formed, the resource is in
/// a state that forbids it.
///
/// Exempted: all GET/HEAD requests and the ops endpoints mounted
/// outside /api/v1 (which this middleware never sees anyway).
/// `/api/v1/admin/*` reads are allowed so operators can still
/// inspect the follower.
pub async fn guard_writes_on_follower(
    axum::extract::State(state): axum::extract::State<crate::state::AppState>,
    req: Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> Response {
    if !state.cluster.is_follower() {
        return next.run(req).await;
    }
    let is_write = !matches!(
        req.method(),
        &axum::http::Method::GET | &axum::http::Method::HEAD | &axum::http::Method::OPTIONS
    );
    if !is_write {
        return next.run(req).await;
    }
    // Structured JSON so clients with an error envelope keep working.
    (
        StatusCode::CONFLICT,
        [(header::CONTENT_TYPE, "application/json")],
        r#"{"code":"read_only_follower","error":"this node is a follower; route writes to the leader"}"#,
    )
        .into_response()
}

/// Reject writes targeted at a bucket whose `home_region` is NOT this
/// node's region. The nebula-client SDK reads the home-region map and
/// routes writes to the right region; this middleware is the safety
/// net for clients that skipped the cache or hit the wrong endpoint
/// directly.
///
/// The response carries the expected region in the body so a
/// retrying client can follow the redirect without a second lookup.
/// Response code: HTTP 421 (Misdirected Request) — semantically
/// exactly this situation, and distinct from the 409 the follower
/// guard uses so clients can tell them apart without parsing bodies.
///
/// Scope: only requests matching `/api/v1/bucket/:bucket/...` or
/// `/api/v1/admin/bucket/:bucket/...` — i.e., paths where the bucket
/// is visible in the URL. Bulk endpoints that identify the bucket
/// elsewhere (there aren't any today) would need a body inspection
/// hook; for now the path-based filter matches the whole write
/// surface.
pub async fn guard_wrong_home_region(
    axum::extract::State(state): axum::extract::State<crate::state::AppState>,
    req: Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> Response {
    // Only reject writes. Reads are allowed anywhere.
    let is_write = !matches!(
        req.method(),
        &axum::http::Method::GET | &axum::http::Method::HEAD | &axum::http::Method::OPTIONS
    );
    if !is_write {
        return next.run(req).await;
    }

    let Some(bucket) = extract_bucket_from_path(req.uri().path()) else {
        return next.run(req).await;
    };

    // Look up this bucket's home. If no home is set (single-region
    // or unconfigured bucket), let the write through — legacy behavior.
    let hr = state
        .index
        .get(bucket, crate::home_region::SEED_DOC_ID)
        .map(|d| crate::home_region::HomeRegion::from_metadata(&d.metadata))
        .unwrap_or_default();
    let Some(home) = hr.region.as_deref() else {
        return next.run(req).await;
    };

    // Compare with this node's region. An absent NEBULA_REGION means
    // the node hasn't opted into multi-region routing — treat as
    // "don't enforce" rather than rejecting every write.
    let Some(my_region) = state.cluster.region.as_deref() else {
        return next.run(req).await;
    };
    if my_region == home {
        return next.run(req).await;
    }

    // Wrong home. 421 with enough body for a client to re-route.
    let body = format!(
        r#"{{"code":"wrong_home_region","error":"bucket home is {home}","home_region":"{home}","home_epoch":{epoch},"node_region":"{my_region}"}}"#,
        home = home,
        epoch = hr.epoch,
        my_region = my_region,
    );
    (
        StatusCode::MISDIRECTED_REQUEST,
        [(header::CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response()
}

/// Pull `<bucket>` out of the relevant prefixes. Middleware installed
/// on the nested `/api/v1` router sees paths already stripped to the
/// `/bucket/...` / `/admin/bucket/...` form, so we match both.
fn extract_bucket_from_path(path: &str) -> Option<&str> {
    // Normalize: strip optional /api/v1 so both nested and full-path
    // callers (tests) work the same way.
    let rest = path.strip_prefix("/api/v1").unwrap_or(path);
    let rest = rest.strip_prefix('/').unwrap_or(rest);
    let rest = if let Some(r) = rest.strip_prefix("admin/bucket/") {
        r
    } else {
        rest.strip_prefix("bucket/")?
    };
    let (bucket, _after) = rest.split_once('/').unwrap_or((rest, ""));
    if bucket.is_empty() {
        None
    } else {
        Some(bucket)
    }
}

#[cfg(test)]
mod bucket_path_tests {
    use super::extract_bucket_from_path;

    #[test]
    fn extracts_from_bucket_path() {
        // Full path (no nest) and nested path (after /api/v1 strip)
        // both work.
        assert_eq!(extract_bucket_from_path("/api/v1/bucket/catalog/doc"), Some("catalog"));
        assert_eq!(extract_bucket_from_path("/bucket/catalog/doc"), Some("catalog"));
        assert_eq!(extract_bucket_from_path("/bucket/catalog/docs/bulk"), Some("catalog"));
        assert_eq!(
            extract_bucket_from_path("/api/v1/admin/bucket/catalog/export"),
            Some("catalog")
        );
        assert_eq!(
            extract_bucket_from_path("/admin/bucket/catalog/export"),
            Some("catalog")
        );
    }

    #[test]
    fn returns_none_for_non_bucket_paths() {
        assert!(extract_bucket_from_path("/api/v1/ai/search").is_none());
        assert!(extract_bucket_from_path("/ai/search").is_none());
        assert!(extract_bucket_from_path("/api/v1/admin/snapshot").is_none());
        assert!(extract_bucket_from_path("/admin/snapshot").is_none());
        assert!(extract_bucket_from_path("/healthz").is_none());
    }

    #[test]
    fn empty_bucket_is_none() {
        assert!(extract_bucket_from_path("/api/v1/bucket//doc").is_none());
        assert!(extract_bucket_from_path("/bucket//doc").is_none());
    }
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
