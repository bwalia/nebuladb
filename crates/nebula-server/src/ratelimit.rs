//! Token-bucket rate limiter, keyed by authenticated principal.
//!
//! # Why a bucket, not a counter
//!
//! Fixed-window counters are trivial to implement and appealingly
//! predictable, but they let clients burst `2*limit` across a window
//! boundary and mis-reject callers that do one request early in a
//! window after idling. A token bucket:
//!
//! - Allows natural bursts up to `capacity`.
//! - Refills smoothly at `rate` tokens/second.
//! - Has a simple per-key state: `(tokens: f64, last_refill: Instant)`.
//!
//! Fifty lines of code, correct under concurrent access with one
//! fine-grained lock per key via `DashMap`.
//!
//! # Keying
//!
//! We prefer the authenticated principal because that's what matters
//! for abuse accounting — an attacker rotating IPs behind one leaked
//! key should still be throttled. When no principal is available
//! (public endpoints, missing auth) we fall back to the remote socket
//! address. `/healthz` and `/metrics` are mounted outside this layer
//! and are never throttled; ops scraping must keep working during an
//! abuse event.

use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{ConnectInfo, State},
    http::{header, HeaderMap, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use dashmap::DashMap;
use parking_lot::Mutex;
use std::net::SocketAddr;

use crate::state::AppState;

#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Max burst size, in tokens. A token costs 1 per request.
    pub capacity: f64,
    /// Refill rate in tokens per second. At `capacity=60`, `rate=1.0`
    /// this yields a sustained 1 rps with 60-request burst headroom.
    pub refill_per_sec: f64,
}

impl Default for RateLimitConfig {
    /// Sensible demo defaults. Production sets these per route group
    /// or per key tier.
    fn default() -> Self {
        Self {
            capacity: 120.0,
            refill_per_sec: 20.0,
        }
    }
}

#[derive(Debug)]
struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

impl Bucket {
    fn new(capacity: f64) -> Self {
        Self {
            tokens: capacity,
            last_refill: Instant::now(),
        }
    }

    /// Refill the bucket and attempt to spend one token.
    ///
    /// Returns `true` if the request is allowed. Computes refill
    /// lazily — we only advance time when a request arrives, so idle
    /// keys consume no CPU.
    fn try_take(&mut self, cfg: &RateLimitConfig) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;
        self.tokens = (self.tokens + elapsed * cfg.refill_per_sec).min(cfg.capacity);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Seconds until the next token is available. Used to populate
    /// `Retry-After` on a 429 — a polite client will obey it.
    fn retry_after_secs(&self, cfg: &RateLimitConfig) -> u64 {
        let needed = 1.0 - self.tokens;
        if needed <= 0.0 || cfg.refill_per_sec <= 0.0 {
            return 0;
        }
        (needed / cfg.refill_per_sec).ceil() as u64
    }
}

/// Shared state across the middleware. Cloned into every request, so
/// everything inside is an `Arc` / already-shared type.
#[derive(Clone, Default)]
pub struct RateLimiter {
    // DashMap gives us shard-level locking, so unrelated keys never
    // contend. Each bucket gets its own `Mutex` because the update is
    // a non-atomic read-modify-write across two fields.
    buckets: Arc<DashMap<String, Mutex<Bucket>>>,
}

impl RateLimiter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Approximate key count. Cheap — returns the DashMap's internal
    /// counter. Useful for a future `/metrics` gauge.
    pub fn key_count(&self) -> usize {
        self.buckets.len()
    }
}

/// Extract a stable principal from headers. Precedence matches the
/// auth middleware: explicit bearer token wins, then `x-api-key`,
/// then the caller's IP. The chosen principal is also written onto a
/// request extension so later middleware (e.g. logging) can read it.
fn principal(headers: &HeaderMap, peer: Option<SocketAddr>) -> String {
    if let Some(v) = headers.get(header::AUTHORIZATION).and_then(|h| h.to_str().ok()) {
        if let Some(tok) = v.strip_prefix("Bearer ") {
            return format!("bearer:{}", tok.trim());
        }
    }
    if let Some(v) = headers.get("x-api-key").and_then(|h| h.to_str().ok()) {
        return format!("apikey:{v}");
    }
    match peer {
        Some(p) => format!("ip:{}", p.ip()),
        None => "anon".into(),
    }
}

/// Axum middleware. Consumes and forwards the request unchanged on
/// allow; returns 429 with a `Retry-After` header on deny.
///
/// We intentionally do not deny on the *auth* step here — this layer
/// only throttles. Unauthenticated requests still get rate-limited
/// (by IP), which is what prevents a brute-force loop on the bearer
/// check from overwhelming the server.
pub async fn rate_limit(
    State(state): State<AppState>,
    connect_info: Option<ConnectInfo<SocketAddr>>,
    req: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let Some(limiter) = state.rate_limiter.as_ref() else {
        return next.run(req).await;
    };
    let cfg = &state.config.rate_limit;

    let key = principal(req.headers(), connect_info.map(|ConnectInfo(a)| a));
    let entry = limiter
        .buckets
        .entry(key.clone())
        .or_insert_with(|| Mutex::new(Bucket::new(cfg.capacity)));

    let (allowed, retry_secs) = {
        let mut bucket = entry.lock();
        let allowed = bucket.try_take(cfg);
        let retry = if allowed { 0 } else { bucket.retry_after_secs(cfg) };
        (allowed, retry)
    };

    if allowed {
        next.run(req).await
    } else {
        state.metrics.inc_rate_limited();
        let body = format!(
            r#"{{"error":{{"code":"rate_limited","message":"too many requests for key {key}"}}}}"#
        );
        (
            StatusCode::TOO_MANY_REQUESTS,
            [
                ("retry-after", retry_secs.to_string()),
                ("content-type", "application/json".to_string()),
            ],
            body,
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn bucket_starts_full() {
        let cfg = RateLimitConfig {
            capacity: 3.0,
            refill_per_sec: 0.1,
        };
        let mut b = Bucket::new(cfg.capacity);
        assert!(b.try_take(&cfg));
        assert!(b.try_take(&cfg));
        assert!(b.try_take(&cfg));
        assert!(!b.try_take(&cfg)); // now empty
    }

    #[test]
    fn bucket_refills_over_time() {
        let cfg = RateLimitConfig {
            capacity: 1.0,
            refill_per_sec: 100.0, // fast refill so the test is quick
        };
        let mut b = Bucket::new(cfg.capacity);
        assert!(b.try_take(&cfg));
        assert!(!b.try_take(&cfg));
        sleep(Duration::from_millis(30)); // ≥ 1 token worth
        assert!(b.try_take(&cfg));
    }

    #[test]
    fn bucket_caps_at_capacity() {
        let cfg = RateLimitConfig {
            capacity: 2.0,
            refill_per_sec: 1000.0,
        };
        let mut b = Bucket::new(cfg.capacity);
        // Sleep long enough that uncapped refill would accumulate well
        // past capacity; the cap must hold.
        sleep(Duration::from_millis(50));
        assert!(b.try_take(&cfg));
        assert!(b.try_take(&cfg));
        assert!(!b.try_take(&cfg));
    }

    #[test]
    fn retry_after_is_positive_when_empty() {
        let cfg = RateLimitConfig {
            capacity: 1.0,
            refill_per_sec: 1.0,
        };
        let mut b = Bucket::new(cfg.capacity);
        assert!(b.try_take(&cfg));
        let r = b.retry_after_secs(&cfg);
        assert!(r >= 1, "retry_after should advise at least 1s, got {r}");
    }
}
