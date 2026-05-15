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
//!
//! When the immediate peer is a configured trusted proxy (e.g. the
//! showcase nginx in front of us, or an L7 load balancer), we read
//! the leftmost `X-Forwarded-For` / `X-Real-IP` entry instead so
//! every browser visitor gets its own bucket rather than sharing
//! one bucket keyed on the proxy's container IP.

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
use std::net::{IpAddr, SocketAddr};

use crate::state::AppState;

/// Which immediate peers we trust to set `X-Forwarded-For` /
/// `X-Real-IP`. `All` is for single-host dev stacks where every
/// upstream is on the same docker bridge; production should always
/// enumerate.
#[derive(Debug, Clone, Default)]
pub enum TrustedProxies {
    #[default]
    None,
    All,
    Only(Vec<IpAddr>),
}

impl TrustedProxies {
    fn trusts(&self, peer: IpAddr) -> bool {
        match self {
            TrustedProxies::None => false,
            TrustedProxies::All => true,
            TrustedProxies::Only(ips) => ips.contains(&peer),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Max burst size, in tokens. A token costs 1 per request.
    pub capacity: f64,
    /// Refill rate in tokens per second. At `capacity=60`, `rate=1.0`
    /// this yields a sustained 1 rps with 60-request burst headroom.
    pub refill_per_sec: f64,
    /// Peers we'll honor `X-Forwarded-For` / `X-Real-IP` from.
    pub trusted_proxies: TrustedProxies,
}

impl Default for RateLimitConfig {
    /// Sensible demo defaults. Production sets these per route group
    /// or per key tier.
    fn default() -> Self {
        Self {
            capacity: 120.0,
            refill_per_sec: 20.0,
            trusted_proxies: TrustedProxies::None,
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
///
/// IP resolution honors `X-Forwarded-For` / `X-Real-IP` only when the
/// immediate peer is in `trusted_proxies` — otherwise any client
/// could spoof their bucket key by sending the header themselves.
fn principal(headers: &HeaderMap, peer: Option<SocketAddr>, trusted: &TrustedProxies) -> String {
    if let Some(v) = headers.get(header::AUTHORIZATION).and_then(|h| h.to_str().ok()) {
        if let Some(tok) = v.strip_prefix("Bearer ") {
            return format!("bearer:{}", tok.trim());
        }
    }
    if let Some(v) = headers.get("x-api-key").and_then(|h| h.to_str().ok()) {
        return format!("apikey:{v}");
    }
    match peer {
        Some(p) => {
            if trusted.trusts(p.ip()) {
                if let Some(client) = client_ip_from_headers(headers) {
                    return format!("ip:{client}");
                }
            }
            format!("ip:{}", p.ip())
        }
        None => "anon".into(),
    }
}

/// Parse the leftmost entry from `X-Forwarded-For`, falling back to
/// `X-Real-IP`. Returns `None` if neither is present or parses.
/// Leftmost is the original client; intermediate proxies append
/// themselves to the right.
fn client_ip_from_headers(headers: &HeaderMap) -> Option<IpAddr> {
    if let Some(xff) = headers.get("x-forwarded-for").and_then(|h| h.to_str().ok()) {
        if let Some(first) = xff.split(',').next() {
            if let Ok(ip) = first.trim().parse::<IpAddr>() {
                return Some(ip);
            }
        }
    }
    headers
        .get("x-real-ip")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.trim().parse::<IpAddr>().ok())
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

    let key = principal(
        req.headers(),
        connect_info.map(|ConnectInfo(a)| a),
        &cfg.trusted_proxies,
    );
    // SAFETY CRITICAL: `dashmap::Entry` returned by `.entry()` holds
    // the shard's write lock for the entire lifetime of the guard.
    // Earlier revisions of this function kept `entry` alive through
    // the `next.run(req).await` below, which held the shard lock
    // across the entire downstream request — any later request that
    // hashed to the same shard wedged on
    // `dashmap::lock::RawRwLock::lock_exclusive_slow` forever, taking
    // the whole tokio runtime down with it. Verified via a watchdog
    // gdb dump (one tokio worker stuck deep in
    // `dashmap::{impl#4}::_yield_write_shard<String, …,
    // nebula_server::ratelimit::Bucket>` while every other worker
    // sat parked at `Context::park` with no work to schedule).
    //
    // Fix: hold the shard guard ONLY long enough to read out the
    // `(allowed, retry_secs)` decision. The guard goes out of scope
    // at the end of the inner block, before any `.await`, so no
    // DashMap-level lock is alive across an async suspension point.
    let (allowed, retry_secs) = {
        let entry = limiter
            .buckets
            .entry(key.clone())
            .or_insert_with(|| Mutex::new(Bucket::new(cfg.capacity)));
        let mut bucket = entry.lock();
        let allowed = bucket.try_take(cfg);
        let retry = if allowed { 0 } else { bucket.retry_after_secs(cfg) };
        (allowed, retry)
        // `entry` drops here → shard write lock released.
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
    fn principal_ignores_xff_from_untrusted_peer() {
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-for", "203.0.113.7".parse().unwrap());
        let peer: SocketAddr = "172.21.0.8:50000".parse().unwrap();
        let key = principal(&h, Some(peer), &TrustedProxies::None);
        assert_eq!(key, "ip:172.21.0.8");
    }

    #[test]
    fn principal_uses_xff_when_peer_is_trusted() {
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-for", "203.0.113.7, 10.0.0.1".parse().unwrap());
        let peer: SocketAddr = "172.21.0.8:50000".parse().unwrap();
        let trusted = TrustedProxies::Only(vec!["172.21.0.8".parse().unwrap()]);
        let key = principal(&h, Some(peer), &trusted);
        assert_eq!(key, "ip:203.0.113.7");
    }

    #[test]
    fn principal_falls_back_to_real_ip_when_xff_missing() {
        let mut h = HeaderMap::new();
        h.insert("x-real-ip", "198.51.100.4".parse().unwrap());
        let peer: SocketAddr = "172.21.0.8:50000".parse().unwrap();
        let key = principal(&h, Some(peer), &TrustedProxies::All);
        assert_eq!(key, "ip:198.51.100.4");
    }

    #[test]
    fn principal_bearer_token_beats_xff() {
        let mut h = HeaderMap::new();
        h.insert("authorization", "Bearer abc".parse().unwrap());
        h.insert("x-forwarded-for", "203.0.113.7".parse().unwrap());
        let peer: SocketAddr = "172.21.0.8:50000".parse().unwrap();
        let key = principal(&h, Some(peer), &TrustedProxies::All);
        assert_eq!(key, "bearer:abc");
    }

    #[test]
    fn principal_ignores_garbage_xff() {
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-for", "not-an-ip".parse().unwrap());
        let peer: SocketAddr = "172.21.0.8:50000".parse().unwrap();
        let key = principal(&h, Some(peer), &TrustedProxies::All);
        assert_eq!(key, "ip:172.21.0.8");
    }

    #[test]
    fn retry_after_is_positive_when_empty() {
        let cfg = RateLimitConfig {
            capacity: 1.0,
            refill_per_sec: 1.0,
            ..Default::default()
        };
        let mut b = Bucket::new(cfg.capacity);
        assert!(b.try_take(&cfg));
        let r = b.retry_after_secs(&cfg);
        assert!(r >= 1, "retry_after should advise at least 1s, got {r}");
    }
}
