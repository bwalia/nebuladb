//! Health-tracked failover across several embedding endpoints.
//!
//! Prod embeds through Ollama instances on one LAN host. Behind a plain
//! Kubernetes Service they were all marked ready forever, so when one
//! died, every connection balanced onto it failed. This wrapper keeps
//! per-endpoint health instead:
//!
//! - Requests go round-robin across endpoints currently believed up.
//! - A retryable failure (network error, timeout, 5xx, 429, garbled
//!   body) marks the endpoint down for a cooldown that doubles on each
//!   consecutive failure (2s → 4s → … capped at 2 min), and the same
//!   request moves on to the next endpoint.
//! - Endpoints in cooldown are still tried last, so a request only fails
//!   when *every* endpoint failed it. A success clears the cooldown.
//! - `fallback` endpoints (e.g. the public gateway) are used only after
//!   every primary has failed the request.
//!
//! Non-retryable errors (4xx other than 429, dimension mismatch, empty
//! batch) come back immediately — another endpoint would answer the
//! same, and a dimension mismatch must never be papered over.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::{EmbedError, Embedder, Result};

const BASE_COOLDOWN: Duration = Duration::from_secs(2);
const MAX_COOLDOWN: Duration = Duration::from_secs(120);

/// One endpoint's health, as last observed.
#[derive(Debug, Clone, serde::Serialize)]
pub struct EndpointStatus {
    pub name: String,
    pub fallback: bool,
    pub healthy: bool,
    pub consecutive_failures: u32,
    /// Milliseconds until the endpoint leaves cooldown (0 when healthy).
    pub retry_in_ms: u64,
    pub last_error: Option<String>,
    pub successes: u64,
    pub failures: u64,
}

#[derive(Default)]
struct Health {
    consecutive_failures: u32,
    down_until: Option<Instant>,
    last_error: Option<String>,
    successes: u64,
    failures: u64,
}

struct Endpoint {
    name: String,
    fallback: bool,
    inner: Arc<dyn Embedder>,
    health: Mutex<Health>,
}

impl Endpoint {
    fn down_until(&self, now: Instant) -> Option<Instant> {
        self.health.lock().down_until.filter(|t| *t > now)
    }

    fn record_ok(&self) {
        let mut h = self.health.lock();
        if h.consecutive_failures > 0 {
            tracing::info!(endpoint = %self.name, "embed endpoint recovered");
        }
        h.consecutive_failures = 0;
        h.down_until = None;
        h.successes += 1;
    }

    fn record_err(&self, err: &EmbedError) {
        let mut h = self.health.lock();
        h.consecutive_failures = h.consecutive_failures.saturating_add(1);
        h.failures += 1;
        let shift = (h.consecutive_failures - 1).min(16);
        let cooldown = BASE_COOLDOWN.saturating_mul(1 << shift).min(MAX_COOLDOWN);
        h.down_until = Some(Instant::now() + cooldown);
        h.last_error = Some(err.to_string());
        tracing::warn!(
            endpoint = %self.name,
            failures = h.consecutive_failures,
            cooldown_ms = cooldown.as_millis() as u64,
            error = %err,
            "embed endpoint failed; cooling down"
        );
    }
}

/// See the module docs.
pub struct FailoverEmbedder {
    endpoints: Vec<Endpoint>,
    next: AtomicUsize,
    dim: usize,
    model: String,
}

impl FailoverEmbedder {
    /// `primaries` and `fallbacks` are `(name, embedder)` pairs. All must
    /// share one dim and model: the vectors land in the same index.
    pub fn new(
        primaries: Vec<(String, Arc<dyn Embedder>)>,
        fallbacks: Vec<(String, Arc<dyn Embedder>)>,
    ) -> Result<Self> {
        let first = primaries.first().ok_or_else(|| {
            EmbedError::Decode("failover embedder needs at least one endpoint".into())
        })?;
        let dim = first.1.dim();
        let model = first.1.model().to_string();
        let mut endpoints = Vec::new();
        for (fallback, list) in [(false, primaries), (true, fallbacks)] {
            for (name, inner) in list {
                if inner.dim() != dim {
                    return Err(EmbedError::DimensionMismatch {
                        expected: dim,
                        actual: inner.dim(),
                    });
                }
                endpoints.push(Endpoint {
                    name,
                    fallback,
                    inner,
                    health: Mutex::new(Health::default()),
                });
            }
        }
        Ok(Self {
            endpoints,
            next: AtomicUsize::new(0),
            dim,
            model,
        })
    }

    pub fn status(&self) -> Vec<EndpointStatus> {
        let now = Instant::now();
        self.endpoints
            .iter()
            .map(|e| {
                let h = e.health.lock();
                let retry_in = h.down_until.and_then(|t| t.checked_duration_since(now));
                EndpointStatus {
                    name: e.name.clone(),
                    fallback: e.fallback,
                    healthy: retry_in.is_none(),
                    consecutive_failures: h.consecutive_failures,
                    retry_in_ms: retry_in.map_or(0, |d| d.as_millis() as u64),
                    last_error: h.last_error.clone(),
                    successes: h.successes,
                    failures: h.failures,
                }
            })
            .collect()
    }

    /// Order to try endpoints in for one request: primaries before
    /// fallbacks; within a tier, healthy ones round-robin, then those in
    /// cooldown, soonest-recovering first.
    fn attempt_order(&self) -> Vec<usize> {
        let now = Instant::now();
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        let mut order = Vec::with_capacity(self.endpoints.len());
        for fallback in [false, true] {
            let tier: Vec<usize> = (0..self.endpoints.len())
                .filter(|&i| self.endpoints[i].fallback == fallback)
                .collect();
            if tier.is_empty() {
                continue;
            }
            let n = tier.len();
            let mut cooling = Vec::new();
            for k in 0..n {
                let i = tier[(start + k) % n];
                match self.endpoints[i].down_until(now) {
                    None => order.push(i),
                    Some(t) => cooling.push((t, i)),
                }
            }
            cooling.sort_by_key(|(t, _)| *t);
            order.extend(cooling.into_iter().map(|(_, i)| i));
        }
        order
    }
}

/// Whether another endpoint might succeed where this one failed.
fn retryable(err: &EmbedError) -> bool {
    match err {
        EmbedError::Http(_) | EmbedError::Decode(_) => true,
        EmbedError::Provider { status, .. } => *status == 429 || *status >= 500,
        _ => false,
    }
}

#[async_trait]
impl Embedder for FailoverEmbedder {
    fn dim(&self) -> usize {
        self.dim
    }

    fn model(&self) -> &str {
        &self.model
    }

    async fn embed(&self, inputs: &[String]) -> Result<Vec<Vec<f32>>> {
        let mut last_err = None;
        for i in self.attempt_order() {
            let ep = &self.endpoints[i];
            match ep.inner.embed(inputs).await {
                Ok(v) => {
                    ep.record_ok();
                    return Ok(v);
                }
                Err(e) if retryable(&e) => {
                    ep.record_err(&e);
                    last_err = Some(e);
                }
                Err(e) => return Err(e),
            }
        }
        Err(last_err.unwrap_or(EmbedError::EmptyBatch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    /// Fails with a 503 while `broken` is set, counting calls.
    struct Flaky {
        broken: std::sync::atomic::AtomicBool,
        calls: AtomicU32,
        status: u16,
    }

    impl Flaky {
        fn new(broken: bool) -> Arc<Self> {
            Self::with_status(broken, 503)
        }
        fn with_status(broken: bool, status: u16) -> Arc<Self> {
            Arc::new(Self {
                broken: broken.into(),
                calls: AtomicU32::new(0),
                status,
            })
        }
        fn calls(&self) -> u32 {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl Embedder for Flaky {
        fn dim(&self) -> usize {
            4
        }
        fn model(&self) -> &str {
            "flaky"
        }
        async fn embed(&self, inputs: &[String]) -> Result<Vec<Vec<f32>>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.broken.load(Ordering::SeqCst) {
                return Err(EmbedError::Provider {
                    status: self.status,
                    body: "down".into(),
                });
            }
            Ok(inputs.iter().map(|_| vec![0.0; 4]).collect())
        }
    }

    fn dyn_(e: &Arc<Flaky>) -> Arc<dyn Embedder> {
        e.clone()
    }

    #[tokio::test]
    async fn a_dead_endpoint_is_skipped_and_never_fails_a_request() {
        let good = Flaky::new(false);
        let dead = Flaky::new(true);
        let f = FailoverEmbedder::new(
            vec![("good".into(), dyn_(&good)), ("dead".into(), dyn_(&dead))],
            vec![],
        )
        .unwrap();
        for _ in 0..20 {
            f.embed_one("x").await.unwrap();
        }
        // The dead one is hit once, then sits out its cooldown.
        assert_eq!(dead.calls(), 1);
        assert_eq!(good.calls(), 20);
        let st = f.status();
        assert!(st[0].healthy && !st[1].healthy);
        assert!(st[1].last_error.as_deref().unwrap().contains("503"));
    }

    #[tokio::test]
    async fn healthy_endpoints_share_the_load() {
        let a = Flaky::new(false);
        let b = Flaky::new(false);
        let f = FailoverEmbedder::new(vec![("a".into(), dyn_(&a)), ("b".into(), dyn_(&b))], vec![])
            .unwrap();
        for _ in 0..10 {
            f.embed_one("x").await.unwrap();
        }
        assert_eq!((a.calls(), b.calls()), (5, 5));
    }

    #[tokio::test]
    async fn fallback_only_when_every_primary_fails() {
        let p = Flaky::new(false);
        let fb = Flaky::new(false);
        let f = FailoverEmbedder::new(vec![("p".into(), dyn_(&p))], vec![("gw".into(), dyn_(&fb))])
            .unwrap();
        f.embed_one("x").await.unwrap();
        assert_eq!(fb.calls(), 0);
        p.broken.store(true, Ordering::SeqCst);
        f.embed_one("x").await.unwrap();
        assert_eq!(fb.calls(), 1);
    }

    #[tokio::test]
    async fn endpoints_in_cooldown_are_still_tried_as_a_last_resort() {
        let only = Flaky::new(true);
        let f = FailoverEmbedder::new(vec![("only".into(), dyn_(&only))], vec![]).unwrap();
        assert!(f.embed_one("x").await.is_err());
        only.broken.store(false, Ordering::SeqCst);
        // Still in cooldown, but it is the only option — so it is tried,
        // succeeds, and is healthy again.
        f.embed_one("x").await.unwrap();
        assert!(f.status()[0].healthy);
    }

    #[tokio::test]
    async fn client_errors_are_not_retried_elsewhere() {
        let a = Flaky::with_status(true, 400);
        let b = Flaky::new(false);
        let f = FailoverEmbedder::new(vec![("a".into(), dyn_(&a)), ("b".into(), dyn_(&b))], vec![])
            .unwrap();
        // Round-robin starts at a.
        assert!(matches!(
            f.embed_one("x").await,
            Err(EmbedError::Provider { status: 400, .. })
        ));
        assert_eq!(b.calls(), 0);
        assert!(
            f.status()[0].healthy,
            "a 400 is the request's fault, not the endpoint's"
        );
    }

    #[test]
    fn mismatched_dims_are_rejected_at_construction() {
        struct D(usize);
        #[async_trait]
        impl Embedder for D {
            fn dim(&self) -> usize {
                self.0
            }
            fn model(&self) -> &str {
                "d"
            }
            async fn embed(&self, _: &[String]) -> Result<Vec<Vec<f32>>> {
                unreachable!()
            }
        }
        let r = FailoverEmbedder::new(
            vec![("a".into(), Arc::new(D(768)) as Arc<dyn Embedder>)],
            vec![("b".into(), Arc::new(D(384)) as Arc<dyn Embedder>)],
        );
        assert!(r.is_err());
    }
}
