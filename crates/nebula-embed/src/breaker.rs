//! Embedder circuit breaker (design 0010 §5).
//!
//! After K consecutive transient failures the circuit opens: calls
//! fail fast with [`EmbedError::CircuitOpen`] instead of each burning
//! a full provider timeout. After a cooldown one probe call is let
//! through (half-open); success closes the circuit, failure re-opens
//! it for another cooldown.
//!
//! The breaker's state doubles as the node's `ai_degraded` signal —
//! [`AiHealth`] is a cheap cloneable handle the server surfaces on
//! `/healthz`, `/metrics`, and `/admin/reliability`. AI degradation is
//! orthogonal to the resource operating mode: a node can be `normal +
//! ai_degraded` and must keep serving every embedder-free path
//! (BM25, client-vector search, SQL, replication) untouched.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::retry::is_transient;
use crate::{EmbedError, Embedder, Result};

#[derive(Debug, Clone)]
pub struct BreakerConfig {
    /// Consecutive transient failures that open the circuit.
    pub failure_threshold: u32,
    /// How long the circuit stays open before a half-open probe.
    pub cooldown: Duration,
}

impl Default for BreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            cooldown: Duration::from_secs(15),
        }
    }
}

/// Shared, lock-free view of the breaker for observability. Cloning
/// is an `Arc` bump; reads are relaxed atomic loads.
#[derive(Debug, Default)]
pub struct AiHealth {
    degraded: AtomicBool,
    /// Times the circuit has opened since boot (counter for /metrics).
    opens_total: AtomicU64,
    /// Calls rejected fast while the circuit was open.
    rejected_total: AtomicU64,
}

impl AiHealth {
    pub fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::Relaxed)
    }
    pub fn opens_total(&self) -> u64 {
        self.opens_total.load(Ordering::Relaxed)
    }
    pub fn rejected_total(&self) -> u64 {
        self.rejected_total.load(Ordering::Relaxed)
    }
}

enum State {
    Closed,
    /// Open since `Instant`; probes allowed after cooldown.
    Open(Instant),
    /// One probe is in flight; other callers still fail fast.
    HalfOpen,
}

pub struct CircuitBreakerEmbedder {
    inner: Arc<dyn Embedder>,
    config: BreakerConfig,
    state: Mutex<State>,
    consecutive_failures: AtomicU32,
    health: Arc<AiHealth>,
}

impl CircuitBreakerEmbedder {
    pub fn new(inner: Arc<dyn Embedder>, config: BreakerConfig) -> Self {
        Self {
            inner,
            config,
            state: Mutex::new(State::Closed),
            consecutive_failures: AtomicU32::new(0),
            health: Arc::new(AiHealth::default()),
        }
    }

    /// Handle for /healthz, /metrics, /admin/reliability.
    pub fn health(&self) -> Arc<AiHealth> {
        Arc::clone(&self.health)
    }

    /// Decide whether this call may proceed. Returns `false` for a
    /// fast-fail; flips Open→HalfOpen when the cooldown has elapsed so
    /// exactly one caller probes the provider.
    fn admit(&self) -> bool {
        let mut state = self.state.lock();
        match *state {
            State::Closed => true,
            State::HalfOpen => false,
            State::Open(since) => {
                if since.elapsed() >= self.config.cooldown {
                    *state = State::HalfOpen;
                    true
                } else {
                    false
                }
            }
        }
    }

    fn on_success(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        let mut state = self.state.lock();
        if !matches!(*state, State::Closed) {
            tracing::info!("embedder circuit closed — provider recovered");
        }
        *state = State::Closed;
        self.health.degraded.store(false, Ordering::Relaxed);
    }

    fn on_transient_failure(&self) {
        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        let mut state = self.state.lock();
        let should_open = match *state {
            // A failed half-open probe re-opens immediately.
            State::HalfOpen => true,
            State::Closed => failures >= self.config.failure_threshold,
            State::Open(_) => false,
        };
        if should_open {
            tracing::warn!(
                consecutive_failures = failures,
                cooldown_secs = self.config.cooldown.as_secs(),
                "embedder circuit OPEN — failing fast; embedder-free paths unaffected"
            );
            *state = State::Open(Instant::now());
            self.health.degraded.store(true, Ordering::Relaxed);
            self.health.opens_total.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[async_trait]
impl Embedder for CircuitBreakerEmbedder {
    fn dim(&self) -> usize {
        self.inner.dim()
    }

    fn model(&self) -> &str {
        self.inner.model()
    }

    async fn embed(&self, inputs: &[String]) -> Result<Vec<Vec<f32>>> {
        if !self.admit() {
            self.health.rejected_total.fetch_add(1, Ordering::Relaxed);
            return Err(EmbedError::CircuitOpen);
        }
        match self.inner.embed(inputs).await {
            Ok(v) => {
                self.on_success();
                Ok(v)
            }
            Err(e) => {
                // Only transient failures count toward opening: a 400
                // (bad input) says nothing about provider health.
                if is_transient(&e) {
                    self.on_transient_failure();
                } else if matches!(self.state.lock().deref_state(), StateKind::HalfOpen) {
                    // A non-transient error still resolves the probe —
                    // the provider answered, so the circuit closes.
                    self.on_success_state_only();
                }
                Err(e)
            }
        }
    }
}

/// Small helpers so the non-transient half-open path above stays
/// readable without exposing `State` publicly.
enum StateKind {
    Closed,
    Open,
    HalfOpen,
}

impl State {
    fn deref_state(&self) -> StateKind {
        match self {
            State::Closed => StateKind::Closed,
            State::Open(_) => StateKind::Open,
            State::HalfOpen => StateKind::HalfOpen,
        }
    }
}

impl CircuitBreakerEmbedder {
    /// Close the circuit without resetting the failure counter — used
    /// when a half-open probe gets a definitive non-transient answer.
    fn on_success_state_only(&self) {
        *self.state.lock() = State::Closed;
        self.health.degraded.store(false, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Scripted {
        // true = fail transiently
        script: Mutex<Vec<bool>>,
    }

    impl Scripted {
        fn new(script: Vec<bool>) -> Arc<Self> {
            Arc::new(Self {
                script: Mutex::new(script),
            })
        }
    }

    #[async_trait]
    impl Embedder for Scripted {
        fn dim(&self) -> usize {
            2
        }
        fn model(&self) -> &str {
            "scripted"
        }
        async fn embed(&self, inputs: &[String]) -> Result<Vec<Vec<f32>>> {
            let fail = {
                let mut s = self.script.lock();
                if s.is_empty() {
                    false
                } else {
                    s.remove(0)
                }
            };
            if fail {
                Err(EmbedError::Provider {
                    status: 503,
                    body: "down".into(),
                })
            } else {
                Ok(inputs.iter().map(|_| vec![0.0, 1.0]).collect())
            }
        }
    }

    fn cfg(threshold: u32, cooldown_ms: u64) -> BreakerConfig {
        BreakerConfig {
            failure_threshold: threshold,
            cooldown: Duration::from_millis(cooldown_ms),
        }
    }

    #[tokio::test]
    async fn opens_after_threshold_and_fails_fast() {
        let b = CircuitBreakerEmbedder::new(Scripted::new(vec![true, true, true]), cfg(2, 60_000));
        let h = b.health();
        assert!(b.embed(&["x".into()]).await.is_err());
        assert!(!h.is_degraded(), "one failure is below threshold");
        assert!(b.embed(&["x".into()]).await.is_err());
        assert!(h.is_degraded(), "second failure opens the circuit");
        // Third call never reaches the provider.
        let err = b.embed(&["x".into()]).await.unwrap_err();
        assert!(matches!(err, EmbedError::CircuitOpen));
        assert_eq!(h.rejected_total(), 1);
        assert_eq!(h.opens_total(), 1);
    }

    #[tokio::test]
    async fn half_open_probe_recovers() {
        // Two failures open it; after cooldown the probe succeeds.
        let b = CircuitBreakerEmbedder::new(Scripted::new(vec![true, true, false]), cfg(2, 10));
        let h = b.health();
        let _ = b.embed(&["x".into()]).await;
        let _ = b.embed(&["x".into()]).await;
        assert!(h.is_degraded());
        tokio::time::sleep(Duration::from_millis(20)).await;
        // Probe goes through and succeeds → closed.
        assert!(b.embed(&["x".into()]).await.is_ok());
        assert!(!h.is_degraded());
        // Subsequent calls flow normally.
        assert!(b.embed(&["x".into()]).await.is_ok());
    }

    #[tokio::test]
    async fn failed_probe_reopens() {
        let b =
            CircuitBreakerEmbedder::new(Scripted::new(vec![true, true, true, false]), cfg(2, 10));
        let h = b.health();
        let _ = b.embed(&["x".into()]).await;
        let _ = b.embed(&["x".into()]).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        // Probe fails → reopen.
        assert!(!matches!(
            b.embed(&["x".into()]).await.unwrap_err(),
            EmbedError::CircuitOpen
        ));
        assert!(h.is_degraded());
        assert_eq!(h.opens_total(), 2);
        // Still open inside the new cooldown: fast-fail.
        assert!(matches!(
            b.embed(&["x".into()]).await.unwrap_err(),
            EmbedError::CircuitOpen
        ));
    }

    #[tokio::test]
    async fn non_transient_errors_do_not_open() {
        struct Always400;
        #[async_trait]
        impl Embedder for Always400 {
            fn dim(&self) -> usize {
                2
            }
            fn model(&self) -> &str {
                "m"
            }
            async fn embed(&self, _inputs: &[String]) -> Result<Vec<Vec<f32>>> {
                Err(EmbedError::Provider {
                    status: 400,
                    body: "bad".into(),
                })
            }
        }
        let b = CircuitBreakerEmbedder::new(Arc::new(Always400), cfg(1, 60_000));
        let h = b.health();
        for _ in 0..5 {
            let _ = b.embed(&["x".into()]).await;
        }
        assert!(!h.is_degraded(), "caller errors must not open the circuit");
    }
}
