//! Bounded retry for transient embedder failures (design 0010 §5).
//!
//! A generic wrapper rather than logic inside [`OpenAiEmbedder`]: the
//! retry policy is provider-agnostic and, as a wrapper over the
//! `Embedder` trait, testable with an injected flaky embedder instead
//! of a live HTTP server.
//!
//! Only *transient* failures retry: connect/transport errors, 429,
//! and 5xx. Caller errors (4xx), decode failures, and dimension
//! mismatches fail immediately — retrying those burns latency on an
//! outcome that cannot change.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::{EmbedError, Embedder, Result};

#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Additional attempts after the first failure (2 = 3 total).
    pub max_retries: u32,
    /// First backoff; doubles per retry. 200ms → 400ms → 800ms keeps
    /// the worst case well under the 30s provider timeout.
    pub base_backoff: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 2,
            base_backoff: Duration::from_millis(200),
        }
    }
}

/// True for failures where a retry can plausibly succeed.
pub fn is_transient(err: &EmbedError) -> bool {
    match err {
        EmbedError::Http(_) => true,
        EmbedError::Provider { status, .. } => *status == 429 || *status >= 500,
        _ => false,
    }
}

pub struct RetryingEmbedder {
    inner: Arc<dyn Embedder>,
    config: RetryConfig,
}

impl RetryingEmbedder {
    pub fn new(inner: Arc<dyn Embedder>, config: RetryConfig) -> Self {
        Self { inner, config }
    }
}

#[async_trait]
impl Embedder for RetryingEmbedder {
    fn dim(&self) -> usize {
        self.inner.dim()
    }

    fn model(&self) -> &str {
        self.inner.model()
    }

    async fn embed(&self, inputs: &[String]) -> Result<Vec<Vec<f32>>> {
        let mut backoff = self.config.base_backoff;
        let mut attempt = 0u32;
        loop {
            match self.inner.embed(inputs).await {
                Ok(v) => return Ok(v),
                Err(e) if is_transient(&e) && attempt < self.config.max_retries => {
                    attempt += 1;
                    tracing::warn!(
                        attempt,
                        max = self.config.max_retries,
                        error = %e,
                        "transient embed failure; retrying"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Fails the first `fail_first` calls with the given error factory,
    /// then succeeds.
    struct Flaky {
        calls: AtomicU32,
        fail_first: u32,
        status: u16,
    }

    #[async_trait]
    impl Embedder for Flaky {
        fn dim(&self) -> usize {
            2
        }
        fn model(&self) -> &str {
            "flaky"
        }
        async fn embed(&self, inputs: &[String]) -> Result<Vec<Vec<f32>>> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if n < self.fail_first {
                return Err(EmbedError::Provider {
                    status: self.status,
                    body: "boom".into(),
                });
            }
            Ok(inputs.iter().map(|_| vec![0.0, 1.0]).collect())
        }
    }

    fn fast_config() -> RetryConfig {
        RetryConfig {
            max_retries: 2,
            base_backoff: Duration::from_millis(1),
        }
    }

    #[tokio::test]
    async fn retries_transient_500_and_succeeds() {
        let flaky = Arc::new(Flaky {
            calls: AtomicU32::new(0),
            fail_first: 2,
            status: 500,
        });
        let r = RetryingEmbedder::new(flaky.clone(), fast_config());
        let out = r.embed(&["a".into()]).await.unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(flaky.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn gives_up_after_max_retries() {
        let flaky = Arc::new(Flaky {
            calls: AtomicU32::new(0),
            fail_first: 10,
            status: 503,
        });
        let r = RetryingEmbedder::new(flaky.clone(), fast_config());
        assert!(r.embed(&["a".into()]).await.is_err());
        // 1 initial + 2 retries.
        assert_eq!(flaky.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn does_not_retry_caller_errors() {
        let flaky = Arc::new(Flaky {
            calls: AtomicU32::new(0),
            fail_first: 10,
            status: 400,
        });
        let r = RetryingEmbedder::new(flaky.clone(), fast_config());
        assert!(r.embed(&["a".into()]).await.is_err());
        assert_eq!(flaky.calls.load(Ordering::SeqCst), 1, "400 must not retry");
    }
}
