//! Redis-backed embedding cache.
//!
//! Wraps any [`Embedder`] with a remote cache keyed by
//! SHA-256(`model || 0x00 || text`). Meant to **sit above** the
//! in-process [`nebula_cache::CachingEmbedder`] — together they form a
//! two-tier cache:
//!
//! ```text
//! call site
//!     │
//!     ▼
//! CachingEmbedder (in-proc, nanoseconds)  ── miss ──▶
//!     │
//!     ▼
//! RedisEmbedCache (network, ~ms)          ── miss ──▶
//!     │
//!     ▼
//! raw Embedder (OpenAI / Ollama, 10-500ms)
//! ```
//!
//! The in-proc layer keeps warm queries cheap; the Redis layer lets
//! multiple replicas share work and survives restarts.
//!
//! # Failure mode: pass-through, never fail
//!
//! A dead or slow Redis must not break embedding. Every Redis error
//! is logged at `warn` and treated as a cache miss. The upstream
//! embedder runs, its result is returned to the caller, and — if
//! possible — a best-effort write attempt populates Redis. Tests
//! verify this (see `redis_down_is_transparent`).
//!
//! # Wire format
//!
//! Values are raw little-endian `f32` bytes. No JSON, no Base64. A
//! 1536-dim vector is 6144 bytes; JSON-encoded it's ~40 KB. Binary is
//! both cheaper and faster to (de)serialize.

use std::time::Duration;

use async_trait::async_trait;
use redis::AsyncCommands;
use sha2::{Digest, Sha256};
use std::sync::Arc;

use nebula_embed::{EmbedError, Embedder};

#[derive(Debug, thiserror::Error)]
pub enum RedisCacheError {
    #[error("redis: {0}")]
    Redis(#[from] redis::RedisError),
}

#[derive(Debug, Clone)]
pub struct RedisCacheConfig {
    /// redis://host:port[/db]. `unix:///path` also works.
    pub url: String,
    /// Key prefix. Lets multiple NebulaDB deployments share one Redis
    /// without colliding — e.g. `"nebula:prod:"`.
    pub prefix: String,
    /// Optional TTL. `None` = persist indefinitely; Redis eviction
    /// policy handles pressure. A TTL is a good idea when embedding
    /// models are rotated frequently in dev.
    pub ttl: Option<Duration>,
    /// Per-operation timeout. Redis should be fast; if it isn't, we
    /// want to bail and hit the upstream embedder rather than stall
    /// every request behind a slow cache.
    pub op_timeout: Duration,
}

impl Default for RedisCacheConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379".into(),
            prefix: "nebula:embed:".into(),
            ttl: None,
            op_timeout: Duration::from_millis(150),
        }
    }
}

/// Simple stat counters. Not atomic-bitwise perfect (we don't count
/// partial-batch hits per-item), but accurate enough to drive a
/// Grafana panel.
#[derive(Debug, Default)]
pub struct RedisCacheStats {
    pub hits: std::sync::atomic::AtomicU64,
    pub misses: std::sync::atomic::AtomicU64,
    pub errors: std::sync::atomic::AtomicU64,
}

impl RedisCacheStats {
    pub fn snapshot(&self) -> (u64, u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        (
            self.hits.load(Relaxed),
            self.misses.load(Relaxed),
            self.errors.load(Relaxed),
        )
    }
}

/// `Arc<dyn Embedder>` with a Redis cache layered in front.
pub struct RedisEmbedCache {
    inner: Arc<dyn Embedder>,
    client: redis::Client,
    config: RedisCacheConfig,
    stats: Arc<RedisCacheStats>,
}

impl RedisEmbedCache {
    /// Construct a cache. Connects lazily: we only validate the URL
    /// here. Real connectivity is checked on the first request — the
    /// goal is to let the binary boot even if Redis is briefly down,
    /// and to keep the cache optional-by-nature.
    pub fn new(inner: Arc<dyn Embedder>, config: RedisCacheConfig) -> Result<Self, RedisCacheError> {
        let client = redis::Client::open(config.url.as_str())?;
        Ok(Self {
            inner,
            client,
            config,
            stats: Arc::new(RedisCacheStats::default()),
        })
    }

    pub fn stats(&self) -> Arc<RedisCacheStats> {
        Arc::clone(&self.stats)
    }

    fn key(&self, model: &str, text: &str) -> String {
        let mut h = Sha256::new();
        h.update(model.as_bytes());
        h.update([0u8]);
        h.update(text.as_bytes());
        let digest = h.finalize();
        // Keep the key short but unambiguous. Hex is 64 chars for
        // SHA-256 — fine for Redis, and trivially greppable.
        format!("{}{:x}", self.config.prefix, digest)
    }

    async fn mget(&self, keys: &[String]) -> Result<Vec<Option<Vec<u8>>>, redis::RedisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        // `MGET` returns `Vec<Option<Vec<u8>>>` directly from redis-rs.
        // Shortcut for empty input to avoid a useless round trip.
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let values: Vec<Option<Vec<u8>>> = conn.mget(keys).await?;
        Ok(values)
    }

    async fn mset(&self, pairs: &[(String, Vec<u8>)]) -> Result<(), redis::RedisError> {
        if pairs.is_empty() {
            return Ok(());
        }
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        // MSET doesn't accept a TTL. When a TTL is configured we
        // pipeline SET px instead; cheap enough for the batch sizes
        // (tens at most) we see here.
        if let Some(ttl) = self.config.ttl {
            let mut pipe = redis::pipe();
            pipe.atomic();
            let px = ttl.as_millis() as u64;
            for (k, v) in pairs {
                pipe.cmd("SET")
                    .arg(k)
                    .arg(v.as_slice())
                    .arg("PX")
                    .arg(px);
            }
            let _: () = pipe.query_async(&mut conn).await?;
        } else {
            let _: () = conn
                .mset(
                    &pairs
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.as_slice()))
                        .collect::<Vec<_>>(),
                )
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Embedder for RedisEmbedCache {
    fn dim(&self) -> usize {
        self.inner.dim()
    }

    fn model(&self) -> &str {
        self.inner.model()
    }

    async fn embed(&self, inputs: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        if inputs.is_empty() {
            return Err(EmbedError::EmptyBatch);
        }
        let dim = self.inner.dim();
        let expected_bytes = dim * 4;
        let model = self.inner.model();

        let keys: Vec<String> = inputs.iter().map(|t| self.key(model, t)).collect();

        // Phase 1: bulk GET. Timeout guards against Redis being
        // sluggish enough to matter — we'd rather take one upstream
        // hit than wait 30s on a network blip.
        let existing = match tokio::time::timeout(
            self.config.op_timeout,
            self.mget(&keys),
        )
        .await
        {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "redis mget failed; passing through");
                self.stats
                    .errors
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                vec![None; keys.len()]
            }
            Err(_) => {
                tracing::warn!("redis mget timed out; passing through");
                self.stats
                    .errors
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                vec![None; keys.len()]
            }
        };

        // Phase 2: classify hits / misses, decode hit bytes.
        let mut results: Vec<Option<Vec<f32>>> = vec![None; inputs.len()];
        let mut miss_positions: Vec<usize> = Vec::new();
        let mut miss_inputs: Vec<String> = Vec::new();
        for (i, raw) in existing.into_iter().enumerate() {
            match raw {
                Some(bytes) if bytes.len() == expected_bytes => {
                    results[i] = Some(bytes_to_f32(&bytes));
                    self.stats
                        .hits
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Some(bytes) => {
                    // Dimension mismatch — the model changed since
                    // the value was written. Treat as a miss and let
                    // the fresh embedding overwrite the stale entry.
                    tracing::warn!(
                        actual = bytes.len(),
                        expected = expected_bytes,
                        "redis value wrong size; treating as miss"
                    );
                    miss_positions.push(i);
                    miss_inputs.push(inputs[i].clone());
                    self.stats
                        .misses
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                None => {
                    miss_positions.push(i);
                    miss_inputs.push(inputs[i].clone());
                    self.stats
                        .misses
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }

        // Phase 3: call upstream for misses only.
        if !miss_inputs.is_empty() {
            let fresh = self.inner.embed(&miss_inputs).await?;
            if fresh.len() != miss_inputs.len() {
                return Err(EmbedError::Decode(format!(
                    "upstream returned {} for batch of {}",
                    fresh.len(),
                    miss_inputs.len()
                )));
            }

            // Phase 4: populate Redis (best-effort) and fill results.
            let mut to_write: Vec<(String, Vec<u8>)> = Vec::with_capacity(fresh.len());
            for (idx, vec) in miss_positions.iter().zip(fresh.iter()) {
                if vec.len() != dim {
                    return Err(EmbedError::DimensionMismatch {
                        expected: dim,
                        actual: vec.len(),
                    });
                }
                to_write.push((keys[*idx].clone(), f32_to_bytes(vec)));
            }

            // Write in the background — we don't want a slow Redis to
            // delay the caller's response once we already have the
            // vectors in hand. The write is small and self-contained;
            // any failure is logged and forgotten.
            let client = self.client.clone();
            let cfg = self.config.clone();
            let stats = Arc::clone(&self.stats);
            let write_set = to_write.clone();
            tokio::spawn(async move {
                let background = RedisEmbedCache {
                    inner: Arc::new(NoopEmbedder), // unused
                    client,
                    config: cfg,
                    stats: Arc::clone(&stats),
                };
                if let Err(e) = tokio::time::timeout(
                    background.config.op_timeout,
                    background.mset(&write_set),
                )
                .await
                .map_err(|_| ())
                .and_then(|r| r.map_err(|_| ()))
                {
                    stats
                        .errors
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let _ = e;
                    tracing::warn!("redis background write failed");
                }
            });

            // Fill the `results` slots with the freshly fetched vectors.
            // `zip` takes any IntoIterator; Rust 1.95's clippy flags
            // the explicit `.into_iter()` as useless. Pass `fresh`
            // directly — same semantics, no allocation.
            for (idx, vec) in miss_positions.into_iter().zip(fresh) {
                results[idx] = Some(vec);
            }
        }

        Ok(results
            .into_iter()
            .map(|o| o.expect("every position filled"))
            .collect())
    }
}

// Internal placeholder used by the background-write spawn to reuse
// `RedisEmbedCache`'s connection logic without holding a real inner
// embedder alive in the task. It panics if embed is called, which
// never happens — `mset` doesn't touch it.
struct NoopEmbedder;

#[async_trait]
impl Embedder for NoopEmbedder {
    fn dim(&self) -> usize {
        0
    }
    fn model(&self) -> &str {
        "noop"
    }
    async fn embed(&self, _inputs: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        unreachable!("background writer never calls embed");
    }
}

fn f32_to_bytes(v: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() * 4);
    for x in v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
}

fn bytes_to_f32(b: &[u8]) -> Vec<f32> {
    let mut out = Vec::with_capacity(b.len() / 4);
    // Chunk over the 4-byte groups. We've already verified the total
    // length matches `dim * 4`, so there's no partial tail to worry
    // about.
    for chunk in b.chunks_exact(4) {
        let bytes: [u8; 4] = chunk.try_into().unwrap();
        out.push(f32::from_le_bytes(bytes));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Counting {
        dim: usize,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl Embedder for Counting {
        fn dim(&self) -> usize {
            self.dim
        }
        fn model(&self) -> &str {
            "counting"
        }
        async fn embed(&self, inputs: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(inputs
                .iter()
                .map(|s| {
                    let mut v = vec![0.0f32; self.dim];
                    v[0] = s.len() as f32;
                    v
                })
                .collect())
        }
    }

    #[tokio::test]
    async fn redis_down_is_transparent() {
        // Point at a port nothing's listening on. Every Redis op must
        // log + fall back. The upstream embedder must still run and
        // the caller must still see valid vectors.
        let inner: Arc<dyn Embedder> = Arc::new(Counting {
            dim: 4,
            calls: AtomicUsize::new(0),
        });
        let upstream = Arc::clone(&inner);
        let cache = RedisEmbedCache::new(
            inner,
            RedisCacheConfig {
                url: "redis://127.0.0.1:1".into(),
                op_timeout: Duration::from_millis(50),
                ..RedisCacheConfig::default()
            },
        )
        .unwrap();

        let _ = upstream; // keep the reference for potential future assertions
        let out = cache.embed(&["hello".into()]).await.unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0][0], 5.0);
        // We can't downcast through the trait object, so rely on the
        // cache's own counters. Expect at least one error (the dead
        // MGET) and exactly one miss (one input).
        let (_, misses, errors) = cache.stats().snapshot();
        assert!(errors >= 1, "expected at least one error");
        assert_eq!(misses, 1);
    }

    #[test]
    fn f32_roundtrip() {
        let v = vec![0.0, 1.5, -2.25, 1e-9_f32, f32::NAN];
        let b = f32_to_bytes(&v);
        let back = bytes_to_f32(&b);
        // NaN != NaN; compare bit patterns.
        for (a, c) in v.iter().zip(back.iter()) {
            assert_eq!(a.to_bits(), c.to_bits(), "roundtrip diverged on {a}");
        }
    }

    #[test]
    fn key_derivation_is_stable_and_unique() {
        let inner: Arc<dyn Embedder> = Arc::new(Counting {
            dim: 4,
            calls: AtomicUsize::new(0),
        });
        let cache = RedisEmbedCache::new(
            inner,
            RedisCacheConfig {
                url: "redis://127.0.0.1:1".into(),
                prefix: "t:".into(),
                ..RedisCacheConfig::default()
            },
        )
        .unwrap();
        let k1 = cache.key("m1", "hello");
        let k2 = cache.key("m1", "hello");
        let k3 = cache.key("m2", "hello");
        let k4 = cache.key("m1", "world");
        assert_eq!(k1, k2);
        assert_ne!(k1, k3);
        assert_ne!(k1, k4);
        assert!(k1.starts_with("t:"));
    }
}

