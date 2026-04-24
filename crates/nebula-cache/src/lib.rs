//! Embedding cache.
//!
//! [`CachingEmbedder`] wraps any [`Embedder`] and skips the upstream
//! call when the same `(model, text)` has been embedded before.
//!
//! # Why it matters
//!
//! In real workloads, embedding is the single biggest latency budget
//! in the ingest and query paths. Two common shapes benefit hugely:
//!
//! - **Repeat ingestion.** Re-running a pipeline on the same corpus
//!   (common during tuning) pays the embedding bill once.
//! - **Warm queries.** Popular queries hit the cache; cold ones flow
//!   through to the provider unchanged.
//!
//! # Semantics
//!
//! Keys are SHA-256 of `model || 0x00 || text`:
//! - The null byte separator prevents `("model", "abc")` colliding
//!   with `("modela", "bc")`.
//! - Collisions on SHA-256 over short inputs are not a practical
//!   concern, but the fixed-size `[u8; 32]` key also gives us a cheap
//!   `Hash` implementation for the LRU.
//!
//! Values are `Arc<Vec<f32>>` so a cache hit clones an 8-byte pointer
//! rather than copying a 1536-float vector.
//!
//! # Batching
//!
//! `embed(inputs)` partitions inputs into hits and misses in a single
//! pass, issues one upstream call with just the misses, then splices
//! the results back into input order. The upstream therefore sees the
//! minimum possible batch — critical for paid APIs that charge per
//! input.
//!
//! # Concurrency
//!
//! The `LruCache` sits behind a `Mutex`. Because the hash is computed
//! outside the lock, contention is minimal even under concurrent hits.
//! A lock-free skiplist would buy marginal throughput at the cost of
//! eviction semantics; not worth it until measurements say so.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use lru::LruCache;
use parking_lot::Mutex;
use sha2::{Digest, Sha256};

use nebula_embed::{EmbedError, Embedder};

type Key = [u8; 32];

/// Atomic cache counters. Exposed so a server can surface hit rate
/// on `/metrics` without pulling the cache lock.
#[derive(Debug, Default)]
pub struct CacheStats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub evictions: AtomicU64,
    pub inserts: AtomicU64,
}

impl CacheStats {
    pub fn snapshot(&self) -> (u64, u64, u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
            self.evictions.load(Ordering::Relaxed),
            self.inserts.load(Ordering::Relaxed),
        )
    }
}

pub struct CachingEmbedder {
    inner: Arc<dyn Embedder>,
    cache: Mutex<LruCache<Key, Arc<Vec<f32>>>>,
    stats: Arc<CacheStats>,
    capacity: usize,
}

impl CachingEmbedder {
    /// Wrap an embedder with a cache of `capacity` entries. `capacity`
    /// must be non-zero; zero would mean "cache nothing" which is
    /// better expressed by not wrapping at all.
    pub fn new(inner: Arc<dyn Embedder>, capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity.max(1)).expect("capacity >= 1");
        Self {
            inner,
            cache: Mutex::new(LruCache::new(cap)),
            stats: Arc::new(CacheStats::default()),
            capacity: cap.get(),
        }
    }

    pub fn stats(&self) -> Arc<CacheStats> {
        Arc::clone(&self.stats)
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Current entry count. Takes the lock; intended for tests /
    /// occasional inspection, not a hot path.
    pub fn len(&self) -> usize {
        self.cache.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn key(model: &str, text: &str) -> Key {
        let mut h = Sha256::new();
        h.update(model.as_bytes());
        h.update([0u8]);
        h.update(text.as_bytes());
        h.finalize().into()
    }
}

#[async_trait]
impl Embedder for CachingEmbedder {
    fn dim(&self) -> usize {
        self.inner.dim()
    }

    fn model(&self) -> &str {
        self.inner.model()
    }

    async fn embed(
        &self,
        inputs: &[String],
    ) -> Result<Vec<Vec<f32>>, EmbedError> {
        if inputs.is_empty() {
            return Err(EmbedError::EmptyBatch);
        }

        let model = self.inner.model();
        let keys: Vec<Key> = inputs.iter().map(|t| Self::key(model, t)).collect();

        // Phase 1: probe the cache in one lock acquisition. `get`
        // mutates LRU order, so we must use the mutable API even for
        // reads — another reason per-entry locks would be premature.
        let mut results: Vec<Option<Arc<Vec<f32>>>> = vec![None; inputs.len()];
        let mut missed_positions: Vec<usize> = Vec::new();
        let mut missed_texts: Vec<String> = Vec::new();
        {
            let mut cache = self.cache.lock();
            for (i, k) in keys.iter().enumerate() {
                if let Some(v) = cache.get(k) {
                    results[i] = Some(Arc::clone(v));
                    self.stats.hits.fetch_add(1, Ordering::Relaxed);
                } else {
                    missed_positions.push(i);
                    missed_texts.push(inputs[i].clone());
                    self.stats.misses.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        // Phase 2: upstream call for misses only. Skipping entirely
        // when everything hit is a real win — the whole reason for
        // this crate.
        if !missed_texts.is_empty() {
            let fresh = self.inner.embed(&missed_texts).await?;
            if fresh.len() != missed_texts.len() {
                return Err(EmbedError::Decode(format!(
                    "upstream returned {} for batch of {}",
                    fresh.len(),
                    missed_texts.len()
                )));
            }

            // Phase 3: insert results under a single lock acquisition.
            // We use `push` (not `put`) because `put` only reports
            // *replacements* — it returns `None` when an over-capacity
            // insert silently evicts the LRU tail. `push` returns the
            // displaced `(key, value)` whenever anything leaves, which
            // is what we actually want for the metric.
            //
            // We distinguish replacement from eviction by comparing
            // the returned key against the key we just inserted.
            {
                let mut cache = self.cache.lock();
                for (pos, vec) in missed_positions.iter().zip(fresh) {
                    let arc = Arc::new(vec);
                    let key = keys[*pos];
                    if let Some((old_key, _)) = cache.push(key, Arc::clone(&arc)) {
                        if old_key != key {
                            self.stats.evictions.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    self.stats.inserts.fetch_add(1, Ordering::Relaxed);
                    results[*pos] = Some(arc);
                }
            }
        }

        // Phase 4: materialize `Vec<Vec<f32>>`. We clone the inner
        // `Vec` because the `Embedder` contract returns owned vectors;
        // a future contract revision could yield `Arc<Vec<f32>>` and
        // skip this copy entirely.
        Ok(results
            .into_iter()
            .map(|o| (*o.expect("every position filled")).clone())
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::AtomicUsize;

    /// An embedder that counts upstream calls and inputs, so a test
    /// can verify "cache hit" really means "upstream not called".
    struct CountingEmbedder {
        dim: usize,
        model: String,
        calls: AtomicUsize,
        inputs_seen: AtomicUsize,
    }

    impl CountingEmbedder {
        fn new(dim: usize, model: &str) -> Self {
            Self {
                dim,
                model: model.into(),
                calls: AtomicUsize::new(0),
                inputs_seen: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl Embedder for CountingEmbedder {
        fn dim(&self) -> usize {
            self.dim
        }
        fn model(&self) -> &str {
            &self.model
        }
        async fn embed(
            &self,
            inputs: &[String],
        ) -> Result<Vec<Vec<f32>>, EmbedError> {
            if inputs.is_empty() {
                return Err(EmbedError::EmptyBatch);
            }
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.inputs_seen
                .fetch_add(inputs.len(), Ordering::Relaxed);
            // Deterministic fake vector per input.
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
    async fn hits_skip_upstream() {
        let upstream = Arc::new(CountingEmbedder::new(8, "m"));
        let cache = CachingEmbedder::new(upstream.clone(), 16);
        let batch = vec!["alpha".to_string(), "beta".to_string()];

        let _ = cache.embed(&batch).await.unwrap();
        assert_eq!(upstream.calls.load(Ordering::Relaxed), 1);
        assert_eq!(upstream.inputs_seen.load(Ordering::Relaxed), 2);

        // Second call: both should hit.
        let _ = cache.embed(&batch).await.unwrap();
        assert_eq!(upstream.calls.load(Ordering::Relaxed), 1, "upstream hit again");

        let (hits, misses, _, _) = cache.stats().snapshot();
        assert_eq!(hits, 2);
        assert_eq!(misses, 2);
    }

    #[tokio::test]
    async fn mixed_batch_only_queries_misses() {
        let upstream = Arc::new(CountingEmbedder::new(4, "m"));
        let cache = CachingEmbedder::new(upstream.clone(), 16);

        let _ = cache.embed(&["a".into()]).await.unwrap();
        assert_eq!(upstream.inputs_seen.load(Ordering::Relaxed), 1);

        // "a" should hit, "b" and "c" miss → upstream sees 2 inputs.
        let _ = cache.embed(&["a".into(), "b".into(), "c".into()]).await.unwrap();
        assert_eq!(upstream.inputs_seen.load(Ordering::Relaxed), 3);
        assert_eq!(upstream.calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn preserves_input_order() {
        let upstream = Arc::new(CountingEmbedder::new(4, "m"));
        let cache = CachingEmbedder::new(upstream, 16);

        let batch = vec!["short".into(), "muchlonger".into(), "mid".into()];
        // Prime the cache with out-of-order entries.
        let _ = cache.embed(&["mid".into(), "short".into()]).await.unwrap();
        // Re-embed the original order and verify alignment with input.
        let out = cache.embed(&batch).await.unwrap();
        assert_eq!(out[0][0], 5.0);
        assert_eq!(out[1][0], 10.0);
        assert_eq!(out[2][0], 3.0);
    }

    #[tokio::test]
    async fn lru_evicts_when_full() {
        let upstream = Arc::new(CountingEmbedder::new(2, "m"));
        let cache = CachingEmbedder::new(upstream.clone(), 2);

        // Fill cache: A, B.
        let _ = cache.embed(&["A".into(), "B".into()]).await.unwrap();
        assert_eq!(cache.len(), 2);
        // Insert C → evict A (least recently used).
        let _ = cache.embed(&["C".into()]).await.unwrap();
        assert_eq!(cache.len(), 2);
        let (_, _, evictions, _) = cache.stats().snapshot();
        assert_eq!(evictions, 1);

        // A should miss again, forcing another upstream call.
        let before = upstream.calls.load(Ordering::Relaxed);
        let _ = cache.embed(&["A".into()]).await.unwrap();
        assert_eq!(upstream.calls.load(Ordering::Relaxed), before + 1);
    }

    #[tokio::test]
    async fn different_models_dont_collide() {
        // Two caches wrapping two embedders with different model names;
        // identical input text should produce independent cache entries.
        let up1 = Arc::new(CountingEmbedder::new(4, "model-a"));
        let up2 = Arc::new(CountingEmbedder::new(4, "model-b"));
        let c1 = CachingEmbedder::new(up1.clone(), 16);
        let c2 = CachingEmbedder::new(up2.clone(), 16);

        let _ = c1.embed(&["same".into()]).await.unwrap();
        let _ = c2.embed(&["same".into()]).await.unwrap();
        assert_eq!(up1.calls.load(Ordering::Relaxed), 1);
        assert_eq!(up2.calls.load(Ordering::Relaxed), 1);

        // Also: same-model hash collisions would be a bug. Guard with
        // a sanity assertion on the key itself.
        let k_a = CachingEmbedder::key("model-a", "same");
        let k_b = CachingEmbedder::key("model-b", "same");
        assert_ne!(k_a, k_b);
    }

    #[tokio::test]
    async fn null_separator_prevents_text_model_collisions() {
        // `("ab", "c")` vs `("a", "bc")` would collide without the
        // separator. Asserting the keys differ documents the design.
        let k1 = CachingEmbedder::key("ab", "c");
        let k2 = CachingEmbedder::key("a", "bc");
        assert_ne!(k1, k2);
    }

    #[tokio::test]
    async fn empty_batch_propagates_error() {
        let upstream = Arc::new(CountingEmbedder::new(2, "m"));
        let cache = CachingEmbedder::new(upstream, 4);
        let err = cache.embed(&[]).await;
        assert!(matches!(err, Err(EmbedError::EmptyBatch)));
    }
}
