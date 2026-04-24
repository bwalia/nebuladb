//! Semantic-result cache.
//!
//! Keyed by `(sql, embedder_model)`, values bounded by both size
//! (LRU) and age (per-entry TTL). Purpose: skip expensive work
//! (embed + HNSW + post-filter) for a query we just ran.
//!
//! # Scope
//!
//! This is a *result* cache, not the *embedding* cache in
//! `nebula-cache`. Those are orthogonal:
//! - `nebula-cache::CachingEmbedder` saves the model-specific vector
//!   for a piece of text, avoiding the embedding API round trip.
//! - `SemanticCache` saves the final query result, avoiding *both*
//!   the embedding and the HNSW traversal.
//!
//! Both can be enabled together; hits at this layer never even reach
//! the embedder cache.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use sha2::{Digest, Sha256};

use crate::executor::QueryResult;

/// 32-byte digest is compact, fast to hash, and avoids keeping whole
/// SQL strings resident for every entry.
pub type Key = [u8; 32];

#[derive(Debug, Clone)]
pub struct SemanticCacheConfig {
    pub capacity: usize,
    pub ttl: Duration,
}

impl Default for SemanticCacheConfig {
    /// Conservative defaults: 512 entries, 30-second TTL. The TTL
    /// matters for correctness — a bucket mutation between cache
    /// writes would otherwise return stale results forever.
    fn default() -> Self {
        Self {
            capacity: 512,
            ttl: Duration::from_secs(30),
        }
    }
}

struct Entry {
    value: QueryResult,
    inserted_at: Instant,
    /// Monotonic "touch" counter. We don't use a linked list for LRU
    /// because the HashMap semantics are simpler and cache misses are
    /// cheap relative to the work they avoid. Eviction scans at most
    /// `capacity` entries; fine at 512, would need a proper LRU list
    /// at 100k+.
    last_touched: u64,
}

pub struct SemanticCache {
    inner: Mutex<Inner>,
    cfg: SemanticCacheConfig,
    // Stats are atomic, not behind the mutex, so `/metrics` readers
    // never contend with cache writers.
    hits: std::sync::atomic::AtomicU64,
    misses: std::sync::atomic::AtomicU64,
    evictions: std::sync::atomic::AtomicU64,
}

struct Inner {
    map: HashMap<Key, Entry>,
    counter: u64,
}

impl SemanticCache {
    pub fn new(cfg: SemanticCacheConfig) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner {
                map: HashMap::with_capacity(cfg.capacity),
                counter: 0,
            }),
            cfg,
            hits: Default::default(),
            misses: Default::default(),
            evictions: Default::default(),
        })
    }

    pub fn key(&self, sql: &str, model: &str) -> Key {
        let mut h = Sha256::new();
        h.update(model.as_bytes());
        h.update([0u8]);
        h.update(sql.as_bytes());
        h.finalize().into()
    }

    pub fn get(&self, k: &Key) -> Option<QueryResult> {
        let mut g = self.inner.lock();
        // Bump the counter first so the mutable borrow on the map
        // below doesn't overlap with the counter field read. Cheap,
        // and makes the borrow checker's life easy.
        g.counter += 1;
        let counter = g.counter;
        let Some(entry) = g.map.get_mut(k) else {
            self.misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return None;
        };
        if entry.inserted_at.elapsed() > self.cfg.ttl {
            // Expired entries are kept until natural eviction; the
            // alternative (remove-on-read) racily churns the map and
            // hides TTL bugs behind "it got re-populated". Report as
            // a miss so caller recomputes, but leave the slot.
            self.misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return None;
        }
        entry.last_touched = counter;
        let v = entry.value.clone();
        self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Some(v)
    }

    pub fn put(&self, k: Key, v: QueryResult) {
        let mut g = self.inner.lock();
        if g.map.len() >= self.cfg.capacity && !g.map.contains_key(&k) {
            // Evict the least-recently-touched. A linear scan is fine
            // at the default capacity of 512; see struct-level note.
            if let Some(victim_key) =
                g.map.iter().min_by_key(|(_, e)| e.last_touched).map(|(k, _)| *k)
            {
                g.map.remove(&victim_key);
                self.evictions
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        g.counter += 1;
        let counter = g.counter;
        g.map.insert(
            k,
            Entry {
                value: v,
                inserted_at: Instant::now(),
                last_touched: counter,
            },
        );
    }

    pub fn len(&self) -> usize {
        self.inner.lock().map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn snapshot(&self) -> (u64, u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        (
            self.hits.load(Relaxed),
            self.misses.load(Relaxed),
            self.evictions.load(Relaxed),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::QueryResult;
    use std::thread::sleep;

    fn result() -> QueryResult {
        QueryResult {
            took_ms: 1,
            rows: vec![],
        }
    }

    #[test]
    fn hit_after_put() {
        let c = SemanticCache::new(SemanticCacheConfig::default());
        let k = c.key("SELECT 1", "m");
        c.put(k, result());
        assert!(c.get(&k).is_some());
        let (h, m, _) = c.snapshot();
        assert_eq!(h, 1);
        assert_eq!(m, 0);
    }

    #[test]
    fn ttl_expires() {
        let c = SemanticCache::new(SemanticCacheConfig {
            capacity: 4,
            ttl: Duration::from_millis(10),
        });
        let k = c.key("SELECT 1", "m");
        c.put(k, result());
        sleep(Duration::from_millis(20));
        assert!(c.get(&k).is_none(), "should be expired");
    }

    #[test]
    fn capacity_evicts_lru() {
        let c = SemanticCache::new(SemanticCacheConfig {
            capacity: 2,
            ttl: Duration::from_secs(60),
        });
        let k1 = c.key("a", "m");
        let k2 = c.key("b", "m");
        let k3 = c.key("c", "m");
        c.put(k1, result());
        c.put(k2, result());
        // Touch k1 so k2 becomes LRU.
        let _ = c.get(&k1);
        c.put(k3, result());
        assert!(c.get(&k1).is_some());
        assert!(c.get(&k2).is_none(), "k2 should have been evicted");
        assert!(c.get(&k3).is_some());
    }

    #[test]
    fn different_models_differ() {
        let c = SemanticCache::new(SemanticCacheConfig::default());
        let k1 = c.key("same", "model-a");
        let k2 = c.key("same", "model-b");
        assert_ne!(k1, k2);
    }
}
