//! Slow-query log.
//!
//! Keeps the slowest N SQL queries seen since boot. This is a
//! "hall of fame" of worst offenders, NOT a rolling time window —
//! operators want to answer "what's the dumbest query we've been
//! running?" without scrolling through every audit entry.
//!
//! # Bounded cost
//!
//! The log is a fixed-capacity `Vec` kept sorted descending by
//! `took_ms`. On insert we drop the fastest entry if we're over
//! capacity. At capacity 32 (the default), the sort + shift is
//! O(32) per SQL query — below measurement noise even at 10k qps.
//!
//! # Thresholding
//!
//! We only record queries that took longer than
//! `min_threshold_ms` (default 10ms) so the log isn't drowned
//! by MockEmbedder's sub-ms queries. Above the threshold, all
//! queries compete for the N slowest slots.

use std::sync::Arc;

use parking_lot::Mutex;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct SlowQueryEntry {
    pub ts_ms: u64,
    pub took_ms: u64,
    pub rows: usize,
    /// SQL text. Capped at 512 chars in storage so a pathological
    /// caller can't blow memory by submitting a 10 MB query string
    /// that also happens to be slow.
    pub sql: String,
    pub ok: bool,
}

pub struct SlowQueryLog {
    inner: Mutex<Inner>,
    capacity: usize,
    min_threshold_ms: u64,
}

struct Inner {
    /// Sorted descending by `took_ms`. Smallest is at the end — the
    /// eviction candidate when we're full.
    entries: Vec<SlowQueryEntry>,
}

impl SlowQueryLog {
    pub fn new(capacity: usize, min_threshold_ms: u64) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner {
                entries: Vec::with_capacity(capacity.max(1)),
            }),
            capacity: capacity.max(1),
            min_threshold_ms,
        })
    }

    pub fn record(&self, sql: &str, took_ms: u64, rows: usize, ok: bool) {
        if took_ms < self.min_threshold_ms {
            return;
        }
        // Cap the stored SQL text size — same rationale as the audit
        // log's principal truncation, just for a different bound.
        const MAX_SQL_LEN: usize = 512;
        let stored_sql = if sql.len() > MAX_SQL_LEN {
            let mut s = sql.chars().take(MAX_SQL_LEN).collect::<String>();
            s.push('…');
            s
        } else {
            sql.to_string()
        };
        let entry = SlowQueryEntry {
            ts_ms: crate::audit::now_ms(),
            took_ms,
            rows,
            sql: stored_sql,
            ok,
        };
        let mut g = self.inner.lock();

        // If we're over capacity, only keep the new entry if it
        // dethrones the current slowest-smallest. Common case when
        // the log is saturated.
        if g.entries.len() >= self.capacity {
            if let Some(tail) = g.entries.last() {
                if took_ms <= tail.took_ms {
                    return;
                }
            }
            g.entries.pop();
        }
        // Insert in sorted position. A binary-search insert is
        // overkill for capacity 32; a linear walk is clearer.
        let pos = g
            .entries
            .iter()
            .position(|e| e.took_ms < took_ms)
            .unwrap_or(g.entries.len());
        g.entries.insert(pos, entry);
    }

    /// Snapshot sorted slowest-first. Cloned so callers can render
    /// without holding the lock.
    pub fn snapshot(&self) -> Vec<SlowQueryEntry> {
        self.inner.lock().entries.clone()
    }

    pub fn len(&self) -> usize {
        self.inner.lock().entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_only_the_slowest_n() {
        let log = SlowQueryLog::new(3, 0);
        for (i, ms) in [10, 5, 50, 20, 30, 100, 2].iter().enumerate() {
            log.record(&format!("q{i}"), *ms, 0, true);
        }
        let snap = log.snapshot();
        assert_eq!(snap.len(), 3);
        assert_eq!(snap[0].took_ms, 100);
        assert_eq!(snap[1].took_ms, 50);
        assert_eq!(snap[2].took_ms, 30);
    }

    #[test]
    fn threshold_drops_fast_queries() {
        let log = SlowQueryLog::new(10, 20);
        log.record("fast", 5, 0, true);
        log.record("borderline", 19, 0, true);
        log.record("slow", 100, 0, true);
        let snap = log.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].sql, "slow");
    }

    #[test]
    fn truncates_long_sql() {
        let log = SlowQueryLog::new(3, 0);
        let big = "x".repeat(1000);
        log.record(&big, 50, 0, true);
        let snap = log.snapshot();
        assert!(snap[0].sql.len() < big.len());
        assert!(snap[0].sql.ends_with('…'));
    }

    #[test]
    fn sorted_descending() {
        let log = SlowQueryLog::new(10, 0);
        for ms in [15, 3, 27, 9, 50, 1, 100] {
            log.record("q", ms, 0, true);
        }
        let snap = log.snapshot();
        for w in snap.windows(2) {
            assert!(w[0].took_ms >= w[1].took_ms, "not sorted: {snap:?}");
        }
    }
}
