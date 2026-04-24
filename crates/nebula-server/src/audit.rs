//! In-memory audit ring buffer.
//!
//! Every mutating request (POST/PUT/PATCH/DELETE against `/api/v1/*`)
//! lands an entry here with: timestamp, principal, method, path,
//! response status. Reads (GET) aren't recorded by default — they're
//! high-volume and produce noise that drowns out actual writes.
//!
//! # Why a ring buffer, not a log file
//!
//! - Zero disk I/O on the request path.
//! - Cheap to read — `GET /api/v1/admin/audit` is a lock + clone.
//! - Fixed memory ceiling.
//!
//! Anyone who needs durable audit should point Prometheus / Loki at
//! the counter + structured `tracing` output. This buffer is a
//! cockpit, not a compliance system.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use serde::Serialize;

/// One recorded request. Fields deliberately narrow — no body, no
/// headers. Bodies can contain secrets (JWTs, API keys, sensitive
/// prompts) and we never want those in memory any longer than it
/// takes to route.
#[derive(Debug, Clone, Serialize)]
pub struct AuditEntry {
    /// Unix millis. Cheap to sort and format in the UI without
    /// needing `chrono` in the server.
    pub ts_ms: u64,
    /// The principal identity we authenticated (first 16 chars of
    /// the bearer token, or "anon" / "ip:..."). Never the full
    /// token — a leaked audit log shouldn't equal a leaked secret.
    pub principal: String,
    pub method: String,
    pub path: String,
    pub status: u16,
}

/// Ring buffer with a fixed capacity. Writes are constant-time; reads
/// snapshot the whole buffer in insertion order.
pub struct AuditLog {
    inner: Mutex<Inner>,
    capacity: usize,
}

struct Inner {
    /// Back-to-back Vec; once we hit capacity we overwrite the
    /// oldest. A true circular buffer (with a start-pointer) would
    /// save one `remove(0)` but `remove(0)` on a small Vec at
    /// capacity 1024 is a nanosecond — not worth the complexity.
    entries: Vec<AuditEntry>,
}

impl AuditLog {
    pub fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner {
                entries: Vec::with_capacity(capacity.max(1)),
            }),
            capacity: capacity.max(1),
        })
    }

    pub fn record(&self, entry: AuditEntry) {
        let mut g = self.inner.lock();
        if g.entries.len() >= self.capacity {
            g.entries.remove(0);
        }
        g.entries.push(entry);
    }

    /// Most-recent-first snapshot, optionally capped. Snapshot is a
    /// `Vec` clone so callers can render without holding the lock.
    pub fn recent(&self, limit: usize) -> Vec<AuditEntry> {
        let g = self.inner.lock();
        let mut out: Vec<AuditEntry> = g.entries.iter().rev().take(limit).cloned().collect();
        // `rev().take()` gives us newest-first; keep it that way for
        // direct UI rendering.
        out.truncate(limit);
        out
    }

    pub fn len(&self) -> usize {
        self.inner.lock().entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Millis since UNIX_EPOCH, usable as an `AuditEntry.ts_ms`. Isolated
/// so tests can substitute a fake clock if ever needed.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Truncate a bearer token to a stable fingerprint. Matches the
/// rate-limit middleware's principal concept — we don't want to
/// correlate audit entries with a full token.
pub fn principal_fingerprint(auth_header: Option<&str>, peer: Option<std::net::SocketAddr>) -> String {
    if let Some(h) = auth_header {
        if let Some(tok) = h.strip_prefix("Bearer ") {
            let short: String = tok.chars().take(16).collect();
            return format!("bearer:{short}");
        }
    }
    match peer {
        Some(a) => format!("ip:{}", a.ip()),
        None => "anon".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(n: u16) -> AuditEntry {
        AuditEntry {
            ts_ms: n as u64,
            principal: "t".into(),
            method: "POST".into(),
            path: format!("/{n}"),
            status: 200,
        }
    }

    #[test]
    fn ring_evicts_oldest_at_capacity() {
        let log = AuditLog::new(3);
        for i in 1..=5u16 {
            log.record(mk(i));
        }
        assert_eq!(log.len(), 3);
        let recent = log.recent(10);
        // Newest-first: 5, 4, 3.
        assert_eq!(
            recent.iter().map(|e| e.ts_ms).collect::<Vec<_>>(),
            vec![5, 4, 3]
        );
    }

    #[test]
    fn recent_limit_honored() {
        let log = AuditLog::new(10);
        for i in 1..=5u16 {
            log.record(mk(i));
        }
        assert_eq!(log.recent(2).len(), 2);
    }

    #[test]
    fn fingerprint_shortens_token() {
        let f = principal_fingerprint(Some("Bearer abcdefghijklmnopqrstuvwxyz"), None);
        assert_eq!(f, "bearer:abcdefghijklmnop");
    }

    #[test]
    fn fingerprint_without_auth_falls_back_to_ip() {
        let peer: std::net::SocketAddr = "10.0.0.1:443".parse().unwrap();
        let f = principal_fingerprint(None, Some(peer));
        assert_eq!(f, "ip:10.0.0.1");
    }
}
