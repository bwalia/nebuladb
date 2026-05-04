//! Shared state for cross-region replication observability.
//!
//! The cross-region consumer task (started from `main.rs` in Phase 2.2)
//! holds a reference to [`CrossRegionStatusHub`] and reports its last
//! applied cursor per remote region. The `/admin/replication` handler
//! reads a snapshot of the hub when responding, so operators can see
//! "we're 12 KiB behind us-west-2 on bucket `catalog`."
//!
//! Kept in a separate module (not inlined into `cluster.rs`) so the
//! consumer task can own its reporting without taking a dep on every
//! server-internal type.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

use serde::Serialize;

/// One remote region's view as observed by this node's consumer task.
///
/// `last_applied_seq` is the remote WAL's segment sequence that was
/// most recently applied locally. Operators correlate this with the
/// remote's `/healthz` + `/admin/replication.local_newest` to see how
/// far behind we are — we don't store the remote tip here to keep the
/// write hot path from doing any outbound probing.
#[derive(Debug, Clone, Serialize)]
pub struct RemoteRegionStatus {
    /// Canonical remote region name (e.g. `"us-west-2"`).
    pub region: String,
    /// Remote gRPC endpoint this node is tailing.
    pub grpc_url: String,
    /// True when the consumer task's last streaming call completed
    /// successfully. False after an error, until the next reconnect.
    pub healthy: bool,
    /// Remote WAL segment the consumer last applied. `None` before
    /// the first successful record.
    pub last_applied_segment: Option<u64>,
    /// Byte offset within that segment.
    pub last_applied_byte_offset: Option<u64>,
    /// Records the consumer has applied from this remote since boot.
    pub applied_records: u64,
    /// Count of consecutive errors since the last success. Non-zero
    /// means the stream is in back-off.
    pub consecutive_errors: u32,
    /// Last error message if any — surfaced in the admin UI so the
    /// operator doesn't have to parse logs.
    pub last_error: Option<String>,
    /// When `last_error` was recorded (ISO-8601 UTC). `None` if no
    /// error has been seen.
    pub last_error_at: Option<String>,
}

impl RemoteRegionStatus {
    pub fn new(region: String, grpc_url: String) -> Self {
        Self {
            region,
            grpc_url,
            healthy: false,
            last_applied_segment: None,
            last_applied_byte_offset: None,
            applied_records: 0,
            consecutive_errors: 0,
            last_error: None,
            last_error_at: None,
        }
    }
}

/// Thread-safe bag of per-remote status. Cheap to clone (it's an Arc)
/// so every subsystem that needs to read or update can hold one.
///
/// Writes are expected to be low-frequency (one update per applied
/// record batch or one per error), so an RwLock-protected HashMap is
/// plenty. If cross-region ever scales to dozens of remotes we can
/// swap in per-region atomics.
#[derive(Clone, Default, Debug)]
pub struct CrossRegionStatusHub {
    inner: Arc<RwLock<HashMap<String, RemoteRegionStatus>>>,
}

/// Implement `nebula_grpc::cross_region::StatusSink` on the hub so
/// the consumer task can report progress without taking a dep on
/// this crate's internals.
impl nebula_grpc::cross_region::StatusSink for CrossRegionStatusHub {
    fn register(&self, region: &str, grpc_url: &str) {
        CrossRegionStatusHub::register(self, region, grpc_url);
    }
    fn record_apply(&self, region: &str, segment: u64, byte_offset: u64) {
        CrossRegionStatusHub::record_apply(self, region, segment, byte_offset);
    }
    fn record_error(&self, region: &str, err: &str) {
        CrossRegionStatusHub::record_error(self, region, err);
    }
}

impl CrossRegionStatusHub {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a remote region that *should* be tailed. Called once
    /// per peer at boot so the admin view shows the intended topology
    /// even when the first stream hasn't connected yet.
    pub fn register(&self, region: &str, grpc_url: &str) {
        let mut g = self.inner.write().expect("cross-region status poisoned");
        g.entry(region.to_string())
            .or_insert_with(|| RemoteRegionStatus::new(region.to_string(), grpc_url.to_string()));
    }

    /// Called after each successfully-applied record. Updates cursor
    /// + increments the applied count + clears any previous error
    /// state so the admin view reflects recovery.
    pub fn record_apply(&self, region: &str, segment: u64, byte_offset: u64) {
        if let Ok(mut g) = self.inner.write() {
            if let Some(s) = g.get_mut(region) {
                s.healthy = true;
                s.last_applied_segment = Some(segment);
                s.last_applied_byte_offset = Some(byte_offset);
                s.applied_records = s.applied_records.saturating_add(1);
                s.consecutive_errors = 0;
                s.last_error = None;
                s.last_error_at = None;
            }
        }
    }

    /// Called when the consumer hits a transport or apply error.
    /// Leaves `last_applied_*` intact — the cursor remains valid, we
    /// just note that we're retrying.
    pub fn record_error(&self, region: &str, err: &str) {
        if let Ok(mut g) = self.inner.write() {
            if let Some(s) = g.get_mut(region) {
                s.healthy = false;
                s.consecutive_errors = s.consecutive_errors.saturating_add(1);
                s.last_error = Some(err.to_string());
                s.last_error_at = Some(iso_now());
            }
        }
    }

    /// Snapshot, sorted by region name for stable admin output.
    pub fn snapshot(&self) -> Vec<RemoteRegionStatus> {
        let g = self.inner.read().expect("cross-region status poisoned");
        let mut out: Vec<_> = g.values().cloned().collect();
        out.sort_by(|a, b| a.region.cmp(&b.region));
        out
    }

    /// Count of registered remotes. Used by `/admin/replication` to
    /// decide whether to emit the cross-region section at all.
    pub fn len(&self) -> usize {
        self.inner.read().map(|g| g.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// ISO-8601 UTC "now" as a string without pulling chrono in.
fn iso_now() -> String {
    // SystemTime formatting is awkward; we emit a best-effort RFC3339
    // using the tracing-friendly SystemTime::now(). Not strictly
    // RFC3339 on all platforms but good enough for an admin screen.
    let t = SystemTime::now();
    let dur = t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    let secs = dur.as_secs();
    // YYYY-MM-DDTHH:MM:SSZ ish via integer arithmetic — chrono would
    // be nicer but this file is called infrequently and the format
    // just needs to be sortable / human-readable.
    let s = secs;
    let days = s / 86_400;
    let rem = s % 86_400;
    let hh = rem / 3600;
    let mm = (rem % 3600) / 60;
    let ss = rem % 60;
    // Rough calendar conversion from days since 1970-01-01. Good
    // enough for an audit field; not load-bearing.
    let (y, mo, d) = days_to_ymd(days as i64);
    format!("{y:04}-{mo:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}Z")
}

/// Convert "days since 1970-01-01" into (year, month, day).
/// Correct for the Gregorian calendar range we actually hit.
fn days_to_ymd(mut days: i64) -> (i32, u32, u32) {
    let mut y = 1970;
    loop {
        let ylen = if is_leap(y) { 366 } else { 365 };
        if days < ylen as i64 {
            break;
        }
        days -= ylen as i64;
        y += 1;
    }
    let months = if is_leap(y) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };
    let mut m = 0;
    while m < 12 && days >= months[m] as i64 {
        days -= months[m] as i64;
        m += 1;
    }
    (y, (m + 1) as u32, (days + 1) as u32)
}

fn is_leap(y: i32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_snapshot() {
        let hub = CrossRegionStatusHub::new();
        hub.register("us-west-2", "http://a:50051");
        hub.register("eu-west-1", "http://b:50051");
        let snap = hub.snapshot();
        assert_eq!(snap.len(), 2);
        // Sorted alphabetically.
        assert_eq!(snap[0].region, "eu-west-1");
        assert_eq!(snap[1].region, "us-west-2");
        for s in &snap {
            assert!(!s.healthy);
            assert_eq!(s.applied_records, 0);
        }
    }

    #[test]
    fn apply_then_error_keeps_cursor() {
        let hub = CrossRegionStatusHub::new();
        hub.register("us-west-2", "http://a:50051");
        hub.record_apply("us-west-2", 3, 1024);
        hub.record_error("us-west-2", "stream reset");
        let snap = hub.snapshot();
        assert!(!snap[0].healthy);
        assert_eq!(snap[0].last_applied_segment, Some(3));
        assert_eq!(snap[0].last_applied_byte_offset, Some(1024));
        assert_eq!(snap[0].consecutive_errors, 1);
        assert_eq!(snap[0].last_error.as_deref(), Some("stream reset"));
    }

    #[test]
    fn apply_after_error_resets_error_counters() {
        let hub = CrossRegionStatusHub::new();
        hub.register("us-west-2", "http://a:50051");
        hub.record_error("us-west-2", "boom");
        hub.record_apply("us-west-2", 1, 100);
        let snap = hub.snapshot();
        assert!(snap[0].healthy);
        assert_eq!(snap[0].consecutive_errors, 0);
        assert!(snap[0].last_error.is_none());
    }

    #[test]
    fn register_is_idempotent() {
        let hub = CrossRegionStatusHub::new();
        hub.register("us-west-2", "http://a:50051");
        hub.record_apply("us-west-2", 5, 50);
        // Re-registering (e.g. after a config reload) must not reset
        // the cursor — that would cause dupe applies on reconnect.
        hub.register("us-west-2", "http://a:50051");
        let snap = hub.snapshot();
        assert_eq!(snap[0].last_applied_segment, Some(5));
    }
}
