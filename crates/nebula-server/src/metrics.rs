//! Minimal Prometheus text-format metrics.
//!
//! Deliberately hand-rolled — a real deployment would use the
//! `prometheus` crate with histograms and labels, but for a vertical
//! slice the hand-rolled version keeps the dependency tree small and
//! is easy to eyeball in tests. The public API matches what a swap to
//! a real client would look like (counter methods), so upgrading is
//! local to this file.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Metrics {
    pub requests_total: AtomicU64,
    pub requests_errors: AtomicU64,
    pub auth_failures: AtomicU64,
    pub docs_inserted: AtomicU64,
    pub docs_deleted: AtomicU64,
    pub searches_vector: AtomicU64,
    pub searches_semantic: AtomicU64,
    pub rag_requests: AtomicU64,
    pub rate_limited: AtomicU64,
    pub jwt_failures: AtomicU64,
    pub writes_rejected: AtomicU64,
    // Backup / restore (design 0002 §9). Hand-rolled like the rest —
    // duration is exposed as sum+count so dashboards can plot averages
    // without pulling in the prometheus crate for a full histogram.
    pub backup_attempts: AtomicU64,
    pub backup_failures: AtomicU64,
    pub backup_duration_sum_ms: AtomicU64,
    pub backup_duration_count: AtomicU64,
    pub backup_last_size_bytes: AtomicU64,
    pub backup_last_throughput_bps: AtomicU64,
    /// Unix seconds of the last successful backup; 0 = never.
    pub backup_last_success_unix: AtomicU64,
    pub restore_attempts: AtomicU64,
    pub restore_failures: AtomicU64,
    pub restore_duration_sum_ms: AtomicU64,
    pub restore_duration_count: AtomicU64,
    pub restore_last_success_unix: AtomicU64,
}

impl Metrics {
    pub fn inc_request(&self) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_error(&self) {
        self.requests_errors.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_auth_failure(&self) {
        self.auth_failures.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_insert(&self) {
        self.docs_inserted.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_delete(&self) {
        self.docs_deleted.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_vector_search(&self) {
        self.searches_vector.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_semantic_search(&self) {
        self.searches_semantic.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_rag(&self) {
        self.rag_requests.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_rate_limited(&self) {
        self.rate_limited.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_jwt_failure(&self) {
        self.jwt_failures.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_write_rejected(&self) {
        self.writes_rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_backup_ok(&self, duration: std::time::Duration, size_bytes: u64) {
        self.backup_attempts.fetch_add(1, Ordering::Relaxed);
        let ms = duration.as_millis() as u64;
        self.backup_duration_sum_ms
            .fetch_add(ms, Ordering::Relaxed);
        self.backup_duration_count.fetch_add(1, Ordering::Relaxed);
        self.backup_last_size_bytes
            .store(size_bytes, Ordering::Relaxed);
        let bps = if duration.as_secs_f64() > 0.0 {
            (size_bytes as f64 / duration.as_secs_f64()) as u64
        } else {
            0
        };
        self.backup_last_throughput_bps
            .store(bps, Ordering::Relaxed);
        self.backup_last_success_unix
            .store(unix_now_secs(), Ordering::Relaxed);
    }

    pub fn record_backup_fail(&self) {
        self.backup_attempts.fetch_add(1, Ordering::Relaxed);
        self.backup_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_restore_ok(&self, duration: std::time::Duration) {
        self.restore_attempts.fetch_add(1, Ordering::Relaxed);
        let ms = duration.as_millis() as u64;
        self.restore_duration_sum_ms
            .fetch_add(ms, Ordering::Relaxed);
        self.restore_duration_count.fetch_add(1, Ordering::Relaxed);
        self.restore_last_success_unix
            .store(unix_now_secs(), Ordering::Relaxed);
    }

    pub fn record_restore_fail(&self) {
        self.restore_attempts.fetch_add(1, Ordering::Relaxed);
        self.restore_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Render Prometheus text format. One line per counter, no labels.
    pub fn render(&self) -> String {
        let mut s = String::new();
        let write = |s: &mut String, name: &str, help: &str, v: &AtomicU64| {
            s.push_str("# HELP ");
            s.push_str(name);
            s.push(' ');
            s.push_str(help);
            s.push('\n');
            s.push_str("# TYPE ");
            s.push_str(name);
            s.push_str(" counter\n");
            s.push_str(name);
            s.push(' ');
            s.push_str(&v.load(Ordering::Relaxed).to_string());
            s.push('\n');
        };
        write(
            &mut s,
            "nebula_requests_total",
            "Total API requests",
            &self.requests_total,
        );
        write(
            &mut s,
            "nebula_requests_errors",
            "API requests returning 5xx",
            &self.requests_errors,
        );
        write(
            &mut s,
            "nebula_auth_failures",
            "Requests rejected by auth",
            &self.auth_failures,
        );
        write(
            &mut s,
            "nebula_docs_inserted",
            "Document upserts",
            &self.docs_inserted,
        );
        write(
            &mut s,
            "nebula_docs_deleted",
            "Document deletes",
            &self.docs_deleted,
        );
        write(
            &mut s,
            "nebula_searches_vector",
            "Raw-vector searches",
            &self.searches_vector,
        );
        write(
            &mut s,
            "nebula_searches_semantic",
            "Text/semantic searches",
            &self.searches_semantic,
        );
        write(
            &mut s,
            "nebula_rag_requests",
            "RAG requests",
            &self.rag_requests,
        );
        write(
            &mut s,
            "nebula_rate_limited",
            "Requests rejected by the rate limiter",
            &self.rate_limited,
        );
        write(
            &mut s,
            "nebula_jwt_failures",
            "Requests rejected by JWT verification",
            &self.jwt_failures,
        );
        write(
            &mut s,
            "nebula_writes_rejected_total",
            "Mutations refused by the disk-critical write gate",
            &self.writes_rejected,
        );
        write(
            &mut s,
            "nebula_backup_attempts_total",
            "Backup jobs started",
            &self.backup_attempts,
        );
        write(
            &mut s,
            "nebula_backup_failures_total",
            "Backup jobs that ended in failure",
            &self.backup_failures,
        );
        write(
            &mut s,
            "nebula_restore_attempts_total",
            "Restore jobs started",
            &self.restore_attempts,
        );
        write(
            &mut s,
            "nebula_restore_failures_total",
            "Restore jobs that ended in failure",
            &self.restore_failures,
        );

        // Summary-shaped duration (sum + count) so Grafana can do
        // rate(sum)/rate(count) for average duration without a full
        // histogram implementation.
        let bak_sum = self.backup_duration_sum_ms.load(Ordering::Relaxed) as f64 / 1000.0;
        let bak_n = self.backup_duration_count.load(Ordering::Relaxed);
        s.push_str("# HELP nebula_backup_duration_seconds Backup wall time (successful jobs)\n");
        s.push_str("# TYPE nebula_backup_duration_seconds summary\n");
        s.push_str(&format!("nebula_backup_duration_seconds_sum {bak_sum:.3}\n"));
        s.push_str(&format!("nebula_backup_duration_seconds_count {bak_n}\n"));

        let rst_sum = self.restore_duration_sum_ms.load(Ordering::Relaxed) as f64 / 1000.0;
        let rst_n = self.restore_duration_count.load(Ordering::Relaxed);
        s.push_str("# HELP nebula_restore_duration_seconds Restore wall time (successful jobs)\n");
        s.push_str("# TYPE nebula_restore_duration_seconds summary\n");
        s.push_str(&format!("nebula_restore_duration_seconds_sum {rst_sum:.3}\n"));
        s.push_str(&format!("nebula_restore_duration_seconds_count {rst_n}\n"));

        let write_gauge = |s: &mut String, name: &str, help: &str, v: u64| {
            s.push_str("# HELP ");
            s.push_str(name);
            s.push(' ');
            s.push_str(help);
            s.push('\n');
            s.push_str("# TYPE ");
            s.push_str(name);
            s.push_str(" gauge\n");
            s.push_str(name);
            s.push(' ');
            s.push_str(&v.to_string());
            s.push('\n');
        };
        write_gauge(
            &mut s,
            "nebula_backup_size_bytes",
            "Size of the most recent successful backup (bytes)",
            self.backup_last_size_bytes.load(Ordering::Relaxed),
        );
        write_gauge(
            &mut s,
            "nebula_backup_throughput_bytes_per_sec",
            "Throughput of the most recent successful backup",
            self.backup_last_throughput_bps.load(Ordering::Relaxed),
        );
        let bak_ts = self.backup_last_success_unix.load(Ordering::Relaxed);
        if bak_ts > 0 {
            write_gauge(
                &mut s,
                "nebula_backup_last_success_timestamp_seconds",
                "Unix time of the most recent successful backup",
                bak_ts,
            );
        }
        let rst_ts = self.restore_last_success_unix.load(Ordering::Relaxed);
        if rst_ts > 0 {
            write_gauge(
                &mut s,
                "nebula_restore_last_success_timestamp_seconds",
                "Unix time of the most recent successful restore",
                rst_ts,
            );
        }
        s
    }
}

fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_prometheus_text() {
        let m = Metrics::default();
        m.inc_request();
        m.inc_insert();
        let out = m.render();
        assert!(out.contains("nebula_requests_total 1"));
        assert!(out.contains("nebula_docs_inserted 1"));
        assert!(out.contains("# TYPE nebula_requests_total counter"));
    }

    #[test]
    fn renders_backup_metrics_after_success() {
        let m = Metrics::default();
        m.record_backup_ok(std::time::Duration::from_millis(1500), 2_000_000);
        let out = m.render();
        assert!(out.contains("nebula_backup_attempts_total 1"));
        assert!(out.contains("nebula_backup_failures_total 0"));
        assert!(out.contains("nebula_backup_duration_seconds_count 1"));
        assert!(out.contains("nebula_backup_size_bytes 2000000"));
        assert!(out.contains("nebula_backup_last_success_timestamp_seconds "));
        // No success yet for restore → timestamp gauge omitted.
        assert!(!out.contains("nebula_restore_last_success_timestamp_seconds"));
    }

    #[test]
    fn backup_failure_increments_both_counters() {
        let m = Metrics::default();
        m.record_backup_fail();
        let out = m.render();
        assert!(out.contains("nebula_backup_attempts_total 1"));
        assert!(out.contains("nebula_backup_failures_total 1"));
        assert!(out.contains("nebula_backup_duration_seconds_count 0"));
    }
}
