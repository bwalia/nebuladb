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
        write(&mut s, "nebula_requests_total", "Total API requests", &self.requests_total);
        write(&mut s, "nebula_requests_errors", "API requests returning 5xx", &self.requests_errors);
        write(&mut s, "nebula_auth_failures", "Requests rejected by auth", &self.auth_failures);
        write(&mut s, "nebula_docs_inserted", "Document upserts", &self.docs_inserted);
        write(&mut s, "nebula_docs_deleted", "Document deletes", &self.docs_deleted);
        write(&mut s, "nebula_searches_vector", "Raw-vector searches", &self.searches_vector);
        write(&mut s, "nebula_searches_semantic", "Text/semantic searches", &self.searches_semantic);
        write(&mut s, "nebula_rag_requests", "RAG requests", &self.rag_requests);
        write(&mut s, "nebula_rate_limited", "Requests rejected by the rate limiter", &self.rate_limited);
        write(&mut s, "nebula_jwt_failures", "Requests rejected by JWT verification", &self.jwt_failures);
        s
    }
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
}
