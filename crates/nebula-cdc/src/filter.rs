//! Subscriber-side filter language. Lets a consumer subscribe to
//! "writes to bucket X" or "deletes only" without seeing the full
//! firehose. Filters are evaluated on the publisher side (in the
//! engine's per-consumer task), so a filtered consumer only pays
//! the cost of events it actually consumes.
//!
//! The filter is intentionally narrow:
//!
//! - `bucket`: optional exact-match. `None` = any bucket.
//! - `ops`: optional set of [`CdcOp`] values. `None` = any op.
//! - `vectors_only`: when true, drops any event without a vector
//!   payload (deletes, empty-bucket, document-deletes). Useful for
//!   the AI-mirror use case from RFC 0005 §0.1's "vector-aware CDC
//!   consumer" gap.
//!
//! No regex, no glob, no AND/OR DSL. RFC 0005 §5.2 calls out a
//! richer language as a follow-up. The narrow shape here is
//! enough for the two consumers in this crate (webhook, file log)
//! and for the most-asked-for filtering case (one webhook per
//! bucket).

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::event::{CdcEvent, CdcOp};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CdcFilter {
    /// Exact bucket match. `None` ⇒ any bucket.
    pub bucket: Option<String>,
    /// Allow-list of operations. `None` ⇒ any op. Empty set is
    /// treated as "match nothing"; that's the literal interpretation
    /// of an explicit empty allowlist (though callers usually want
    /// `None` instead).
    pub ops: Option<BTreeSet<CdcOp>>,
    /// When true, drop events that don't carry a vector payload.
    /// Useful for an external HNSW-mirror that doesn't care about
    /// deletes or empty-bucket events.
    #[serde(default)]
    pub vectors_only: bool,
}

impl CdcFilter {
    /// Convenience: a filter that matches everything.
    pub fn any() -> Self {
        Self::default()
    }

    /// Match only events whose bucket equals `bucket`.
    pub fn bucket(bucket: impl Into<String>) -> Self {
        Self {
            bucket: Some(bucket.into()),
            ..Default::default()
        }
    }

    /// Match only the listed operations.
    pub fn ops(ops: impl IntoIterator<Item = CdcOp>) -> Self {
        Self {
            ops: Some(ops.into_iter().collect()),
            ..Default::default()
        }
    }

    /// Drop events that don't carry vector payload.
    pub fn vectors_only() -> Self {
        Self {
            vectors_only: true,
            ..Default::default()
        }
    }

    /// Combine: `self.with_bucket(b)` returns a filter matching this
    /// filter's existing constraints AND the new bucket.
    pub fn with_bucket(mut self, bucket: impl Into<String>) -> Self {
        self.bucket = Some(bucket.into());
        self
    }

    pub fn with_ops(mut self, ops: impl IntoIterator<Item = CdcOp>) -> Self {
        self.ops = Some(ops.into_iter().collect());
        self
    }

    pub fn with_vectors_only(mut self, on: bool) -> Self {
        self.vectors_only = on;
        self
    }

    /// Predicate. AND across the populated fields.
    pub fn matches(&self, event: &CdcEvent) -> bool {
        if let Some(b) = &self.bucket {
            if &event.key.bucket != b {
                return false;
            }
        }
        if let Some(ops) = &self.ops {
            if !ops.contains(&event.op) {
                return false;
            }
        }
        if self.vectors_only && !event.has_vector() {
            return false;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nebula_wal::{WalCursor, WalEntry, WalRecord};

    /// Build an event from a record. The `bucket` arg is unused —
    /// the bucket lives inside the record itself — but kept in the
    /// signature so the test bodies read naturally as
    /// `evt("docs", upsert("docs", "x"))`.
    fn evt(_bucket: &str, op_record: WalRecord) -> CdcEvent {
        CdcEvent::from_wal_entry(WalEntry {
            cursor: WalCursor::BEGIN,
            next_cursor: WalCursor::BEGIN,
            record: op_record,
        })
    }

    fn upsert(bucket: &str, id: &str) -> WalRecord {
        WalRecord::UpsertText {
            bucket: bucket.into(),
            external_id: id.into(),
            text: "t".into(),
            vector: vec![1.0],
            metadata_json: "null".into(),
        }
    }

    fn delete(bucket: &str, id: &str) -> WalRecord {
        WalRecord::Delete {
            bucket: bucket.into(),
            external_id: id.into(),
        }
    }

    #[test]
    fn any_matches_everything() {
        let f = CdcFilter::any();
        assert!(f.matches(&evt("a", upsert("a", "x"))));
        assert!(f.matches(&evt("b", delete("b", "y"))));
    }

    #[test]
    fn bucket_filter_matches_only_target_bucket() {
        let f = CdcFilter::bucket("docs");
        assert!(f.matches(&evt("docs", upsert("docs", "x"))));
        assert!(!f.matches(&evt("other", upsert("other", "x"))));
    }

    #[test]
    fn op_filter_excludes_other_ops() {
        let f = CdcFilter::ops([CdcOp::Delete]);
        assert!(f.matches(&evt("b", delete("b", "x"))));
        assert!(!f.matches(&evt("b", upsert("b", "x"))));
    }

    #[test]
    fn empty_op_set_matches_nothing() {
        // Explicit empty allowlist: literal "match nothing." Distinct
        // from `None` (which means "match everything"). Pin this
        // semantic so a future refactor doesn't accidentally change it.
        let f = CdcFilter {
            ops: Some(BTreeSet::new()),
            ..Default::default()
        };
        assert!(!f.matches(&evt("b", upsert("b", "x"))));
        assert!(!f.matches(&evt("b", delete("b", "x"))));
    }

    #[test]
    fn vectors_only_drops_deletes() {
        let f = CdcFilter::vectors_only();
        assert!(f.matches(&evt("b", upsert("b", "x"))));
        assert!(!f.matches(&evt("b", delete("b", "x"))));
    }

    #[test]
    fn combined_filter_is_and() {
        let f = CdcFilter::bucket("docs").with_ops([CdcOp::Insert]);
        assert!(f.matches(&evt("docs", upsert("docs", "x"))));
        assert!(!f.matches(&evt("docs", delete("docs", "x")))); // wrong op
        assert!(!f.matches(&evt("other", upsert("other", "x")))); // wrong bucket
    }
}
