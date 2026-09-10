//! Server-side glue for automatic multi-region failover (design 0011 §4).
//!
//! The failover *policy* (grace, hysteresis, cooldown, single-candidate)
//! lives in `nebula_grpc::cross_region::RegionFailoverMonitor`, which is
//! provider-agnostic. This module supplies the two server-specific
//! pieces the monitor needs:
//!
//! - the [`SeedWriter`](nebula_grpc::cross_region::SeedWriter) that reads
//!   and bumps a bucket's `home_epoch` through the normal (WAL-durable,
//!   replicated) upsert path, and
//! - [`build_candidates_from_env`], which parses the operator's
//!   failover-candidate declaration.
//!
//! Wiring into `main.rs` is gated on `NEBULA_REGION_FAILOVER_CANDIDATE`
//! so a non-candidate node pays nothing.

use std::sync::Arc;

use nebula_grpc::cross_region::{FailoverCandidate, SeedWriter};
use nebula_index::TextIndex;

use crate::home_region::{HomeRegion, SEED_DOC_ID};

/// A [`SeedWriter`] that promotes a bucket by rewriting its seed doc's
/// `home_region` / `home_epoch` through `TextIndex::upsert_text`. The
/// upsert goes through the same durable, replicated path as any client
/// write, so the promotion is crash-safe and propagates to peers.
pub struct IndexSeedWriter {
    pub index: Arc<TextIndex>,
}

#[async_trait::async_trait]
impl SeedWriter for IndexSeedWriter {
    fn current_epoch(&self, bucket: &str) -> u64 {
        self.index
            .get(bucket, SEED_DOC_ID)
            .map(|d| HomeRegion::from_metadata(&d.metadata).epoch)
            .unwrap_or(0)
    }

    async fn promote(&self, bucket: &str, my_region: &str, new_epoch: u64) -> Result<(), String> {
        // Preserve any existing seed-doc metadata (kind, replicated_to,
        // operator annotations) and overlay only the home fields — a
        // failover changes *who owns* the bucket, not the rest of the
        // operator's coordination state.
        let mut metadata = self
            .index
            .get(bucket, SEED_DOC_ID)
            .map(|d| d.metadata)
            .unwrap_or_else(|| serde_json::json!({}));
        let obj = metadata.as_object_mut().ok_or_else(|| {
            format!("seed doc for bucket {bucket} has non-object metadata; refusing to promote")
        })?;
        obj.insert(
            "home_region".into(),
            serde_json::Value::String(my_region.to_string()),
        );
        obj.insert("home_epoch".into(), serde_json::Value::from(new_epoch));
        obj.entry("kind")
            .or_insert_with(|| serde_json::Value::String("nebuladb-operator-seed".into()));
        obj.entry("bucket")
            .or_insert_with(|| serde_json::Value::String(bucket.to_string()));

        // The seed doc's text is unused for routing; keep a stable marker
        // so an embedder still gets non-empty input.
        self.index
            .upsert_text(bucket, SEED_DOC_ID, "nebuladb operator seed", metadata)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Parse `NEBULA_REGION_FAILOVER_BUCKETS` into failover candidates.
///
/// Format: `bucket=home_region,bucket2=home_region2`. Each entry names a
/// remote-homed bucket this region will take over if that home region
/// dies. Empty / unset yields no candidates (the monitor then no-ops).
///
/// Malformed entries are an error so a typo fails at boot rather than
/// silently disabling failover for a bucket the operator meant to cover.
pub fn parse_failover_candidates(s: &str) -> Result<Vec<FailoverCandidate>, String> {
    let mut out = Vec::new();
    for raw in s.split(',') {
        let raw = raw.trim();
        if raw.is_empty() {
            continue;
        }
        let (bucket, home) = raw
            .split_once('=')
            .ok_or_else(|| format!("failover candidate missing '=' in: {raw}"))?;
        let bucket = bucket.trim();
        let home = home.trim();
        if bucket.is_empty() || home.is_empty() {
            return Err(format!(
                "failover candidate has empty bucket or home region: {raw}"
            ));
        }
        out.push(FailoverCandidate {
            bucket: bucket.to_string(),
            home_region: home.to_string(),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_candidates_happy_path() {
        let c = parse_failover_candidates("catalog=us-east-1, orders=eu-west-1").unwrap();
        assert_eq!(c.len(), 2);
        assert_eq!(c[0].bucket, "catalog");
        assert_eq!(c[0].home_region, "us-east-1");
        assert_eq!(c[1].bucket, "orders");
        assert_eq!(c[1].home_region, "eu-west-1");
    }

    #[test]
    fn parse_candidates_empty_and_malformed() {
        assert!(parse_failover_candidates("").unwrap().is_empty());
        assert!(parse_failover_candidates("  ,  ").unwrap().is_empty());
        assert!(parse_failover_candidates("catalog").is_err());
        assert!(parse_failover_candidates("catalog=").is_err());
        assert!(parse_failover_candidates("=us-east-1").is_err());
    }
}
