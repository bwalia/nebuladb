//! Home-region metadata stored inside each bucket's seed document.
//!
//! NebulaDB buckets are implicit — they come into existence the first
//! time something is upserted. The operator's `NebulaBucket` controller
//! writes a known "seed" document (id = [`SEED_DOC_ID`]) whose metadata
//! carries the bucket's coordination state. That's also where
//! cross-region coordination lives: one home region per bucket, plus a
//! monotonic `home_epoch` the operator bumps on every failover.
//!
//! Keeping this in the seed doc (rather than a separate `__system`
//! bucket) avoids a chicken-and-egg bootstrap problem — the bucket's
//! own data doesn't depend on a different bucket existing first.
//!
//! # Wire shape (JSON metadata of the seed doc)
//!
//! ```json
//! {
//!     "kind": "nebuladb-operator-seed",
//!     "bucket": "catalog",
//!     "home_region": "us-east-1",
//!     "home_epoch": 3,
//!     "replicated_to": ["us-west-2", "eu-west-1"]
//! }
//! ```
//!
//! `home_region` and `home_epoch` are the required fields for
//! cross-region routing; `replicated_to` is informational.

use serde::{Deserialize, Serialize};

/// Stable id of the operator seed document. Matches the string the
/// `NebulaBucket` controller upserts — don't change without bumping a
/// CRD version, it's part of the wire contract.
pub const SEED_DOC_ID: &str = "__nebuladb_operator_seed__";

/// Parsed view of the home-region fields inside a seed doc's metadata.
/// Unknown regions or missing fields collapse to `None` so that
/// single-region deployments continue to behave like they always did —
/// no home_region means "writes land wherever they arrive."
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HomeRegion {
    /// Canonical region name. `None` means the bucket has no home yet.
    pub region: Option<String>,
    /// Monotonic counter bumped on each failover. `0` when no home is
    /// configured (don't compare epochs across buckets).
    pub epoch: u64,
    /// Informational list of regions that receive the WAL tail.
    #[serde(default)]
    pub replicated_to: Vec<String>,
}

impl HomeRegion {
    /// Extract the home-region fields from a seed document's metadata.
    /// Ignores unexpected fields so a future operator can layer extra
    /// metadata without breaking old servers.
    pub fn from_metadata(metadata: &serde_json::Value) -> Self {
        let obj = match metadata.as_object() {
            Some(o) => o,
            None => return Self::default(),
        };
        let region = obj
            .get("home_region")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        let epoch = obj
            .get("home_epoch")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let replicated_to = obj
            .get("replicated_to")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| x.as_str())
                    .map(|s| s.to_string())
                    .collect()
            })
            .unwrap_or_default();
        Self {
            region,
            epoch,
            replicated_to,
        }
    }

    /// True when this bucket has a home configured. Needed distinct from
    /// "epoch > 0" because epoch is 1-based on first assignment.
    pub fn has_home(&self) -> bool {
        self.region.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_full_payload() {
        let m = json!({
            "kind": "nebuladb-operator-seed",
            "bucket": "catalog",
            "home_region": "us-east-1",
            "home_epoch": 5,
            "replicated_to": ["us-west-2", "eu-west-1"],
        });
        let h = HomeRegion::from_metadata(&m);
        assert_eq!(h.region.as_deref(), Some("us-east-1"));
        assert_eq!(h.epoch, 5);
        assert_eq!(h.replicated_to, vec!["us-west-2", "eu-west-1"]);
        assert!(h.has_home());
    }

    #[test]
    fn defaults_when_fields_missing() {
        let h = HomeRegion::from_metadata(&json!({"kind": "seed"}));
        assert!(h.region.is_none());
        assert_eq!(h.epoch, 0);
        assert!(h.replicated_to.is_empty());
        assert!(!h.has_home());
    }

    #[test]
    fn ignores_non_object_metadata() {
        let h = HomeRegion::from_metadata(&json!(null));
        assert_eq!(h, HomeRegion::default());
        let h = HomeRegion::from_metadata(&json!("a string"));
        assert_eq!(h, HomeRegion::default());
    }

    #[test]
    fn empty_region_string_treated_as_none() {
        let h = HomeRegion::from_metadata(&json!({"home_region": ""}));
        assert!(h.region.is_none());
    }
}
