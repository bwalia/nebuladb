//! Backup manifest — the canonical inventory written *last* in a
//! backup so its presence is the commit point.
//!
//! Every restore reads the manifest first, validates artifact
//! checksums against it, then refuses to proceed on any mismatch.
//! That makes the manifest the single source of truth for what's in
//! a backup; missing-or-corrupt artifacts surface as a clean error
//! instead of a half-restored cluster.

use serde::{Deserialize, Serialize};

/// One artifact (file in object storage) referenced by a manifest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Artifact {
    /// Object name relative to the backup's prefix, e.g.
    /// `"base.tar.zst"`.
    pub name: String,
    /// Uncompressed-after-multipart total size — used for "is this
    /// download done?" progress reporting.
    pub size_bytes: u64,
    /// Hex-encoded SHA-256 of the bytes as written to storage.
    /// Validated on restore before unpacking; mismatch is fatal.
    pub sha256: String,
}

/// Backup kind. v1 only ships `Full`; `Incremental` is reserved for
/// the v1.1 WAL-trickle path.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum BackupKind {
    Full,
    Incremental,
}

/// WAL byte range covered by this backup. Used by PITR replay to
/// know where to stop and by the operator to prove "your most recent
/// write is in this backup."
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalRange {
    pub start_seq: u64,
    pub end_seq: u64,
    pub end_byte_offset: u64,
}

/// Embedder identity at backup time. Mismatched embedders on restore
/// would silently corrupt similarity scores, so we validate.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EmbedderInfo {
    pub model: String,
    pub dim: usize,
}

/// Per-bucket coordination state captured for cross-region restore.
/// On restore we re-seed each bucket's seed doc with these values so
/// the new cluster's home_epoch matches what the backup observed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BucketState {
    pub bucket: String,
    pub doc_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub home_region: Option<String>,
    #[serde(default)]
    pub home_epoch: u64,
}

/// The full manifest. Serialized as `manifest.json` and uploaded
/// last so a partial backup is invisible to consumers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    /// ULID. Sortable by creation time, no clock-collision risk.
    pub backup_id: String,
    pub cluster_name: String,
    /// Region the backup was taken from. Restore validates that the
    /// target's region matches OR that `--force-cross-cluster` is set.
    #[serde(default)]
    pub region: String,
    /// Server crate version (`CARGO_PKG_VERSION`). Restore refuses
    /// if the target binary is older than this.
    pub server_version: String,
    /// Build sha at backup time; informational only.
    #[serde(default)]
    pub git_commit: String,
    pub kind: BackupKind,
    /// ISO-8601 UTC.
    pub started_at: String,
    pub completed_at: String,
    pub embedder: EmbedderInfo,
    pub wal_range: WalRange,
    pub artifacts: Vec<Artifact>,
    pub doc_count: u64,
    pub bucket_count: usize,
    /// Per-bucket coordination state captured at backup time.
    #[serde(default)]
    pub buckets: Vec<BucketState>,
}

impl Manifest {
    /// Find an artifact by name. Used by restore to look up the
    /// expected sha256 of a downloaded blob.
    pub fn artifact(&self, name: &str) -> Option<&Artifact> {
        self.artifacts.iter().find(|a| a.name == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_json() {
        let m = Manifest {
            backup_id: "01HV6ABCDEF".into(),
            cluster_name: "nebula-prod".into(),
            region: "us-east-1".into(),
            server_version: "0.1.1".into(),
            git_commit: "abc1234".into(),
            kind: BackupKind::Full,
            started_at: "2026-05-05T10:00:00Z".into(),
            completed_at: "2026-05-05T10:01:23Z".into(),
            embedder: EmbedderInfo {
                model: "text-embedding-3-small".into(),
                dim: 1536,
            },
            wal_range: WalRange {
                start_seq: 0,
                end_seq: 12,
                end_byte_offset: 4096123,
            },
            artifacts: vec![Artifact {
                name: "base.tar.zst".into(),
                size_bytes: 145234567,
                sha256: "deadbeef".into(),
            }],
            doc_count: 4_521_337,
            bucket_count: 18,
            buckets: vec![BucketState {
                bucket: "catalog".into(),
                doc_count: 100,
                home_region: Some("us-east-1".into()),
                home_epoch: 3,
            }],
        };
        let s = serde_json::to_string_pretty(&m).unwrap();
        let back: Manifest = serde_json::from_str(&s).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn artifact_lookup_finds_by_name() {
        let m = Manifest {
            backup_id: "x".into(),
            cluster_name: "c".into(),
            region: String::new(),
            server_version: "0.1.0".into(),
            git_commit: String::new(),
            kind: BackupKind::Full,
            started_at: "t".into(),
            completed_at: "t".into(),
            embedder: EmbedderInfo {
                model: "m".into(),
                dim: 8,
            },
            wal_range: WalRange {
                start_seq: 0,
                end_seq: 0,
                end_byte_offset: 0,
            },
            artifacts: vec![Artifact {
                name: "base.tar.zst".into(),
                size_bytes: 1,
                sha256: "abc".into(),
            }],
            doc_count: 0,
            bucket_count: 0,
            buckets: vec![],
        };
        assert!(m.artifact("base.tar.zst").is_some());
        assert!(m.artifact("missing").is_none());
    }
}
