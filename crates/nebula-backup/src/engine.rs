//! Backup + restore orchestrators.
//!
//! Lifts the snapshot/WAL/storage pieces into one entry point each.
//! Server crate calls `Backup::run()` from the `/admin/backup`
//! handler and `Restore::run()` from `/admin/restore`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use bytes::Bytes;
use tracing::{info, warn};
use ulid::Ulid;

use nebula_index::TextIndex;

use crate::bundle::{pack_tar_zstd, unpack_tar_zstd};
use crate::error::{Error, Result};
use crate::manifest::{
    Artifact, BackupKind, BucketState, EmbedderInfo, Manifest, WalRange,
};
use crate::target::BackupTarget;

/// Stable seed-doc id — kept in sync with
/// `nebula_server::home_region::SEED_DOC_ID`. Duplicated as a string
/// here rather than depending on nebula-server because the backup
/// crate must not depend on the HTTP layer.
const SEED_DOC_ID: &str = "__nebuladb_operator_seed__";

/// Build a backup of an open `TextIndex` and upload it to `target`.
pub struct Backup {
    /// Live index. We only call read-only methods (snapshot, get,
    /// bucket_stats, wal_stats); a backup never mutates state.
    pub index: Arc<TextIndex>,
    /// Storage destination.
    pub target: Arc<dyn BackupTarget>,
    /// Cluster name embedded in the manifest. The operator passes
    /// the `NebulaCluster.metadata.name` here.
    pub cluster_name: String,
    /// Region name. Empty for single-region installs.
    pub region: String,
    /// Server build identity.
    pub server_version: String,
    pub git_commit: String,
    /// Where to stage tarballs during build. Should be a fast disk
    /// with at least 2× the snapshot+WAL size free; the operator
    /// usually points this at an emptyDir volume.
    pub staging_dir: PathBuf,
}

impl Backup {
    /// Run a full backup.
    ///
    /// The flow is:
    ///
    /// 1. Mint a ULID for the backup_id.
    /// 2. Take a snapshot (no-op on in-memory indexes — error out).
    /// 3. List WAL segments since the snapshot's wal_seq_captured.
    /// 4. Pack snapshot + WAL segments into a single `base.tar.zst`
    ///    on the staging disk.
    /// 5. Upload `base.tar.zst` to the target via streaming multipart.
    /// 6. Build manifest.json (with sha256 + size from steps 4-5).
    /// 7. PUT `manifest.json` last — this is the commit point.
    pub async fn run(&self) -> Result<Manifest> {
        let backup_id = Ulid::new().to_string();
        let started_at = iso_now();
        info!(backup_id = %backup_id, "backup starting");

        // Step 2: snapshot. Returns the file path + WAL seq at the
        // moment of capture.
        let snap = self.index.snapshot().map_err(Error::from_index)?;
        let wal_seq_captured = snap.wal_seq_captured;
        let snap_path = snap.path.clone();
        info!(?snap_path, wal_seq_captured, "snapshot captured");

        // Step 3: enumerate WAL segments AT and AFTER the snapshot's
        // captured seq. The snapshot covers everything before the
        // boundary; segments at-or-after are the redo log we ship
        // alongside it so a recovery can roll forward.
        let wal_handle = self
            .index
            .wal()
            .ok_or_else(|| Error::Other("index has no WAL; backup requires a persistent index".into()))?;
        let wal_segments = wal_handle
            .segments_since(wal_seq_captured)
            .map_err(Error::from_wal)?;
        let wal_newest = wal_handle.newest_cursor();
        info!(
            wal_segments = wal_segments.len(),
            newest_seq = wal_newest.segment_seq,
            "WAL slice enumerated",
        );

        // Step 4: pack into a tarball at staging.
        tokio::fs::create_dir_all(&self.staging_dir).await?;
        let tar_path = self.staging_dir.join(format!("{backup_id}.tar.zst"));
        let mut entries: Vec<(String, PathBuf)> = Vec::new();
        // Snapshot lands at `base/` so restore puts it back into
        // NEBULA_DATA_DIR/snapshots/.
        let snap_arc = format!(
            "snapshots/{}",
            snap_path
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(|| Error::Other("snapshot path has no filename".into()))?,
        );
        entries.push((snap_arc, snap_path.clone()));
        // Same for the snapshot's commit marker (`.ok` sibling).
        let ok_path = snap_path.with_extension("nsnap.ok");
        if ok_path.exists() {
            let ok_arc = format!(
                "snapshots/{}",
                ok_path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or(".unknown.ok"),
            );
            entries.push((ok_arc, ok_path));
        }
        // WAL segments under wal/ — same layout as on-disk.
        for (seq, p) in &wal_segments {
            entries.push((format!("wal/{seq:010}.nwal"), p.clone()));
        }

        let tar_path_clone = tar_path.clone();
        let entries_for_pack = entries.clone();
        let (tar_size, tar_sha) = tokio::task::spawn_blocking(move || {
            pack_tar_zstd(&tar_path_clone, &entries_for_pack)
        })
        .await
        .map_err(|e| Error::Other(format!("pack join: {e}")))??;
        info!(tar_size, "tarball staged");

        // Step 5: stream the tarball to the target.
        let target_key = format!("backups/{backup_id}/base.tar.zst");
        let mut upload = self.target.put_streaming(&target_key).await?;
        let mut f = tokio::fs::File::open(&tar_path).await?;
        use tokio::io::AsyncReadExt;
        let mut buf = vec![0u8; 1024 * 1024];
        loop {
            let n = f.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            // The streaming target hashes internally; we already
            // know the sha from pack_tar_zstd. We trust the upload's
            // hash for the manifest — they should match, and if they
            // don't (transient corruption in transit) the streaming
            // hash is what restore will see.
            if let Err(e) = upload.write(Bytes::copy_from_slice(&buf[..n])).await {
                let _ = upload.abort().await;
                return Err(e);
            }
        }
        let outcome = upload.finish().await?;
        // Defence in depth: cross-check our locally-computed sha
        // against what flowed through the upload. A mismatch here
        // means somebody scribbled on the staging file mid-flight.
        if outcome.sha256_hex != tar_sha {
            return Err(Error::Other(format!(
                "stage vs upload sha mismatch: {} vs {}",
                tar_sha, outcome.sha256_hex
            )));
        }
        info!(target_key = %target_key, "base.tar.zst uploaded");

        // Step 6: build the manifest. Per-bucket state is captured
        // at backup time — restoring writes these home_region values
        // back into seed docs so a cross-region restore comes up
        // with the same coordination state.
        let bucket_stats = self.index.bucket_stats(0);
        let bucket_count = bucket_stats.len();
        let mut buckets: Vec<BucketState> = Vec::with_capacity(bucket_count);
        let mut total_docs: u64 = 0;
        for b in &bucket_stats {
            total_docs += b.docs as u64;
            let mut state = BucketState {
                bucket: b.bucket.clone(),
                doc_count: b.docs as u64,
                home_region: None,
                home_epoch: 0,
            };
            if let Some(seed) = self.index.get(&b.bucket, SEED_DOC_ID) {
                if let Some(obj) = seed.metadata.as_object() {
                    state.home_region = obj
                        .get("home_region")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    state.home_epoch = obj.get("home_epoch").and_then(|v| v.as_u64()).unwrap_or(0);
                }
            }
            buckets.push(state);
        }

        let manifest = Manifest {
            backup_id: backup_id.clone(),
            cluster_name: self.cluster_name.clone(),
            region: self.region.clone(),
            server_version: self.server_version.clone(),
            git_commit: self.git_commit.clone(),
            kind: BackupKind::Full,
            started_at,
            completed_at: iso_now(),
            embedder: EmbedderInfo {
                model: self.index.embedder_model().to_string(),
                dim: self.index.dim(),
            },
            wal_range: WalRange {
                start_seq: wal_seq_captured,
                end_seq: wal_newest.segment_seq,
                end_byte_offset: wal_newest.byte_offset,
            },
            artifacts: vec![Artifact {
                name: "base.tar.zst".into(),
                size_bytes: tar_size,
                sha256: tar_sha.clone(),
            }],
            doc_count: total_docs,
            bucket_count,
            buckets,
        };

        // Step 7: write the manifest LAST. Until this exists in the
        // target, the backup is invalid — restore looks for the
        // manifest first and refuses to proceed without it.
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)
            .map_err(|e| Error::Manifest(format!("serialize: {e}")))?;
        self.target
            .put(
                &format!("backups/{backup_id}/manifest.json"),
                Bytes::from(manifest_bytes),
            )
            .await?;
        info!(backup_id = %backup_id, "backup committed");

        // Best-effort cleanup of the staging tarball.
        let _ = tokio::fs::remove_file(&tar_path).await;
        Ok(manifest)
    }
}

/// Restore an existing backup into a fresh `NEBULA_DATA_DIR`.
pub struct Restore {
    pub target: Arc<dyn BackupTarget>,
    /// Backup id to restore (matches a `backups/<id>/` prefix in storage).
    pub backup_id: String,
    /// Local staging directory for the downloaded tarball before unpack.
    pub staging_dir: PathBuf,
    /// Where the unpacked snapshot + WAL segments should land.
    /// Must be empty — refuse otherwise to avoid silent overwrite.
    pub data_dir: PathBuf,
    /// The target server's identity, validated against the manifest.
    pub server_version: String,
    pub embedder_model: String,
    pub embedder_dim: usize,
}

impl Restore {
    /// Run the full restore flow:
    ///
    /// 1. Pre-flight: data_dir empty, version compatible, embedder match.
    /// 2. Download manifest, parse.
    /// 3. Download base.tar.zst, sha256-verify against manifest.
    /// 4. Unpack into data_dir.
    ///
    /// Caller boots a server pointed at `data_dir`; the existing
    /// recovery code handles WAL replay through to the end.
    pub async fn run(&self) -> Result<Manifest> {
        // Step 1: pre-flight.
        if data_dir_has_payload(&self.data_dir).await? {
            return Err(Error::DataDirNotEmpty {
                path: self.data_dir.display().to_string(),
            });
        }

        // Step 2: manifest.
        let manifest_bytes = self
            .target
            .get(&format!("backups/{}/manifest.json", self.backup_id))
            .await?;
        let manifest: Manifest = serde_json::from_slice(&manifest_bytes)
            .map_err(|e| Error::Manifest(format!("decode: {e}")))?;

        // Version + embedder compatibility.
        if version_lt(&self.server_version, &manifest.server_version) {
            return Err(Error::VersionSkew {
                backup_version: manifest.server_version.clone(),
                target_version: self.server_version.clone(),
            });
        }
        if manifest.embedder.model != self.embedder_model
            || manifest.embedder.dim != self.embedder_dim
        {
            return Err(Error::EmbedderMismatch {
                backup: manifest.embedder.model.clone(),
                backup_dim: manifest.embedder.dim,
                target: self.embedder_model.clone(),
                target_dim: self.embedder_dim,
            });
        }

        // Step 3: download base.tar.zst with checksum verification.
        let base = manifest
            .artifact("base.tar.zst")
            .ok_or_else(|| Error::Manifest("manifest is missing base.tar.zst entry".into()))?;
        tokio::fs::create_dir_all(&self.staging_dir).await?;
        let tar_path = self.staging_dir.join(format!("{}.tar.zst", self.backup_id));
        let key = format!("backups/{}/base.tar.zst", self.backup_id);
        let outcome = self.target.download_to_path(&key, &tar_path).await?;
        if outcome.sha256_hex != base.sha256 {
            return Err(Error::Checksum {
                artifact: "base.tar.zst".into(),
                expected: base.sha256.clone(),
                got: outcome.sha256_hex,
            });
        }
        if outcome.bytes_written != base.size_bytes {
            warn!(
                expected = base.size_bytes,
                got = outcome.bytes_written,
                "size mismatch but sha256 matched — manifest may be stale; continuing",
            );
        }

        // Step 4: unpack.
        let dest = self.data_dir.clone();
        let tar_path_clone = tar_path.clone();
        tokio::task::spawn_blocking(move || unpack_tar_zstd(&tar_path_clone, &dest))
            .await
            .map_err(|e| Error::Other(format!("unpack join: {e}")))??;

        // Best-effort cleanup.
        let _ = tokio::fs::remove_file(&tar_path).await;
        info!(backup_id = %self.backup_id, "restore complete; boot server pointing at data_dir to roll WAL");
        Ok(manifest)
    }
}

/// Returns true if `dir` exists and contains anything that looks like
/// NebulaDB persistent state (`wal/` or `snapshots/` populated).
async fn data_dir_has_payload(dir: &Path) -> Result<bool> {
    let wal_dir = dir.join("wal");
    let snap_dir = dir.join("snapshots");
    for d in [wal_dir, snap_dir] {
        if !d.exists() {
            continue;
        }
        let mut walker = tokio::fs::read_dir(&d).await?;
        if walker.next_entry().await?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Naive semver comparator: returns true when `a < b` by component.
/// We only use it for the version-skew check, which is allowed to be
/// approximate — non-semver strings collapse to a "trust it" outcome
/// (returns false, i.e. "not older").
fn version_lt(a: &str, b: &str) -> bool {
    let parse = |s: &str| -> Option<(u32, u32, u32)> {
        let mut it = s.split('.').map(|c| c.split('-').next().unwrap_or("0"));
        let major = it.next()?.parse().ok()?;
        let minor = it.next()?.parse().ok()?;
        let patch = it.next()?.parse().ok()?;
        Some((major, minor, patch))
    };
    match (parse(a), parse(b)) {
        (Some(ax), Some(bx)) => ax < bx,
        _ => false,
    }
}

/// ISO-8601 UTC "now" without dragging in `chrono`. Same shape as
/// `cross_region_status::iso_now` in nebula-server.
fn iso_now() -> String {
    let secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = secs / 86_400;
    let rem = secs % 86_400;
    let (y, mo, d) = days_to_ymd(days as i64);
    let hh = rem / 3600;
    let mm = (rem % 3600) / 60;
    let ss = rem % 60;
    format!("{y:04}-{mo:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}Z")
}

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
    fn version_lt_basic() {
        assert!(version_lt("0.1.0", "0.1.1"));
        assert!(version_lt("0.1.9", "0.2.0"));
        assert!(!version_lt("0.2.0", "0.1.9"));
        assert!(!version_lt("0.1.1", "0.1.1"));
        // Non-semver inputs default to "not older" — never block a
        // restore on a parse failure.
        assert!(!version_lt("garbage", "0.1.0"));
        assert!(!version_lt("0.1.0", "garbage"));
    }
}
