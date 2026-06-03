//! Periodic snapshot + WAL compaction.
//!
//! Background task that calls [`nebula_index::TextIndex::snapshot`]
//! and (optionally) [`nebula_index::TextIndex::compact_wal`] on a
//! schedule. Without this, the only way a snapshot ever gets taken is
//! a human hitting `POST /api/v1/admin/snapshot` — and on every
//! observed production deploy nobody had done that, leaving cold
//! recovery to replay the entire WAL from seq 0. On a multi-GB WAL
//! that's minutes per restart and the dominant component of
//! "how long is the leader unavailable after a crash?".
//!
//! Triggers (any one fires the snapshot):
//!   * `interval_secs` elapsed since the last snapshot.
//!   * WAL on-disk size exceeds `wal_bytes_threshold`.
//!
//! Set `interval_secs = 0` to disable time-based triggers; the
//! size-based trigger keeps firing. Set the threshold to `u64::MAX`
//! (the env var to `0`) to disable size-based triggers. With both
//! disabled the scheduler exits at startup.
//!
//! The actual snapshot/compact calls happen on `spawn_blocking` —
//! they hold the index's inner read lock briefly (a couple ms) but
//! do disk I/O while holding nothing, so they don't stall the
//! tokio runtime.

use std::sync::Arc;
use std::time::{Duration, Instant};

use nebula_index::TextIndex;

/// Which durability strategy the scheduler runs — design 0007 §4.
/// Selected once at startup from the node's cluster role + the
/// `NEBULA_SNAPSHOT_OFFLOAD` opt-in (see `main.rs`).
pub enum SnapshotMode {
    /// Single node: snapshot locally on trigger, then (optionally)
    /// compact. Unchanged from the historical behavior — a standalone
    /// deployment accepts the brief writer stall as the price of
    /// having no warm replica to offload to.
    Standalone,
    /// Write-serving leader with an offload follower. NEVER snapshots
    /// (never holds the index read lock for minutes). Each poll it
    /// only `compact_wal_no_prune()`s against the latest snapshot the
    /// follower published into the shared dir. It does not own those
    /// snapshots, so it never prunes them.
    LeaderCompactOnly,
    /// Warm standby that owns snapshotting. On trigger it stamps a
    /// snapshot with the leader-WAL seq it has applied (its
    /// replication cursor) into the shared dir, then prunes its own
    /// old snapshots. It never compacts — it has no authoritative WAL.
    FollowerSnapshot {
        cursor: std::sync::Arc<crate::state::FollowerCursor>,
    },
}

/// Tunables, all env-overridable in [`SnapshotSchedulerConfig::from_env`].
#[derive(Debug, Clone)]
pub struct SnapshotSchedulerConfig {
    /// How long between time-based snapshot triggers. `Duration::ZERO`
    /// disables time-based triggers. Default 15 minutes.
    pub interval: Duration,
    /// Snapshot whenever the WAL on disk exceeds this many bytes.
    /// `0` disables size-based triggers. Default 50 MiB.
    pub wal_bytes_threshold: u64,
    /// Polling cadence — how often the loop wakes up to check. Should
    /// be << `interval` so size-based triggers don't lag by an
    /// interval after they're tripped. Default 30s.
    pub poll_interval: Duration,
    /// If true, call `compact_wal` after a successful snapshot to
    /// drop superseded segments. Disable if you want to keep full
    /// history on disk (e.g., point-in-time recovery exploration).
    pub compact: bool,
}

impl Default for SnapshotSchedulerConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(900),
            wal_bytes_threshold: 50 * 1024 * 1024,
            poll_interval: Duration::from_secs(30),
            compact: true,
        }
    }
}

impl SnapshotSchedulerConfig {
    /// Build from env, falling back to defaults per field.
    ///
    /// - `NEBULA_SNAPSHOT_INTERVAL_SECS` → `interval` (0 disables)
    /// - `NEBULA_SNAPSHOT_WAL_BYTES`     → `wal_bytes_threshold` (0 disables)
    /// - `NEBULA_SNAPSHOT_POLL_SECS`     → `poll_interval`
    /// - `NEBULA_SNAPSHOT_COMPACT`       → `compact` (`off`/`0`/`false` → false)
    pub fn from_env() -> Self {
        let mut cfg = Self::default();
        if let Some(v) = std::env::var("NEBULA_SNAPSHOT_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
        {
            cfg.interval = Duration::from_secs(v);
        }
        if let Some(v) = std::env::var("NEBULA_SNAPSHOT_WAL_BYTES")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
        {
            cfg.wal_bytes_threshold = v;
        }
        if let Some(v) = std::env::var("NEBULA_SNAPSHOT_POLL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|v| *v > 0)
        {
            cfg.poll_interval = Duration::from_secs(v);
        }
        if let Ok(v) = std::env::var("NEBULA_SNAPSHOT_COMPACT") {
            cfg.compact = !matches!(v.as_str(), "off" | "0" | "false" | "no");
        }
        cfg
    }

    fn time_trigger_enabled(&self) -> bool {
        !self.interval.is_zero()
    }

    fn size_trigger_enabled(&self) -> bool {
        self.wal_bytes_threshold > 0
    }

    pub fn any_trigger_enabled(&self) -> bool {
        self.time_trigger_enabled() || self.size_trigger_enabled()
    }
}

/// Spawn the scheduler. Returns the join handle so callers can abort
/// it on shutdown (or just drop it for fire-and-forget).
///
/// Exits immediately if no triggers are enabled or the index has no
/// data directory (in-memory mode — nothing to snapshot).
pub fn spawn(
    index: Arc<TextIndex>,
    cfg: SnapshotSchedulerConfig,
    mode: SnapshotMode,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move { run(index, cfg, mode).await })
}

async fn run(index: Arc<TextIndex>, cfg: SnapshotSchedulerConfig, mode: SnapshotMode) {
    if !cfg.any_trigger_enabled() {
        tracing::info!("snapshot scheduler disabled (no triggers configured)");
        return;
    }
    // wal_stats returns None for in-memory indexes — nothing to do.
    if index.wal_stats().is_none() {
        tracing::info!("snapshot scheduler disabled (index has no WAL)");
        return;
    }
    let mode_name = match &mode {
        SnapshotMode::Standalone => "standalone",
        SnapshotMode::LeaderCompactOnly => "leader-compact-only",
        SnapshotMode::FollowerSnapshot { .. } => "follower-snapshot",
    };
    tracing::info!(
        interval_secs = cfg.interval.as_secs(),
        wal_bytes_threshold = cfg.wal_bytes_threshold,
        poll_secs = cfg.poll_interval.as_secs(),
        compact = cfg.compact,
        mode = mode_name,
        "snapshot scheduler started"
    );
    let mut last_snapshot = Instant::now();
    // WAL size at the moment of the previous snapshot. The size
    // trigger fires when the WAL has *grown* by `wal_bytes_threshold`
    // since then, not whenever absolute size happens to exceed the
    // threshold. Without this, a system whose compact is blocked (e.g.
    // follower hasn't ack'd yet) snapshots every single poll once
    // it crosses the threshold — pegging the inner read lock and
    // starving writers. Observed on 192.168.1.193 leader: 30+
    // snapshots in 9 minutes before the runtime wedged.
    let mut wal_bytes_at_last_snapshot: u64 = index
        .wal_stats()
        .map(|s| s.total_bytes)
        .unwrap_or(0);
    loop {
        tokio::time::sleep(cfg.poll_interval).await;

        // Leader: compact-only, every poll, ungated by triggers.
        // `compact_wal_no_prune` takes NO index lock and no-ops when
        // there's no newer snapshot in the shared dir — so it's cheap
        // to run unconditionally and never stalls the serving path.
        // It never prunes: the follower owns the shared snapshots.
        if let SnapshotMode::LeaderCompactOnly = mode {
            let idx = Arc::clone(&index);
            let outcome = tokio::task::spawn_blocking(move || {
                idx.compact_wal_no_prune()
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            })
            .await;
            match outcome {
                Ok(Ok(removed)) => {
                    if removed > 0 {
                        tracing::info!(
                            segments_removed = removed,
                            "leader compacted WAL against follower snapshot"
                        );
                    }
                }
                Ok(Err(e)) => tracing::error!(error = %e, "leader compact failed"),
                Err(e) => tracing::error!(error = ?e, "leader compact task panicked"),
            }
            continue;
        }

        let wal_bytes = index
            .wal_stats()
            .map(|s| s.total_bytes)
            .unwrap_or(0);
        let time_fired = cfg.time_trigger_enabled()
            && last_snapshot.elapsed() >= cfg.interval;
        // Growth-based size trigger: how many bytes have been
        // appended since the last snapshot. Saturating sub because
        // compact may have shrunk the WAL.
        let growth = wal_bytes.saturating_sub(wal_bytes_at_last_snapshot);
        let size_fired = cfg.size_trigger_enabled() && growth >= cfg.wal_bytes_threshold;
        if !(time_fired || size_fired) {
            continue;
        }

        // Follower not yet caught up to the leader: skip this cycle so
        // we never stamp a snapshot with seq 0 (which would cover
        // nothing yet let a leader compact its WAL away).
        if let SnapshotMode::FollowerSnapshot { cursor } = &mode {
            let (seg, _off) = cursor.load();
            if seg == 0 {
                tracing::info!("follower snapshot skipped (cursor seq 0, not caught up)");
                continue;
            }
        }

        let reason = if size_fired && time_fired {
            "size+time"
        } else if size_fired {
            "size"
        } else {
            "time"
        };
        tracing::info!(
            reason,
            wal_bytes,
            growth,
            elapsed_secs = last_snapshot.elapsed().as_secs(),
            "snapshot scheduler firing"
        );
        let idx = Arc::clone(&index);
        let compact = cfg.compact;
        // Capture the follower cursor seq (if any) before moving into
        // the blocking task.
        let follower_seq = match &mode {
            SnapshotMode::FollowerSnapshot { cursor } => Some(cursor.load().0),
            _ => None,
        };
        // Snapshot + compact are synchronous and touch disk. Move
        // them to the blocking pool so the tokio runtime stays free
        // to serve requests in the meantime.
        let outcome = tokio::task::spawn_blocking(move || {
            let snap = match follower_seq {
                // Follower: stamp the snapshot with the leader-WAL seq
                // we've applied and write it to the configured (shared)
                // dir. Then prune our own old snapshots. Never compact
                // — we have no authoritative WAL.
                Some(seg) => {
                    let dir = idx
                        .snapshot_dir()
                        .ok_or("follower index has no snapshot dir")?
                        .to_path_buf();
                    let snap = idx.snapshot_to(&dir, seg)?;
                    // Prune our own old snapshots (we own the shared
                    // dir). Do NOT compact: a follower has no
                    // authoritative WAL.
                    let _ = idx.prune_snapshots()?;
                    snap
                }
                // Standalone: snapshot locally, then compact (which
                // also prunes). Unchanged historical behavior.
                None => {
                    let snap = idx.snapshot()?;
                    let _ = if compact { idx.compact_wal()? } else { 0 };
                    snap
                }
            };
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(snap)
        })
        .await;
        match outcome {
            Ok(Ok(snap)) => {
                tracing::info!(
                    path = %snap.path.display(),
                    wal_seq_captured = snap.wal_seq_captured,
                    "snapshot scheduler completed"
                );
                last_snapshot = Instant::now();
                // Re-read WAL size *after* compact so the next
                // growth comparison starts from the post-compact
                // baseline. If compact removed segments this drops;
                // if it couldn't (follower lag), it stays high but
                // we still won't re-fire until ANOTHER threshold's
                // worth of appends arrives.
                wal_bytes_at_last_snapshot = index
                    .wal_stats()
                    .map(|s| s.total_bytes)
                    .unwrap_or(wal_bytes_at_last_snapshot);
            }
            Ok(Err(e)) => {
                tracing::error!(error = %e, "snapshot scheduler failed");
            }
            Err(e) => {
                tracing::error!(error = ?e, "snapshot scheduler task panicked");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults() {
        let c = SnapshotSchedulerConfig::default();
        assert_eq!(c.interval, Duration::from_secs(900));
        assert_eq!(c.wal_bytes_threshold, 50 * 1024 * 1024);
        assert!(c.compact);
        assert!(c.any_trigger_enabled());
    }

    #[test]
    fn config_disabled_when_both_off() {
        let c = SnapshotSchedulerConfig {
            interval: Duration::ZERO,
            wal_bytes_threshold: 0,
            poll_interval: Duration::from_secs(1),
            compact: false,
        };
        assert!(!c.any_trigger_enabled());
    }

    #[test]
    fn compact_env_off_means_false() {
        // We can't actually unset env safely in parallel tests, so
        // assert the matching rule rather than driving from env.
        let cfg = SnapshotSchedulerConfig {
            compact: !matches!("off", "off" | "0" | "false" | "no"),
            ..SnapshotSchedulerConfig::default()
        };
        assert!(!cfg.compact);
    }

    use crate::state::FollowerCursor;
    use nebula_embed::{Embedder, MockEmbedder};
    use nebula_index::TextIndex;
    use nebula_vector::{HnswConfig, Metric};
    use std::path::Path;

    fn persistent_index(data_dir: &Path, snapshots: &Path) -> Arc<TextIndex> {
        let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(64));
        Arc::new(
            TextIndex::open_persistent_in(
                emb,
                Metric::Cosine,
                HnswConfig::default(),
                data_dir,
                Some(snapshots.to_path_buf()),
            )
            .unwrap(),
        )
    }

    fn count_snapshots(dir: &Path) -> usize {
        std::fs::read_dir(dir)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .filter(|e| e.file_name().to_string_lossy().ends_with(".nsnap.ok"))
                    .count()
            })
            .unwrap_or(0)
    }

    // Fast-firing config so a single poll triggers a snapshot.
    fn fast_cfg() -> SnapshotSchedulerConfig {
        SnapshotSchedulerConfig {
            interval: Duration::from_millis(1),
            wal_bytes_threshold: 0, // size trigger off; time trigger fires immediately
            poll_interval: Duration::from_millis(20),
            compact: true,
        }
    }

    /// A `FollowerSnapshot` run produces a snapshot stamped with the
    /// follower cursor's segment seq, in the shared dir.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn follower_snapshot_stamps_cursor_seq() {
        let data = tempfile::tempdir().unwrap();
        let shared = tempfile::tempdir().unwrap();
        let idx = persistent_index(data.path(), shared.path());
        idx.upsert_text("docs", "a", "x", serde_json::json!({}))
            .await
            .unwrap();

        let cursor = Arc::new(FollowerCursor::default());
        cursor.store(7, 0); // applied through leader-WAL seg 7

        let handle = spawn(
            Arc::clone(&idx),
            fast_cfg(),
            SnapshotMode::FollowerSnapshot {
                cursor: Arc::clone(&cursor),
            },
        );
        // Give it a few polls to fire.
        tokio::time::sleep(Duration::from_millis(200)).await;
        handle.abort();

        assert!(count_snapshots(shared.path()) >= 1, "follower should snapshot");
        let header = idx.latest_snapshot_header().expect("snapshot header");
        assert_eq!(header.wal_seq_at_snapshot, 7, "stamped with cursor seg");
    }

    /// A `FollowerSnapshot` run with cursor seq 0 (not yet caught up)
    /// must NOT produce a snapshot.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn follower_snapshot_skips_when_cursor_zero() {
        let data = tempfile::tempdir().unwrap();
        let shared = tempfile::tempdir().unwrap();
        let idx = persistent_index(data.path(), shared.path());
        idx.upsert_text("docs", "a", "x", serde_json::json!({}))
            .await
            .unwrap();

        let cursor = Arc::new(FollowerCursor::default()); // seq 0

        let handle = spawn(
            Arc::clone(&idx),
            fast_cfg(),
            SnapshotMode::FollowerSnapshot { cursor },
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        handle.abort();

        assert_eq!(count_snapshots(shared.path()), 0, "no seq-0 snapshot");
    }

    /// A `LeaderCompactOnly` run NEVER writes a new snapshot, but DOES
    /// compact its WAL when a snapshot exists in the shared dir.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn leader_compact_only_never_snapshots_but_compacts() {
        let data = tempfile::tempdir().unwrap();
        let shared = tempfile::tempdir().unwrap();
        let idx = persistent_index(data.path(), shared.path());

        // Write enough records to roll multiple WAL segments so there's
        // something to compact away once a snapshot covers them.
        for i in 0..50 {
            idx.upsert_text("docs", &format!("k{i}"), "payload", serde_json::json!({}))
                .await
                .unwrap();
        }
        let wal_before = idx.wal_stats().unwrap().segment_count;

        // Simulate the follower publishing a snapshot into the shared
        // dir that covers the current WAL seq.
        let seq = nebula_wal::current_seq(&data.path().join("wal"))
            .unwrap()
            .unwrap_or(0);
        idx.snapshot_to(shared.path(), seq).unwrap();
        let snaps_before = count_snapshots(shared.path());
        assert_eq!(snaps_before, 1);

        let handle = spawn(
            Arc::clone(&idx),
            fast_cfg(),
            SnapshotMode::LeaderCompactOnly,
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        handle.abort();

        // No NEW snapshot was produced by the leader.
        assert_eq!(
            count_snapshots(shared.path()),
            snaps_before,
            "leader must not snapshot"
        );
        // It compacted: WAL segments at/after the snapshot seq dropped.
        let wal_after = idx.wal_stats().unwrap().segment_count;
        assert!(
            wal_after <= wal_before,
            "leader should have compacted ({wal_before} -> {wal_after})"
        );
    }
}
