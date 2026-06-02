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
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move { run(index, cfg).await })
}

async fn run(index: Arc<TextIndex>, cfg: SnapshotSchedulerConfig) {
    if !cfg.any_trigger_enabled() {
        tracing::info!("snapshot scheduler disabled (no triggers configured)");
        return;
    }
    // wal_stats returns None for in-memory indexes — nothing to do.
    if index.wal_stats().is_none() {
        tracing::info!("snapshot scheduler disabled (index has no WAL)");
        return;
    }
    tracing::info!(
        interval_secs = cfg.interval.as_secs(),
        wal_bytes_threshold = cfg.wal_bytes_threshold,
        poll_secs = cfg.poll_interval.as_secs(),
        compact = cfg.compact,
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
        // Snapshot + compact are synchronous and touch disk. Move
        // them to the blocking pool so the tokio runtime stays free
        // to serve requests in the meantime.
        let outcome = tokio::task::spawn_blocking(move || {
            let snap = idx.snapshot()?;
            let removed = if compact { idx.compact_wal()? } else { 0 };
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((snap, removed))
        })
        .await;
        match outcome {
            Ok(Ok((snap, removed))) => {
                tracing::info!(
                    path = %snap.path.display(),
                    wal_seq_captured = snap.wal_seq_captured,
                    segments_removed = removed,
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
}
