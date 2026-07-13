//! Cross-region consumer task.
//!
//! Each node (leader-role only) can tail one or more remote regions'
//! [`CrossRegionReplicationService`]. Incoming [`CrossRegionEntry`]s
//! are decoded to [`WalRecord`]s and applied to the local index via
//! `TextIndex::apply_wal_record`.
//!
//! Compared to the same-region follower flow in [`crate::follower`]:
//!
//! - We tail *many* remotes, not one. The [`spawn_all`] entry point
//!   creates a separate task per remote.
//! - Each record carries a `bucket` + `home_epoch`. We consult the
//!   local seed doc before applying: if the caller's epoch is lower
//!   than our known epoch, we skip (this is the "deposed region
//!   leaking old writes" guard described in design-0001).
//! - Status is reported into a shared [`CrossRegionStatusHub`] so
//!   the admin endpoint can show who's connected and what cursor
//!   they're at.
//!
//! At-least-once delivery, idempotent apply, same as the single-region
//! follower. We deliberately do NOT persist cursors — the local index
//! is considered "warm cache for non-home buckets" today; a restart
//! replays from each remote's `BEGIN`. A future revision can add
//! per-region cursors if the replay cost becomes significant.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, info, warn};

use nebula_index::TextIndex;
use nebula_wal::{WalCursor, WalRecord};

use crate::pb;

/// Stable error code published on `/admin/replication` when the consumer
/// rejects a record with a stale epoch. Surfaced in `last_error` so
/// operators can tell a transient disconnect from a deposed-region
/// silently dropping writes.
pub const STALE_EPOCH: &str = "stale_home_epoch";

/// Id of the operator seed document that carries each bucket's
/// `home_region` / `home_epoch`. Mirror of `nebula_server::SEED_DOC_ID`
/// — duplicated here so `nebula-grpc` stays free of a `nebula-server`
/// dependency. See design-0001 for why the seed doc is the source of
/// truth.
pub const SEED_DOC_ID: &str = "__nebuladb_operator_seed__";

/// Shared status sink — trait instead of a concrete type so
/// `nebula-grpc` doesn't depend on `nebula-server` (the actual
/// implementation lives in server's `cross_region_status.rs`).
pub trait StatusSink: Send + Sync + 'static {
    fn register(&self, region: &str, grpc_url: &str);
    fn record_apply(&self, region: &str, segment: u64, byte_offset: u64);
    fn record_error(&self, region: &str, err: &str);
}

/// One remote peer to tail.
#[derive(Clone, Debug)]
pub struct RemotePeer {
    pub region: String,
    pub grpc_url: String,
}

/// Snapshot of which buckets this node already owns — records for
/// these must not be applied from remote streams even if the remote
/// sends them (defence in depth; the server-side filter should have
/// dropped them already).
pub trait OwnedBuckets: Send + Sync + 'static {
    fn snapshot(&self) -> HashSet<String>;
}

/// An `OwnedBuckets` that reads from the local index by scanning
/// every seed doc and picking buckets whose `home_region` equals this
/// node's region. Recomputed on each iteration so a freshly-created
/// bucket comes into scope without restart.
///
/// Not cheap (linear in bucket count) but called on a human-scale
/// cadence (reconnects). For very large deployments this can be
/// replaced with a cached view that the bucket controller writes to.
pub struct IndexOwnedBuckets {
    pub index: Arc<TextIndex>,
    pub my_region: String,
    pub seed_doc_id: &'static str,
}

impl OwnedBuckets for IndexOwnedBuckets {
    fn snapshot(&self) -> HashSet<String> {
        let mut out = HashSet::new();
        for b in self.index.bucket_stats(0) {
            if let Some(doc) = self.index.get(&b.bucket, self.seed_doc_id) {
                if let Some(home) = doc
                    .metadata
                    .as_object()
                    .and_then(|m| m.get("home_region"))
                    .and_then(|v| v.as_str())
                {
                    if home == self.my_region {
                        out.insert(b.bucket);
                    }
                }
            }
        }
        out
    }
}

/// Spawn one tailer task per peer. Each task reconnects forever with
/// exponential backoff; callers own the returned `JoinHandle`s and
/// can abort the whole fan-out on shutdown.
///
/// `my_region` is passed to the service so the source can tag
/// incoming reconnects (observability only; the protocol is one-way).
pub fn spawn_all(
    peers: Vec<RemotePeer>,
    index: Arc<TextIndex>,
    owned: Arc<dyn OwnedBuckets>,
    status: Arc<dyn StatusSink>,
    my_region: String,
) -> Vec<JoinHandle<()>> {
    peers
        .into_iter()
        .map(|p| {
            status.register(&p.region, &p.grpc_url);
            let index = index.clone();
            let owned = owned.clone();
            let status = status.clone();
            let my_region = my_region.clone();
            tokio::spawn(async move {
                tail_forever(p, index, owned, status, my_region).await;
            })
        })
        .collect()
}

/// Run until the task is aborted. On transport or apply errors we
/// record into status + back off. Back-off caps at 30s — matches
/// the in-region follower's ceiling.
async fn tail_forever(
    peer: RemotePeer,
    index: Arc<TextIndex>,
    owned: Arc<dyn OwnedBuckets>,
    status: Arc<dyn StatusSink>,
    my_region: String,
) {
    let mut backoff_ms: u64 = 100;
    loop {
        match tail_once(&peer, &index, &owned, &status, &my_region).await {
            Ok(()) => {
                // Server closed cleanly — usually a shutdown or a
                // lagged subscriber. Reset backoff and retry.
                backoff_ms = 100;
            }
            Err(e) => {
                warn!(region = %peer.region, error = %e, "cross-region tail errored, backing off");
                status.record_error(&peer.region, &e.to_string());
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(30_000);
            }
        }
    }
}

/// One iteration: open the stream, apply until it ends or errors.
/// Cursor resumption is intentionally from BEGIN — see module doc.
async fn tail_once(
    peer: &RemotePeer,
    index: &Arc<TextIndex>,
    owned: &Arc<dyn OwnedBuckets>,
    status: &Arc<dyn StatusSink>,
    my_region: &str,
) -> Result<(), CrossRegionError> {
    let endpoint =
        Endpoint::from_shared(peer.grpc_url.clone()).map_err(|e| CrossRegionError::Transport(e.to_string()))?;
    let channel: Channel = endpoint
        .connect()
        .await
        .map_err(|e| CrossRegionError::Transport(e.to_string()))?;
    let mut client = pb::cross_region_replication_service_client::CrossRegionReplicationServiceClient::new(channel);

    let owned_snapshot = owned.snapshot();
    info!(
        region = %peer.region,
        owned = owned_snapshot.len(),
        "cross-region subscribe",
    );
    let req = pb::TailCrossRegionRequest {
        start: Some(pb::WalCursor {
            segment_seq: WalCursor::BEGIN.segment_seq,
            byte_offset: WalCursor::BEGIN.byte_offset,
        }),
        caller_region: my_region.to_string(),
        buckets_owned_by_caller: owned_snapshot.into_iter().collect(),
    };
    let mut stream = client
        .tail_cross_region(req)
        .await
        .map_err(|s| CrossRegionError::Rpc(format!("{s}")))?
        .into_inner();

    while let Some(item) = stream.next().await {
        let entry = item.map_err(|s| CrossRegionError::Rpc(format!("{s}")))?;

        // Decode the WAL record.
        let rec: WalRecord = bincode::deserialize(&entry.record_bincode)
            .map_err(|e| CrossRegionError::Decode(e.to_string()))?;

        // Home-epoch check. If the local seed doc's epoch is higher
        // than the incoming, the sender is deposed — silently skip.
        if !epoch_is_fresh(index, entry.bucket.as_str(), entry.home_epoch) {
            warn!(
                region = %peer.region,
                bucket = %entry.bucket,
                incoming_epoch = entry.home_epoch,
                "skipping stale-epoch cross-region record",
            );
            status.record_error(&peer.region, STALE_EPOCH);
            continue;
        }

        if let Err(e) = index.apply_wal_record(&rec) {
            return Err(CrossRegionError::Apply(e.to_string()));
        }

        let next = entry.next_cursor.unwrap_or(pb::WalCursor {
            segment_seq: 0,
            byte_offset: 0,
        });
        status.record_apply(&peer.region, next.segment_seq, next.byte_offset);
        debug!(
            region = %peer.region,
            seg = next.segment_seq,
            off = next.byte_offset,
            "applied cross-region record",
        );
    }
    Ok(())
}

/// Read the local seed doc for `bucket`; `true` when the incoming
/// `home_epoch` is >= what we have locally (or we have nothing, which
/// is treated as "anyone can write"). This is the safety net against
/// a deposed region reconnecting and streaming pre-failover writes.
fn epoch_is_fresh(index: &TextIndex, bucket: &str, incoming_epoch: u64) -> bool {
    let local_epoch = index
        .get(bucket, SEED_DOC_ID)
        .and_then(|d| {
            d.metadata
                .as_object()
                .and_then(|m| m.get("home_epoch"))
                .and_then(|v| v.as_u64())
        })
        .unwrap_or(0);
    incoming_epoch >= local_epoch
}

#[derive(Debug, thiserror::Error)]
pub enum CrossRegionError {
    #[error("transport: {0}")]
    Transport(String),
    #[error("rpc: {0}")]
    Rpc(String),
    #[error("decode: {0}")]
    Decode(String),
    #[error("apply: {0}")]
    Apply(String),
}

// ============================================================
// Automatic multi-region failover (design 0011 §4)
// ============================================================

/// Read side of the cross-region status hub, consumed by the failover
/// monitor. Kept as a trait (mirroring [`StatusSink`]) so `nebula-grpc`
/// stays free of a `nebula-server` dependency — the hub in server's
/// `cross_region_status.rs` implements it.
pub trait RegionHealthSource: Send + Sync + 'static {
    /// True when the named remote region's consumer stream is currently
    /// healthy (its last streaming call succeeded). A region we have no
    /// record of is treated as healthy — we only fail over regions we
    /// are actively tailing and have seen fail.
    fn is_region_healthy(&self, region: &str) -> bool;
}

/// A bucket this node is willing to take over if its remote home region
/// dies. One entry per remote-homed bucket the operator marks as a
/// failover target for this region.
#[derive(Clone, Debug)]
pub struct FailoverCandidate {
    /// Bucket name.
    pub bucket: String,
    /// The region that currently owns the bucket (the one we watch).
    pub home_region: String,
}

/// Configuration for the [`RegionFailoverMonitor`].
#[derive(Clone, Debug)]
pub struct FailoverConfig {
    /// This node's own region — the region we promote buckets *to*.
    pub my_region: String,
    /// How long a home region must be continuously unreachable before we
    /// self-promote. Anti-flap: a reconnect inside this window resets
    /// the counter.
    pub grace: Duration,
    /// After a self-promotion, suppress re-promoting the same bucket for
    /// this long. Bounds churn if a link is genuinely flapping.
    pub cooldown: Duration,
    /// Poll cadence. The monitor checks health every `tick`.
    pub tick: Duration,
}

impl Default for FailoverConfig {
    fn default() -> Self {
        Self {
            my_region: "default".into(),
            grace: Duration::from_secs(60),
            cooldown: Duration::from_secs(300),
            tick: Duration::from_secs(5),
        }
    }
}

/// Sink for performing a promotion — writing the bucket's seed doc with
/// a bumped epoch and this node's region as the new home. Abstracted as
/// a trait so the monitor doesn't need the full `TextIndex` write path
/// wired through; the server supplies an impl that goes through the
/// normal (WAL-durable, replicated) upsert.
#[async_trait::async_trait]
pub trait SeedWriter: Send + Sync + 'static {
    /// Read the current `home_epoch` for `bucket` from the local seed
    /// doc (0 if absent).
    fn current_epoch(&self, bucket: &str) -> u64;
    /// Write the seed doc: set `home_region = my_region`, `home_epoch =
    /// new_epoch`. Durable + replicated. Returns an error string on
    /// failure so the monitor can log and retry next tick.
    async fn promote(&self, bucket: &str, my_region: &str, new_epoch: u64) -> Result<(), String>;
}

/// Per-bucket runtime state the monitor tracks across ticks.
#[derive(Debug, Default, Clone)]
struct BucketWatch {
    /// When the home region was first observed unreachable in the
    /// current unhealthy streak. `None` while healthy.
    unhealthy_since: Option<tokio::time::Instant>,
    /// When we last self-promoted this bucket. `None` if never.
    last_promoted: Option<tokio::time::Instant>,
}

/// Watches remote home regions and self-promotes failover-candidate
/// buckets when their home is down past the grace period.
///
/// Anti-oscillation is enforced by three independent mechanisms
/// (design 0011 §4.3): opt-in single-candidate (only nodes given
/// candidates run this at all), grace + hysteresis (continuous
/// unreachability required, reset on any reconnect), monotonic epochs
/// with no auto-failback, and a post-promotion cooldown.
pub struct RegionFailoverMonitor {
    config: FailoverConfig,
    candidates: Vec<FailoverCandidate>,
    health: Arc<dyn RegionHealthSource>,
    seed: Arc<dyn SeedWriter>,
}

impl RegionFailoverMonitor {
    pub fn new(
        config: FailoverConfig,
        candidates: Vec<FailoverCandidate>,
        health: Arc<dyn RegionHealthSource>,
        seed: Arc<dyn SeedWriter>,
    ) -> Self {
        Self {
            config,
            candidates,
            health,
            seed,
        }
    }

    /// Spawn the monitor loop. Returns the join handle; abort on
    /// shutdown. A no-op (returns immediately) when there are no
    /// candidates so a non-candidate node pays nothing.
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move { self.run().await })
    }

    async fn run(self) {
        if self.candidates.is_empty() {
            debug!("region failover monitor: no candidates, not running");
            return;
        }
        info!(
            region = %self.config.my_region,
            candidates = self.candidates.len(),
            grace_secs = self.config.grace.as_secs(),
            "region failover monitor started",
        );
        let mut watches: std::collections::HashMap<String, BucketWatch> =
            std::collections::HashMap::new();
        loop {
            let now = tokio::time::Instant::now();
            for cand in &self.candidates {
                self.evaluate(cand, now, watches.entry(cand.bucket.clone()).or_default())
                    .await;
            }
            tokio::time::sleep(self.config.tick).await;
        }
    }

    /// Evaluate one candidate bucket for one tick. Extracted so it is
    /// unit-testable without spawning the loop.
    async fn evaluate(&self, cand: &FailoverCandidate, now: tokio::time::Instant, w: &mut BucketWatch) {
        let healthy = self.health.is_region_healthy(&cand.home_region);
        if healthy {
            // Reconnected (or never down) — reset the unhealthy streak.
            // Hysteresis: a flap that recovers inside the grace window
            // costs us nothing.
            w.unhealthy_since = None;
            return;
        }

        // Region is unhealthy. Start (or continue) the streak clock.
        let since = *w.unhealthy_since.get_or_insert(now);
        if now.duration_since(since) < self.config.grace {
            // Still inside the grace window — wait for it to clear or
            // to age past grace.
            return;
        }

        // Past grace. Respect the post-promotion cooldown so a flapping
        // link doesn't make us bump the epoch every tick.
        if let Some(last) = w.last_promoted {
            if now.duration_since(last) < self.config.cooldown {
                return;
            }
        }

        // Self-promote: bump the epoch and take the home. Epochs are
        // monotonic, so even a concurrent promotion by another region is
        // resolved by the epoch fence downstream.
        let new_epoch = self.seed.current_epoch(&cand.bucket).saturating_add(1);
        match self
            .seed
            .promote(&cand.bucket, &self.config.my_region, new_epoch)
            .await
        {
            Ok(()) => {
                warn!(
                    bucket = %cand.bucket,
                    old_home = %cand.home_region,
                    new_home = %self.config.my_region,
                    new_epoch,
                    "AUTO-FAILOVER: promoted bucket to local region (design 0011 §4)",
                );
                w.last_promoted = Some(now);
                // Reset the streak so we don't immediately re-evaluate
                // as still-unhealthy on the next tick.
                w.unhealthy_since = None;
            }
            Err(e) => {
                warn!(
                    bucket = %cand.bucket,
                    error = %e,
                    "AUTO-FAILOVER: promotion write failed; will retry next tick",
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nebula_embed::{Embedder, MockEmbedder};
    use nebula_vector::{HnswConfig, Metric};

    fn idx() -> Arc<TextIndex> {
        let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(8));
        Arc::new(TextIndex::new(emb, Metric::Cosine, HnswConfig::default()).unwrap())
    }

    #[tokio::test]
    async fn epoch_check_passes_when_local_has_no_home() {
        let index = idx();
        assert!(epoch_is_fresh(&index, "any", 1));
        assert!(epoch_is_fresh(&index, "any", 0));
    }

    // Multi-thread runtime: TextIndex::upsert_text uses
    // tokio::task::block_in_place (see crates/nebula-index/src/lib.rs:410)
    // which panics on the default single-threaded test runtime.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn epoch_check_rejects_older_incoming() {
        let index = idx();
        index
            .upsert_text(
                "b",
                "__nebuladb_operator_seed__",
                "seed",
                serde_json::json!({"home_region": "us-east-1", "home_epoch": 5u64}),
            )
            .await
            .unwrap();
        assert!(epoch_is_fresh(&index, "b", 5));
        assert!(epoch_is_fresh(&index, "b", 6));
        assert!(!epoch_is_fresh(&index, "b", 4));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn owned_buckets_reflect_local_home_region() {
        let index = idx();
        // Two buckets: one homed here, one elsewhere.
        index
            .upsert_text(
                "mine",
                "__nebuladb_operator_seed__",
                "s",
                serde_json::json!({"home_region": "us-east-1", "home_epoch": 1u64}),
            )
            .await
            .unwrap();
        index
            .upsert_text(
                "theirs",
                "__nebuladb_operator_seed__",
                "s",
                serde_json::json!({"home_region": "us-west-2", "home_epoch": 1u64}),
            )
            .await
            .unwrap();
        let owned = IndexOwnedBuckets {
            index: index.clone(),
            my_region: "us-east-1".into(),
            seed_doc_id: "__nebuladb_operator_seed__",
        };
        let snap = owned.snapshot();
        assert_eq!(snap.len(), 1);
        assert!(snap.contains("mine"));
    }

    // ---- failover monitor (design 0011 §4) ----

    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    /// Health source whose answer flips via an atomic, so a test can
    /// simulate a region going down and coming back.
    struct FlagHealth {
        healthy: AtomicBool,
    }
    impl RegionHealthSource for FlagHealth {
        fn is_region_healthy(&self, _region: &str) -> bool {
            self.healthy.load(Ordering::SeqCst)
        }
    }

    /// Seed writer that records promotions in an atomic counter instead
    /// of touching an index, so `evaluate` is testable in isolation.
    struct CountingSeed {
        epoch: AtomicU64,
        promotions: AtomicU64,
        fail: AtomicBool,
    }
    #[async_trait::async_trait]
    impl SeedWriter for CountingSeed {
        fn current_epoch(&self, _bucket: &str) -> u64 {
            self.epoch.load(Ordering::SeqCst)
        }
        async fn promote(&self, _bucket: &str, _region: &str, new_epoch: u64) -> Result<(), String> {
            if self.fail.load(Ordering::SeqCst) {
                return Err("simulated write failure".into());
            }
            self.epoch.store(new_epoch, Ordering::SeqCst);
            self.promotions.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn monitor_with(
        healthy: bool,
        fail_promote: bool,
        grace: Duration,
        cooldown: Duration,
    ) -> (RegionFailoverMonitor, Arc<CountingSeed>) {
        let health = Arc::new(FlagHealth {
            healthy: AtomicBool::new(healthy),
        });
        let seed = Arc::new(CountingSeed {
            epoch: AtomicU64::new(3),
            promotions: AtomicU64::new(0),
            fail: AtomicBool::new(fail_promote),
        });
        let config = FailoverConfig {
            my_region: "us-west-2".into(),
            grace,
            cooldown,
            tick: Duration::from_millis(1),
        };
        let candidates = vec![FailoverCandidate {
            bucket: "catalog".into(),
            home_region: "us-east-1".into(),
        }];
        let monitor = RegionFailoverMonitor::new(
            config,
            candidates,
            health.clone(),
            seed.clone(),
        );
        (monitor, seed)
    }

    #[tokio::test]
    async fn healthy_region_never_promotes() {
        let (m, seed) = monitor_with(true, false, Duration::ZERO, Duration::ZERO);
        let cand = m.candidates[0].clone();
        let mut w = BucketWatch::default();
        let now = tokio::time::Instant::now();
        m.evaluate(&cand, now, &mut w).await;
        assert_eq!(seed.promotions.load(Ordering::SeqCst), 0);
        assert!(w.unhealthy_since.is_none());
    }

    #[tokio::test]
    async fn unhealthy_within_grace_waits() {
        // Grace is 10s; a single tick right after the region drops must
        // NOT promote — hysteresis.
        let (m, seed) = monitor_with(false, false, Duration::from_secs(10), Duration::ZERO);
        let cand = m.candidates[0].clone();
        let mut w = BucketWatch::default();
        let now = tokio::time::Instant::now();
        m.evaluate(&cand, now, &mut w).await;
        assert_eq!(seed.promotions.load(Ordering::SeqCst), 0);
        assert!(w.unhealthy_since.is_some(), "streak clock should have started");
    }

    #[tokio::test]
    async fn reconnect_resets_grace_streak() {
        // A watch that already has a streak in progress, evaluated by a
        // now-healthy monitor, must have its streak cleared — this is
        // the hysteresis that prevents a flap from ever promoting.
        let (m, seed) = monitor_with(true, false, Duration::from_secs(10), Duration::ZERO);
        let now = tokio::time::Instant::now();
        let mut w = BucketWatch {
            unhealthy_since: Some(now - Duration::from_secs(5)),
            last_promoted: None,
        };
        m.evaluate(&m.candidates[0].clone(), now, &mut w).await;
        assert!(
            w.unhealthy_since.is_none(),
            "a healthy observation must clear the streak"
        );
        assert_eq!(seed.promotions.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn past_grace_promotes_and_bumps_epoch() {
        let (m, seed) = monitor_with(false, false, Duration::from_secs(10), Duration::from_secs(60));
        let cand = m.candidates[0].clone();
        // Pretend the region has been down since 20s ago (past 10s grace).
        let now = tokio::time::Instant::now();
        let mut w = BucketWatch {
            unhealthy_since: Some(now - Duration::from_secs(20)),
            last_promoted: None,
        };
        m.evaluate(&cand, now, &mut w).await;
        assert_eq!(seed.promotions.load(Ordering::SeqCst), 1);
        // Epoch bumped from 3 -> 4 (monotonic).
        assert_eq!(seed.epoch.load(Ordering::SeqCst), 4);
        assert!(w.last_promoted.is_some());
    }

    #[tokio::test]
    async fn cooldown_suppresses_reprovote() {
        let (m, seed) = monitor_with(false, false, Duration::from_secs(1), Duration::from_secs(300));
        let cand = m.candidates[0].clone();
        let now = tokio::time::Instant::now();
        // Just promoted 5s ago; cooldown is 300s, region still down past grace.
        let mut w = BucketWatch {
            unhealthy_since: Some(now - Duration::from_secs(10)),
            last_promoted: Some(now - Duration::from_secs(5)),
        };
        m.evaluate(&cand, now, &mut w).await;
        assert_eq!(
            seed.promotions.load(Ordering::SeqCst),
            0,
            "cooldown must suppress a second promotion"
        );
    }

    #[tokio::test]
    async fn failed_promotion_does_not_set_cooldown() {
        let (m, seed) = monitor_with(false, true, Duration::from_secs(1), Duration::from_secs(60));
        let cand = m.candidates[0].clone();
        let now = tokio::time::Instant::now();
        let mut w = BucketWatch {
            unhealthy_since: Some(now - Duration::from_secs(10)),
            last_promoted: None,
        };
        m.evaluate(&cand, now, &mut w).await;
        assert_eq!(seed.promotions.load(Ordering::SeqCst), 0);
        assert!(
            w.last_promoted.is_none(),
            "a failed write must not start the cooldown — we want to retry"
        );
    }
}
