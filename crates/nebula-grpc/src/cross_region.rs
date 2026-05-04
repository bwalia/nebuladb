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
    // Mirror of home_region::SEED_DOC_ID; see design-0001 for why the
    // seed doc is the source of truth.
    const SEED_DOC_ID: &str = "__nebuladb_operator_seed__";
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

    #[tokio::test]
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

    #[tokio::test]
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
}
