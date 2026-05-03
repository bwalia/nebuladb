//! Follower-side replication client.
//!
//! A follower runs [`run`] (or [`run_once`] for a single pass) in
//! the background. It opens a gRPC stream to the leader's
//! [`ReplicationService::TailWal`], decodes each [`WalTailEntry`],
//! and applies the underlying `WalRecord` into a local
//! [`TextIndex`] via the crate's public `apply_wal_record` path.
//!
//! The follower's local index typically has its own WAL on disk
//! (for crash recovery), but [`apply_wal_record`] deliberately
//! doesn't re-append to it during this flow — the leader is the
//! single source of truth for what has been durably accepted.
//! Followers that persist replicated state ought to do so through a
//! separate commit-path so a split-brain recovery can tell "my
//! local writes" from "writes I mirrored from the leader". That's
//! a future session; for now replication is RAM-only.
//!
//! # Resume semantics
//!
//! At-least-once. The follower tracks the last `next_cursor` it
//! successfully applied; on reconnect it hands that cursor back in
//! the subscribe request. A record may be applied more than once
//! across reconnects (the WAL doesn't dedupe), and `apply_wal_record`
//! is idempotent under the single-writer assumption because upserts
//! are keyed by `(bucket, external_id)` and deletes ignore "not
//! found".

use std::sync::Arc;
use std::time::Duration;

use tonic::transport::Channel;
use tonic::Streaming;
use tracing::{debug, info, warn};

use nebula_index::TextIndex;
use nebula_wal::{WalCursor, WalRecord};

use crate::pb;

/// Error cases the follower surfaces to its caller. Every variant
/// is recoverable with a reconnect (possibly from BEGIN) except
/// `Apply`, which indicates the follower's local state has drifted
/// from what the leader sent — at that point a fresh snapshot is
/// the only safe recovery.
#[derive(Debug, thiserror::Error)]
pub enum FollowerError {
    #[error("transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("rpc: {0}")]
    Rpc(#[from] tonic::Status),
    #[error("decode record: {0}")]
    Decode(String),
    #[error("apply: {0}")]
    Apply(String),
}

/// Handle returned by [`spawn`]. Dropping it doesn't cancel — call
/// [`FollowerHandle::abort`] explicitly. The inner task loops
/// reconnect-on-error by default; [`run_once`] is the non-looping
/// variant used by tests.
pub struct FollowerHandle {
    task: tokio::task::JoinHandle<()>,
}

impl FollowerHandle {
    pub fn abort(self) {
        self.task.abort();
    }

    pub async fn join(self) -> Result<(), tokio::task::JoinError> {
        self.task.await
    }
}

/// One pass of tailing: subscribe at `start`, apply records until
/// the stream ends (leader shutdown) or the first error. Returns
/// the cursor past the last successfully-applied record so a
/// reconnect can resume at the right place.
///
/// This is the building block [`run`] uses in a loop. Tests call it
/// directly to inspect one subscription's result.
pub async fn run_once(
    channel: Channel,
    index: Arc<TextIndex>,
    start: WalCursor,
) -> Result<WalCursor, FollowerError> {
    let mut client = pb::replication_service_client::ReplicationServiceClient::new(channel);
    let req = pb::TailWalRequest {
        start: Some(pb::WalCursor {
            segment_seq: start.segment_seq,
            byte_offset: start.byte_offset,
        }),
    };
    let stream = client.tail_wal(req).await?.into_inner();
    drain_stream(stream, index, start).await
}

async fn drain_stream(
    mut stream: Streaming<pb::WalTailEntry>,
    index: Arc<TextIndex>,
    initial: WalCursor,
) -> Result<WalCursor, FollowerError> {
    let mut applied_next = initial;
    loop {
        match stream.message().await {
            Ok(Some(entry)) => {
                let rec: WalRecord = bincode::deserialize(&entry.record_bincode)
                    .map_err(|e| FollowerError::Decode(e.to_string()))?;
                index
                    .apply_wal_record(&rec)
                    .map_err(|e| FollowerError::Apply(e.to_string()))?;
                if let Some(next) = entry.next_cursor {
                    applied_next = WalCursor {
                        segment_seq: next.segment_seq,
                        byte_offset: next.byte_offset,
                    };
                }
                debug!(
                    seg = applied_next.segment_seq,
                    off = applied_next.byte_offset,
                    "follower applied record"
                );
            }
            Ok(None) => {
                // Leader closed the stream cleanly. Return so the
                // caller can decide whether to reconnect.
                return Ok(applied_next);
            }
            Err(status) => {
                return Err(FollowerError::Rpc(status));
            }
        }
    }
}

/// Long-running follower: re-subscribes on stream end or error
/// with exponential backoff. Persists the latest cursor in-memory
/// only — a crashing follower loses progress and restarts from
/// `initial_cursor`. A production path would persist the cursor to
/// disk after each apply.
pub fn spawn(
    channel: Channel,
    index: Arc<TextIndex>,
    initial_cursor: WalCursor,
) -> FollowerHandle {
    let task = tokio::spawn(async move {
        let mut cursor = initial_cursor;
        let mut backoff = Duration::from_millis(100);
        loop {
            match run_once(channel.clone(), index.clone(), cursor).await {
                Ok(last) => {
                    info!(
                        seg = last.segment_seq,
                        off = last.byte_offset,
                        "follower stream ended cleanly; reconnecting"
                    );
                    cursor = last;
                    backoff = Duration::from_millis(100);
                }
                Err(e) => {
                    warn!(error = %e, "follower stream error; backing off");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
            }
        }
    });
    FollowerHandle { task }
}
