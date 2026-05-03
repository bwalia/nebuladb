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

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tonic::transport::Channel;
use tonic::Streaming;
use tracing::{debug, info, warn};

use nebula_index::TextIndex;
use nebula_wal::{WalCursor, WalRecord};

use crate::pb;

/// Where the follower remembers its last-applied cursor so a
/// restart resumes instead of replaying the whole WAL from BEGIN.
///
/// Two implementations ship: [`FileCursorStore`] for real use and
/// [`MemoryCursorStore`] for tests. Both are `Send + Sync + 'static`
/// so the follower task can own an `Arc<dyn CursorStore>`.
///
/// The trait is intentionally tiny — a pluggable store shouldn't
/// need to know about gRPC, the index, or the record type. Callers
/// hand it an opaque cursor and get one back.
pub trait CursorStore: Send + Sync + 'static {
    /// Load the last-saved cursor, or `None` if nothing has been
    /// saved yet (fresh follower, first boot).
    fn load(&self) -> Result<Option<WalCursor>, CursorStoreError>;

    /// Atomically persist the given cursor. Called after every
    /// applied record in the hot path — implementations should
    /// avoid doing work proportional to the number of records
    /// already seen.
    fn save(&self, cursor: WalCursor) -> Result<(), CursorStoreError>;
}

#[derive(Debug, thiserror::Error)]
pub enum CursorStoreError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("codec: {0}")]
    Codec(String),
}

/// File-backed cursor store. Writes go to a sibling `.tmp` file,
/// `fsync`, then rename over the target — the classic atomic
/// write. A mid-crash leaves either the old cursor (rename didn't
/// happen) or the new one (rename did), never a half-written mess.
///
/// The file holds JSON: two integers. Keeping it human-readable
/// makes operator recovery trivial — a stuck follower can be
/// reset by editing or removing the file.
pub struct FileCursorStore {
    path: PathBuf,
}

impl FileCursorStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedCursor {
    segment_seq: u64,
    byte_offset: u64,
}

impl CursorStore for FileCursorStore {
    fn load(&self) -> Result<Option<WalCursor>, CursorStoreError> {
        let bytes = match std::fs::read(&self.path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let p: PersistedCursor = serde_json::from_slice(&bytes)
            .map_err(|e| CursorStoreError::Codec(e.to_string()))?;
        Ok(Some(WalCursor {
            segment_seq: p.segment_seq,
            byte_offset: p.byte_offset,
        }))
    }

    fn save(&self, cursor: WalCursor) -> Result<(), CursorStoreError> {
        // Parent may not exist on first save (operator pointed us
        // at a fresh path). Create it lazily rather than at init
        // so a readonly-by-accident config fails at first save, not
        // at boot.
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = self.path.with_extension("tmp");
        let body = serde_json::to_vec(&PersistedCursor {
            segment_seq: cursor.segment_seq,
            byte_offset: cursor.byte_offset,
        })
        .map_err(|e| CursorStoreError::Codec(e.to_string()))?;
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&tmp)?;
            f.write_all(&body)?;
            f.sync_data()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

/// In-memory cursor store — tests and cases where the follower is
/// throwaway. Implements `Clone` so tests can inspect what got
/// saved after the follower task finishes.
#[derive(Default, Clone)]
pub struct MemoryCursorStore {
    inner: Arc<Mutex<Option<WalCursor>>>,
}

impl MemoryCursorStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Test helper: inspect the current saved cursor without
    /// interacting through the trait.
    pub fn peek(&self) -> Option<WalCursor> {
        *self.inner.lock()
    }
}

impl CursorStore for MemoryCursorStore {
    fn load(&self) -> Result<Option<WalCursor>, CursorStoreError> {
        Ok(*self.inner.lock())
    }

    fn save(&self, cursor: WalCursor) -> Result<(), CursorStoreError> {
        *self.inner.lock() = Some(cursor);
        Ok(())
    }
}

/// Convenience: pick the sensible store given a data directory.
/// Returns a [`FileCursorStore`] under `dir/follower.cursor`.
pub fn file_store_in(dir: impl AsRef<Path>) -> FileCursorStore {
    FileCursorStore::new(dir.as_ref().join("follower.cursor"))
}

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
/// If `store` is `Some`, the cursor is persisted after each applied
/// record. That's safe to call in the hot path — the default
/// [`FileCursorStore`] does one small atomic write per save.
///
/// This is the building block [`run`] uses in a loop. Tests call it
/// directly to inspect one subscription's result.
pub async fn run_once(
    channel: Channel,
    index: Arc<TextIndex>,
    start: WalCursor,
) -> Result<WalCursor, FollowerError> {
    run_once_with_store(channel, index, start, None).await
}

/// Variant of [`run_once`] that persists the cursor after every
/// applied record. Used by [`spawn_with_store`] and by tests that
/// want to assert on store contents.
pub async fn run_once_with_store(
    channel: Channel,
    index: Arc<TextIndex>,
    start: WalCursor,
    store: Option<Arc<dyn CursorStore>>,
) -> Result<WalCursor, FollowerError> {
    let mut client = pb::replication_service_client::ReplicationServiceClient::new(channel);
    let req = pb::TailWalRequest {
        start: Some(pb::WalCursor {
            segment_seq: start.segment_seq,
            byte_offset: start.byte_offset,
        }),
    };
    let stream = client.tail_wal(req).await?.into_inner();
    drain_stream(stream, index, start, store).await
}

async fn drain_stream(
    mut stream: Streaming<pb::WalTailEntry>,
    index: Arc<TextIndex>,
    initial: WalCursor,
    store: Option<Arc<dyn CursorStore>>,
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
                // Persist AFTER apply — if save fails we still
                // have the record applied locally, and we'll
                // redeliver on restart from whatever the last
                // successful save pointed at. That's at-least-once,
                // which is what we advertise. Saving BEFORE apply
                // would invert that and risk *losing* a record.
                if let Some(s) = store.as_ref() {
                    if let Err(e) = s.save(applied_next) {
                        // Non-fatal: log and keep going. A persistent
                        // store failure is an operator problem (disk
                        // full, wrong permissions) — the stream
                        // should keep flowing and we'll try to save
                        // again next record. Cursor-save errors
                        // never stall replication.
                        warn!(error = %e, "follower cursor save failed");
                    }
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
/// with exponential backoff. Cursor progress is held only in
/// memory — on crash, the task restarts from `initial_cursor`.
/// Use [`spawn_with_store`] to persist progress across restarts.
pub fn spawn(
    channel: Channel,
    index: Arc<TextIndex>,
    initial_cursor: WalCursor,
) -> FollowerHandle {
    spawn_with_store(channel, index, initial_cursor, None)
}

/// Like [`spawn`] but persists the cursor after every record
/// through the supplied [`CursorStore`]. On start, if the store
/// already has a cursor, that one is used instead of
/// `initial_cursor` — so a restarting follower resumes cleanly.
pub fn spawn_with_store(
    channel: Channel,
    index: Arc<TextIndex>,
    initial_cursor: WalCursor,
    store: Option<Arc<dyn CursorStore>>,
) -> FollowerHandle {
    let task = tokio::spawn(async move {
        // Prefer a persisted cursor over the caller's default.
        // If loading errors, log and fall back — the follower
        // will replay from `initial_cursor`, which is a safe
        // over-read, not a data-loss case.
        let mut cursor = match store.as_ref().and_then(|s| match s.load() {
            Ok(c) => c,
            Err(e) => {
                warn!(error = %e, "follower cursor load failed; using initial");
                None
            }
        }) {
            Some(c) => {
                info!(
                    seg = c.segment_seq,
                    off = c.byte_offset,
                    "follower resuming from persisted cursor"
                );
                c
            }
            None => initial_cursor,
        };
        let mut backoff = Duration::from_millis(100);
        loop {
            match run_once_with_store(
                channel.clone(),
                index.clone(),
                cursor,
                store.clone(),
            )
            .await
            {
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
