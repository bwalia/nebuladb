//! The CDC engine: subscribes to the WAL on behalf of a set of
//! [`CdcConsumer`]s, applies each consumer's filter, hands events
//! through. One spawned task per consumer.
//!
//! # Design constraints
//!
//! - **Don't block the WAL writer.** The engine subscribes via
//!   `Wal::subscribe`, which uses the broadcast channel. A slow
//!   consumer surfaces as `Lagged`; we reconnect from the consumer's
//!   last cursor instead of stalling the leader's append path.
//!
//! - **Per-consumer cursor.** The engine keeps the last
//!   delivered-cursor per consumer in memory. RFC 0005 §5.3 covers
//!   making this persistent; today, an engine restart re-tails from
//!   wherever the caller starts each consumer.
//!
//! - **Filter on the engine side, not the WAL side.** The WAL
//!   broadcasts every record to every subscriber; we filter inside
//!   the per-consumer task. This keeps the WAL hot path filter-free
//!   and lets each consumer have an independent filter without
//!   coordination.
//!
//! # Failure handling
//!
//! Three error classes:
//!
//! 1. **`Lagged`** from the broadcast channel: the consumer is too
//!    slow and dropped messages. We log + reconnect from the last
//!    delivered cursor. `nebula-wal::SubscriberHub::BROADCAST_CAPACITY`
//!    bounds how far behind a consumer can fall before this fires.
//! 2. **`ConsumerError`** from `apply`: logged at `warn`, event is
//!    dropped, cursor advances. No retry today (RFC 0005 §5.5).
//! 3. **WAL gone**: subscription returns `None`. The task exits.

use std::sync::Arc;

use parking_lot::Mutex;
use tokio::task::JoinHandle;

use nebula_wal::{Wal, WalCursor, WalEntry};

use crate::consumer::CdcConsumer;
use crate::event::CdcEvent;
use crate::filter::CdcFilter;

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("engine: subscribe failed: {0}")]
    Subscribe(String),
}

/// Handle returned by [`CdcEngine::start_consumer`]. Dropping it
/// detaches the consumer's spawn handle without aborting it; call
/// [`EngineHandle::abort`] explicitly to stop a consumer.
pub struct EngineHandle {
    consumer_id: String,
    join: JoinHandle<()>,
    last_cursor: Arc<Mutex<WalCursor>>,
}

impl EngineHandle {
    pub fn consumer_id(&self) -> &str {
        &self.consumer_id
    }

    /// Latest cursor delivered to this consumer's `apply`. Useful to
    /// callers that want to checkpoint somewhere external (e.g. a
    /// Redis key) so a subsequent process can resume.
    pub fn last_delivered_cursor(&self) -> WalCursor {
        *self.last_cursor.lock()
    }

    /// Stop this consumer. Future events will not be delivered to it.
    /// Safe to call multiple times — the underlying `JoinHandle::abort`
    /// is idempotent.
    pub fn abort(&self) {
        self.join.abort();
    }

    /// Wait for the consumer task to exit. Useful in tests.
    pub async fn join(self) -> Result<(), tokio::task::JoinError> {
        self.join.await
    }
}

/// Engine that fans out one shared WAL into N consumers.
///
/// Cheap to clone — internally just an `Arc<Wal>`. Multiple engines
/// pointing at the same WAL is fine; they just open independent
/// subscriptions.
#[derive(Clone)]
pub struct CdcEngine {
    wal: Arc<Wal>,
}

impl CdcEngine {
    pub fn new(wal: Arc<Wal>) -> Self {
        Self { wal }
    }

    /// Spawn a task that drives one consumer. Returns a handle that
    /// can stop it and report the latest delivered cursor.
    ///
    /// `start` is the cursor to resume from. Pass [`WalCursor::BEGIN`]
    /// for "every record from the oldest surviving segment forward."
    /// Pass a previously-saved `last_delivered_cursor` to resume a
    /// restarted consumer.
    pub fn start_consumer<C>(
        &self,
        consumer: C,
        filter: CdcFilter,
        start: WalCursor,
    ) -> Result<EngineHandle, EngineError>
    where
        C: CdcConsumer,
    {
        let consumer = Arc::new(consumer);
        let consumer_id = consumer.id().to_string();
        let last_cursor = Arc::new(Mutex::new(start));
        let last_cursor_for_task = last_cursor.clone();
        let wal = self.wal.clone();

        let join = tokio::spawn(async move {
            run_consumer_loop(wal, consumer, filter, start, last_cursor_for_task).await;
        });

        Ok(EngineHandle {
            consumer_id,
            join,
            last_cursor,
        })
    }
}

/// The core loop. Pulled out as a free function so the spawned task
/// captures only the bits it needs (no `Arc<CdcEngine>` clone).
async fn run_consumer_loop<C: CdcConsumer + ?Sized>(
    wal: Arc<Wal>,
    consumer: Arc<C>,
    filter: CdcFilter,
    mut start: WalCursor,
    last_cursor: Arc<Mutex<WalCursor>>,
) {
    'reconnect: loop {
        let mut sub = match wal.subscribe(start) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(
                    consumer = consumer.id(),
                    error = %e,
                    "cdc: subscribe failed; consumer task exiting"
                );
                return;
            }
        };

        loop {
            match sub.next().await {
                None => {
                    tracing::info!(
                        consumer = consumer.id(),
                        "cdc: WAL closed; consumer task exiting"
                    );
                    return;
                }
                Some(Err(e)) => {
                    // Most common: `Lagged`. Reconnect from the last
                    // cursor we delivered. The `start` we pass back
                    // into `Wal::subscribe` is the cursor *of* the
                    // last delivered record, not past it — the
                    // catch-up phase will re-deliver that record
                    // (at-least-once semantics), and the consumer's
                    // `apply` is expected to be idempotent.
                    tracing::warn!(
                        consumer = consumer.id(),
                        error = %e,
                        "cdc: subscriber error; reconnecting"
                    );
                    start = *last_cursor.lock();
                    continue 'reconnect;
                }
                Some(Ok(entry)) => {
                    let WalEntry { next_cursor, .. } = entry.clone();
                    let event = CdcEvent::from_wal_entry(entry);
                    if !filter.matches(&event) {
                        // Skip but advance the cursor — a skipped
                        // event is "delivered as far as we care."
                        *last_cursor.lock() = next_cursor;
                        sub.ack(next_cursor);
                        continue;
                    }
                    if let Err(e) = consumer.apply(&event).await {
                        tracing::warn!(
                            consumer = consumer.id(),
                            cursor = ?event.cursor,
                            error = %e,
                            "cdc: consumer apply failed; dropping event"
                        );
                        // Still advance — see RFC 0005 §5.5 for the
                        // retry-policy follow-up. Today: drop and
                        // move on. A consumer that needs at-least-once
                        // can implement retry inside `apply`.
                    }
                    *last_cursor.lock() = next_cursor;
                    sub.ack(next_cursor);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::FileLogConsumer;
    use crate::event::CdcOp;
    use crate::filter::CdcFilter;
    use nebula_wal::{Wal, WalConfig, WalRecord};
    use std::time::Duration;

    fn upsert(bucket: &str, id: &str) -> WalRecord {
        WalRecord::UpsertText {
            bucket: bucket.into(),
            external_id: id.into(),
            text: format!("text for {id}"),
            vector: vec![1.0, 2.0, 3.0],
            metadata_json: "null".into(),
        }
    }

    async fn read_lines(path: &std::path::Path) -> Vec<serde_json::Value> {
        let body = tokio::fs::read_to_string(path).await.unwrap_or_default();
        body.lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    /// Wait until `pred()` returns true, polling every 25ms, up to 2s.
    /// Fail the test loudly if not. This is the standard pattern for
    /// "the spawned consumer task should have processed the event by
    /// now" — the tail is async so we can't synchronously assert.
    async fn wait_until<F: FnMut() -> bool>(mut pred: F) {
        let start = std::time::Instant::now();
        while !pred() {
            if start.elapsed() > Duration::from_secs(2) {
                panic!("wait_until timed out");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    #[tokio::test]
    async fn engine_routes_event_to_consumer() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let log_path = dir.path().join("events.jsonl");
        let wal = Arc::new(Wal::open(&wal_dir, WalConfig::default()).unwrap());

        let engine = CdcEngine::new(wal.clone());
        let consumer = FileLogConsumer::new("logger", &log_path);
        let handle = engine
            .start_consumer(consumer, CdcFilter::any(), WalCursor::BEGIN)
            .unwrap();

        // Append a record after the consumer is spawned.
        wal.append(&upsert("docs", "d1")).unwrap();
        wal.append(&upsert("docs", "d2")).unwrap();

        wait_until(|| {
            std::fs::read_to_string(&log_path)
                .map(|s| s.lines().count() >= 2)
                .unwrap_or(false)
        })
        .await;

        let lines = read_lines(&log_path).await;
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["key"]["id"], "d1");
        assert_eq!(lines[1]["key"]["id"], "d2");

        handle.abort();
    }

    #[tokio::test]
    async fn engine_filter_drops_non_matching_events() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let log_path = dir.path().join("events.jsonl");
        let wal = Arc::new(Wal::open(&wal_dir, WalConfig::default()).unwrap());

        let engine = CdcEngine::new(wal.clone());
        let consumer = FileLogConsumer::new("only-docs-bucket", &log_path);
        let filter = CdcFilter::bucket("docs");
        let handle = engine
            .start_consumer(consumer, filter, WalCursor::BEGIN)
            .unwrap();

        wal.append(&upsert("docs", "yes")).unwrap();
        wal.append(&upsert("logs", "no")).unwrap(); // wrong bucket
        wal.append(&upsert("docs", "yes2")).unwrap();

        wait_until(|| {
            std::fs::read_to_string(&log_path)
                .map(|s| s.lines().count() >= 2)
                .unwrap_or(false)
        })
        .await;

        // Give the engine a beat to surface the filtered event (it
        // would have shown up by now if the filter were broken).
        tokio::time::sleep(Duration::from_millis(100)).await;

        let lines = read_lines(&log_path).await;
        assert_eq!(lines.len(), 2, "filter should drop 'logs' event");
        for l in &lines {
            assert_eq!(l["key"]["bucket"], "docs");
        }
        handle.abort();
    }

    #[tokio::test]
    async fn engine_consumer_handle_reports_last_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let log_path = dir.path().join("events.jsonl");
        let wal = Arc::new(Wal::open(&wal_dir, WalConfig::default()).unwrap());

        let engine = CdcEngine::new(wal.clone());
        let consumer = FileLogConsumer::new("logger", &log_path);
        let handle = engine
            .start_consumer(consumer, CdcFilter::any(), WalCursor::BEGIN)
            .unwrap();

        // Initially BEGIN; advances after the first delivery.
        assert_eq!(handle.last_delivered_cursor(), WalCursor::BEGIN);

        wal.append(&upsert("docs", "d1")).unwrap();
        wait_until(|| {
            std::fs::read_to_string(&log_path)
                .map(|s| s.lines().count() >= 1)
                .unwrap_or(false)
        })
        .await;

        let cur = handle.last_delivered_cursor();
        assert!(cur > WalCursor::BEGIN, "cursor should advance: {cur:?}");
        handle.abort();
    }

    #[tokio::test]
    async fn engine_two_consumers_run_independently() {
        // The whole point of CDC is "many subscribers, one log."
        // Confirm two engines with different filters end up with
        // different output sets.
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let log_a = dir.path().join("a.jsonl");
        let log_b = dir.path().join("b.jsonl");
        let wal = Arc::new(Wal::open(&wal_dir, WalConfig::default()).unwrap());
        let engine = CdcEngine::new(wal.clone());

        let h_a = engine
            .start_consumer(
                FileLogConsumer::new("a", &log_a),
                CdcFilter::bucket("docs"),
                WalCursor::BEGIN,
            )
            .unwrap();
        let h_b = engine
            .start_consumer(
                FileLogConsumer::new("b", &log_b),
                CdcFilter::ops([CdcOp::Delete]),
                WalCursor::BEGIN,
            )
            .unwrap();

        wal.append(&upsert("docs", "d1")).unwrap();
        wal.append(&WalRecord::Delete {
            bucket: "logs".into(),
            external_id: "x".into(),
        })
        .unwrap();
        wal.append(&upsert("logs", "skip-a")).unwrap();

        wait_until(|| {
            let a_count = std::fs::read_to_string(&log_a)
                .map(|s| s.lines().count())
                .unwrap_or(0);
            let b_count = std::fs::read_to_string(&log_b)
                .map(|s| s.lines().count())
                .unwrap_or(0);
            a_count >= 1 && b_count >= 1
        })
        .await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        let a = read_lines(&log_a).await;
        let b = read_lines(&log_b).await;
        assert_eq!(a.len(), 1, "consumer A: only docs/d1");
        assert_eq!(a[0]["key"]["bucket"], "docs");
        assert_eq!(b.len(), 1, "consumer B: only the delete");
        assert_eq!(b[0]["op"], "delete");

        h_a.abort();
        h_b.abort();
    }

    #[tokio::test]
    async fn engine_replays_history_for_late_consumer() {
        // Append, *then* spawn the consumer with cursor=BEGIN. The
        // catch-up phase of `Wal::subscribe` should deliver the
        // history before live tail kicks in.
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let log_path = dir.path().join("events.jsonl");
        let wal = Arc::new(Wal::open(&wal_dir, WalConfig::default()).unwrap());

        wal.append(&upsert("docs", "history-1")).unwrap();
        wal.append(&upsert("docs", "history-2")).unwrap();

        let engine = CdcEngine::new(wal.clone());
        let h = engine
            .start_consumer(
                FileLogConsumer::new("late", &log_path),
                CdcFilter::any(),
                WalCursor::BEGIN,
            )
            .unwrap();

        wait_until(|| {
            std::fs::read_to_string(&log_path)
                .map(|s| s.lines().count() >= 2)
                .unwrap_or(false)
        })
        .await;

        let lines = read_lines(&log_path).await;
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["key"]["id"], "history-1");
        assert_eq!(lines[1]["key"]["id"], "history-2");
        h.abort();
    }
}
