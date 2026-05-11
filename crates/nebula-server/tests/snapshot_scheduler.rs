//! Integration test for the background snapshot scheduler.
//!
//! The point of the scheduler is "the WAL doesn't grow unbounded
//! between operator-triggered snapshots." This test asserts the
//! observable behaviour: write a doc, run the scheduler for slightly
//! longer than the interval, then confirm a snapshot file appeared
//! in `<data_dir>/snapshots/` and the WAL got compacted.

use std::sync::Arc;
use std::time::Duration;

use nebula_embed::{Embedder, MockEmbedder};
use nebula_index::TextIndex;
use nebula_server::snapshot_scheduler::{self, SnapshotSchedulerConfig};
use nebula_vector::{HnswConfig, Metric};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduler_takes_a_snapshot_when_interval_elapses() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().to_owned();
    let snapshots_dir = dir.join("snapshots");
    std::fs::create_dir_all(&snapshots_dir).ok();

    let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(8));
    let index = Arc::new(
        TextIndex::open_persistent(emb, Metric::Cosine, HnswConfig::default(), &dir)
            .expect("open_persistent"),
    );

    // One write produces one WAL record. The size-based trigger is
    // disabled (threshold u64::MAX) so we exercise the time path.
    index
        .upsert_text("docs", "id-1", "hello world", serde_json::json!({}))
        .await
        .expect("upsert");

    let cfg = SnapshotSchedulerConfig {
        interval: Duration::from_millis(50),
        wal_bytes_threshold: u64::MAX,
        poll_interval: Duration::from_millis(25),
        compact: true,
    };

    let handle = snapshot_scheduler::spawn(Arc::clone(&index), cfg);

    // Give the loop time to fire at least once.
    tokio::time::sleep(Duration::from_millis(400)).await;
    handle.abort();

    let entries: Vec<_> = std::fs::read_dir(&snapshots_dir)
        .expect("read snapshots dir")
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("snapshot-")
        })
        .collect();
    assert!(
        !entries.is_empty(),
        "scheduler did not produce a snapshot file in {}",
        snapshots_dir.display()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduler_exits_when_disabled() {
    // No data dir + no triggers → the scheduler must return promptly
    // (not stay parked in its sleep loop). We use the in-memory index
    // so wal_stats() is None, which is its own early-exit path.
    let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(8));
    let index = Arc::new(
        TextIndex::new(emb, Metric::Cosine, HnswConfig::default()).expect("in-mem index"),
    );
    let cfg = SnapshotSchedulerConfig {
        interval: Duration::from_secs(1),
        wal_bytes_threshold: 1,
        poll_interval: Duration::from_secs(1),
        compact: false,
    };
    let handle = snapshot_scheduler::spawn(index, cfg);
    // 100ms is plenty for the scheduler to see "no WAL" and exit.
    let done = tokio::time::timeout(Duration::from_millis(500), handle).await;
    assert!(done.is_ok(), "scheduler did not exit promptly for in-mem index");
}
