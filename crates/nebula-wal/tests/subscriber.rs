//! Integration tests for the WAL subscriber API.
//!
//! These exercise the three flows a follower will actually take:
//!
//! 1. **Full catch-up then live tail** — subscribe at BEGIN, drain
//!    history, receive subsequent appends.
//! 2. **Resume from cursor** — subscribe with the last-ack cursor
//!    after a restart; deliver only records after that point.
//! 3. **Lag handling** — a stalled subscriber that misses broadcast
//!    entries gets a `Lagged` error it can use to reconnect.
//!
//! Also verified: compact() will not delete a segment the live
//! subscriber still needs (ack-bounded compaction), and the
//! subscriber survives rotation mid-stream.

use std::sync::Arc;

use nebula_wal::{Wal, WalConfig, WalCursor, WalRecord};
use tempfile::tempdir;

fn mk(i: u32) -> WalRecord {
    WalRecord::UpsertText {
        bucket: "b".into(),
        external_id: format!("id-{i}"),
        text: format!("t{i}"),
        vector: vec![0.1; 4],
        metadata_json: "{}".into(),
    }
}

fn id_of(r: &WalRecord) -> String {
    match r {
        WalRecord::UpsertText { external_id, .. } => external_id.clone(),
        _ => panic!("unexpected variant in test"),
    }
}

/// The canonical follower flow: subscribe at BEGIN, receive all
/// history, then continue getting live appends until the writer
/// stops.
#[tokio::test]
async fn catchup_then_live_tail_sees_every_record() {
    let dir = tempdir().unwrap();
    let wal = Arc::new(Wal::open(dir.path(), WalConfig::default()).unwrap());

    // Pre-populate — these become the "historical" catch-up set.
    for i in 0..5 {
        wal.append(&mk(i)).unwrap();
    }

    let mut sub = wal.subscribe(WalCursor::BEGIN).unwrap();

    // Live appends, racing with the subscriber reading.
    let wal2 = wal.clone();
    let live_task = tokio::spawn(async move {
        for i in 5..10 {
            wal2.append(&mk(i)).unwrap();
        }
    });

    let mut seen = Vec::new();
    while seen.len() < 10 {
        let entry = sub.next().await.unwrap().unwrap();
        seen.push(id_of(&entry.record));
        sub.ack(entry.next_cursor);
    }
    live_task.await.unwrap();

    let expected: Vec<String> = (0..10).map(|i| format!("id-{i}")).collect();
    assert_eq!(seen, expected, "should see every record in order");
}

/// A follower that crashed and restarts hands its last-ack cursor
/// back in — the WAL must deliver exactly the records it hadn't
/// seen yet.
#[tokio::test]
async fn resume_from_cursor_skips_already_acked_records() {
    let dir = tempdir().unwrap();
    let wal = Wal::open(dir.path(), WalConfig::default()).unwrap();

    for i in 0..5 {
        wal.append(&mk(i)).unwrap();
    }

    // Consume the first 3 and ack the cursor past them.
    let mut sub = wal.subscribe(WalCursor::BEGIN).unwrap();
    let mut last_next = WalCursor::BEGIN;
    for _ in 0..3 {
        let e = sub.next().await.unwrap().unwrap();
        last_next = e.next_cursor;
    }
    drop(sub); // simulate follower crash

    // Reconnect at the cursor we "persisted".
    let mut sub2 = wal.subscribe(last_next).unwrap();
    let mut resumed = Vec::new();
    for _ in 0..2 {
        let e = sub2.next().await.unwrap().unwrap();
        resumed.push(id_of(&e.record));
    }
    assert_eq!(resumed, vec!["id-3", "id-4"]);
}

/// The broadcast channel has a bounded capacity. A subscriber that
/// never calls `next()` eventually misses entries; it must get a
/// `Lagged` error so it can reconnect.
#[tokio::test]
async fn slow_subscriber_observes_lag_error() {
    let dir = tempdir().unwrap();
    let wal = Arc::new(Wal::open(dir.path(), WalConfig::default()).unwrap());

    let mut sub = wal.subscribe(WalCursor::BEGIN).unwrap();

    // Overflow the broadcast buffer (1024 capacity) by a large
    // margin while the subscriber sits idle. Every append publishes
    // one entry; with 2000 appends and zero consumers, earliest
    // messages are dropped.
    for i in 0..2000 {
        wal.append(&mk(i)).unwrap();
    }

    // First call should yield either historical data (which catch-up
    // captured before any lag) or eventually a Lagged error once
    // we've drained catch-up and hit the broadcast.
    //
    // Drain catch-up (which is a Vec so never lags) until we reach
    // the live tail, then expect Lagged.
    let mut lagged = false;
    for _ in 0..2100 {
        match sub.next().await {
            Some(Ok(_)) => {}
            Some(Err(_)) => {
                lagged = true;
                break;
            }
            None => break,
        }
    }
    assert!(lagged, "expected Lagged once broadcast overflowed");
}

/// compact() must not delete a segment the live subscriber still
/// depends on. Without this clamp, a slow follower would wake up to
/// find its resume cursor pointing at a deleted file.
#[tokio::test]
async fn compact_respects_subscriber_ack_bound() {
    let dir = tempdir().unwrap();
    // Tiny segments so a handful of records forces rotation.
    let wal = Arc::new(
        Wal::open(
            dir.path(),
            WalConfig {
                segment_size_bytes: 256,
                fsync_on_append: false,
            },
        )
        .unwrap(),
    );

    for i in 0..20 {
        wal.append(&mk(i)).unwrap();
    }
    wal.flush().unwrap();

    // Subscriber is parked at BEGIN — it hasn't acked anything past
    // the oldest segment, so the compact call below must no-op
    // (except for the "don't delete current" guard that was always
    // there).
    let sub = wal.subscribe(WalCursor::BEGIN).unwrap();

    let before = count_segments(dir.path());
    assert!(before >= 2, "rotation required for this test");
    let removed = wal.compact(u64::MAX).unwrap();
    assert_eq!(removed, 0, "subscriber at BEGIN pins every segment");

    // Drop the subscriber — compaction should proceed.
    drop(sub);
    let removed2 = wal.compact(u64::MAX).unwrap();
    assert!(removed2 > 0, "no subscribers → compact should make progress");
    // Current segment must always survive.
    let after = count_segments(dir.path());
    assert_eq!(after, 1);
}

/// Rotation mid-stream: a subscriber reading through segment N must
/// transparently continue into segment N+1 without special handling.
#[tokio::test]
async fn subscriber_follows_segment_rotation() {
    let dir = tempdir().unwrap();
    let wal = Arc::new(
        Wal::open(
            dir.path(),
            WalConfig {
                segment_size_bytes: 256,
                fsync_on_append: false,
            },
        )
        .unwrap(),
    );

    // Fill enough to rotate at least once before subscribing —
    // so catch-up has to span >1 segment.
    for i in 0..15 {
        wal.append(&mk(i)).unwrap();
    }
    wal.flush().unwrap();
    assert!(count_segments(dir.path()) >= 2);

    let mut sub = wal.subscribe(WalCursor::BEGIN).unwrap();
    let mut ids = Vec::new();
    for _ in 0..15 {
        let e = sub.next().await.unwrap().unwrap();
        ids.push(id_of(&e.record));
    }
    let expected: Vec<String> = (0..15).map(|i| format!("id-{i}")).collect();
    assert_eq!(ids, expected, "catch-up must span rotated segments");
}

fn count_segments(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".nwal"))
        .count()
}
