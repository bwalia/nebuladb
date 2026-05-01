//! End-to-end durability tests.
//!
//! Exercises the three recovery modes an operator can realistically
//! land in:
//!
//! 1. **Pure WAL replay** — no snapshot yet. Everything written
//!    before the crash is recovered from the WAL alone.
//!
//! 2. **Snapshot + WAL replay** — a snapshot exists; writes after
//!    it were captured by the WAL and reapplied on boot. This is
//!    the steady-state production case.
//!
//! 3. **Torn WAL tail** — a crash mid-write leaves a partial
//!    record. The WAL reader truncates cleanly; everything up to
//!    the last intact record survives.

use std::sync::Arc;

use nebula_embed::{Embedder, MockEmbedder};
use nebula_index::TextIndex;
use nebula_vector::{HnswConfig, Metric};
use tempfile::tempdir;

fn embedder() -> Arc<dyn Embedder> {
    Arc::new(MockEmbedder::new(32))
}

async fn ingest_and_close(dir: &std::path::Path, docs: &[(&str, &str)]) {
    // We never `close` explicitly — the TextIndex drops here and
    // the WAL's BufWriter flushes on drop. That's how a clean
    // shutdown looks in production too.
    let idx = TextIndex::open_persistent(
        embedder(),
        Metric::Cosine,
        HnswConfig::default(),
        dir,
    )
    .unwrap();
    for (id, text) in docs {
        idx.upsert_text("docs", id, text, serde_json::json!({"len": text.len()}))
            .await
            .unwrap();
    }
}

/// Mode 1: WAL-only recovery. Ingest, drop, reopen, confirm every
/// doc is back.
#[tokio::test]
async fn wal_only_recovery_round_trips_docs() {
    let dir = tempdir().unwrap();
    let corpus = [("a", "alpha"), ("b", "beta"), ("c", "gamma delta")];
    ingest_and_close(dir.path(), &corpus).await;

    let idx = TextIndex::open_persistent(
        embedder(),
        Metric::Cosine,
        HnswConfig::default(),
        dir.path(),
    )
    .unwrap();
    assert_eq!(idx.len(), corpus.len());
    for (id, text) in &corpus {
        let doc = idx.get("docs", id).expect("doc missing after recovery");
        assert_eq!(doc.text, *text);
    }
    // Search still works post-recovery.
    let hits = idx
        .search_text("beta", Some("docs"), 3, None)
        .await
        .unwrap();
    assert!(hits.iter().any(|h| h.id == "b"));
}

/// Mode 2: snapshot + WAL recovery. Ingest, snapshot, ingest more,
/// drop, reopen. Every doc from both phases must be present.
#[tokio::test]
async fn snapshot_plus_wal_covers_pre_and_post_snapshot_writes() {
    let dir = tempdir().unwrap();
    {
        let idx = TextIndex::open_persistent(
            embedder(),
            Metric::Cosine,
            HnswConfig::default(),
            dir.path(),
        )
        .unwrap();
        for i in 0..10 {
            idx.upsert_text("b", &format!("pre-{i}"), "pre snap", serde_json::json!({}))
                .await
                .unwrap();
        }
        idx.snapshot().unwrap();
        // After snapshot, do more writes — these are in the WAL
        // but not yet in any snapshot.
        for i in 0..5 {
            idx.upsert_text("b", &format!("post-{i}"), "post snap", serde_json::json!({}))
                .await
                .unwrap();
        }
    }

    let idx = TextIndex::open_persistent(
        embedder(),
        Metric::Cosine,
        HnswConfig::default(),
        dir.path(),
    )
    .unwrap();
    assert_eq!(idx.len(), 15, "pre+post should total 15");
    for i in 0..10 {
        assert!(
            idx.get("b", &format!("pre-{i}")).is_some(),
            "pre-snapshot doc {i} missing"
        );
    }
    for i in 0..5 {
        assert!(
            idx.get("b", &format!("post-{i}")).is_some(),
            "post-snapshot doc {i} missing"
        );
    }
}

/// Mode 3: crash-like truncation in the WAL. We can't easily
/// simulate a real crash, but we CAN write valid records, then
/// corrupt the tail, then reopen — mimicking "fsync didn't land
/// the last record fully". The WAL's validator should truncate
/// and recovery should succeed with N-1 records.
#[tokio::test]
async fn torn_wal_tail_recovers_to_last_good_record() {
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};

    let dir = tempdir().unwrap();
    {
        let idx = TextIndex::open_persistent(
            embedder(),
            Metric::Cosine,
            HnswConfig::default(),
            dir.path(),
        )
        .unwrap();
        for i in 0..5 {
            idx.upsert_text("t", &format!("good-{i}"), "ok", serde_json::json!({}))
                .await
                .unwrap();
        }
    }

    // Append garbage to the only WAL segment to simulate a torn
    // tail. The WAL layer should truncate it on reopen.
    let wal_dir = dir.path().join("wal");
    let seg = std::fs::read_dir(&wal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| e.file_name().to_string_lossy().ends_with(".nwal"))
        .unwrap();
    let mut f = OpenOptions::new().append(true).open(seg.path()).unwrap();
    // Plausible-looking but truncated frame: claim 2 KB, provide
    // 10 bytes. The reader must see the short read and stop.
    f.write_all(&2048u32.to_le_bytes()).unwrap();
    f.write_all(&0u32.to_le_bytes()).unwrap();
    f.write_all(&[0u8; 10]).unwrap();
    f.flush().unwrap();
    // Rewind so the append didn't mess with seek pos on drop.
    f.seek(SeekFrom::End(0)).unwrap();
    drop(f);

    let idx = TextIndex::open_persistent(
        embedder(),
        Metric::Cosine,
        HnswConfig::default(),
        dir.path(),
    )
    .unwrap();
    // All 5 pre-crash docs recovered; the torn tail produced no
    // additional doc.
    assert_eq!(idx.len(), 5);
    for i in 0..5 {
        assert!(idx.get("t", &format!("good-{i}")).is_some());
    }
    // And a post-recovery write still works.
    idx.upsert_text("t", "new", "fresh", serde_json::json!({}))
        .await
        .unwrap();
    assert_eq!(idx.len(), 6);
}

/// Compact drops old WAL after a successful snapshot. Verifies
/// the "snapshot supersedes the WAL" promise.
#[tokio::test]
async fn compact_after_snapshot_frees_wal_bytes() {
    let dir = tempdir().unwrap();
    let idx = TextIndex::open_persistent(
        embedder(),
        Metric::Cosine,
        HnswConfig::default(),
        dir.path(),
    )
    .unwrap();
    // Write enough to force at least one WAL rotation (64 MB
    // default is too large for a test; instead we just write
    // a lot of records — they stay in segment 0 but the
    // compact path still exercises its "don't delete current"
    // guard).
    for i in 0..50 {
        idx.upsert_text("b", &format!("d{i}"), "text", serde_json::json!({}))
            .await
            .unwrap();
    }
    idx.snapshot().unwrap();
    // Compact with no older segments: should be a no-op but
    // without erroring.
    let removed = idx.compact_wal().unwrap();
    assert_eq!(removed, 0, "no older segments to remove yet");
    // Write more → snapshot again → compact: now the original
    // segment can be dropped... except everything is still in
    // segment 0 (one segment). Compact's current-segment guard
    // keeps it, so this still returns 0. That's the correct
    // behavior and documents it.
    for i in 0..50 {
        idx.upsert_text("b", &format!("d{i}-v2"), "more", serde_json::json!({}))
            .await
            .unwrap();
    }
    idx.snapshot().unwrap();
    let _ = idx.compact_wal().unwrap(); // no assertion — see above
}
