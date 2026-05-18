//! Recovery-throughput benchmark.
//!
//! Per ADR 0006, this measures *the actual* records/sec rate of WAL
//! replay during cold recovery. The number drives the option choice
//! in §5: if Option C (~2-4x batching) is enough to clear a real SLA,
//! we ship that and skip the lock-shard refactor; if it's not, the
//! number quantifies the gap and motivates Option A or B.
//!
//! # What this is and isn't
//!
//! - **Is**: a `#[ignore]`'d test binary that ingests N records,
//!   drops the index, reopens it from disk, and reports the wall
//!   time + records/sec for the reopen path.
//! - **Isn't**: a CI-running test. Marked `#[ignore]` because
//!   it's slow and noisy; run explicitly via the command at the
//!   bottom of this comment.
//! - **Isn't**: a microbenchmark. We measure the integrated
//!   recovery path (WAL read + bincode decode + apply_wal_record +
//!   HNSW insert) end-to-end because that's what production
//!   experiences — not the cost of any individual function.
//!
//! # Why a test, not a `criterion` bench
//!
//! `criterion` runs each measured function many times in a hot loop.
//! Recovery is a once-per-process operation that can't be looped
//! without rebuilding the data dir between iterations. A plain
//! `#[test]` with explicit timing is the right shape — it ingests
//! once, recovers once, prints once.
//!
//! # How to run
//!
//! ```text
//! # Default: 50k records, 100 buckets
//! cargo test --release -p nebula-index --test recovery_bench -- --ignored --nocapture
//!
//! # Bigger workload to reproduce 192.168.1.193-class WAL sizes:
//! NEBULA_BENCH_DOCS=500000 NEBULA_BENCH_BUCKETS=200 \
//!   cargo test --release -p nebula-index --test recovery_bench -- --ignored --nocapture
//! ```
//!
//! Output names the records/sec rate and the wall-clock recovery time.
//! That's the input to the §4.1 question in ADR 0006: "what's the
//! actual replay throughput today?"

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use nebula_embed::{Embedder, MockEmbedder};
use nebula_index::TextIndex;
use nebula_vector::{HnswConfig, Metric};
use tempfile::tempdir;

fn embedder() -> Arc<dyn Embedder> {
    // Mock — we don't want network on the embed path biasing the
    // measurement. The vector size matches what compose ships
    // for the showcase (384), so HNSW work is realistic-shaped.
    Arc::new(MockEmbedder::new(384))
}

/// Read tunables from env. Defaults sized so the bench finishes in
/// a few minutes on a laptop while still producing a useful number.
struct BenchConfig {
    docs: usize,
    buckets: usize,
    /// Take a snapshot mid-way so the recovery path exercises the
    /// "snapshot + WAL replay" mode (the steady-state production
    /// case from durability.rs §2). When false, recovery replays
    /// every record from scratch (Bug-C-on-192.168.1.193 mode).
    snapshot_at_half: bool,
}

impl BenchConfig {
    fn from_env() -> Self {
        Self {
            docs: env_usize("NEBULA_BENCH_DOCS", 50_000),
            buckets: env_usize("NEBULA_BENCH_BUCKETS", 100),
            snapshot_at_half: std::env::var("NEBULA_BENCH_SNAPSHOT")
                .map(|v| v != "0" && v != "off")
                .unwrap_or(true),
        }
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Ingest `n` records spread across `buckets` buckets. Each record
/// is small but each carries metadata + a real chunked text body
/// so the WAL records' on-disk size matches production shape.
async fn ingest(dir: &Path, cfg: &BenchConfig) {
    let idx = Arc::new(
        TextIndex::open_persistent(embedder(), Metric::Cosine, HnswConfig::default(), dir)
            .unwrap(),
    );

    println!(
        "[bench] ingesting {} records across {} buckets...",
        cfg.docs, cfg.buckets
    );
    let start = Instant::now();
    let mut snapshot_taken = false;

    for i in 0..cfg.docs {
        // Round-robin across buckets so the distribution is
        // predictable and the per-bucket count is roughly even.
        let bucket = format!("b{:04}", i % cfg.buckets);
        let id = format!("d{:08}", i);
        // Body is a sentence with the doc id in it — enough to
        // produce a non-degenerate vector and a few hundred bytes
        // of WAL record payload.
        let text = format!(
            "loadtest record {i} in bucket {bucket} with deterministic content for benchmarking",
        );
        let metadata = serde_json::json!({
            "i": i,
            "bucket": bucket,
            "tag": format!("tag-{}", i % 7),
        });
        idx.upsert_text(&bucket, &id, &text, metadata)
            .await
            .unwrap();

        // Snapshot at halfway through — exercises the production
        // "snapshot + WAL tail" recovery mode. Without this the
        // bench measures the worse-case "everything since
        // forever" replay, which is what 192.168.1.193 hit when
        // the scheduler was disabled but isn't the steady state.
        if cfg.snapshot_at_half && !snapshot_taken && i == cfg.docs / 2 {
            println!("[bench] taking mid-ingest snapshot at i={i}...");
            let snap_t = Instant::now();
            // We can't call snapshot() from the test thread directly
            // (it holds the read lock during streaming write); do it
            // in a blocking task to match production discipline.
            let idx_for_snap = Arc::clone(&idx);
            tokio::task::spawn_blocking(move || idx_for_snap.snapshot())
                .await
                .unwrap()
                .unwrap();
            println!(
                "[bench] snapshot took {:.2}s",
                snap_t.elapsed().as_secs_f64()
            );
            snapshot_taken = true;
        }

        if (i + 1) % 10_000 == 0 {
            let rate = (i + 1) as f64 / start.elapsed().as_secs_f64();
            println!("[bench]   ingested {} records ({:.0}/sec)", i + 1, rate);
        }
    }

    let elapsed = start.elapsed();
    println!(
        "[bench] ingest done: {} records in {:.2}s ({:.0}/sec)",
        cfg.docs,
        elapsed.as_secs_f64(),
        cfg.docs as f64 / elapsed.as_secs_f64()
    );

    // Drop here flushes the WAL writer + closes file handles. The
    // dropped Arc is the only one outstanding because spawn_blocking
    // returns before this line.
    drop(idx);
}

/// Reopen the persistent index from `dir` and time it. Returns
/// (records_recovered, elapsed).
fn recover_and_time(dir: &Path) -> (usize, std::time::Duration) {
    println!("[bench] reopening from disk (this is the recovery measurement)...");
    let start = Instant::now();
    let idx = TextIndex::open_persistent(embedder(), Metric::Cosine, HnswConfig::default(), dir)
        .unwrap();
    let elapsed = start.elapsed();
    let count = idx.len();
    println!(
        "[bench] recovery complete: {} docs in {:.2}s ({:.0}/sec)",
        count,
        elapsed.as_secs_f64(),
        count as f64 / elapsed.as_secs_f64()
    );
    (count, elapsed)
}

fn report_disk(dir: &Path) {
    let wal = dir.join("wal");
    let snaps = dir.join("snapshots");
    let wal_bytes: u64 = std::fs::read_dir(&wal)
        .ok()
        .map(|rd| {
            rd.flatten()
                .filter_map(|e| e.metadata().ok().map(|m| m.len()))
                .sum()
        })
        .unwrap_or(0);
    let snap_bytes: u64 = std::fs::read_dir(&snaps)
        .ok()
        .map(|rd| {
            rd.flatten()
                .filter_map(|e| e.metadata().ok().map(|m| m.len()))
                .sum()
        })
        .unwrap_or(0);
    println!(
        "[bench] on-disk: wal={:.1} MiB  snapshots={:.1} MiB",
        wal_bytes as f64 / 1_048_576.0,
        snap_bytes as f64 / 1_048_576.0,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "long-running benchmark — run explicitly with --ignored"]
async fn measure_recovery_throughput() {
    let cfg = BenchConfig::from_env();
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    println!(
        "[bench] config: docs={} buckets={} snapshot_at_half={}",
        cfg.docs, cfg.buckets, cfg.snapshot_at_half
    );

    ingest(&path, &cfg).await;
    report_disk(&path);

    let (recovered, elapsed) = tokio::task::spawn_blocking(move || recover_and_time(&path))
        .await
        .unwrap();

    println!();
    println!("============================================================");
    println!("RECOVERY BENCHMARK RESULT");
    println!("============================================================");
    println!("docs ingested:           {}", cfg.docs);
    println!("docs recovered:          {}", recovered);
    println!("snapshot taken at half:  {}", cfg.snapshot_at_half);
    println!("recovery wall time:      {:.3}s", elapsed.as_secs_f64());
    println!(
        "recovery throughput:     {:.0} records/sec",
        recovered as f64 / elapsed.as_secs_f64()
    );
    println!("============================================================");
    println!();
    println!("Use the throughput number to answer ADR 0006 §4.1.");
    println!("Cross-reference with the per-option speedup table in §3.");

    // Sanity assert so the test fails loudly if recovery dropped data.
    assert_eq!(
        recovered, cfg.docs,
        "recovery lost docs: expected {}, got {}",
        cfg.docs, recovered
    );
}
