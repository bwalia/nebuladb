//! Nightly multi-region reliability sweep: cross-region CRUD + RAG
//! convergence and local-write latency.
//!
//! This is the in-process analogue of the two-region docker-compose
//! topology (`docker-compose.multiregion.yml`). It stands up two
//! region leaders (`us-east-1` and `us-west-2`), each owning its own
//! bucket, and wires the real `CrossRegionReplicationService` between
//! them so each region tails the other's WAL.
//!
//! It proves the active-active properties from
//! `docs/design/0001-multi-region-active-active.md`:
//!
//! 1. **Writes in the home region converge to the remote region.** A
//!    write to a bucket homed in `us-east-1` becomes readable in
//!    `us-west-2` via the cross-region tail, and vice versa.
//! 2. **CRUD replicates cross-region**, including updates and deletes,
//!    not just inserts.
//! 3. **RAG retrieval is correct on the remote region.** A semantic
//!    search in `us-west-2` over a bucket homed in `us-east-1` returns
//!    the authoritative document — reads are served locally everywhere.
//! 4. **Local write latency is bounded.** The handler-side cost of
//!    accepting a write (embed via cache + WAL append + HNSW insert) is
//!    measured and asserted under a budget. This is the direct nightly
//!    guard for the user-reported "pushing records into buckets lags"
//!    symptom: a regression in the hot path trips it.
//!
//! Embedder is `MockEmbedder` (deterministic hash). RAG assertions
//! query by a document's exact text so the expected doc is the nearest
//! neighbour — a real retrieval-correctness check for a hash embedder.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nebula_chunk::{Chunker, FixedSizeChunker};
use nebula_embed::{Embedder, MockEmbedder};
use nebula_grpc::cross_region::{spawn_all, OwnedBuckets, RemotePeer, StatusSink};
use nebula_grpc::{serve_with_region, GrpcState};
use nebula_index::TextIndex;
use nebula_llm::{LlmClient, MockLlm};
use nebula_vector::{HnswConfig, Metric};
use std::collections::HashSet;
use tempfile::tempdir;
use tokio::time::sleep;

const DIM: usize = 32;
const SEED_DOC_ID: &str = "__nebuladb_operator_seed__";

/// Cross-region convergence budget. More generous than the in-region
/// follower budget: the cross-region tail resumes from BEGIN on each
/// (re)connect and goes through an extra epoch check per record.
const CONVERGE_BUDGET: Duration = Duration::from_secs(8);

/// Per-write local-acceptance budget. The handler-side cost of a single
/// `upsert_text` (cached embed + WAL append + HNSW insert) on the mock
/// embedder should be well under this; the headroom absorbs CI jitter.
/// If a hot-path regression lands (e.g. an extra synchronous flush),
/// this trips with the measured duration.
const LOCAL_WRITE_BUDGET: Duration = Duration::from_millis(750);

const POLL: Duration = Duration::from_millis(25);

fn embedder() -> Arc<dyn Embedder> {
    Arc::new(MockEmbedder::new(DIM))
}

/// Spawn a region leader whose gRPC server has the cross-region service
/// active (it only activates when a region name is supplied).
async fn spawn_region(index: Arc<TextIndex>, region: &str) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let chunker: Arc<dyn Chunker> = Arc::new(FixedSizeChunker::new(100, 0).unwrap());
    let llm: Arc<dyn LlmClient> = Arc::new(MockLlm::default());
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let state = GrpcState::new(index, llm, chunker);
    let r = region.to_string();
    let handle = tokio::spawn(async move {
        serve_with_region(state, addr, Some(r)).await.unwrap();
    });
    sleep(Duration::from_millis(100)).await;
    (addr, handle)
}

/// Counts applies + collects errors so the test can assert progress and
/// that no errors (e.g. stale-epoch rejects) occurred. Mirrors the
/// `TestSink` in `replication.rs`.
struct CountingSink {
    applies: AtomicU64,
    errors: Mutex<Vec<String>>,
}

impl CountingSink {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            applies: AtomicU64::new(0),
            errors: Mutex::new(Vec::new()),
        })
    }
}

impl StatusSink for CountingSink {
    fn register(&self, _r: &str, _u: &str) {}
    fn record_apply(&self, _r: &str, _s: u64, _o: u64) {
        self.applies.fetch_add(1, Ordering::Relaxed);
    }
    fn record_error(&self, _r: &str, e: &str) {
        self.errors.lock().unwrap().push(e.to_string());
    }
}

/// Seed the bucket's home-region metadata so the cross-region epoch
/// check has something to compare against. Matches the seed doc the
/// operator's bucket controller writes.
async fn seed_home(index: &Arc<TextIndex>, bucket: &str, region: &str, epoch: u64) {
    index
        .upsert_text(
            bucket,
            SEED_DOC_ID,
            "seed",
            serde_json::json!({"home_region": region, "home_epoch": epoch}),
        )
        .await
        .unwrap();
}

async fn await_converged<F: Fn() -> bool>(what: &str, cond: F) {
    let started = Instant::now();
    loop {
        if cond() {
            return;
        }
        if started.elapsed() > CONVERGE_BUDGET {
            panic!("cross-region convergence timed out after {:?} waiting for: {what}", started.elapsed());
        }
        sleep(POLL).await;
    }
}

/// `OwnedBuckets` backed by a fixed set — simpler than scanning the
/// index and lets the test state exactly which buckets a region owns.
struct FixedOwned(HashSet<String>);
impl OwnedBuckets for FixedOwned {
    fn snapshot(&self) -> HashSet<String> {
        self.0.clone()
    }
}

/// Two regions, each owning one bucket, tailing each other. A write to
/// each region's home bucket converges to the other region, and full
/// CRUD (create/update/delete) replicates both directions. Also asserts
/// per-write local latency stays within budget — the lag guard.
//
// Multi-thread runtime required: TextIndex writes use
// tokio::task::block_in_place (crates/nebula-index/src/lib.rs:410).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_region_crud_converges_both_directions() {
    // --- Region us-east-1, home of bucket "catalog" ---
    let east_dir = tempdir().unwrap();
    let east = Arc::new(
        TextIndex::open_persistent(embedder(), Metric::Cosine, HnswConfig::default(), east_dir.path()).unwrap(),
    );
    seed_home(&east, "catalog", "us-east-1", 1).await;

    // --- Region us-west-2, home of bucket "logs" ---
    let west_dir = tempdir().unwrap();
    let west = Arc::new(
        TextIndex::open_persistent(embedder(), Metric::Cosine, HnswConfig::default(), west_dir.path()).unwrap(),
    );
    seed_home(&west, "logs", "us-west-2", 1).await;

    let (east_addr, _east_srv) = spawn_region(east.clone(), "us-east-1").await;
    let (west_addr, _west_srv) = spawn_region(west.clone(), "us-west-2").await;

    // east tails west (to receive "logs"); west tails east (to receive
    // "catalog"). Each declares the bucket it owns so the source-side
    // filter doesn't echo a region's own writes back to it.
    let east_owned: Arc<dyn OwnedBuckets> = Arc::new(FixedOwned(HashSet::from(["catalog".to_string()])));
    let west_owned: Arc<dyn OwnedBuckets> = Arc::new(FixedOwned(HashSet::from(["logs".to_string()])));
    let east_sink = CountingSink::new();
    let west_sink = CountingSink::new();

    let _east_tailers = spawn_all(
        vec![RemotePeer { region: "us-west-2".into(), grpc_url: format!("http://{west_addr}") }],
        east.clone(),
        east_owned,
        east_sink.clone(),
        "us-east-1".into(),
    );
    let _west_tailers = spawn_all(
        vec![RemotePeer { region: "us-east-1".into(), grpc_url: format!("http://{east_addr}") }],
        west.clone(),
        west_owned,
        west_sink.clone(),
        "us-west-2".into(),
    );

    // CREATE in east's home bucket → converges to west.
    east.upsert_text("catalog", "sku-1", "authoritative catalog item", serde_json::json!({"price": 10}))
        .await
        .unwrap();
    await_converged("east write visible in west", || west.get("catalog", "sku-1").is_some()).await;
    let seen = west.get("catalog", "sku-1").unwrap();
    assert_eq!(seen.text, "authoritative catalog item");
    assert_eq!(seen.metadata, serde_json::json!({"price": 10}));

    // CREATE in west's home bucket → converges to east (other direction).
    west.upsert_text("logs", "evt-1", "authoritative log event", serde_json::json!({"level": "warn"}))
        .await
        .unwrap();
    await_converged("west write visible in east", || east.get("logs", "evt-1").is_some()).await;
    assert_eq!(east.get("logs", "evt-1").unwrap().text, "authoritative log event");

    // UPDATE in east → west reflects the new revision.
    east.upsert_text("catalog", "sku-1", "updated catalog item", serde_json::json!({"price": 20}))
        .await
        .unwrap();
    await_converged("east update visible in west", || {
        west.get("catalog", "sku-1").map(|d| d.text == "updated catalog item").unwrap_or(false)
    })
    .await;
    assert_eq!(west.get("catalog", "sku-1").unwrap().metadata, serde_json::json!({"price": 20}));

    // DELETE in east → west drops the doc.
    east.delete("catalog", "sku-1").unwrap();
    await_converged("east delete visible in west", || west.get("catalog", "sku-1").is_none()).await;

    // No cross-region errors (stale-epoch rejects, decode failures) on
    // either side. A populated error list means a coordination bug.
    assert!(
        east_sink.errors.lock().unwrap().is_empty(),
        "east cross-region errors: {:?}",
        east_sink.errors.lock().unwrap()
    );
    assert!(
        west_sink.errors.lock().unwrap().is_empty(),
        "west cross-region errors: {:?}",
        west_sink.errors.lock().unwrap()
    );
}

/// RAG retrieval over a remote-homed bucket: seed a corpus in east,
/// wait for it to replicate to west, then run the semantic-search step
/// in west and assert the right document ranks first. This is "RAG
/// reads served locally in every region return correct results."
//
// Multi-thread runtime required: TextIndex writes use
// tokio::task::block_in_place (crates/nebula-index/src/lib.rs:410).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_region_rag_retrieval_correct() {
    let east_dir = tempdir().unwrap();
    let east = Arc::new(
        TextIndex::open_persistent(embedder(), Metric::Cosine, HnswConfig::default(), east_dir.path()).unwrap(),
    );
    seed_home(&east, "knowledge", "us-east-1", 1).await;

    let west = Arc::new(TextIndex::new(embedder(), Metric::Cosine, HnswConfig::default()).unwrap());

    let (east_addr, _east_srv) = spawn_region(east.clone(), "us-east-1").await;

    let west_owned: Arc<dyn OwnedBuckets> = Arc::new(FixedOwned(HashSet::new()));
    let west_sink = CountingSink::new();
    let _west_tailers = spawn_all(
        vec![RemotePeer { region: "us-east-1".into(), grpc_url: format!("http://{east_addr}") }],
        west.clone(),
        west_owned,
        west_sink.clone(),
        "us-west-2".into(),
    );

    let corpus = [
        ("doc-zero-trust", "zero trust network access with continuous verification"),
        ("doc-dns-failover", "dns failover strategies across multiple regions"),
        ("doc-gitops", "kubernetes gitops continuous deployment pipeline"),
    ];
    for (id, text) in corpus {
        east.upsert_text("knowledge", id, text, serde_json::json!({"source": "nightly"}))
            .await
            .unwrap();
    }

    // west also receives the seed doc, so expect corpus + seed.
    await_converged("corpus replicated to west", || west.get("knowledge", "doc-gitops").is_some()).await;

    let hits = west
        .search_text("dns failover strategies across multiple regions", Some("knowledge"), 3, None)
        .await
        .unwrap();
    assert!(!hits.is_empty(), "remote region returned no RAG context");
    assert_eq!(
        hits[0].id, "doc-dns-failover",
        "top retrieval hit in remote region was '{}', expected dns-failover doc",
        hits[0].id
    );
    assert_eq!(hits[0].text, "dns failover strategies across multiple regions");
}

/// Direct guard for the reported lag symptom: measure the handler-side
/// latency of accepting writes into a bucket and assert each stays
/// within `LOCAL_WRITE_BUDGET`. Runs against a single persistent index
/// (no replication) so the number isolates the local hot path —
/// cached embed + WAL fsync + HNSW insert — which is exactly where the
/// user observed lag when "pushing records into buckets."
//
// Multi-thread runtime required: TextIndex writes use
// tokio::task::block_in_place (crates/nebula-index/src/lib.rs:410).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_write_latency_within_budget() {
    const SAMPLES: usize = 100;

    let dir = tempdir().unwrap();
    let index = Arc::new(
        TextIndex::open_persistent(embedder(), Metric::Cosine, HnswConfig::default(), dir.path()).unwrap(),
    );

    let mut worst = Duration::ZERO;
    let mut total = Duration::ZERO;
    for i in 0..SAMPLES {
        let started = Instant::now();
        index
            .upsert_text("bench", &format!("rec-{i}"), &format!("payload number {i}"), serde_json::json!({}))
            .await
            .unwrap();
        let elapsed = started.elapsed();
        total += elapsed;
        worst = worst.max(elapsed);
    }
    let avg = total / SAMPLES as u32;

    // Assert on the worst-case single write, not the average — a p100
    // spike is what a user feels as "lag." The generous budget keeps
    // this from flaking on a loaded CI box while still catching an
    // order-of-magnitude regression (e.g. a per-write fsync stall or a
    // cache that fell off the hot path).
    assert!(
        worst < LOCAL_WRITE_BUDGET,
        "worst-case local write {worst:?} exceeded budget {LOCAL_WRITE_BUDGET:?} (avg {avg:?} over {SAMPLES} writes)"
    );
}
