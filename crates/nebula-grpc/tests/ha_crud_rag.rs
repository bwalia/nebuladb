//! Nightly HA reliability sweep: CRUD + RAG retrieval across a
//! leader/follower pair.
//!
//! This is the in-process analogue of the docker-compose
//! leader+follower topology (`nebula-server` + `nebula-follower`).
//! It stands a persistent leader and a fresh follower in one process,
//! wires them over the real Tonic `ReplicationService`, and proves the
//! properties an operator cares about for high availability:
//!
//! 1. **CRUD converges.** Every create/update/delete on the leader is
//!    mirrored to the follower through WAL-tail replication.
//! 2. **RAG retrieval works on the replica.** A semantic search issued
//!    against the follower returns the same authoritative document the
//!    leader holds — i.e. reads survive a leader outage with correct
//!    results, not just a 200.
//! 3. **Replication lag is bounded.** Convergence happens within a
//!    wall-clock budget; the test fails loudly if the follower falls
//!    behind, which is the signal that replication has regressed.
//!
//! Why this lives in `nebula-grpc/tests` and not a shell script: it
//! exercises the actual gRPC stream + `apply_wal_record` apply path
//! with deterministic timing, so it runs in the fast `unit` job every
//! night without needing a Docker stack. The compose-based end-to-end
//! variant covers the containerized path separately.
//!
//! The embedder is `MockEmbedder` — a deterministic hash, not a
//! language model. RAG assertions therefore query by a known
//! document's *exact* text (which produces an identical query vector)
//! and assert that document ranks first. That is a real retrieval
//! correctness check for a hash embedder; semantic-paraphrase recall
//! is out of scope here (it needs a real provider, covered by the
//! Ollama e2e script).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nebula_chunk::{Chunker, FixedSizeChunker};
use nebula_embed::{Embedder, MockEmbedder};
use nebula_grpc::{follower, serve, GrpcState};
use nebula_index::TextIndex;
use nebula_llm::{LlmClient, MockLlm};
use nebula_vector::{HnswConfig, Metric};
use nebula_wal::WalCursor;
use tempfile::tempdir;
use tokio::time::sleep;
use tonic::transport::Channel;

/// Mock embedder dimension. Small keeps HNSW build cheap; the value is
/// irrelevant to correctness as long as leader and follower agree.
const DIM: usize = 32;

/// Upper bound on how long a single replicated record may take to land
/// on the follower. This is the lag SLO the nightly enforces — if the
/// WAL-tail path regresses (e.g. an accidental synchronous flush added
/// on the apply side), convergence blows past this and the test fails
/// with the measured value rather than hanging.
const LAG_BUDGET: Duration = Duration::from_secs(5);

/// Poll interval while waiting for convergence. Tens of ms matches the
/// broadcast tail's observed latency on a fresh channel.
const POLL: Duration = Duration::from_millis(25);

fn embedder() -> Arc<dyn Embedder> {
    Arc::new(MockEmbedder::new(DIM))
}

/// Stand up a gRPC `ReplicationService` in front of `index`. Mirrors
/// the helper in `replication.rs`; duplicated because integration test
/// files are separate crates and can't share a module.
async fn spawn_leader(index: Arc<TextIndex>) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let chunker: Arc<dyn Chunker> = Arc::new(FixedSizeChunker::new(100, 0).unwrap());
    let llm: Arc<dyn LlmClient> = Arc::new(MockLlm::default());

    // Bind to discover a free port, drop, then hand the address to
    // Tonic — Tonic's Server doesn't expose its own listener.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let state = GrpcState::new(index, llm, chunker);
    let handle = tokio::spawn(async move {
        serve(state, addr).await.unwrap();
    });
    sleep(Duration::from_millis(100)).await;
    (addr, handle)
}

async fn channel(addr: SocketAddr) -> Channel {
    Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .expect("connect")
}

/// Block until `cond` holds or `LAG_BUDGET` elapses. Returns the
/// elapsed time on success so callers can assert the lag was within
/// budget; panics with a descriptive message on timeout.
async fn await_converged<F: Fn() -> bool>(what: &str, cond: F) -> Duration {
    let started = Instant::now();
    loop {
        if cond() {
            return started.elapsed();
        }
        if started.elapsed() > LAG_BUDGET {
            panic!(
                "convergence timed out after {:?} waiting for: {what}",
                started.elapsed()
            );
        }
        sleep(POLL).await;
    }
}

/// Full CRUD lifecycle replicated leader → follower, with explicit lag
/// measurement at each step. Covers create, update (re-upsert same id),
/// and delete — the three mutations an operator relies on surviving a
/// failover.
//
// Multi-thread runtime required: TextIndex writes use
// tokio::task::block_in_place (crates/nebula-index/src/lib.rs:410).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crud_converges_on_follower_within_lag_budget() {
    let leader_dir = tempdir().unwrap();
    let leader = Arc::new(
        TextIndex::open_persistent(embedder(), Metric::Cosine, HnswConfig::default(), leader_dir.path())
            .unwrap(),
    );
    let follower = Arc::new(TextIndex::new(embedder(), Metric::Cosine, HnswConfig::default()).unwrap());

    let (addr, _srv) = spawn_leader(leader.clone()).await;
    let ch = channel(addr).await;
    let fh = follower::spawn(ch, follower.clone(), WalCursor::BEGIN);

    // CREATE: write a doc on the leader, expect it on the follower.
    leader
        .upsert_text("catalog", "sku-1", "first revision text", serde_json::json!({"v": 1}))
        .await
        .unwrap();
    let create_lag = await_converged("create to replicate", || follower.get("catalog", "sku-1").is_some()).await;
    let replicated = follower.get("catalog", "sku-1").unwrap();
    assert_eq!(replicated.text, "first revision text");
    assert_eq!(replicated.metadata, serde_json::json!({"v": 1}));
    assert!(
        create_lag < LAG_BUDGET,
        "create replication lag {create_lag:?} exceeded budget {LAG_BUDGET:?}"
    );

    // UPDATE: re-upsert the same id with new text + metadata. The
    // follower must reflect the latest revision, not the original.
    leader
        .upsert_text("catalog", "sku-1", "second revision text", serde_json::json!({"v": 2}))
        .await
        .unwrap();
    await_converged("update to replicate", || {
        follower
            .get("catalog", "sku-1")
            .map(|d| d.text == "second revision text")
            .unwrap_or(false)
    })
    .await;
    let updated = follower.get("catalog", "sku-1").unwrap();
    assert_eq!(updated.metadata, serde_json::json!({"v": 2}));

    // DELETE: remove on the leader, expect the follower to drop it.
    leader.delete("catalog", "sku-1").unwrap();
    await_converged("delete to replicate", || follower.get("catalog", "sku-1").is_none()).await;
    assert!(follower.get("catalog", "sku-1").is_none(), "follower kept a deleted doc");

    fh.abort();
}

/// RAG retrieval correctness on the replica: seed a small corpus on the
/// leader, let it replicate, then issue a semantic search *against the
/// follower* and assert the expected authoritative document ranks
/// first. This is the property "reads keep returning correct results
/// when they fail over to a replica."
//
// Multi-thread runtime required: TextIndex writes use
// tokio::task::block_in_place (crates/nebula-index/src/lib.rs:410).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rag_retrieval_correct_on_follower() {
    let leader_dir = tempdir().unwrap();
    let leader = Arc::new(
        TextIndex::open_persistent(embedder(), Metric::Cosine, HnswConfig::default(), leader_dir.path())
            .unwrap(),
    );
    let follower = Arc::new(TextIndex::new(embedder(), Metric::Cosine, HnswConfig::default()).unwrap());

    let (addr, _srv) = spawn_leader(leader.clone()).await;
    let ch = channel(addr).await;
    let fh = follower::spawn(ch, follower.clone(), WalCursor::BEGIN);

    // A small distinct corpus. With the deterministic hash embedder,
    // searching by a document's exact text yields that document as the
    // nearest neighbour (identical vector → distance 0).
    let corpus = [
        ("doc-zero-trust", "zero trust network access with continuous verification"),
        ("doc-dns-failover", "dns failover strategies across multiple regions"),
        ("doc-gitops", "kubernetes gitops continuous deployment pipeline"),
    ];
    for (id, text) in corpus {
        leader
            .upsert_text("knowledge", id, text, serde_json::json!({"source": "nightly"}))
            .await
            .unwrap();
    }

    await_converged("corpus to replicate", || follower.len() == corpus.len()).await;

    // Issue the RAG retrieval step against the FOLLOWER. We search by
    // the exact text of the dns-failover doc; it must come back first.
    let hits = follower
        .search_text("dns failover strategies across multiple regions", Some("knowledge"), 3, None)
        .await
        .unwrap();
    assert!(!hits.is_empty(), "follower returned no RAG context");
    assert_eq!(
        hits[0].id, "doc-dns-failover",
        "top retrieval hit on the replica was '{}', expected the dns-failover doc",
        hits[0].id
    );
    // The retrieved context carries the original text + metadata, which
    // is what gets fed into the LLM prompt in the real RAG path.
    assert_eq!(hits[0].text, "dns failover strategies across multiple regions");
    assert_eq!(hits[0].metadata, serde_json::json!({"source": "nightly"}));

    fh.abort();
}

/// A burst of writes all converge, and the *aggregate* convergence
/// stays within budget. This is the closest in-process proxy for the
/// user-reported symptom — "pushing records into buckets lags." If a
/// batch of inserts collectively blows the lag budget, this fails and
/// reports how far the follower got, turning an anecdotal "feels slow"
/// into a hard nightly signal.
//
// Multi-thread runtime required: TextIndex writes use
// tokio::task::block_in_place (crates/nebula-index/src/lib.rs:410).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_burst_converges_within_budget() {
    const BURST: usize = 200;

    let leader_dir = tempdir().unwrap();
    let leader = Arc::new(
        TextIndex::open_persistent(embedder(), Metric::Cosine, HnswConfig::default(), leader_dir.path())
            .unwrap(),
    );
    let follower = Arc::new(TextIndex::new(embedder(), Metric::Cosine, HnswConfig::default()).unwrap());

    let (addr, _srv) = spawn_leader(leader.clone()).await;
    let ch = channel(addr).await;
    let fh = follower::spawn(ch, follower.clone(), WalCursor::BEGIN);

    for i in 0..BURST {
        leader
            .upsert_text("bulk", &format!("rec-{i}"), &format!("record number {i}"), serde_json::json!({}))
            .await
            .unwrap();
    }

    let lag = await_converged("write burst to fully replicate", || follower.len() == BURST).await;
    assert_eq!(follower.len(), BURST, "follower did not converge to full burst");
    assert!(
        lag < LAG_BUDGET,
        "burst of {BURST} writes took {lag:?} to converge, over budget {LAG_BUDGET:?}; \
         follower reached {} docs",
        follower.len()
    );

    fh.abort();
}
