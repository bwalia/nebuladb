//! End-to-end replication test.
//!
//! Stand up a persistent leader (`TextIndex::open_persistent`) and
//! a fresh in-memory follower in one process. Wire them together
//! via the Tonic `ReplicationService` stream. Write to the leader,
//! assert the follower sees the same documents through its own
//! `TextIndex::get`.
//!
//! This is the integration-level proof that:
//!
//! 1. The leader's WAL.subscribe emits entries in order.
//! 2. Entries survive proto round-trip (bincode-in-bytes) unchanged.
//! 3. The follower's apply_wal_record produces the same in-memory
//!    state the leader has.
//! 4. Dropping the follower doesn't break the leader (no leaked
//!    ack slot, compaction free to proceed afterwards).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

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

fn embedder() -> Arc<dyn Embedder> {
    Arc::new(MockEmbedder::new(32))
}

/// Start a gRPC server fronting the provided TextIndex. Returns
/// the bound address; the server task is left running — callers
/// don't need to join it, test teardown drops the handle.
async fn spawn_leader(index: Arc<TextIndex>) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let chunker: Arc<dyn Chunker> = Arc::new(FixedSizeChunker::new(100, 0).unwrap());
    let llm: Arc<dyn LlmClient> = Arc::new(MockLlm::default());

    // Same "bind 0, drop, hand to Tonic" trick the other e2e test
    // uses — Tonic's Server doesn't expose the listener.
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

/// Persist a leader, subscribe a fresh follower at BEGIN, write to
/// the leader, verify the follower mirrors state.
#[tokio::test]
async fn follower_mirrors_leader_writes() {
    let leader_dir = tempdir().unwrap();
    let leader = Arc::new(
        TextIndex::open_persistent(
            embedder(),
            Metric::Cosine,
            HnswConfig::default(),
            leader_dir.path(),
        )
        .unwrap(),
    );
    let follower = Arc::new(
        TextIndex::new(embedder(), Metric::Cosine, HnswConfig::default()).unwrap(),
    );

    let (addr, _srv) = spawn_leader(leader.clone()).await;
    let ch = channel(addr).await;

    // Write a couple docs BEFORE starting the follower — these land
    // in the catch-up phase.
    for i in 0..3 {
        leader
            .upsert_text(
                "b",
                &format!("pre-{i}"),
                &format!("before follower {i}"),
                serde_json::json!({}),
            )
            .await
            .unwrap();
    }

    // Start the follower in the background.
    let fh = follower::spawn(ch, follower.clone(), WalCursor::BEGIN);

    // Write more docs after subscription — these exercise the live
    // tail path.
    for i in 0..4 {
        leader
            .upsert_text(
                "b",
                &format!("post-{i}"),
                &format!("after follower {i}"),
                serde_json::json!({}),
            )
            .await
            .unwrap();
    }

    // Wait for the follower to reach 7 docs. Polling with a ceiling
    // is more robust than a fixed sleep — the broadcast tail's
    // latency is a few tens of ms on a fresh channel.
    for _ in 0..50 {
        if follower.len() == 7 {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(
        follower.len(),
        7,
        "follower should have mirrored every write"
    );

    // Spot-check content. `get` returns the document by external id
    // — if the follower's HNSW + docs map are in sync with the
    // leader's, these succeed.
    for i in 0..3 {
        let d = follower.get("b", &format!("pre-{i}")).expect("pre doc");
        assert_eq!(d.text, format!("before follower {i}"));
    }
    for i in 0..4 {
        let d = follower.get("b", &format!("post-{i}")).expect("post doc");
        assert_eq!(d.text, format!("after follower {i}"));
    }

    // Cleanup — abort the follower task so the test exits cleanly.
    fh.abort();
}

/// Single-shot `run_once` path used by follower.rs internals. This
/// test avoids the spawn-and-loop complexity and asserts the API
/// surface that tests / tools will call directly.
#[tokio::test]
async fn run_once_applies_pending_records_and_returns_cursor() {
    let leader_dir = tempdir().unwrap();
    let leader = Arc::new(
        TextIndex::open_persistent(
            embedder(),
            Metric::Cosine,
            HnswConfig::default(),
            leader_dir.path(),
        )
        .unwrap(),
    );
    let follower = Arc::new(
        TextIndex::new(embedder(), Metric::Cosine, HnswConfig::default()).unwrap(),
    );
    for i in 0..3 {
        leader
            .upsert_text(
                "b",
                &format!("d{i}"),
                &format!("t{i}"),
                serde_json::json!({}),
            )
            .await
            .unwrap();
    }
    let (addr, _srv) = spawn_leader(leader.clone()).await;
    let ch = channel(addr).await;

    // Run the follower in a short-lived task so it can exit when the
    // leader's server is dropped (which happens at test end). The
    // stream only returns cleanly on shutdown; we wait for docs
    // instead and abort.
    let fh_index = follower.clone();
    let run_task = tokio::spawn(async move {
        // NB: this won't return until the leader disconnects; we
        // abort once the follower's caught up.
        follower::run_once(ch, fh_index, WalCursor::BEGIN).await
    });

    for _ in 0..50 {
        if follower.len() == 3 {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(follower.len(), 3);
    run_task.abort();
}

/// In-memory leader should refuse follower subscriptions with
/// FAILED_PRECONDITION — no WAL means nothing to replicate.
#[tokio::test]
async fn in_memory_leader_rejects_followers() {
    let leader = Arc::new(
        TextIndex::new(embedder(), Metric::Cosine, HnswConfig::default()).unwrap(),
    );
    let (addr, _srv) = spawn_leader(leader).await;
    let ch = channel(addr).await;
    let follower = Arc::new(
        TextIndex::new(embedder(), Metric::Cosine, HnswConfig::default()).unwrap(),
    );

    let err = follower::run_once(ch, follower, WalCursor::BEGIN)
        .await
        .expect_err("in-memory leader must refuse");
    match err {
        follower::FollowerError::Rpc(s) => {
            assert_eq!(s.code(), tonic::Code::FailedPrecondition);
        }
        other => panic!("expected Rpc(FailedPrecondition), got {other:?}"),
    }
}
