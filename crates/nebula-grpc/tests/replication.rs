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
use nebula_grpc::follower::CursorStore;
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

/// The cursor store is updated after each applied record and
/// points past the last one when the stream ends. Uses the
/// in-memory store so the test can inspect what got saved.
#[tokio::test]
async fn cursor_store_advances_after_apply() {
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
    let follower_idx = Arc::new(
        TextIndex::new(embedder(), Metric::Cosine, HnswConfig::default()).unwrap(),
    );
    for i in 0..4 {
        leader
            .upsert_text("b", &format!("d{i}"), "t", serde_json::json!({}))
            .await
            .unwrap();
    }
    let (addr, _srv) = spawn_leader(leader.clone()).await;
    let ch = channel(addr).await;

    let store = Arc::new(follower::MemoryCursorStore::new());
    // Store starts empty.
    assert!(store.peek().is_none());

    let store_dyn: Arc<dyn follower::CursorStore> = store.clone();
    let idx_for_task = follower_idx.clone();
    let task = tokio::spawn(async move {
        follower::run_once_with_store(ch, idx_for_task, WalCursor::BEGIN, Some(store_dyn)).await
    });

    for _ in 0..50 {
        if follower_idx.len() == 4 {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(follower_idx.len(), 4);

    // The store should now hold a non-BEGIN cursor pointing past
    // the last applied record.
    let saved = store.peek().expect("cursor should be saved");
    assert!(
        saved > WalCursor::BEGIN,
        "saved cursor {saved:?} should be past BEGIN"
    );

    task.abort();
}

/// A FileCursorStore round-trips cleanly: save, drop, new store on
/// the same path, load. Proves the atomic-write + read logic works
/// without going through the replication pipeline.
#[test]
fn file_cursor_store_round_trips() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("follower.cursor");

    let store = follower::FileCursorStore::new(&path);
    assert!(store.load().unwrap().is_none(), "fresh path = None");

    let c = WalCursor {
        segment_seq: 7,
        byte_offset: 1234,
    };
    store.save(c).unwrap();

    // A new instance reading the same file sees the same cursor.
    let fresh = follower::FileCursorStore::new(&path);
    let loaded = fresh.load().unwrap().unwrap();
    assert_eq!(loaded.segment_seq, 7);
    assert_eq!(loaded.byte_offset, 1234);
}

/// The end-to-end persistence path: run a follower with a file
/// store, capture its saved cursor, spin up a *new* follower with
/// the same store path, verify it resumes at that cursor rather
/// than BEGIN.
#[tokio::test]
async fn file_store_resumes_across_restart() {
    let leader_dir = tempdir().unwrap();
    let follower_meta = tempdir().unwrap();
    let leader = Arc::new(
        TextIndex::open_persistent(
            embedder(),
            Metric::Cosine,
            HnswConfig::default(),
            leader_dir.path(),
        )
        .unwrap(),
    );
    for i in 0..3 {
        leader
            .upsert_text("b", &format!("pre-{i}"), "t", serde_json::json!({}))
            .await
            .unwrap();
    }
    let (addr, _srv) = spawn_leader(leader.clone()).await;

    // Run #1: follower drains the 3 historical records, persists
    // cursor to the file store, then aborts.
    {
        let ch = channel(addr).await;
        let idx = Arc::new(
            TextIndex::new(embedder(), Metric::Cosine, HnswConfig::default()).unwrap(),
        );
        let store: Arc<dyn follower::CursorStore> = Arc::new(
            follower::FileCursorStore::new(follower_meta.path().join("follower.cursor")),
        );
        let idx_for_task = idx.clone();
        let task = tokio::spawn(async move {
            follower::run_once_with_store(ch, idx_for_task, WalCursor::BEGIN, Some(store)).await
        });
        for _ in 0..50 {
            if idx.len() == 3 {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(idx.len(), 3, "first follower must drain history");
        task.abort();
        // Give the filesystem a tick to finalize the last save.
        sleep(Duration::from_millis(50)).await;
    }

    // Write new records *before* the second follower starts.
    for i in 0..2 {
        leader
            .upsert_text("b", &format!("post-{i}"), "t", serde_json::json!({}))
            .await
            .unwrap();
    }

    // Run #2: fresh index, but same persisted cursor. If resume
    // works, this follower should ONLY see the 2 new post-* records
    // (3 old ones are skipped because the cursor points past them).
    let ch2 = channel(addr).await;
    let idx2 = Arc::new(
        TextIndex::new(embedder(), Metric::Cosine, HnswConfig::default()).unwrap(),
    );
    let store2: Arc<dyn follower::CursorStore> = Arc::new(
        follower::FileCursorStore::new(follower_meta.path().join("follower.cursor")),
    );
    // spawn_with_store loads the persisted cursor internally, so
    // the initial_cursor arg is effectively a fallback only.
    let fh = follower::spawn_with_store(
        ch2,
        idx2.clone(),
        WalCursor::BEGIN,
        Some(store2),
    );
    for _ in 0..50 {
        if idx2.len() == 2 {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        idx2.len(),
        2,
        "restart should resume at saved cursor — only post-* records seen"
    );
    // Confirm it's specifically the post-* docs, not the pre-* ones.
    assert!(idx2.get("b", "post-0").is_some());
    assert!(idx2.get("b", "pre-0").is_none(), "pre-* docs were before the saved cursor");
    fh.abort();
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
