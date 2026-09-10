//! Deferred-embedding tests (design 0010 §5): a write accepted
//! without a vector must be durable, BM25-visible, drainable by the
//! worker, and must survive crash recovery in either state.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use nebula_embed::{EmbedError, Embedder, MockEmbedder};
use nebula_index::TextIndex;
use nebula_vector::{HnswConfig, Metric};

/// Embedder that can be switched off to simulate a provider outage.
struct Switchable {
    inner: MockEmbedder,
    down: AtomicBool,
}

impl Switchable {
    fn new(dim: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: MockEmbedder::new(dim),
            down: AtomicBool::new(false),
        })
    }
}

#[async_trait]
impl Embedder for Switchable {
    fn dim(&self) -> usize {
        self.inner.dim()
    }
    fn model(&self) -> &str {
        self.inner.model()
    }
    async fn embed(&self, inputs: &[String]) -> nebula_embed::Result<Vec<Vec<f32>>> {
        if self.down.load(Ordering::SeqCst) {
            return Err(EmbedError::Provider {
                status: 503,
                body: "outage".into(),
            });
        }
        self.inner.embed(inputs).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deferred_upsert_succeeds_while_provider_is_down() {
    let emb = Switchable::new(16);
    emb.down.store(true, Ordering::SeqCst);
    let index = Arc::new(
        TextIndex::new(
            emb.clone() as Arc<dyn Embedder>,
            Metric::Cosine,
            HnswConfig::default(),
        )
        .unwrap(),
    );

    // Synchronous path would fail — deferred must succeed.
    assert!(index
        .upsert_text("b", "d1", "the moon landing", serde_json::json!({"k":"v"}))
        .await
        .is_err());
    index
        .upsert_text_deferred("b", "d1", "the moon landing", serde_json::json!({"k":"v"}))
        .unwrap();

    // Immediately readable + lexically searchable, not vector-searchable.
    assert!(index.get("b", "d1").is_some());
    let bm25 = index.search_bm25("moon landing", Some("b"), 5);
    assert_eq!(bm25.len(), 1);
    assert_eq!(index.pending_embedding_count(), 1);

    // Draining while down fails without losing the doc.
    assert!(index.embed_pending_batch(10).await.is_err());
    assert_eq!(index.pending_embedding_count(), 1);

    // Provider recovers → drain completes → vector search covers it.
    emb.down.store(false, Ordering::SeqCst);
    assert_eq!(index.embed_pending_batch(10).await.unwrap(), 1);
    assert_eq!(index.pending_embedding_count(), 0);
    let hits = Arc::clone(&index)
        .search_text_blocking("the moon landing".into(), Some("b".into()), 3, Some(32))
        .await
        .unwrap();
    assert!(hits.iter().any(|h| h.id == "d1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacing_a_pending_doc_cancels_its_embedding() {
    let emb = Switchable::new(16);
    let index = TextIndex::new(
        emb.clone() as Arc<dyn Embedder>,
        Metric::Cosine,
        HnswConfig::default(),
    )
    .unwrap();
    index
        .upsert_text_deferred("b", "d1", "old text", serde_json::json!({}))
        .unwrap();
    assert_eq!(index.pending_embedding_count(), 1);
    // Synchronous replace supersedes the pending version entirely.
    index
        .upsert_text("b", "d1", "new text", serde_json::json!({}))
        .await
        .unwrap();
    assert_eq!(index.pending_embedding_count(), 0);
    assert_eq!(index.get("b", "d1").unwrap().text, "new text");
    // Drain is a no-op, not an error.
    assert_eq!(index.embed_pending_batch(10).await.unwrap(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_docs_survive_crash_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let emb = Switchable::new(16);

    {
        let index = TextIndex::open_persistent(
            emb.clone() as Arc<dyn Embedder>,
            Metric::Cosine,
            HnswConfig::default(),
            dir.path(),
        )
        .unwrap();
        index
            .upsert_text_deferred("b", "pend", "still waiting", serde_json::json!({}))
            .unwrap();
        index
            .upsert_text("b", "done", "already embedded", serde_json::json!({}))
            .await
            .unwrap();
        // Drop without snapshot = simulated crash; WAL is the record.
    }

    let index = TextIndex::open_persistent(
        emb.clone() as Arc<dyn Embedder>,
        Metric::Cosine,
        HnswConfig::default(),
        dir.path(),
    )
    .unwrap();
    assert_eq!(index.len(), 2);
    assert_eq!(
        index.pending_embedding_count(),
        1,
        "pending state must replay"
    );
    assert!(index.get("b", "pend").is_some());

    // Drain after recovery completes the write.
    assert_eq!(index.embed_pending_batch(10).await.unwrap(), 1);
    assert_eq!(index.pending_embedding_count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_deferred_write_replays_as_completed() {
    let dir = tempfile::tempdir().unwrap();
    let emb = Switchable::new(16);
    {
        let index = TextIndex::open_persistent(
            emb.clone() as Arc<dyn Embedder>,
            Metric::Cosine,
            HnswConfig::default(),
            dir.path(),
        )
        .unwrap();
        index
            .upsert_text_deferred("b", "d1", "deferred then done", serde_json::json!({}))
            .unwrap();
        assert_eq!(index.embed_pending_batch(10).await.unwrap(), 1);
    }
    // Replay applies pending record then the completed UpsertText —
    // final state must be completed (no pending residue), and the
    // embedder must not be needed during recovery.
    emb.down.store(true, Ordering::SeqCst);
    let index = TextIndex::open_persistent(
        emb as Arc<dyn Embedder>,
        Metric::Cosine,
        HnswConfig::default(),
        dir.path(),
    )
    .unwrap();
    assert_eq!(index.len(), 1);
    assert_eq!(index.pending_embedding_count(), 0);
}
