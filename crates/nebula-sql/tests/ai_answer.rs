//! End-to-end tests for `SELECT ai_answer(...)` — design 0008 §5.
//! Real TextIndex + MockEmbedder + MockLlm, driven through parsed SQL.

use std::sync::Arc;

use nebula_embed::{Embedder, MockEmbedder};
use nebula_index::TextIndex;
use nebula_llm::{LlmClient, MockLlm};
use nebula_sql::SqlEngine;
use nebula_vector::{HnswConfig, Metric};

async fn engine_with_llm(docs: &[(&str, &str)]) -> SqlEngine {
    let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(32));
    let index = Arc::new(TextIndex::new(emb, Metric::Cosine, HnswConfig::default()).unwrap());
    for (id, text) in docs {
        index
            .upsert_text("docs", id, text, serde_json::json!({}))
            .await
            .unwrap();
    }
    let llm: Arc<dyn LlmClient> = Arc::new(MockLlm::default());
    SqlEngine::new(index).with_llm(llm)
}

// Multi-thread runtime required: TextIndex writes use
// tokio::task::block_in_place.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ai_answer_returns_single_answer_row_with_sources() {
    let eng = engine_with_llm(&[
        ("1", "the company holiday policy grants public holidays off"),
        ("2", "vpn access requires multi-factor authentication"),
    ])
    .await;

    let out = eng
        .run("SELECT ai_answer('what is the holiday policy?')")
        .await
        .unwrap();

    // Exactly one synthesized row.
    assert_eq!(out.rows.len(), 1);
    let row = &out.rows[0];
    assert_eq!(row.id, "ai_answer");
    // Answer present and non-empty.
    let answer = row.fields.get("answer").unwrap().as_str().unwrap();
    assert!(!answer.is_empty());
    // Sources array attributes the retrieved chunks.
    let sources = row.fields.get("sources").unwrap().as_array().unwrap();
    assert!(!sources.is_empty());
    assert!(sources[0].get("doc").is_some());
    assert!(sources[0].get("score").is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ai_answer_accepts_bucket_and_top_k_args() {
    let eng = engine_with_llm(&[("1", "alpha document about kubernetes")]).await;
    let out = eng
        .run("SELECT ai_answer('kubernetes', 'docs', 3)")
        .await
        .unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0].bucket, "docs");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ai_answer_without_llm_errors_clearly() {
    // Engine built without .with_llm(): ai_answer must fail with a
    // helpful message, not panic or silently return nothing.
    let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(32));
    let index = Arc::new(TextIndex::new(emb, Metric::Cosine, HnswConfig::default()).unwrap());
    index
        .upsert_text("docs", "1", "some content", serde_json::json!({}))
        .await
        .unwrap();
    let eng = SqlEngine::new(index);

    let err = eng.run("SELECT ai_answer('anything?')").await.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("requires an LLM"), "unexpected error: {msg}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ai_answer_rejects_from_clause() {
    let eng = engine_with_llm(&[("1", "content")]).await;
    let err = eng
        .run("SELECT ai_answer('q') FROM docs")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("FROM"), "unexpected: {err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ai_answer_rejects_non_string_question() {
    let eng = engine_with_llm(&[("1", "content")]).await;
    let err = eng.run("SELECT ai_answer(42)").await.unwrap_err();
    assert!(err.to_string().contains("string literal"), "unexpected: {err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ai_answer_explain_shows_answer_node() {
    let eng = engine_with_llm(&[("1", "content")]).await;
    let plan = eng.explain("SELECT ai_answer('q', 'docs', 7)").unwrap();
    let json = serde_json::to_value(&plan).unwrap();
    assert_eq!(json["node"], "answer");
    assert_eq!(json["query"], "q");
    assert_eq!(json["bucket"], "docs");
    assert_eq!(json["top_k"], 7);
}
