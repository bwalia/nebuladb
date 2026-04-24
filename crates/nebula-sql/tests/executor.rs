//! End-to-end executor tests: real TextIndex + MockEmbedder, drive
//! via parsed SQL, assert on the produced rows.

use std::sync::Arc;

use nebula_embed::{Embedder, MockEmbedder};
use nebula_index::TextIndex;
use nebula_sql::SqlEngine;
use nebula_vector::{HnswConfig, Metric};

async fn engine_with_docs(docs: &[(&str, &str, serde_json::Value)]) -> SqlEngine {
    let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(32));
    let index = Arc::new(
        TextIndex::new(emb, Metric::Cosine, HnswConfig::default()).unwrap(),
    );
    for (id, text, meta) in docs {
        index
            .upsert_text("docs", id, text, meta.clone())
            .await
            .unwrap();
    }
    SqlEngine::new(index)
}

#[tokio::test]
async fn select_star_returns_rows_with_text() {
    let eng = engine_with_docs(&[
        ("1", "zero trust networking", serde_json::json!({"region": "eu"})),
        ("2", "dns failover strategies", serde_json::json!({"region": "us"})),
    ])
    .await;

    let out = eng
        .run("SELECT * FROM docs WHERE semantic_match(content, 'zero trust') LIMIT 5")
        .await
        .unwrap();
    assert!(!out.rows.is_empty());
    assert!(out.rows.iter().all(|r| r.bucket == "docs"));
    // `*` projection includes the text field.
    let text = out.rows[0].fields.get("text").unwrap().as_str().unwrap();
    assert!(!text.is_empty());
}

#[tokio::test]
async fn residual_filter_drops_non_matching_region() {
    let eng = engine_with_docs(&[
        ("1", "zero trust", serde_json::json!({"region": "eu"})),
        ("2", "zero trust", serde_json::json!({"region": "us"})),
        ("3", "zero trust", serde_json::json!({"region": "ap"})),
    ])
    .await;

    let out = eng
        .run(
            "SELECT id, region FROM docs \
             WHERE semantic_match(content, 'zero trust') AND region = 'eu' LIMIT 10",
        )
        .await
        .unwrap();
    assert!(!out.rows.is_empty());
    for row in &out.rows {
        assert_eq!(row.fields["region"], "eu");
    }
}

#[tokio::test]
async fn in_list_filter_accepts_multiple_values() {
    let eng = engine_with_docs(&[
        ("1", "x", serde_json::json!({"region": "eu"})),
        ("2", "x", serde_json::json!({"region": "us"})),
        ("3", "x", serde_json::json!({"region": "ap"})),
    ])
    .await;

    let out = eng
        .run(
            "SELECT id, region FROM docs \
             WHERE semantic_match(content, 'x') AND region IN ('eu', 'us')",
        )
        .await
        .unwrap();
    for row in &out.rows {
        let r = row.fields["region"].as_str().unwrap();
        assert!(r == "eu" || r == "us");
    }
}

#[tokio::test]
async fn projection_includes_virtual_columns() {
    let eng = engine_with_docs(&[(
        "1",
        "some text",
        serde_json::json!({"region": "eu", "nested": {"k": 7}}),
    )])
    .await;

    let out = eng
        .run(
            "SELECT id, score, text, nested.k FROM docs \
             WHERE semantic_match(content, 'some')",
        )
        .await
        .unwrap();
    let row = &out.rows[0];
    assert_eq!(row.fields["id"], "1");
    assert!(row.fields["score"].is_number());
    assert_eq!(row.fields["text"], "some text");
    assert_eq!(row.fields["nested.k"], 7);
}

#[tokio::test]
async fn limit_truncates_after_filter() {
    let docs: Vec<(&str, &str, serde_json::Value)> = (0..20)
        .map(|i| -> (&str, &str, serde_json::Value) {
            // Leak the id strings so the tuple can hold &'static str
            // without gymnastics. Fine for tests.
            let id: &'static str = Box::leak(format!("d{i}").into_boxed_str());
            (id, "same content", serde_json::json!({"region": "eu"}))
        })
        .collect();
    let eng = engine_with_docs(&docs).await;

    let out = eng
        .run(
            "SELECT id FROM docs \
             WHERE semantic_match(content, 'same') AND region = 'eu' LIMIT 3",
        )
        .await
        .unwrap();
    assert_eq!(out.rows.len(), 3);
}

#[tokio::test]
async fn order_by_metadata_sorts() {
    let eng = engine_with_docs(&[
        ("1", "x", serde_json::json!({"region": "c"})),
        ("2", "x", serde_json::json!({"region": "a"})),
        ("3", "x", serde_json::json!({"region": "b"})),
    ])
    .await;

    let out = eng
        .run(
            "SELECT id, region FROM docs \
             WHERE semantic_match(content, 'x') ORDER BY region ASC",
        )
        .await
        .unwrap();
    let regions: Vec<&str> = out
        .rows
        .iter()
        .map(|r| r.fields["region"].as_str().unwrap())
        .collect();
    assert_eq!(regions, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn cache_returns_same_result_across_calls() {
    use nebula_sql::{cache::SemanticCacheConfig, SemanticCache};

    // Build the index directly so we can Arc-share it with the engine.
    let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(32));
    let index = Arc::new(
        TextIndex::new(emb, Metric::Cosine, HnswConfig::default()).unwrap(),
    );
    index
        .upsert_text("docs", "1", "alpha", serde_json::json!({"r": "eu"}))
        .await
        .unwrap();

    let cache = SemanticCache::new(SemanticCacheConfig::default());
    let eng = SqlEngine::new(index).with_cache(Arc::clone(&cache));

    let sql = "SELECT * FROM docs WHERE semantic_match(content, 'alpha') LIMIT 1";
    let a = eng.run(sql).await.unwrap();
    let b = eng.run(sql).await.unwrap();
    assert_eq!(a.rows, b.rows);
    let (hits, _, _) = cache.snapshot();
    assert_eq!(hits, 1, "second call should be a cache hit");
}
