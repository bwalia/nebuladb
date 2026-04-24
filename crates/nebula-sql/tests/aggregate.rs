//! GROUP BY + aggregate executor tests.

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
async fn count_star_groups_by_region() {
    let eng = engine_with_docs(&[
        ("1", "widgets", serde_json::json!({"region": "eu"})),
        ("2", "widgets", serde_json::json!({"region": "eu"})),
        ("3", "widgets", serde_json::json!({"region": "us"})),
    ])
    .await;

    let out = eng
        .run(
            "SELECT region, COUNT(*) AS n FROM docs \
             WHERE semantic_match(content, 'widgets') \
             GROUP BY region ORDER BY n DESC",
        )
        .await
        .unwrap();
    assert_eq!(out.rows.len(), 2);
    // Ordered by n descending, eu (2) first.
    assert_eq!(out.rows[0].fields["region"], "eu");
    assert_eq!(out.rows[0].fields["n"], 2);
    assert_eq!(out.rows[1].fields["region"], "us");
    assert_eq!(out.rows[1].fields["n"], 1);
}

#[tokio::test]
async fn sum_and_avg_work_with_default_alias() {
    let eng = engine_with_docs(&[
        ("1", "x", serde_json::json!({"region": "eu", "v": 10})),
        ("2", "x", serde_json::json!({"region": "eu", "v": 20})),
        ("3", "x", serde_json::json!({"region": "us", "v": 5})),
    ])
    .await;

    let out = eng
        .run(
            "SELECT region, SUM(v), AVG(v) FROM docs \
             WHERE semantic_match(content, 'x') \
             GROUP BY region ORDER BY region",
        )
        .await
        .unwrap();
    assert_eq!(out.rows.len(), 2);
    assert_eq!(out.rows[0].fields["region"], "eu");
    assert_eq!(out.rows[0].fields["sum_v"], 30.0);
    assert_eq!(out.rows[0].fields["avg_v"], 15.0);
    assert_eq!(out.rows[1].fields["sum_v"], 5.0);
}

#[tokio::test]
async fn min_max_on_strings() {
    let eng = engine_with_docs(&[
        ("1", "x", serde_json::json!({"region": "eu", "name": "beta"})),
        ("2", "x", serde_json::json!({"region": "eu", "name": "alpha"})),
        ("3", "x", serde_json::json!({"region": "eu", "name": "gamma"})),
    ])
    .await;

    let out = eng
        .run(
            "SELECT region, MIN(name) AS first, MAX(name) AS last FROM docs \
             WHERE semantic_match(content, 'x') GROUP BY region",
        )
        .await
        .unwrap();
    assert_eq!(out.rows[0].fields["first"], "alpha");
    assert_eq!(out.rows[0].fields["last"], "gamma");
}

#[tokio::test]
async fn projection_must_be_in_group_by_or_aggregated() {
    let eng = engine_with_docs(&[("1", "x", serde_json::json!({"region": "eu"}))]).await;
    // `name` is neither grouped nor aggregated.
    let err = eng
        .run(
            "SELECT region, name FROM docs \
             WHERE semantic_match(content, 'x') GROUP BY region",
        )
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("must be in GROUP BY") || msg.contains("aggregated"), "{msg}");
}

#[tokio::test]
async fn rejects_select_star_with_group_by() {
    let eng = engine_with_docs(&[("1", "x", serde_json::json!({"region": "eu"}))]).await;
    let err = eng
        .run(
            "SELECT * FROM docs WHERE semantic_match(content, 'x') GROUP BY region",
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("SELECT *"));
}

#[tokio::test]
async fn order_by_must_reference_known_key() {
    let eng = engine_with_docs(&[("1", "x", serde_json::json!({"region": "eu"}))]).await;
    let err = eng
        .run(
            "SELECT region FROM docs WHERE semantic_match(content, 'x') \
             GROUP BY region ORDER BY unknown_thing",
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not a group-by column or aggregate"));
}
