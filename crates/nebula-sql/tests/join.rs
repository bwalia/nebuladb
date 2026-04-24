//! INNER JOIN executor tests.

use std::sync::Arc;

use nebula_embed::{Embedder, MockEmbedder};
use nebula_index::TextIndex;
use nebula_sql::SqlEngine;
use nebula_vector::{HnswConfig, Metric};

async fn engine() -> SqlEngine {
    let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(32));
    let index = Arc::new(
        TextIndex::new(emb, Metric::Cosine, HnswConfig::default()).unwrap(),
    );
    // Bucket "users": id is the external id, region is metadata.
    index
        .upsert_text("users", "u1", "alpha team", serde_json::json!({"region": "eu"}))
        .await
        .unwrap();
    index
        .upsert_text("users", "u2", "alpha team", serde_json::json!({"region": "us"}))
        .await
        .unwrap();
    index
        .upsert_text("users", "u3", "alpha team", serde_json::json!({"region": "eu"}))
        .await
        .unwrap();
    // Bucket "orders": each carries a region so we can join on region.
    index
        .upsert_text("orders", "o1", "order one", serde_json::json!({"region": "eu", "total": 10}))
        .await
        .unwrap();
    index
        .upsert_text("orders", "o2", "order two", serde_json::json!({"region": "us", "total": 7}))
        .await
        .unwrap();
    index
        .upsert_text("orders", "o3", "order three", serde_json::json!({"region": "eu", "total": 20}))
        .await
        .unwrap();
    SqlEngine::new(index)
}

#[tokio::test]
async fn inner_join_on_metadata_column() {
    let eng = engine().await;
    let out = eng
        .run(
            "SELECT u.id, o.id, u.region, o.region, o.total \
             FROM users AS u JOIN orders AS o ON u.region = o.region \
             WHERE semantic_match(u.content, 'alpha') \
               AND semantic_match(o.content, 'order') \
             ORDER BY o.total ASC",
        )
        .await
        .unwrap();

    // 2 eu users × 2 eu orders = 4; 1 us user × 1 us order = 1 → 5 total.
    assert_eq!(out.rows.len(), 5);

    // All pairs have matching regions on both sides of the join.
    for row in &out.rows {
        let ur = row.fields["u.region"].as_str().unwrap();
        let or = row.fields["o.region"].as_str().unwrap();
        assert_eq!(ur, or, "join key mismatch: {row:?}");
    }

    // Sorted by total ascending: first row must be the smallest total.
    let totals: Vec<f64> = out
        .rows
        .iter()
        .map(|r| r.fields["o.total"].as_f64().unwrap())
        .collect();
    let mut sorted = totals.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(totals, sorted);
}

#[tokio::test]
async fn join_with_residual_filter_per_side() {
    let eng = engine().await;
    // Restrict to EU only on the left side; the join still constrains
    // the right side to matching keys.
    let out = eng
        .run(
            "SELECT u.id, o.id \
             FROM users AS u JOIN orders AS o ON u.region = o.region \
             WHERE semantic_match(u.content, 'alpha') AND u.region = 'eu' \
               AND semantic_match(o.content, 'order')",
        )
        .await
        .unwrap();
    // 2 eu users × 2 eu orders = 4.
    assert_eq!(out.rows.len(), 4);
}

#[tokio::test]
async fn rejects_outer_join() {
    let eng = engine().await;
    let err = eng
        .run(
            "SELECT * FROM users AS u LEFT JOIN orders AS o ON u.region = o.region \
             WHERE semantic_match(u.content, 'x') AND semantic_match(o.content, 'y')",
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("INNER JOIN"));
}

#[tokio::test]
async fn rejects_unqualified_where() {
    let eng = engine().await;
    let err = eng
        .run(
            "SELECT * FROM users AS u JOIN orders AS o ON u.region = o.region \
             WHERE region = 'eu'",
        )
        .await
        .unwrap_err();
    // Unqualified columns can't be routed to a side.
    assert!(err.to_string().contains("qualify columns"));
}

#[tokio::test]
async fn rejects_multi_way_join() {
    let eng = engine().await;
    let err = eng
        .run(
            "SELECT * FROM a JOIN b ON a.x = b.x JOIN c ON b.y = c.y \
             WHERE semantic_match(a.content, 'x') AND semantic_match(b.content, 'y') \
               AND semantic_match(c.content, 'z')",
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("multi-way"));
}
