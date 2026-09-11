//! EXPLAIN / EXPLAIN ANALYZE: explained runs must return exactly what
//! normal runs return, and the explanation must say something true and
//! useful about how the rows were produced.

use std::sync::Arc;

use nebula_embed::{Embedder, MockEmbedder};
use nebula_index::explain::NoteLevel;
use nebula_index::TextIndex;
use nebula_llm::{LlmClient, MockLlm};
use nebula_sql::{SemanticCache, SqlEngine};
use nebula_vector::{HnswConfig, Metric};
use serde_json::json;

async fn engine() -> SqlEngine {
    let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(32));
    let index = Arc::new(TextIndex::new(emb, Metric::Cosine, HnswConfig::default()).unwrap());
    let leads = [
        ("l1", "florist in london", json!({"city": "London", "status": "1", "employees": 4})),
        ("l2", "flower shop", json!({"city": "London", "status": "0", "employees": 9})),
        ("l3", "engineering firm", json!({"city": "Leeds", "status": "1", "employees": 40})),
        ("l4", "motor parts", json!({"city": "Leeds", "status": "1", "employees": 12})),
        ("l5", "training services", json!({"city": "Bristol", "status": "0", "employees": 3})),
        ("l6", "food wholesale", json!({"city": "London", "status": "1", "employees": 22})),
    ];
    for (id, text, meta) in leads {
        index.upsert_text("leads", id, text, meta).await.unwrap();
    }
    for (id, text, meta) in [
        ("o1", "order one", json!({"lead": "l1", "total": 10})),
        ("o2", "order two", json!({"lead": "l3", "total": 7})),
    ] {
        index.upsert_text("orders", id, text, meta).await.unwrap();
    }
    let llm: Arc<dyn LlmClient> = Arc::new(MockLlm::default());
    SqlEngine::new(index)
        .with_cache(SemanticCache::new(Default::default()))
        .with_llm(llm)
}

const QUERIES: &[&str] = &[
    "SELECT * FROM leads WHERE semantic_match(text, 'flowers') LIMIT 3",
    "SELECT id, city FROM leads WHERE semantic_match(text, 'shop') AND city = 'London' LIMIT 5",
    "SELECT id FROM leads WHERE semantic_match(text, 'x') AND city IN ('Leeds', 'Bristol') ORDER BY employees DESC LIMIT 4",
    "SELECT city, COUNT(*) AS n, AVG(employees) AS avg FROM leads WHERE semantic_match(text, 'business') GROUP BY city ORDER BY n DESC",
    "SELECT l.id, o.total FROM leads l JOIN orders o ON l.id = o.lead WHERE semantic_match(l.text, 'florist') AND semantic_match(o.text, 'order') LIMIT 10",
];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explained_runs_return_the_same_rows() {
    let eng = engine().await;
    for sql in QUERIES {
        let plain = eng.run(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        let (explained, ex) = eng.execute(sql, true).await.unwrap();
        assert_eq!(explained.rows, plain.rows, "{sql}");
        let ex = ex.expect("explain requested");
        assert!(ex.analyzed);
        assert!(!ex.summary.is_empty() && !ex.text.is_empty(), "{sql}");
        assert!(ex.plan.is_some());
        assert!(ex.stages.iter().any(|s| s.name == "hnsw"), "{sql}: no retrieval stage");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scan_stages_and_per_row_filters() {
    let eng = engine().await;
    let (out, ex) = eng
        .execute("SELECT id FROM leads WHERE semantic_match(text, 'shop') AND city = 'London' LIMIT 5", true)
        .await
        .unwrap();
    let ex = ex.unwrap();
    let names: Vec<_> = ex.stages.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(
        names,
        ["parse", "plan", "top_k", "embed", "hnsw", "bucket_filter", "filter", "sort", "limit", "project"]
    );
    let filter = ex.stages.iter().find(|s| s.name == "filter").unwrap();
    assert_eq!(filter.rows_out, Some(out.rows.len()));
    assert_eq!(ex.hits.len(), out.rows.len());
    for (row, hit) in out.rows.iter().zip(&ex.hits) {
        assert_eq!(row.id, hit.id);
        let checks = hit.filters.as_ref().unwrap();
        assert_eq!(checks[0].predicate, "city = 'London'");
        assert!(checks[0].passed);
    }
    assert!(ex.notes.iter().any(|n| n.message.contains("result cache was bypassed")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn filter_diagnostics_name_the_real_problem() {
    let eng = engine().await;
    // Misspelt field.
    let (_, ex) = eng
        .execute("SELECT id FROM leads WHERE semantic_match(text, 'x') AND town = 'London' LIMIT 3", true)
        .await
        .unwrap();
    let notes = ex.unwrap().notes;
    assert!(
        notes.iter().any(|n| n.level == NoteLevel::Warn
            && n.message.contains("metadata field 'town'")
            && n.message.contains("Fields present: city, employees, status")),
        "{notes:?}"
    );
    // Right field, wrong case.
    let (_, ex) = eng
        .execute("SELECT id FROM leads WHERE semantic_match(text, 'x') AND city = 'LONDON' LIMIT 3", true)
        .await
        .unwrap();
    let notes = ex.unwrap().notes;
    let values = notes.iter().find(|n| n.message.contains("Values seen for city")).expect("values note");
    assert!(values.message.contains("'London' ×3"), "{}", values.message);
    // Only 6 leads exist and all were retrieved, so this is not
    // starvation — nothing further out could have matched.
    assert!(!notes.iter().any(|n| n.message.starts_with("Filter starvation")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explain_analyze_statement_returns_query_plan_rows() {
    let eng = engine().await;
    let (out, ex) = eng
        .execute("EXPLAIN ANALYZE SELECT id FROM leads WHERE semantic_match(text, 'shop') LIMIT 2", false)
        .await
        .unwrap();
    let ex = ex.unwrap();
    assert!(ex.analyzed);
    assert_eq!(out.rows.len(), ex.text.len());
    assert_eq!(out.rows[0].fields["QUERY PLAN"], json!(ex.text[0]));
    assert!(ex.text.last().unwrap().starts_with("Execution Time:"));
    // `run` (pgwire's entry point) returns the same rows.
    let via_run = eng
        .run("EXPLAIN ANALYZE SELECT id FROM leads WHERE semantic_match(text, 'shop') LIMIT 2")
        .await
        .unwrap();
    assert_eq!(via_run.rows.len(), out.rows.len());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_explain_executes_nothing() {
    let eng = engine().await;
    let (out, ex) = eng
        .execute("EXPLAIN SELECT id FROM leads WHERE semantic_match(text, 'shop') AND city = 'London' LIMIT 5", false)
        .await
        .unwrap();
    let ex = ex.unwrap();
    assert!(!ex.analyzed);
    assert!(ex.stages.iter().all(|s| s.took_us == 0 && s.rows_out.is_none()));
    assert!(ex.stages.iter().any(|s| s.detail.contains("32 candidates")), "{:?}", ex.stages);
    assert!(ex.hits.is_empty());
    assert!(out.rows.iter().all(|r| r.fields.get("QUERY PLAN").is_some()));
    assert!(ex.summary.contains("Nothing was executed"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ai_answer_explain_carries_the_prompt() {
    let eng = engine().await;
    let (_, ex) = eng
        .execute("SELECT ai_answer('who sells flowers?', 'leads', 2)", true)
        .await
        .unwrap();
    let ex = ex.unwrap();
    let names: Vec<_> = ex.stages.iter().map(|s| s.name.as_str()).collect();
    assert!(names.ends_with(&["prompt", "llm"]), "{names:?}");
    let prompt = ex.prompt.expect("prompt recorded");
    assert!(prompt.contains("Question: who sells flowers?"));
}

#[test]
fn every_plan_shape_serializes() {
    // Aggregates with a column argument used to fail to serialize, so
    // /query/explain returned 500 for them.
    for sql in QUERIES {
        let plan = nebula_sql::plan_tree::build(nebula_sql::parser::parse(sql).unwrap()).unwrap();
        let v = serde_json::to_value(&plan).unwrap_or_else(|e| panic!("{sql}: {e}"));
        if sql.contains("AVG(") {
            assert!(v.to_string().contains(r#"{"col":"employees","fn":"avg"}"#), "{v}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn filter_starvation_is_flagged_when_more_candidates_exist() {
    let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(32));
    let index = Arc::new(TextIndex::new(emb, Metric::Cosine, HnswConfig::default()).unwrap());
    for i in 0..60 {
        let city = if i % 20 == 0 { "London" } else { "Leeds" };
        index
            .upsert_text("leads", &format!("l{i}"), &format!("lead number {i}"), json!({ "city": city }))
            .await
            .unwrap();
    }
    let eng = SqlEngine::new(index);
    let (out, ex) = eng
        .execute("SELECT id FROM leads WHERE semantic_match(text, 'lead') AND city = 'London' LIMIT 5", true)
        .await
        .unwrap();
    assert!(out.rows.len() < 5);
    let ex = ex.unwrap();
    assert!(
        ex.notes.iter().any(|n| n.message.starts_with("Filter starvation")),
        "{:?}",
        ex.notes
    );
    assert!(ex.summary.contains("Most rows were dropped by Filter (WHERE)"), "{}", ex.summary);
}
