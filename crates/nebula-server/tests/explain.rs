//! HTTP-level EXPLAIN: every query surface returns an `explain` object
//! when asked, and responses are unchanged when it isn't.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::ServiceExt;

use nebula_embed::{Embedder, MockEmbedder};
use nebula_index::TextIndex;
use nebula_server::{build_router, AppConfig, AppState};
use nebula_vector::{HnswConfig, Metric};

async fn state() -> AppState {
    let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(32));
    let index = Arc::new(TextIndex::new(emb, Metric::Cosine, HnswConfig::default()).unwrap());
    for (id, text, city) in [
        ("l1", "County: Kent\nRegion: South East florist", "Canterbury"),
        ("l2", "County: London\nRegion: London florist", "London"),
        ("l3", "County: Essex\nRegion: East builder", "Chelmsford"),
    ] {
        index.upsert_text("leads", id, text, json!({ "city": city })).await.unwrap();
    }
    AppState::new(index, AppConfig::default())
}

async fn post(state: AppState, path: &str, body: Value) -> (StatusCode, String) {
    let res = build_router(state)
        .oneshot(
            Request::post(path)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = to_bytes(res.into_body(), 4 * 1024 * 1024).await.unwrap();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

fn stage_names(ex: &Value) -> Vec<String> {
    ex["stages"].as_array().unwrap().iter().map(|s| s["name"].as_str().unwrap().to_string()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_explain_is_opt_in() {
    let st = state().await;
    let (code, body) = post(st.clone(), "/api/v1/ai/search", json!({"query": "florist", "top_k": 2})).await;
    assert_eq!(code, StatusCode::OK);
    assert!(!body.contains("\"explain\""), "unrequested explain leaked: {body}");

    let (code, body) = post(
        st,
        "/api/v1/ai/search",
        json!({"query": "florist Kent", "top_k": 2, "bucket": "leads", "hybrid": true, "explain": true}),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{body}");
    let v: Value = serde_json::from_str(&body).unwrap();
    let ex = &v["explain"];
    assert_eq!(ex["kind"], "search");
    assert_eq!(stage_names(ex), ["embed", "hnsw", "bucket_filter", "bm25", "bucket_filter", "fuse"]);
    assert_eq!(ex["hits"].as_array().unwrap().len(), v["hits"].as_array().unwrap().len());
    assert!(ex["hits"][0]["bm25"]["weight"].is_number());
    assert!(ex["summary"].as_str().unwrap().starts_with("Hybrid search"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_explain_flag_and_statement() {
    let st = state().await;
    let sql = "SELECT id, city FROM leads WHERE semantic_match(text, 'florist') AND city = 'London' LIMIT 2";
    let (_, plain) = post(st.clone(), "/api/v1/query", json!({ "sql": sql })).await;
    assert!(!plain.contains("\"explain\""));
    let (code, body) = post(st.clone(), "/api/v1/query", json!({ "sql": sql, "explain": true })).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    let v: Value = serde_json::from_str(&body).unwrap();
    let p: Value = serde_json::from_str(&plain).unwrap();
    assert_eq!(v["rows"], p["rows"], "explain changed the result");
    assert_eq!(v["explain"]["kind"], "sql");
    assert!(stage_names(&v["explain"]).contains(&"filter".to_string()));

    let (code, body) = post(st, "/api/v1/query", json!({ "sql": format!("EXPLAIN ANALYZE {sql}") })).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    let v: Value = serde_json::from_str(&body).unwrap();
    let first = v["rows"][0]["fields"]["QUERY PLAN"].as_str().unwrap();
    assert!(first.starts_with("Project (SELECT list)"), "{first}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rag_stream_sends_explain_before_done() {
    let (code, body) = post(
        state().await,
        "/api/v1/ai/rag",
        json!({"query": "who sells flowers?", "top_k": 2, "stream": true, "explain": true}),
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    let explain_at = body.find("event: explain").expect("explain frame");
    let last_delta = body.rfind("event: answer_delta").expect("answer");
    let done_at = body.find("event: done").expect("done");
    assert!(last_delta < explain_at && explain_at < done_at, "frame order wrong:\n{body}");
    assert_eq!(body.matches("event: explain").count(), 1);
    let data = body[explain_at..].lines().nth(1).unwrap().trim_start_matches("data: ");
    let ex: Value = serde_json::from_str(data).unwrap();
    assert!(stage_names(&ex).ends_with(&["prompt".to_string(), "llm".to_string()]));
    assert!(ex["prompt"].as_str().unwrap().contains("Question: who sells flowers?"));

    // Without explain the stream carries no explain frame.
    let (_, body) = post(state().await, "/api/v1/ai/rag", json!({"query": "flowers", "stream": true})).await;
    assert!(!body.contains("event: explain"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rag_json_and_answer_endpoints_explain() {
    let (_, body) = post(
        state().await,
        "/api/v1/ai/rag",
        json!({"query": "florist", "top_k": 2, "explain": true}),
    )
    .await;
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["explain"]["kind"], "rag");
    assert!(v["explain"]["text"].as_array().unwrap().len() > 2);

    let (code, body) = post(
        state().await,
        "/api/v1/rag/answer",
        json!({"query": "florist", "top_k": 2, "hybrid": true, "rerank": true, "explain": true}),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{body}");
    let v: Value = serde_json::from_str(&body).unwrap();
    let names = stage_names(&v["explain"]);
    assert!(names.contains(&"fuse".to_string()) && names.contains(&"rerank".to_string()), "{names:?}");
    assert_eq!(v["explain"]["hits"].as_array().unwrap().len(), v["sources"].as_array().unwrap().len());
}
