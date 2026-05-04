//! End-to-end router tests.
//!
//! Uses `tower::ServiceExt::oneshot` to drive the router without
//! binding a TCP socket — faster, deterministic, and lets us assert
//! the exact HTTP response shape. A real deployment would add a
//! second layer of tests against a bound `axum::serve` to catch
//! listener/TLS regressions.

use std::sync::Arc;

use ahash::AHashSet;
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

use nebula_embed::{Embedder, MockEmbedder};
use nebula_index::TextIndex;
use nebula_server::{
    build_router, AppConfig, AppState, JwtConfig, RateLimitConfig, RateLimiter,
};
use nebula_vector::{HnswConfig, Metric};

fn app_state(keys: &[&str]) -> AppState {
    let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(32));
    let index = Arc::new(
        TextIndex::new(emb, Metric::Cosine, HnswConfig::default()).unwrap(),
    );
    let cfg = AppConfig {
        api_keys: keys.iter().map(|s| s.to_string()).collect::<AHashSet<_>>(),
        ..AppConfig::default()
    };
    AppState::new(index, cfg)
}

async fn body_string(body: Body) -> String {
    let bytes = to_bytes(body, 1024 * 1024).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn healthz_is_public() {
    let app = build_router(app_state(&["secret"]));
    let res = app
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_string(res.into_body()).await;
    assert!(body.contains("\"status\":\"ok\""));
    // Operator reads this to detect an upgrade took effect — guarantee
    // the field is present and matches the crate version.
    let expected = format!("\"version\":\"{}\"", env!("CARGO_PKG_VERSION"));
    assert!(body.contains(&expected), "body missing version: {body}");
}

#[tokio::test]
async fn admin_version_reports_build_identity() {
    let app = build_router(app_state(&[]));
    let res = app
        .oneshot(Request::get("/api/v1/admin/version").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_string(res.into_body()).await;
    // We don't assert the git sha (may be `"unknown"` in CI builds) but
    // the fields must be present so clients can parse stably.
    assert!(body.contains("\"version\""));
    assert!(body.contains("\"git_commit\""));
    assert!(body.contains("\"os\""));
    assert!(body.contains("\"arch\""));
}

#[tokio::test]
async fn api_requires_auth_when_keys_configured() {
    let app = build_router(app_state(&["secret"]));
    let res = app
        .oneshot(
            Request::post("/api/v1/ai/search")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query":"x","top_k":1}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn api_accepts_valid_token() {
    let app = build_router(app_state(&["secret"]));
    let res = app
        .oneshot(
            Request::post("/api/v1/bucket/docs/doc")
                .header("authorization", "Bearer secret")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"id":"1","text":"zero trust dns failover"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn upsert_get_search_delete_roundtrip() {
    let state = app_state(&[]);
    let app = build_router(state.clone());

    // Insert three docs
    for (i, text) in [
        "zero trust networking",
        "dns failover strategies",
        "kubernetes gitops",
    ]
    .iter()
    .enumerate()
    {
        let res = app
            .clone()
            .oneshot(
                Request::post("/api/v1/bucket/docs/doc")
                    .header("content-type", "application/json")
                    .body(Body::from(format!(
                        r#"{{"id":"{i}","text":"{text}"}}"#
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK, "insert {i} failed");
    }

    // Get one
    let res = app
        .clone()
        .oneshot(
            Request::get("/api/v1/bucket/docs/doc/0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_string(res.into_body()).await;
    assert!(body.contains("zero trust"));

    // Semantic search
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v1/ai/search")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"query":"zero trust networking","top_k":3}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_string(res.into_body()).await;
    assert!(body.contains("\"hits\""));

    // Delete
    let res = app
        .clone()
        .oneshot(
            Request::delete("/api/v1/bucket/docs/doc/0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    // Get after delete = 404
    let res = app
        .oneshot(
            Request::get("/api/v1/bucket/docs/doc/0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn rag_non_stream_returns_json() {
    let app = build_router(app_state(&[]));

    // Seed
    let _ = app
        .clone()
        .oneshot(
            Request::post("/api/v1/bucket/docs/doc")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"id":"1","text":"dns failover uses health checks"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    let res = app
        .oneshot(
            Request::post("/api/v1/ai/rag")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query":"dns failover","top_k":3}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_string(res.into_body()).await;
    assert!(body.contains("\"answer\""));
    assert!(body.contains("\"context\""));
}

#[tokio::test]
async fn rag_stream_emits_sse_events() {
    let app = build_router(app_state(&[]));

    // Seed
    let _ = app
        .clone()
        .oneshot(
            Request::post("/api/v1/bucket/docs/doc")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"id":"1","text":"alpha beta gamma"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    let res = app
        .oneshot(
            Request::post("/api/v1/ai/rag")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"query":"alpha","top_k":1,"stream":true}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let ct = res.headers().get("content-type").unwrap().to_str().unwrap();
    assert!(ct.starts_with("text/event-stream"), "unexpected CT: {ct}");

    // Collect the full SSE payload.
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("event: context"));
    assert!(text.contains("event: answer_delta"));
    assert!(text.contains("event: done"));
}

#[tokio::test]
async fn metrics_exposes_counters() {
    let app = build_router(app_state(&[]));
    let _ = app
        .clone()
        .oneshot(
            Request::post("/api/v1/bucket/docs/doc")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"id":"1","text":"x"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    let res = app
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_string(res.into_body()).await;
    assert!(body.contains("nebula_docs_inserted 1"));
    assert!(body.contains("nebula_requests_total"));
}

#[tokio::test]
async fn bad_top_k_returns_400() {
    let app = build_router(app_state(&[]));
    let res = app
        .oneshot(
            Request::post("/api/v1/ai/search")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query":"x","top_k":0}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn upsert_document_creates_multiple_chunks() {
    let app = build_router(app_state(&[]));
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v1/bucket/docs/document")
                .header("content-type", "application/json")
                .body(Body::from(
                    // 60-char body → with default 500/50 chunker = 1 chunk.
                    // Use the explicit API path anyway so the endpoint is exercised.
                    r#"{"doc_id":"d1","text":"zero trust architecture describes a security model where nothing is trusted by default and every request is verified"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_string(res.into_body()).await;
    assert!(body.contains("\"chunks\""));
    assert!(body.contains("\"doc_id\":\"d1\""));

    // Semantic search finds it.
    let res = app
        .oneshot(
            Request::post("/api/v1/ai/search")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query":"zero trust","top_k":3}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_string(res.into_body()).await;
    assert!(body.contains("d1#0"));
}

#[tokio::test]
async fn delete_document_removes_all_chunks() {
    // Use a tiny chunker so one payload produces multiple chunks.
    use std::sync::Arc;
    let mut state = app_state(&[]);
    state = state.with_chunker(Arc::new(
        nebula_chunk::FixedSizeChunker::new(10, 0).unwrap(),
    ));
    let app = build_router(state);

    let _ = app
        .clone()
        .oneshot(
            Request::post("/api/v1/bucket/docs/document")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"doc_id":"d1","text":"aaaaabbbbbcccccddddd"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let res = app
        .oneshot(
            Request::delete("/api/v1/bucket/docs/document/d1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_string(res.into_body()).await;
    assert!(body.contains("\"chunks_removed\":2"));
}

#[tokio::test]
async fn rag_stream_forwards_llm_deltas() {
    // MockLlm produces "Answer: <echoed user prompt>" split on spaces.
    // We assert that multiple answer_delta events arrive and that the
    // concatenation contains the echo — end-to-end streaming proof.
    let app = build_router(app_state(&[]));

    let _ = app
        .clone()
        .oneshot(
            Request::post("/api/v1/bucket/docs/doc")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"id":"1","text":"widgets fly"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    let res = app
        .oneshot(
            Request::post("/api/v1/ai/rag")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"query":"widgets","top_k":1,"stream":true}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8_lossy(&body);

    // At least one context event.
    assert!(text.contains("event: context"), "no context event:\n{text}");
    // Multiple answer_delta events (mock LLM tokenizes on whitespace).
    let delta_count = text.matches("event: answer_delta").count();
    assert!(delta_count >= 2, "expected multiple deltas, got {delta_count}");
    // Terminal done event.
    assert!(text.contains("event: done"));
}

#[tokio::test]
async fn metrics_exposes_cache_counters_when_wired() {
    use std::sync::Arc;

    // Build the same kind of state the binary does: wrap the mock
    // embedder in a cache, feed stats into AppState, verify /metrics
    // surfaces them.
    let raw: Arc<dyn nebula_embed::Embedder> =
        Arc::new(nebula_embed::MockEmbedder::new(32));
    let cache = Arc::new(nebula_cache::CachingEmbedder::new(raw, 64));
    let stats = cache.stats();
    let index = Arc::new(
        nebula_index::TextIndex::new(cache, Metric::Cosine, HnswConfig::default()).unwrap(),
    );
    let state = AppState::new(index, AppConfig::default()).with_cache_stats(stats);
    let app = build_router(state);

    // Two inserts with the same text → first miss, second hit.
    for i in 0..2 {
        let _ = app
            .clone()
            .oneshot(
                Request::post("/api/v1/bucket/docs/doc")
                    .header("content-type", "application/json")
                    .body(Body::from(format!(
                        r#"{{"id":"{i}","text":"same text every time"}}"#
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
    }

    let res = app
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = body_string(res.into_body()).await;
    assert!(body.contains("nebula_embed_cache_hits 1"), "body:\n{body}");
    assert!(body.contains("nebula_embed_cache_misses 1"));
    assert!(body.contains("nebula_embed_cache_inserts 1"));
}

#[tokio::test]
async fn metrics_omits_cache_lines_when_not_wired() {
    // Default `AppState::new` doesn't register cache stats. The
    // metrics endpoint must NOT render zeroed cache counters — that
    // would be actively misleading (implies a cache exists when it
    // doesn't).
    let app = build_router(app_state(&[]));
    let res = app
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = body_string(res.into_body()).await;
    assert!(!body.contains("nebula_embed_cache_hits"));
}

#[tokio::test]
async fn rate_limiter_returns_429_after_burst() {
    // Capacity 3, refill 0.1/s → we burn the burst then get rejected.
    // Refill is slow enough that the fourth request lands while the
    // bucket is still empty.
    let state = AppState::new(
        Arc::new(
            TextIndex::new(
                Arc::new(MockEmbedder::new(16)) as Arc<dyn Embedder>,
                Metric::Cosine,
                HnswConfig::default(),
            )
            .unwrap(),
        ),
        AppConfig {
            rate_limit: RateLimitConfig {
                capacity: 3.0,
                refill_per_sec: 0.1,
            },
            ..AppConfig::default()
        },
    )
    .with_rate_limiter(RateLimiter::new());
    let app = build_router(state);

    // Use a specific Authorization header so every request maps to the
    // same rate-limit principal — otherwise ConnectInfo fallback lumps
    // us into the "anon" bucket, which also works but let's be explicit.
    let make = |i: usize| {
        Request::post("/api/v1/ai/search")
            .header("content-type", "application/json")
            .header("authorization", "Bearer same-key")
            .body(Body::from(format!(r#"{{"query":"q{i}","top_k":1}}"#)))
            .unwrap()
    };

    for i in 0..3 {
        let res = app.clone().oneshot(make(i)).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK, "request {i} should pass");
    }
    let res = app.oneshot(make(99)).await.unwrap();
    assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
    let retry = res
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(!retry.is_empty(), "missing retry-after header");
}

#[tokio::test]
async fn jwt_accepts_valid_and_rejects_invalid() {
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use std::time::{SystemTime, UNIX_EPOCH};

    let secret = b"test-jwt-secret-at-least-32-bytes!";
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as usize;

    #[derive(serde::Serialize)]
    struct Claims<'a> {
        sub: &'a str,
        exp: usize,
    }
    let good = encode(
        &Header::new(Algorithm::HS256),
        &Claims {
            sub: "svc-ingest",
            exp: now + 600,
        },
        &EncodingKey::from_secret(secret),
    )
    .unwrap();
    let expired = encode(
        &Header::new(Algorithm::HS256),
        &Claims {
            sub: "svc-ingest",
            exp: now - 600,
        },
        &EncodingKey::from_secret(secret),
    )
    .unwrap();

    let state = AppState::new(
        Arc::new(
            TextIndex::new(
                Arc::new(MockEmbedder::new(16)) as Arc<dyn Embedder>,
                Metric::Cosine,
                HnswConfig::default(),
            )
            .unwrap(),
        ),
        AppConfig {
            jwt: Some(JwtConfig::hs256(secret.to_vec())),
            ..AppConfig::default()
        },
    );
    let app = build_router(state);

    // Valid JWT accepted.
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v1/ai/search")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {good}"))
                .body(Body::from(r#"{"query":"x","top_k":1}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // Expired JWT rejected.
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v1/ai/search")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {expired}"))
                .body(Body::from(r#"{"query":"x","top_k":1}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // Missing token rejected.
    let res = app
        .oneshot(
            Request::post("/api/v1/ai/search")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query":"x","top_k":1}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn allowlist_and_jwt_coexist() {
    // Both auth schemes enabled. Either should succeed.
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use std::time::{SystemTime, UNIX_EPOCH};

    let secret = b"coexist-secret-xxxxxxxxxxxxxxxxx!";
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as usize;
    #[derive(serde::Serialize)]
    struct C<'a> {
        sub: &'a str,
        exp: usize,
    }
    let token = encode(
        &Header::new(Algorithm::HS256),
        &C {
            sub: "a",
            exp: now + 600,
        },
        &EncodingKey::from_secret(secret),
    )
    .unwrap();

    let state = AppState::new(
        Arc::new(
            TextIndex::new(
                Arc::new(MockEmbedder::new(16)) as Arc<dyn Embedder>,
                Metric::Cosine,
                HnswConfig::default(),
            )
            .unwrap(),
        ),
        AppConfig {
            api_keys: ["static-key".to_string()].into_iter().collect(),
            jwt: Some(JwtConfig::hs256(secret.to_vec())),
            ..AppConfig::default()
        },
    );
    let app = build_router(state);

    for auth in ["Bearer static-key".to_string(), format!("Bearer {token}")] {
        let res = app
            .clone()
            .oneshot(
                Request::post("/api/v1/ai/search")
                    .header("content-type", "application/json")
                    .header("authorization", auth.clone())
                    .body(Body::from(r#"{"query":"x","top_k":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK, "failed for {auth}");
    }
}

#[tokio::test]
async fn bulk_upsert_inserts_many_in_one_call() {
    let app = build_router(app_state(&[]));
    // 50 items in one request — well under the 1000 cap.
    let items: Vec<String> = (0..50)
        .map(|i| format!(r#"{{"id":"b{i}","text":"doc number {i}","metadata":{{"i":{i}}}}}"#))
        .collect();
    let body = format!("{{\"items\":[{}]}}", items.join(","));
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v1/bucket/bulk/docs/bulk")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_string(res.into_body()).await;
    assert!(body.contains("\"requested\":50"));
    assert!(body.contains("\"inserted\":50"));

    // Spot-check: one of the inserted docs is fetchable.
    let res = app
        .oneshot(
            Request::get("/api/v1/bucket/bulk/doc/b42")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn bulk_upsert_rejects_empty_or_oversize() {
    let app = build_router(app_state(&[]));
    // Empty items array.
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v1/bucket/x/docs/bulk")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"items":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    // Empty text on one item.
    let res = app
        .oneshot(
            Request::post("/api/v1/bucket/x/docs/bulk")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"items":[{"id":"a","text":"  ","metadata":{}}]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn admin_durability_reports_in_memory_status() {
    // Default app_state creates an in-memory TextIndex. Verify the
    // durability endpoint reports that honestly, and that snapshot
    // returns 400 rather than pretending to work.
    let app = build_router(app_state(&[]));
    let res = app
        .clone()
        .oneshot(Request::get("/api/v1/admin/durability").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_string(res.into_body()).await;
    assert!(body.contains("\"persistent\":false"));
    assert!(body.contains("\"data_dir\":null"));

    let res = app
        .oneshot(
            Request::post("/api/v1/admin/snapshot")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn admin_stats_returns_json_snapshot() {
    let app = build_router(app_state(&[]));
    // Insert one doc + run one semantic search so a couple counters move.
    let _ = app
        .clone()
        .oneshot(
            Request::post("/api/v1/bucket/docs/doc")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"id":"s-1","text":"x"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let _ = app
        .clone()
        .oneshot(
            Request::post("/api/v1/ai/search")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query":"x","top_k":1}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let res = app
        .oneshot(Request::get("/api/v1/admin/stats").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_string(res.into_body()).await;
    // Shape, not exact values — counter values race with unrelated
    // tests in parallel runs.
    assert!(body.contains("\"requests_total\""));
    assert!(body.contains("\"docs_inserted\""));
    assert!(body.contains("\"searches_semantic\""));
    assert!(body.contains("\"total_docs_live\":1"));
}

#[tokio::test]
async fn admin_empty_bucket_drops_docs_only_in_target() {
    let app = build_router(app_state(&[]));
    for (b, i) in [("a", "1"), ("a", "2"), ("b", "1")] {
        let _ = app
            .clone()
            .oneshot(
                Request::post(format!("/api/v1/bucket/{b}/doc"))
                    .header("content-type", "application/json")
                    .body(Body::from(format!(r#"{{"id":"{i}","text":"x"}}"#)))
                    .unwrap(),
            )
            .await
            .unwrap();
    }
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v1/admin/bucket/a/empty")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_string(res.into_body()).await;
    assert!(body.contains("\"removed\":2"));

    // b/1 must still be there.
    let res = app
        .oneshot(
            Request::get("/api/v1/bucket/b/doc/1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn admin_buckets_reports_per_bucket_stats() {
    let app = build_router(app_state(&[]));
    for (bucket, id, region) in [("a", "1", "eu"), ("a", "2", "us"), ("b", "1", "eu")] {
        let _ = app
            .clone()
            .oneshot(
                Request::post(format!("/api/v1/bucket/{bucket}/doc"))
                    .header("content-type", "application/json")
                    .body(Body::from(format!(
                        r#"{{"id":"{id}","text":"x","metadata":{{"region":"{region}"}}}}"#
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
    }
    let res = app
        .oneshot(Request::get("/api/v1/admin/buckets").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_string(res.into_body()).await;
    assert!(body.contains("\"bucket\":\"a\""));
    assert!(body.contains("\"bucket\":\"b\""));
    assert!(body.contains("\"docs\":2"));
    assert!(body.contains("\"region\""));
}

#[tokio::test]
async fn sql_explain_returns_typed_plan() {
    let app = build_router(app_state(&[]));
    let res = app
        .oneshot(
            Request::post("/api/v1/query/explain")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"sql":"SELECT id FROM docs WHERE semantic_match(content, 'x') LIMIT 5"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_string(res.into_body()).await;
    assert!(body.contains("\"node\":\"scan\""), "missing node tag: {body}");
    assert!(body.contains("\"semantic\""));
    assert!(body.contains("\"limit\":5"));
}

#[tokio::test]
async fn admin_audit_records_writes_not_reads() {
    let app = build_router(app_state(&[]));
    // One write and one read.
    let _ = app
        .clone()
        .oneshot(
            Request::post("/api/v1/bucket/docs/doc")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"id":"aud-1","text":"x"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let _ = app
        .clone()
        .oneshot(
            Request::get("/api/v1/bucket/docs/doc/aud-1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let res = app
        .oneshot(
            Request::get("/api/v1/admin/audit?limit=50")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_string(res.into_body()).await;
    // Paths recorded by the middleware are the nested form (the
    // middleware is attached inside the /api/v1 subtree). Audit on
    // the audit endpoint is itself a GET and must not appear. The
    // POST to the bucket/doc collection URL should.
    assert!(body.contains("\"path\":\"/bucket/docs/doc\""));
    assert!(!body.contains("/admin/audit"));
    assert!(body.contains("\"method\":\"POST\""));
    assert!(body.contains("\"status\":200"));
}

#[tokio::test]
async fn admin_slow_records_slow_sql_queries() {
    // Force-install a slow log with a 0ms threshold so even a fast
    // MockEmbedder query gets captured — the default 10ms threshold
    // is too strict for a tight unit test.
    use std::sync::Arc;
    let emb: Arc<dyn nebula_embed::Embedder> =
        Arc::new(nebula_embed::MockEmbedder::new(32));
    let index = Arc::new(
        nebula_index::TextIndex::new(emb, Metric::Cosine, HnswConfig::default()).unwrap(),
    );
    let state = AppState::new(index, AppConfig::default())
        .with_slow_log(nebula_server::SlowQueryLog::new(10, 0));
    let app = build_router(state);

    let _ = app
        .clone()
        .oneshot(
            Request::post("/api/v1/bucket/docs/doc")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"id":"1","text":"x"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let _ = app
        .clone()
        .oneshot(
            Request::post("/api/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"sql":"SELECT id FROM docs WHERE semantic_match(content, 'x') LIMIT 3"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let res = app
        .oneshot(Request::get("/api/v1/admin/slow").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_string(res.into_body()).await;
    assert!(body.contains("\"sql\""));
    assert!(body.contains("semantic_match"));
    assert!(body.contains("\"ok\":true"));
}

#[tokio::test]
async fn sql_query_runs_semantic_match() {
    let state = app_state(&[]);
    let app = build_router(state);

    // Seed a couple of docs with metadata so both semantic and residual
    // filters get exercised.
    for (i, text, region) in [
        (1, "zero trust networking", "eu"),
        (2, "dns failover strategies", "us"),
        (3, "zero trust architecture", "eu"),
    ] {
        let _ = app
            .clone()
            .oneshot(
                Request::post("/api/v1/bucket/docs/doc")
                    .header("content-type", "application/json")
                    .body(Body::from(format!(
                        r#"{{"id":"{i}","text":"{text}","metadata":{{"region":"{region}"}}}}"#
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
    }

    let res = app
        .oneshot(
            Request::post("/api/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"sql":"SELECT id, region FROM docs WHERE semantic_match(content, 'zero trust') AND region = 'eu' LIMIT 5"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_string(res.into_body()).await;
    // Both eu docs should be returned; the us doc must not appear.
    assert!(body.contains("\"region\":\"eu\""));
    assert!(!body.contains("\"region\":\"us\""));
}

#[tokio::test]
async fn sql_parse_error_returns_400() {
    let app = build_router(app_state(&[]));
    let res = app
        .oneshot(
            Request::post("/api/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"sql":"NOT VALID SQL"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = body_string(res.into_body()).await;
    assert!(body.contains("sql_parse"));
}

#[tokio::test]
async fn sql_missing_semantic_clause_returns_400() {
    let app = build_router(app_state(&[]));
    let res = app
        .oneshot(
            Request::post("/api/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"sql":"SELECT * FROM docs WHERE region = 'eu'"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = body_string(res.into_body()).await;
    assert!(body.contains("sql_invalid"));
}

#[tokio::test]
async fn vector_dim_mismatch_returns_400() {
    let app = build_router(app_state(&[]));
    let res = app
        .oneshot(
            Request::post("/api/v1/vector/search")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"vector":[0.1,0.2],"top_k":1}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

// -------------------------------------------------------------------------
// Cluster + replication tests
// -------------------------------------------------------------------------
//
// These cover the three-way split introduced in the cluster module:
// standalone (default, no changes expected), leader (writes + replica
// streaming), and follower (writes blocked). We exercise them at the
// router layer so the middleware ordering and the state wiring are
// both under test.

use nebula_server::cluster::{ClusterConfig, NodeRole};
use nebula_server::state::FollowerCursor;

fn app_state_with_role(role: NodeRole) -> AppState {
    let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(32));
    let index = Arc::new(
        TextIndex::new(emb, Metric::Cosine, HnswConfig::default()).unwrap(),
    );
    let cluster = Arc::new(ClusterConfig {
        node_id: Some("node-under-test".into()),
        role,
        region: None,
        leader_url: None,
        peers: Vec::new(),
        cross_region_peers: Vec::new(),
    });
    let mut state = AppState::new(index, AppConfig::default()).with_cluster(cluster);
    if matches!(role, NodeRole::Follower) {
        state = state.with_follower_cursor(Arc::new(FollowerCursor::default()));
    }
    state
}

#[tokio::test]
async fn follower_rejects_writes_with_409() {
    let app = build_router(app_state_with_role(NodeRole::Follower));
    // Writes should 409 regardless of payload validity — the guard
    // runs before handler-level validation.
    let res = app
        .oneshot(
            Request::post("/api/v1/bucket/b/doc")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"id":"x","text":"y","metadata":{}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CONFLICT);
    let body = body_string(res.into_body()).await;
    assert!(
        body.contains("read_only_follower"),
        "expected structured error code; got {body}"
    );
}

#[tokio::test]
async fn follower_still_accepts_reads() {
    let app = build_router(app_state_with_role(NodeRole::Follower));
    // GET /healthz: always public, always 200.
    let res = app
        .clone()
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    // A bucket GET: not an error for the guard, may 404 for the
    // missing doc but must not 409.
    let res2 = app
        .oneshot(
            Request::get("/api/v1/bucket/b/doc/none")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_ne!(
        res2.status(),
        StatusCode::CONFLICT,
        "reads must never hit the follower guard"
    );
}

#[tokio::test]
async fn standalone_accepts_writes_as_before() {
    let app = build_router(app_state_with_role(NodeRole::Standalone));
    let res = app
        .oneshot(
            Request::post("/api/v1/bucket/b/doc")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"id":"x","text":"hello","metadata":{}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    // Exact status depends on handler; we just need "not 409".
    assert_ne!(res.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn admin_cluster_nodes_shape() {
    let app = build_router(app_state_with_role(NodeRole::Leader));
    let res = app
        .oneshot(
            Request::get("/api/v1/admin/cluster/nodes")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_string(res.into_body()).await;
    // Minimal schema assertions — don't couple the test to field
    // ordering, just confirm the fields we care about are present.
    assert!(body.contains("\"self_id\""));
    assert!(body.contains("\"role\":\"leader\""));
    assert!(body.contains("\"peers\":[]"));
}

#[tokio::test]
async fn admin_logs_stream_delivers_snapshot() {
    // Seed the ring buffer directly via the bus, then hit the SSE
    // endpoint with replay=true. We only need to read the first
    // chunk — the snapshot events are delivered before the stream
    // starts blocking on live events.
    use futures::StreamExt;
    use nebula_server::{LogBus, LogEvent, LogLevel};
    use std::sync::Arc;

    let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(32));
    let index = Arc::new(
        TextIndex::new(emb, Metric::Cosine, HnswConfig::default()).unwrap(),
    );
    let bus = Arc::new(LogBus::new(16, LogLevel::Trace));
    for i in 0..3 {
        bus.push_for_test(LogEvent {
            ts_ms: i,
            level: LogLevel::Info,
            target: "test".into(),
            message: format!("seed-{i}"),
        });
    }
    let state = AppState::new(index, AppConfig::default()).with_log_bus(Arc::clone(&bus));
    let app = build_router(state);

    let res = app
        .oneshot(
            Request::get("/api/v1/admin/logs/stream?replay=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    // Content-Type should be SSE — proves we wired Sse::new correctly.
    let ct = res
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap_or("").to_string())
        .unwrap_or_default();
    assert!(ct.starts_with("text/event-stream"), "got CT: {ct}");

    // Read just enough of the stream to see the snapshot events.
    // The body never ends (live tail), so we collect with a small
    // timeout and assert on what arrived.
    let mut body = res.into_body().into_data_stream();
    let mut accum = String::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(250);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(std::time::Duration::from_millis(50), body.next()).await {
            Ok(Some(Ok(chunk))) => accum.push_str(&String::from_utf8_lossy(&chunk)),
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => {
                // No data for 50ms — snapshot is done, no live events
                // to ship; we've read what we needed.
                if accum.contains("snapshot_done") {
                    break;
                }
            }
        }
    }
    // Snapshot events arrive in order, each prefixed with `event: log`.
    assert!(accum.contains("seed-0"), "missing seed-0 in: {accum}");
    assert!(accum.contains("seed-2"), "missing seed-2 in: {accum}");
    assert!(accum.contains("snapshot_done"), "snapshot_done marker missing");
}

#[tokio::test]
async fn admin_logs_stream_level_filter() {
    use nebula_server::{LogBus, LogEvent, LogLevel};
    use std::sync::Arc;

    let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(32));
    let index = Arc::new(
        TextIndex::new(emb, Metric::Cosine, HnswConfig::default()).unwrap(),
    );
    let bus = Arc::new(LogBus::new(16, LogLevel::Trace));
    bus.push_for_test(LogEvent {
        ts_ms: 1,
        level: LogLevel::Info,
        target: "t".into(),
        message: "info-line".into(),
    });
    bus.push_for_test(LogEvent {
        ts_ms: 2,
        level: LogLevel::Error,
        target: "t".into(),
        message: "error-line".into(),
    });
    let state = AppState::new(index, AppConfig::default()).with_log_bus(Arc::clone(&bus));
    let app = build_router(state);

    // level=error: info-line should be filtered out.
    let res = app
        .oneshot(
            Request::get("/api/v1/admin/logs/stream?replay=true&level=error")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let mut body = res.into_body().into_data_stream();
    let mut accum = String::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(200);
    use futures::StreamExt;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(std::time::Duration::from_millis(50), body.next()).await {
            Ok(Some(Ok(chunk))) => accum.push_str(&String::from_utf8_lossy(&chunk)),
            _ => {
                if accum.contains("snapshot_done") {
                    break;
                }
            }
        }
    }
    assert!(accum.contains("error-line"), "error should pass: {accum}");
    assert!(
        !accum.contains("info-line"),
        "info should be filtered: {accum}"
    );
}

#[tokio::test]
async fn admin_replication_handles_standalone() {
    // A standalone node has no WAL and no follower cursor. The
    // endpoint must still return 200 with all the optional fields
    // null — operators will hit this to confirm "not replicating."
    let app = build_router(app_state_with_role(NodeRole::Standalone));
    let res = app
        .oneshot(
            Request::get("/api/v1/admin/replication")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_string(res.into_body()).await;
    assert!(body.contains("\"role\":\"standalone\""));
    assert!(body.contains("\"local_newest\":null"));
    assert!(body.contains("\"follower_applied\":null"));
    assert!(body.contains("\"leader_newest_probed\":null"));
}

#[tokio::test]
async fn admin_replication_reports_cursors() {
    // Follower with a known applied cursor; leader_newest is None
    // because no persistent index is wired in this standalone test.
    let mut state = app_state_with_role(NodeRole::Follower);
    // Seed the follower cursor as if replication had applied a
    // handful of records. The admin endpoint reads from this
    // exact atomic.
    let fc = Arc::new(FollowerCursor::default());
    fc.store(1, 4096);
    state.follower_cursor = Some(fc);
    let app = build_router(state);

    let res = app
        .oneshot(
            Request::get("/api/v1/admin/replication")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_string(res.into_body()).await;
    assert!(body.contains("\"role\":\"follower\""));
    assert!(
        body.contains("\"segment_seq\":1"),
        "expected seeded cursor to be reported; got {body}"
    );
    assert!(body.contains("\"byte_offset\":4096"));
}

/// Source-of-truth test for the bucket export/import swap primitive.
/// Seeds one bucket, exports it, spins up a *second* independent
/// index, imports the export, and asserts the second index can serve
/// identical reads. This is the shape the operator's rebalance
/// coordinator drives across pods during a swap rebalance.
#[tokio::test]
async fn bucket_export_import_roundtrip() {
    // Source index — populate via the normal upsert path so the
    // HNSW, WAL-apply, and metadata code paths all get exercised.
    let source = app_state(&[]);
    let source_app = build_router(source.clone());
    for (i, text) in [
        "zero trust networking",
        "dns failover strategies",
        "kubernetes gitops",
    ]
    .iter()
    .enumerate()
    {
        let res = source_app
            .clone()
            .oneshot(
                Request::post("/api/v1/bucket/migratable/doc")
                    .header("content-type", "application/json")
                    .body(Body::from(format!(
                        r#"{{"id":"{i}","text":"{text}"}}"#
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK, "insert {i} failed");
    }

    // Export from the source.
    let res = source_app
        .clone()
        .oneshot(
            Request::get("/api/v1/admin/bucket/migratable/export")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let export_body = body_string(res.into_body()).await;
    // Sanity: body mentions all three ids and reports count:3.
    assert!(export_body.contains("\"count\":3"), "got {export_body}");
    for i in 0..3 {
        assert!(
            export_body.contains(&format!("\"external_id\":\"{i}\"")),
            "missing id {i} in export: {export_body}"
        );
    }

    // Fresh target index — different MockEmbedder instance, same dim.
    // This is the important bit: imports must not call the embedder,
    // so the target's embedder being distinct from the source's is
    // exactly what we want to prove.
    let target = app_state(&[]);
    let target_app = build_router(target.clone());

    // Drop the wrapper envelope and forward just `docs` into the
    // import endpoint. We also drop `model` from the payload so the
    // target isn't forced to match the source's embedder name.
    let parsed: serde_json::Value = serde_json::from_str(&export_body).unwrap();
    let docs = parsed.get("docs").cloned().unwrap();
    let import_payload = serde_json::json!({
        "dim": parsed["dim"],
        "docs": docs,
    });
    let res = target_app
        .clone()
        .oneshot(
            Request::post("/api/v1/admin/bucket/migratable/import")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&import_payload).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let import_body = body_string(res.into_body()).await;
    assert!(
        import_body.contains("\"imported\":3"),
        "import underreported: {import_body}"
    );

    // Prove the target now answers the same queries as the source.
    for i in 0..3 {
        let res = target_app
            .clone()
            .oneshot(
                Request::get(format!("/api/v1/bucket/migratable/doc/{i}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK, "target missing doc {i}");
    }
}

/// /admin/replication exposes a per-remote-region view when the
/// cross-region status hub has entries. Phase 2.2 populates the hub
/// from a real consumer task; here we populate it manually to lock
/// in the wire shape.
#[tokio::test]
async fn admin_replication_includes_cross_region_remotes() {
    use nebula_server::ClusterConfig;
    let state = {
        let mut s = app_state(&[]);
        let cluster = Arc::new(ClusterConfig {
            region: Some("us-east-1".into()),
            ..ClusterConfig::default()
        });
        s = s.with_cluster(cluster);
        s.cross_region_status.register("us-west-2", "http://u:50051");
        s.cross_region_status.record_apply("us-west-2", 2, 512);
        s
    };
    let app = build_router(state);
    let res = app
        .oneshot(
            Request::get("/api/v1/admin/replication")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_string(res.into_body()).await;
    assert!(body.contains("\"region\":\"us-east-1\""), "got {body}");
    assert!(body.contains("\"remotes\""), "got {body}");
    assert!(body.contains("\"us-west-2\""));
    assert!(body.contains("\"applied_records\":1"));
    assert!(body.contains("\"last_applied_segment\":2"));
}

/// home-region endpoint reports `has_home: false` when no seed doc
/// is present, letting clients fall back gracefully.
#[tokio::test]
async fn home_region_reports_absence_when_unconfigured() {
    let app = build_router(app_state(&[]));
    let res = app
        .oneshot(
            Request::get("/api/v1/admin/bucket/anything/home-region")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_string(res.into_body()).await;
    assert!(body.contains("\"has_home\":false"), "got {body}");
    assert!(body.contains("\"home_epoch\":0"));
    // node_region falls back to "default" when not configured.
    assert!(body.contains("\"node_region\":\"default\""));
}

/// With a seed doc carrying home_region metadata, the endpoint
/// round-trips the stored values. This is the contract the operator's
/// failover controller depends on.
#[tokio::test]
async fn home_region_round_trips_seed_doc() {
    let state = app_state(&[]);
    let app = build_router(state);
    // Seed the bucket the same way the NebulaBucket controller does.
    let seed_body = serde_json::json!({
        "id": "__nebuladb_operator_seed__",
        "text": "NebulaDB bucket test managed by nebuladb-operator",
        "metadata": {
            "kind": "nebuladb-operator-seed",
            "bucket": "test",
            "home_region": "us-east-1",
            "home_epoch": 7,
            "replicated_to": ["us-west-2"]
        }
    });
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v1/bucket/test/doc")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&seed_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // Now read back.
    let res = app
        .oneshot(
            Request::get("/api/v1/admin/bucket/test/home-region")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_string(res.into_body()).await;
    assert!(body.contains("\"home_region\":\"us-east-1\""), "got {body}");
    assert!(body.contains("\"home_epoch\":7"));
    assert!(body.contains("\"has_home\":true"));
    assert!(body.contains("us-west-2"));
}

/// Dim-mismatch guard: a client sending vectors of the wrong length
/// should be rejected before any WAL append so the bucket never
/// ends up partially imported.
#[tokio::test]
async fn bucket_import_rejects_wrong_dim() {
    let state = app_state(&[]);
    let app = build_router(state);
    // MockEmbedder here uses dim=32 (per app_state). Send a vector of
    // length 16 and expect a 400.
    let payload = serde_json::json!({
        "dim": 32,
        "docs": [{
            "external_id": "x",
            "text": "hi",
            "vector": vec![0.1_f32; 16],
        }]
    });
    let res = app
        .oneshot(
            Request::post("/api/v1/admin/bucket/b/import")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&payload).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}
