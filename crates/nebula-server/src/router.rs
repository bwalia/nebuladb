//! HTTP router and handlers.
//!
//! Handler responsibilities are kept narrow: validate input, call the
//! index, shape the response. Business logic lives in `nebula-index`.

use std::convert::Infallible;
use std::time::Duration;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use futures::stream::{self, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;

use nebula_llm::{build_rag_prompt, LlmChunk};

use crate::error::ApiError;
use crate::state::AppState;

pub fn build_router(state: AppState) -> Router {
    let limit = state.config.max_body_bytes;

    // Auth + rate limit apply to the domain API only. Ops endpoints
    // (/healthz, /metrics) stay unthrottled so a scrape still works
    // during an abuse event.
    //
    // Layer order (outermost first):
    //   rate_limit -> auth -> body_limit -> trace -> handler
    // Rate limit runs first so a flood of unauthenticated requests
    // can't burn the auth middleware's CPU budget.
    let api = Router::new()
        .route("/bucket/:bucket/doc", post(upsert_doc))
        .route("/bucket/:bucket/doc/:id", get(get_doc).delete(delete_doc))
        .route("/bucket/:bucket/document", post(upsert_document))
        .route(
            "/bucket/:bucket/document/:doc_id",
            axum::routing::delete(delete_document),
        )
        .route("/vector/search", post(vector_search))
        .route("/ai/search", post(ai_search))
        .route("/ai/rag", post(ai_rag))
        .route("/query", post(sql_query))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::middleware::require_auth,
        ))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::ratelimit::rate_limit,
        ));

    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics_handler))
        .nest("/api/v1", api)
        .layer(RequestBodyLimitLayer::new(limit))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

// ---------- ops ----------

#[derive(Serialize)]
struct Health {
    status: &'static str,
    docs: usize,
    dim: usize,
    model: String,
}

async fn healthz(State(s): State<AppState>) -> Json<Health> {
    Json(Health {
        status: "ok",
        docs: s.index.len(),
        dim: s.index.dim(),
        model: s.index.embedder_model().to_string(),
    })
}

async fn metrics_handler(State(s): State<AppState>) -> (StatusCode, [(&'static str, &'static str); 1], String) {
    let mut body = s.metrics.render();
    // Append embedding-cache counters when a cache is registered. We
    // render from here (not inside `Metrics::render`) because the
    // cache is a separate subsystem with its own counters and we
    // don't want `nebula-server::metrics` to depend on `nebula-cache`.
    if let Some(stats) = &s.cache_stats {
        let (hits, misses, evictions, inserts) = stats.snapshot();
        use std::fmt::Write as _;
        let _ = writeln!(body, "# HELP nebula_embed_cache_hits Cache hits on embedding lookup");
        let _ = writeln!(body, "# TYPE nebula_embed_cache_hits counter");
        let _ = writeln!(body, "nebula_embed_cache_hits {hits}");
        let _ = writeln!(body, "# HELP nebula_embed_cache_misses Cache misses on embedding lookup");
        let _ = writeln!(body, "# TYPE nebula_embed_cache_misses counter");
        let _ = writeln!(body, "nebula_embed_cache_misses {misses}");
        let _ = writeln!(body, "# HELP nebula_embed_cache_evictions Entries evicted from the LRU");
        let _ = writeln!(body, "# TYPE nebula_embed_cache_evictions counter");
        let _ = writeln!(body, "nebula_embed_cache_evictions {evictions}");
        let _ = writeln!(body, "# HELP nebula_embed_cache_inserts New entries written into the cache");
        let _ = writeln!(body, "# TYPE nebula_embed_cache_inserts counter");
        let _ = writeln!(body, "nebula_embed_cache_inserts {inserts}");
    }
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
}

// ---------- documents ----------

#[derive(Deserialize)]
struct UpsertDoc {
    /// External id. Required — we don't auto-assign because a caller
    /// that re-POSTs for idempotency needs to control the key.
    id: String,
    /// Text to embed + index.
    text: String,
    #[serde(default)]
    metadata: serde_json::Value,
}

#[derive(Serialize)]
struct UpsertResponse {
    bucket: String,
    id: String,
    dim: usize,
}

async fn upsert_doc(
    State(s): State<AppState>,
    Path(bucket): Path<String>,
    Json(body): Json<UpsertDoc>,
) -> Result<Json<UpsertResponse>, ApiError> {
    if body.text.trim().is_empty() {
        return Err(ApiError::BadRequest("text must be non-empty".into()));
    }
    s.index
        .upsert_text(&bucket, &body.id, &body.text, body.metadata)
        .await?;
    s.metrics.inc_insert();
    Ok(Json(UpsertResponse {
        bucket,
        id: body.id,
        dim: s.index.dim(),
    }))
}

#[derive(Serialize)]
struct GetDocResponse {
    bucket: String,
    id: String,
    text: String,
    metadata: serde_json::Value,
}

async fn get_doc(
    State(s): State<AppState>,
    Path((bucket, id)): Path<(String, String)>,
) -> Result<Json<GetDocResponse>, ApiError> {
    let d = s
        .index
        .get(&bucket, &id)
        .ok_or_else(|| ApiError::NotFound(format!("{bucket}/{id}")))?;
    Ok(Json(GetDocResponse {
        bucket: d.bucket,
        id: d.external_id,
        text: d.text,
        metadata: d.metadata,
    }))
}

async fn delete_doc(
    State(s): State<AppState>,
    Path((bucket, id)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    s.index.delete(&bucket, &id)?;
    s.metrics.inc_delete();
    Ok(StatusCode::NO_CONTENT)
}

// ---------- chunked documents ----------

#[derive(Deserialize)]
struct UpsertDocument {
    /// Stable parent id. Chunks are stored as `{doc_id}#{i}`.
    doc_id: String,
    text: String,
    #[serde(default)]
    metadata: serde_json::Value,
}

#[derive(Serialize)]
struct UpsertDocumentResponse {
    bucket: String,
    doc_id: String,
    chunks: usize,
}

async fn upsert_document(
    State(s): State<AppState>,
    Path(bucket): Path<String>,
    Json(body): Json<UpsertDocument>,
) -> Result<Json<UpsertDocumentResponse>, ApiError> {
    if body.text.trim().is_empty() {
        return Err(ApiError::BadRequest("text must be non-empty".into()));
    }
    let chunks = s
        .index
        .upsert_document(&bucket, &body.doc_id, &body.text, s.chunker.as_ref(), body.metadata)
        .await?;
    s.metrics.inc_insert();
    Ok(Json(UpsertDocumentResponse {
        bucket,
        doc_id: body.doc_id,
        chunks,
    }))
}

#[derive(Serialize)]
struct DeleteDocumentResponse {
    bucket: String,
    doc_id: String,
    chunks_removed: usize,
}

async fn delete_document(
    State(s): State<AppState>,
    Path((bucket, doc_id)): Path<(String, String)>,
) -> Result<Json<DeleteDocumentResponse>, ApiError> {
    let n = s.index.delete_document(&bucket, &doc_id)?;
    s.metrics.inc_delete();
    Ok(Json(DeleteDocumentResponse {
        bucket,
        doc_id,
        chunks_removed: n,
    }))
}

// ---------- search ----------

#[derive(Deserialize)]
struct VectorSearchRequest {
    vector: Vec<f32>,
    #[serde(default = "default_top_k")]
    top_k: usize,
    #[serde(default)]
    bucket: Option<String>,
    #[serde(default)]
    ef: Option<usize>,
}

#[derive(Deserialize)]
struct AiSearchRequest {
    query: String,
    #[serde(default = "default_top_k")]
    top_k: usize,
    #[serde(default)]
    bucket: Option<String>,
    #[serde(default)]
    ef: Option<usize>,
}

fn default_top_k() -> usize {
    10
}

#[derive(Serialize)]
struct SearchResponse {
    hits: Vec<nebula_index::Hit>,
    took_ms: u64,
}

async fn vector_search(
    State(s): State<AppState>,
    Json(req): Json<VectorSearchRequest>,
) -> Result<Json<SearchResponse>, ApiError> {
    let top_k = validate_top_k(req.top_k, s.config.max_top_k)?;
    if req.vector.len() != s.index.dim() {
        return Err(ApiError::BadRequest(format!(
            "vector has dim {}, index expects {}",
            req.vector.len(),
            s.index.dim()
        )));
    }
    let started = std::time::Instant::now();
    let hits = s
        .index
        .search_vector(&req.vector, req.bucket.as_deref(), top_k, req.ef)?;
    s.metrics.inc_vector_search();
    Ok(Json(SearchResponse {
        hits,
        took_ms: started.elapsed().as_millis() as u64,
    }))
}

async fn ai_search(
    State(s): State<AppState>,
    Json(req): Json<AiSearchRequest>,
) -> Result<Json<SearchResponse>, ApiError> {
    if req.query.trim().is_empty() {
        return Err(ApiError::BadRequest("query must be non-empty".into()));
    }
    let top_k = validate_top_k(req.top_k, s.config.max_top_k)?;
    let started = std::time::Instant::now();
    let hits = s
        .index
        .search_text(&req.query, req.bucket.as_deref(), top_k, req.ef)
        .await?;
    s.metrics.inc_semantic_search();
    Ok(Json(SearchResponse {
        hits,
        took_ms: started.elapsed().as_millis() as u64,
    }))
}

fn validate_top_k(top_k: usize, max: usize) -> Result<usize, ApiError> {
    if top_k == 0 {
        return Err(ApiError::BadRequest("top_k must be > 0".into()));
    }
    if top_k > max {
        return Err(ApiError::BadRequest(format!(
            "top_k exceeds max ({max})"
        )));
    }
    Ok(top_k)
}

// ---------- RAG ----------

#[derive(Deserialize)]
struct RagRequest {
    query: String,
    #[serde(default = "default_rag_top_k")]
    top_k: usize,
    #[serde(default)]
    bucket: Option<String>,
    /// If true, response is `text/event-stream` (SSE). Default false
    /// returns a single JSON object.
    #[serde(default)]
    stream: bool,
}

#[derive(Deserialize)]
struct RagQueryParams {
    /// Allow `?stream=true` as an alternate to the body flag — handy
    /// for `curl` demos and for browsers that only control query
    /// strings via `EventSource`.
    #[serde(default)]
    stream: Option<bool>,
}

fn default_rag_top_k() -> usize {
    5
}

#[derive(Serialize)]
struct RagResponse {
    query: String,
    context: Vec<nebula_index::Hit>,
    /// Synthesized answer. A real deployment plugs in an LLM client
    /// here; the vertical slice returns a deterministic summary so the
    /// contract and streaming plumbing are testable end-to-end.
    answer: String,
}

async fn ai_rag(
    State(s): State<AppState>,
    Query(qs): Query<RagQueryParams>,
    Json(req): Json<RagRequest>,
) -> Result<Response, ApiError> {
    if req.query.trim().is_empty() {
        return Err(ApiError::BadRequest("query must be non-empty".into()));
    }
    let top_k = validate_top_k(req.top_k, s.config.max_top_k)?;
    s.metrics.inc_rag();

    let hits = s
        .index
        .search_text(&req.query, req.bucket.as_deref(), top_k, None)
        .await?;
    let snippets: Vec<&str> = hits.iter().map(|h| h.text.as_str()).collect();
    let prompt = build_rag_prompt(&req.query, &snippets);

    let wants_stream = req.stream || qs.stream.unwrap_or(false);
    if wants_stream {
        // Kick off the LLM stream inside the handler so a provider
        // error (bad key, dead Ollama) surfaces as an HTTP 5xx
        // *before* we commit to an `Sse` response. Once we hand axum
        // an `Sse` there's no way to send a status code.
        let llm_stream = s
            .llm
            .generate(prompt)
            .await
            .map_err(|e| ApiError::Internal(format!("llm: {e}")))?;
        Ok(rag_sse_response(req.query, hits, llm_stream))
    } else {
        // Non-streaming path: drain the LLM stream into a single
        // string and return JSON. Useful for curl / ORMs without SSE
        // support.
        let mut llm_stream = s
            .llm
            .generate(prompt)
            .await
            .map_err(|e| ApiError::Internal(format!("llm: {e}")))?;
        let mut answer = String::new();
        while let Some(item) = llm_stream.next().await {
            match item.map_err(|e| ApiError::Internal(format!("llm: {e}")))? {
                LlmChunk::Delta(t) => answer.push_str(&t),
                LlmChunk::Done => break,
            }
        }
        Ok(Json(RagResponse {
            query: req.query,
            context: hits,
            answer,
        })
        .into_response())
    }
}

/// SSE event shape:
/// - `event: context` — one per retrieved chunk, data is the JSON `Hit`.
/// - `event: answer_delta` — one per LLM token-group.
/// - `event: error` — if the LLM stream produces an error mid-flight.
/// - `event: done` — terminal marker.
///
/// We emit `context` first so clients can render citations before the
/// answer completes. The LLM stream is forwarded as-is — its chunk
/// boundaries come from the upstream provider.
fn rag_sse_response(
    query: String,
    hits: Vec<nebula_index::Hit>,
    llm_stream: futures::stream::BoxStream<'static, nebula_llm::Result<LlmChunk>>,
) -> Response {
    let context_events = hits
        .into_iter()
        .map(|h| Ok::<_, Infallible>(Event::default().event("context").json_data(h).unwrap()));

    let answer_events = llm_stream.map(|item| match item {
        Ok(LlmChunk::Delta(t)) => Ok::<_, Infallible>(Event::default().event("answer_delta").data(t)),
        Ok(LlmChunk::Done) => Ok(Event::default()
            .event("done")
            .json_data(serde_json::json!({ "reason": "llm_done" }))
            .unwrap()),
        Err(e) => Ok(Event::default().event("error").data(e.to_string())),
    });

    let trailer = Ok::<_, Infallible>(
        Event::default()
            .event("done")
            .json_data(serde_json::json!({ "query": query }))
            .unwrap(),
    );

    let stream: std::pin::Pin<Box<dyn Stream<Item = Result<Event, Infallible>> + Send>> =
        Box::pin(
            stream::iter(context_events)
                .chain(answer_events)
                .chain(stream::iter(std::iter::once(trailer))),
        );

    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keep-alive"),
        )
        .into_response()
}

// ---------- SQL ----------

#[derive(Deserialize)]
struct SqlQueryRequest {
    sql: String,
}

#[derive(Serialize)]
struct SqlQueryResponse {
    took_ms: u64,
    rows: Vec<nebula_sql::executor::Row>,
}

async fn sql_query(
    State(s): State<AppState>,
    Json(req): Json<SqlQueryRequest>,
) -> Result<Json<SqlQueryResponse>, ApiError> {
    if req.sql.trim().is_empty() {
        return Err(ApiError::BadRequest("sql must be non-empty".into()));
    }
    let out = s.sql.run(&req.sql).await?;
    Ok(Json(SqlQueryResponse {
        took_ms: out.took_ms,
        rows: out.rows,
    }))
}
