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
        .route("/bucket/:bucket/docs/bulk", post(upsert_docs_bulk))
        .route("/bucket/:bucket/document", post(upsert_document))
        .route(
            "/bucket/:bucket/document/:doc_id",
            axum::routing::delete(delete_document),
        )
        .route("/vector/search", post(vector_search))
        .route("/ai/search", post(ai_search))
        .route("/ai/rag", post(ai_rag))
        .route("/query", post(sql_query))
        .route("/query/explain", post(sql_explain))
        .route("/admin/buckets", get(admin_buckets))
        .route("/admin/audit", get(admin_audit))
        .route("/admin/stats", get(admin_stats))
        .route("/admin/slow", get(admin_slow))
        .route("/admin/durability", get(admin_durability))
        .route("/admin/snapshot", post(admin_snapshot))
        .route("/admin/wal/compact", post(admin_wal_compact))
        .route("/admin/bucket/:bucket/empty", post(admin_empty_bucket))
        .route("/admin/cluster/nodes", get(admin_cluster_nodes))
        .route("/admin/replication", get(admin_replication))
        .route("/admin/logs/stream", get(admin_logs_stream))
        // Layer order (innermost last; request flows top-to-bottom,
        // response bottom-to-top):
        //   rate_limit → follower_guard → audit → auth → handler
        // Follower guard runs BEFORE auth so an unauthenticated
        // write on a follower still 409s — there's no point
        // telling the client "auth first" when the answer is going
        // to be "wrong node regardless." It runs AFTER audit so a
        // rejected write still shows up in the audit log.
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::middleware::require_auth,
        ))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::middleware::guard_writes_on_follower,
        ))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::middleware::audit_writes,
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

#[derive(Deserialize)]
struct BulkUpsertRequest {
    items: Vec<UpsertDoc>,
}

#[derive(Serialize)]
struct BulkUpsertResponse {
    bucket: String,
    inserted: usize,
    requested: usize,
}

/// Batched upsert. Embeds every item in a single upstream call, then
/// inserts sequentially into HNSW. Partial success is allowed: if
/// one HNSW insert fails, that item is rolled back and the rest go
/// through (we'd rather seed 999/1000 than 0/1000). A hard ceiling
/// on batch size prevents a rogue client from pinning a giant embed
/// call — common providers also reject > ~2k-item batches, so this
/// matches upstream behaviour.
async fn upsert_docs_bulk(
    State(s): State<AppState>,
    Path(bucket): Path<String>,
    Json(body): Json<BulkUpsertRequest>,
) -> Result<Json<BulkUpsertResponse>, ApiError> {
    const MAX_BATCH: usize = 1000;
    if body.items.is_empty() {
        return Err(ApiError::BadRequest("items must be non-empty".into()));
    }
    if body.items.len() > MAX_BATCH {
        return Err(ApiError::BadRequest(format!(
            "batch of {} exceeds max {MAX_BATCH}",
            body.items.len()
        )));
    }
    // Reject empty fields up-front so every failure is 400 not 500.
    for it in &body.items {
        if it.text.trim().is_empty() {
            return Err(ApiError::BadRequest("each item.text must be non-empty".into()));
        }
    }

    let requested = body.items.len();
    let prepared: Vec<(String, String, serde_json::Value)> = body
        .items
        .into_iter()
        .map(|d| (d.id, d.text, d.metadata))
        .collect();
    let inserted = s.index.upsert_text_bulk(&bucket, &prepared).await?;
    s.metrics.inc_insert();
    Ok(Json(BulkUpsertResponse {
        bucket,
        inserted,
        requested,
    }))
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
    // Time the whole `run` call here so the slow-log captures wall
    // time even on the error path — where `SqlEngine::run` doesn't
    // reach the point of computing `took_ms`. A query that burns
    // 5s and then errors is exactly what operators want to see.
    let started = std::time::Instant::now();
    let result = s.sql.run(&req.sql).await;
    let took_ms = started.elapsed().as_millis() as u64;
    match result {
        Ok(out) => {
            s.slow_log.record(&req.sql, took_ms, out.rows.len(), true);
            Ok(Json(SqlQueryResponse {
                took_ms: out.took_ms,
                rows: out.rows,
            }))
        }
        Err(e) => {
            s.slow_log.record(&req.sql, took_ms, 0, false);
            Err(e.into())
        }
    }
}

// ---------- admin ----------

/// EXPLAIN: parse + plan only, never execute. Returns the typed
/// `QueryPlan` tree so operators can see which retrieval clause was
/// picked, how WHERE split across a join, which filters became
/// residual, etc. Does not touch the result cache.
async fn sql_explain(
    State(s): State<AppState>,
    Json(req): Json<SqlQueryRequest>,
) -> Result<Json<nebula_sql::QueryPlan>, ApiError> {
    if req.sql.trim().is_empty() {
        return Err(ApiError::BadRequest("sql must be non-empty".into()));
    }
    Ok(Json(s.sql.explain(&req.sql)?))
}

#[derive(Deserialize)]
struct BucketsQuery {
    /// Max metadata keys per bucket to return. The UI caps the
    /// display at ~10 anyway, but we let callers override for
    /// introspection.
    #[serde(default = "default_top_keys")]
    top_keys: usize,
}

fn default_top_keys() -> usize {
    20
}

async fn admin_buckets(
    State(s): State<AppState>,
    axum::extract::Query(q): axum::extract::Query<BucketsQuery>,
) -> Result<Json<Vec<nebula_index::BucketStats>>, ApiError> {
    Ok(Json(s.index.bucket_stats(q.top_keys)))
}

#[derive(Deserialize)]
struct AuditQuery {
    #[serde(default = "default_audit_limit")]
    limit: usize,
}

fn default_audit_limit() -> usize {
    200
}

async fn admin_audit(
    State(s): State<AppState>,
    axum::extract::Query(q): axum::extract::Query<AuditQuery>,
) -> Json<Vec<crate::audit::AuditEntry>> {
    Json(s.audit.recent(q.limit))
}

/// Slow-query log. Fixed-capacity priority queue of the slowest
/// SQL queries seen since boot. Returned sorted descending by
/// `took_ms` — the first entry is the worst offender.
async fn admin_slow(
    State(s): State<AppState>,
) -> Json<Vec<crate::slow_log::SlowQueryEntry>> {
    Json(s.slow_log.snapshot())
}

#[derive(Serialize)]
struct DurabilityInfo {
    /// `true` when the server was booted with NEBULA_DATA_DIR and
    /// every mutation is going through the WAL.
    persistent: bool,
    /// Absolute path to the data directory when persistent.
    data_dir: Option<String>,
    /// WAL stats: segment count, bytes on disk, seq range.
    wal: Option<nebula_wal::WalStats>,
}

async fn admin_durability(State(s): State<AppState>) -> Json<DurabilityInfo> {
    Json(DurabilityInfo {
        persistent: s.index.is_persistent(),
        data_dir: s.index.data_dir().map(|p| p.display().to_string()),
        wal: s.index.wal_stats(),
    })
}

async fn admin_snapshot(
    State(s): State<AppState>,
) -> Result<Json<nebula_index::SnapshotOutcome>, ApiError> {
    let out = s.index.snapshot()?;
    Ok(Json(out))
}

#[derive(Serialize)]
struct WalCompactResponse {
    removed_segments: usize,
}

async fn admin_wal_compact(
    State(s): State<AppState>,
) -> Result<Json<WalCompactResponse>, ApiError> {
    let removed_segments = s.index.compact_wal()?;
    Ok(Json(WalCompactResponse { removed_segments }))
}

// ---- cluster + replication ----

#[derive(Serialize)]
struct ClusterResponse {
    self_id: Option<String>,
    role: crate::cluster::NodeRole,
    leader_url: Option<String>,
    peers: Vec<PeerView>,
}

#[derive(Serialize)]
struct PeerView {
    id: String,
    base_url: String,
    /// `Some(true)` healthy, `Some(false)` reachable but not 200,
    /// `None` unreachable (timeout, DNS fail). Operators reading
    /// this want the three-state distinction — "I didn't probe"
    /// vs. "I probed and nothing answered" vs. "it answered sick".
    healthy: Option<bool>,
    /// Docs count from the peer's /healthz, when reachable. Lets
    /// an operator eyeball replication drift without hitting each
    /// peer individually.
    docs: Option<usize>,
    /// Millisecond round-trip for the probe. Only present when
    /// reachable; nil when the probe itself failed.
    probe_ms: Option<u64>,
}

async fn admin_cluster_nodes(State(s): State<AppState>) -> Json<ClusterResponse> {
    let peers = if s.cluster.peers.is_empty() {
        Vec::new()
    } else {
        probe_peers(&s.cluster.peers).await
    };
    Json(ClusterResponse {
        self_id: s.cluster.node_id.clone(),
        role: s.cluster.role,
        leader_url: s.cluster.leader_url.clone(),
        peers,
    })
}

/// Probe every peer's `/healthz` concurrently with a short timeout.
/// We deliberately don't cache — the endpoint is a low-frequency
/// operator view, and a stale health state is worse than a slow
/// one when an operator is diagnosing a split cluster.
async fn probe_peers(peers: &[crate::cluster::PeerInfo]) -> Vec<PeerView> {
    // 500ms is generous for a healthz probe on a local network
    // and tight enough that an unreachable node doesn't stall the
    // operator. Client is rebuilt per call — the peer list is
    // tiny and this keeps the middleware story simple.
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(500))
        .build()
    {
        Ok(c) => c,
        Err(_) => {
            // Can't build a client — surface unreachable for all
            // peers rather than 500ing the whole admin endpoint.
            return peers
                .iter()
                .map(|p| PeerView {
                    id: p.id.clone(),
                    base_url: p.base_url.clone(),
                    healthy: None,
                    docs: None,
                    probe_ms: None,
                })
                .collect();
        }
    };

    let tasks = peers.iter().map(|p| {
        let client = client.clone();
        let p = p.clone();
        async move {
            let url = format!("{}/healthz", p.base_url.trim_end_matches('/'));
            let started = std::time::Instant::now();
            match client.get(&url).send().await {
                Ok(resp) => {
                    let probe_ms = started.elapsed().as_millis() as u64;
                    let healthy = resp.status().is_success();
                    let docs = if healthy {
                        resp.json::<serde_json::Value>()
                            .await
                            .ok()
                            .and_then(|v| v.get("docs")?.as_u64())
                            .map(|n| n as usize)
                    } else {
                        None
                    };
                    PeerView {
                        id: p.id,
                        base_url: p.base_url,
                        healthy: Some(healthy),
                        docs,
                        probe_ms: Some(probe_ms),
                    }
                }
                Err(_) => PeerView {
                    id: p.id,
                    base_url: p.base_url,
                    healthy: None,
                    docs: None,
                    probe_ms: None,
                },
            }
        }
    });
    futures::future::join_all(tasks).await
}

#[derive(Serialize)]
struct ReplicationInfo {
    role: crate::cluster::NodeRole,
    /// This node's own WAL newest cursor. On a leader this is
    /// what followers are tailing toward. On a follower it's
    /// just the local durability tip (mostly uninteresting unless
    /// the follower is also persisting replicated writes — which
    /// we don't do today).
    local_newest: Option<CursorView>,
    /// Follower-side: what this node's replication task has
    /// applied so far. `None` on a leader or standalone.
    follower_applied: Option<CursorView>,
    /// Where the leader's WAL tip is *right now*, probed over
    /// HTTP when this node is a follower. `None` when this node
    /// is not a follower, when NEBULA_LEADER_REST_URL isn't set,
    /// or when the probe failed.
    leader_newest_probed: Option<CursorView>,
    /// `leader_newest_probed - follower_applied` in bytes when
    /// both are known and live in the same segment.
    ///
    /// Cross-segment lag is reported as `None` — the two cursors
    /// live in different files, so a byte delta alone is
    /// meaningless (it'd ignore any intermediate full segments).
    /// Consumers should fall back to `behind` when this is nil.
    lag_bytes: Option<u64>,
    /// Whether the follower is trailing the probed leader tip.
    /// `None` on a leader, standalone, or when probe fails.
    behind: Option<bool>,
}

#[derive(Serialize)]
struct CursorView {
    segment_seq: u64,
    byte_offset: u64,
}

async fn admin_replication(State(s): State<AppState>) -> Json<ReplicationInfo> {
    // This node's own WAL tip — meaningful on a leader, noise on
    // a follower (since we don't currently persist replicated
    // writes into the follower's local WAL).
    let local_newest = s.index.wal().map(|w| {
        let c = w.newest_cursor();
        CursorView {
            segment_seq: c.segment_seq,
            byte_offset: c.byte_offset,
        }
    });

    let follower_applied = s.follower_cursor.as_ref().map(|fc| {
        let (seg, off) = fc.load();
        CursorView {
            segment_seq: seg,
            byte_offset: off,
        }
    });

    // Probe the leader's REST /admin/replication for its
    // local_newest — that's the number we compare against to
    // compute lag. Only do this on followers; leaders and
    // standalones have nothing to measure against.
    let (leader_newest_probed, lag_bytes, behind) = match (
        s.cluster.is_follower(),
        &follower_applied,
    ) {
        (true, Some(applied)) => match fetch_leader_newest().await {
            Some(leader) => {
                let behind_flag = leader.segment_seq > applied.segment_seq
                    || (leader.segment_seq == applied.segment_seq
                        && leader.byte_offset > applied.byte_offset);
                let lag = if leader.segment_seq == applied.segment_seq {
                    Some(leader.byte_offset.saturating_sub(applied.byte_offset))
                } else {
                    None
                };
                (Some(leader), lag, Some(behind_flag))
            }
            None => (None, None, None),
        },
        _ => (None, None, None),
    };

    Json(ReplicationInfo {
        role: s.cluster.role,
        local_newest,
        follower_applied,
        leader_newest_probed,
        lag_bytes,
        behind,
    })
}

// ---- log streaming ----

#[derive(Deserialize)]
struct LogsStreamQuery {
    /// Minimum level to include: trace/debug/info/warn/error.
    /// Missing means "whatever the bus's current min_level is" —
    /// the operator can narrow but not broaden past the bus cap.
    level: Option<String>,
    /// Case-sensitive substring on `target`. Useful to scope to
    /// a subsystem (`target=nebula_grpc`) without decoding every
    /// message.
    target: Option<String>,
    /// Initial snapshot from the recent-events ring. Defaults to
    /// true — kubectl-style "what was happening just before you
    /// connected" is the usual want. Set `replay=false` for a
    /// pure tail.
    replay: Option<bool>,
}

/// SSE endpoint. Each event has:
///
/// - `event: log` — one log line (JSON-encoded LogEvent).
/// - `event: snapshot_done` — sentinel after the initial replay.
/// - `event: lagged` — broadcast overflow. The client should
///   reconnect for a fresh snapshot; we don't close the stream,
///   the send is just a marker.
///
/// Keep-alives every 15s mirror the RAG endpoint so proxies that
/// close idle streams don't chew on this one.
async fn admin_logs_stream(
    State(s): State<AppState>,
    Query(q): Query<LogsStreamQuery>,
) -> Response {
    use crate::log_stream::{LogEvent, LogLevel};
    use futures::stream::StreamExt;

    // Level filter: default to the bus's current min_level.
    let min_level: LogLevel = q
        .level
        .as_deref()
        .and_then(LogLevel::parse)
        .unwrap_or_else(|| s.log_bus.min_level());
    let target_filter: Option<String> = q.target;
    let replay = q.replay.unwrap_or(true);

    let keep = move |e: &LogEvent| -> bool {
        if e.level < min_level {
            return false;
        }
        match target_filter.as_ref() {
            Some(sub) => e.target.contains(sub.as_str()),
            None => true,
        }
    };

    // Snapshot FIRST so the client's first real frame is the
    // historical tail, matching kubectl-logs behavior.
    let snapshot_events: Vec<Result<Event, Infallible>> = if replay {
        s.log_bus
            .snapshot()
            .into_iter()
            .filter(|e| keep(e))
            .map(|e| {
                Ok::<_, Infallible>(
                    Event::default()
                        .event("log")
                        .json_data(&e)
                        .unwrap_or_else(|_| Event::default().event("log").data("{}")),
                )
            })
            .collect()
    } else {
        Vec::new()
    };

    let snapshot_done = Ok::<_, Infallible>(Event::default().event("snapshot_done").data(""));

    // Live tail: convert the broadcast receiver into a stream so
    // we can chain it after the historical snapshot.
    let rx = s.log_bus.subscribe();
    let live = tokio_stream::wrappers::BroadcastStream::new(rx).filter_map(
        move |item| {
            let keep = keep.clone();
            async move {
                match item {
                    Ok(e) if keep(&e) => Some(Ok::<_, Infallible>(
                        Event::default()
                            .event("log")
                            .json_data(&e)
                            .unwrap_or_else(|_| Event::default().event("log").data("{}")),
                    )),
                    // Filtered out — don't emit anything.
                    Ok(_) => None,
                    Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                        Some(Ok::<_, Infallible>(
                            Event::default()
                                .event("lagged")
                                .data(format!("missed {n} events; reconnect for a fresh snapshot")),
                        ))
                    }
                }
            }
        },
    );

    let stream: std::pin::Pin<Box<dyn Stream<Item = Result<Event, Infallible>> + Send>> =
        Box::pin(
            stream::iter(snapshot_events)
                .chain(stream::iter(std::iter::once(snapshot_done)))
                .chain(live),
        );

    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keep-alive"),
        )
        .into_response()
}

/// Probe the leader's /admin/replication to read its newest cursor.
/// Returns `None` on any failure — the admin view degrades to
/// "unknown" rather than erroring.
async fn fetch_leader_newest() -> Option<CursorView> {
    // The follower knows its leader's gRPC URL (NEBULA_FOLLOW_LEADER)
    // but lag here is an HTTP probe, and gRPC/REST live on different
    // ports in every real deployment. Rather than guessing port
    // mappings, we look for an explicit sibling var. When it's
    // absent we skip the probe — better than returning wrong lag.
    let rest_base = std::env::var("NEBULA_LEADER_REST_URL").ok()?;
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(500))
        .build()
        .ok()?;
    let url = format!("{}/api/v1/admin/replication", rest_base.trim_end_matches('/'));
    let v: serde_json::Value = client.get(&url).send().await.ok()?.json().await.ok()?;
    let newest = v.get("local_newest")?;
    Some(CursorView {
        segment_seq: newest.get("segment_seq")?.as_u64()?,
        byte_offset: newest.get("byte_offset")?.as_u64()?,
    })
}

#[derive(Serialize)]
struct StatsResponse {
    requests_total: u64,
    requests_errors: u64,
    auth_failures: u64,
    rate_limited: u64,
    jwt_failures: u64,
    docs_inserted: u64,
    docs_deleted: u64,
    searches_vector: u64,
    searches_semantic: u64,
    rag_requests: u64,
    embed_cache_hits: u64,
    embed_cache_misses: u64,
    embed_cache_evictions: u64,
    embed_cache_inserts: u64,
    total_docs_live: usize,
}

/// Snapshot the server's counters as JSON. Parallels `/metrics`
/// (Prometheus text) but with a stable typed shape — the UI can
/// consume this every few seconds without re-parsing the text format
/// on every tick. Field names match the Prometheus metric names with
/// the `nebula_` prefix dropped.
#[derive(Serialize)]
struct EmptyBucketResponse {
    bucket: String,
    removed: usize,
}

/// Tombstone every doc in a bucket. "Empty" rather than "drop"
/// because NebulaDB buckets are implicit — the bucket namespace
/// survives an empty, which is the usual admin intent ("clear demo
/// data, keep structure").
async fn admin_empty_bucket(
    State(s): State<AppState>,
    axum::extract::Path(bucket): axum::extract::Path<String>,
) -> Result<Json<EmptyBucketResponse>, ApiError> {
    if bucket.is_empty() {
        return Err(ApiError::BadRequest("bucket name required".into()));
    }
    let removed = s.index.empty_bucket(&bucket);
    s.metrics.inc_delete();
    Ok(Json(EmptyBucketResponse { bucket, removed }))
}

async fn admin_stats(State(s): State<AppState>) -> Json<StatsResponse> {
    use std::sync::atomic::Ordering::Relaxed;
    let m = &s.metrics;
    let (hits, misses, evictions, inserts) = s
        .cache_stats
        .as_ref()
        .map(|c| c.snapshot())
        .unwrap_or((0, 0, 0, 0));
    Json(StatsResponse {
        requests_total: m.requests_total.load(Relaxed),
        requests_errors: m.requests_errors.load(Relaxed),
        auth_failures: m.auth_failures.load(Relaxed),
        rate_limited: m.rate_limited.load(Relaxed),
        jwt_failures: m.jwt_failures.load(Relaxed),
        docs_inserted: m.docs_inserted.load(Relaxed),
        docs_deleted: m.docs_deleted.load(Relaxed),
        searches_vector: m.searches_vector.load(Relaxed),
        searches_semantic: m.searches_semantic.load(Relaxed),
        rag_requests: m.rag_requests.load(Relaxed),
        embed_cache_hits: hits,
        embed_cache_misses: misses,
        embed_cache_evictions: evictions,
        embed_cache_inserts: inserts,
        total_docs_live: s.index.len(),
    })
}
