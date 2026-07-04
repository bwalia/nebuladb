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
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use nebula_llm::{build_rag_prompt, LlmChunk};

use crate::error::ApiError;
use crate::state::AppState;

pub fn build_router(state: AppState) -> Router {
    let limit = state.config.max_body_bytes;
    let request_timeout = state.config.request_timeout;

    // Auth + rate limit apply to the domain API only. Ops endpoints
    // (/healthz, /metrics) stay unthrottled so a scrape still works
    // during an abuse event.
    //
    // Layer order (outermost first):
    //   rate_limit -> auth -> body_limit -> trace -> handler
    // Rate limit runs first so a flood of unauthenticated requests
    // can't burn the auth middleware's CPU budget.

    // Streaming endpoints (SSE) opt out of the request timeout —
    // they're long-lived by design and a 30s cutoff would kill
    // the live log tail and the RAG token stream.
    let api_streaming = Router::new()
        .route("/admin/logs/stream", get(admin_logs_stream))
        .route("/ai/rag", post(ai_rag));

    // Everything else gets the optional per-request timeout.
    let api_normal = Router::new()
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
        .route("/rag/answer", post(rag_answer))
        .route("/query", post(sql_query))
        .route("/query/explain", post(sql_explain))
        .route("/admin/buckets", get(admin_buckets))
        .route("/admin/audit", get(admin_audit))
        .route("/admin/stats", get(admin_stats))
        .route("/admin/slow", get(admin_slow))
        .route("/admin/durability", get(admin_durability))
        .route("/admin/version", get(admin_version))
        .route("/admin/snapshot", post(admin_snapshot))
        .route("/admin/wal/compact", post(admin_wal_compact))
        .route("/admin/bucket/:bucket/empty", post(admin_empty_bucket))
        .route("/admin/bucket/:bucket/export", get(admin_export_bucket))
        .route("/admin/bucket/:bucket/import", post(admin_import_bucket))
        .route("/admin/bucket/:bucket/home-region", get(admin_home_region))
        .route(
            "/admin/backup",
            post(crate::backup_routes::admin_backup_start),
        )
        .route(
            "/admin/backup/:id",
            get(crate::backup_routes::admin_backup_status),
        )
        .route(
            "/admin/backups",
            get(crate::backup_routes::admin_backups_list),
        )
        .route(
            "/admin/backup/manifest",
            post(crate::backup_routes::admin_backup_manifest),
        )
        .route(
            "/admin/restore",
            post(crate::backup_routes::admin_restore_start),
        )
        .route(
            "/admin/restore/:id",
            get(crate::backup_routes::admin_restore_status),
        )
        .route("/admin/cluster/nodes", get(admin_cluster_nodes))
        .route("/admin/replication", get(admin_replication))
        .route("/admin/reliability", get(admin_reliability))
        .route("/admin/promote", post(admin_promote));
    let api_normal = if let Some(t) = request_timeout {
        api_normal.layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            t,
        ))
    } else {
        api_normal
    };

    let api = api_normal
        .merge(api_streaming)
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
        // Disk-critical write gate (design 0010 §3). Runs alongside
        // the follower guard — before auth for the same reason (the
        // answer is "not now" regardless of who's asking), after
        // audit so refused writes are still recorded.
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::middleware::guard_writes_under_disk_pressure,
        ))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::middleware::guard_wrong_home_region,
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
        // Liveness probe: 200 as soon as the process is serving HTTP
        // at all, independent of WAL recovery state. The docker
        // healthcheck uses this so a slow recovery doesn't trip the
        // unhealthy → kill loop. /healthz remains the rich readiness
        // probe (returns 503 during recovery via the boot stub).
        .route("/healthz/live", get(|| async { (StatusCode::OK, "alive") }))
        // Catch-up readiness (design 0009 §6): 200 when this node is
        // caught up to its leader (or is a leader/standalone), 503 while
        // it trails. The rolling orchestrator polls this between steps.
        .route("/healthz/caught-up", get(healthz_caught_up))
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
    /// Server crate version from Cargo. Stable across restarts; the
    /// operator uses this to detect an upgrade has taken effect without
    /// having to shell into the pod.
    version: &'static str,
    /// Git sha at build time when exposed via the `NEBULADB_GIT_SHA`
    /// environment variable during `cargo build`. `"unknown"` otherwise.
    git_commit: &'static str,
    /// Operating mode from the resource manager (design 0010 §3):
    /// "normal", "cpu_pressure", "memory_pressure", "disk_pressure",
    /// or "disk_critical". The status stays "ok" while degraded —
    /// readiness and degradation are different signals; a node under
    /// memory pressure still serves.
    mode: &'static str,
}

async fn healthz(State(s): State<AppState>) -> Json<Health> {
    Json(Health {
        status: "ok",
        docs: s.index.len(),
        dim: s.index.dim(),
        model: s.index.embedder_model().to_string(),
        version: crate::build_info::VERSION,
        git_commit: crate::build_info::GIT_COMMIT,
        mode: s.resource.mode().as_str(),
    })
}

/// `GET /api/v1/admin/reliability` — the operator's one-stop answer to
/// "what mode is this node in, why, and since when" (design 0010 §9):
/// current mode, per-resource pressure levels, the raw readings behind
/// them, and the recent transition history with causes.
async fn admin_reliability(
    State(s): State<AppState>,
) -> Json<nebula_resource::ReliabilitySnapshot> {
    Json(s.resource.snapshot())
}

async fn metrics_handler(
    State(s): State<AppState>,
) -> (StatusCode, [(&'static str, &'static str); 1], String) {
    let mut body = s.metrics.render();
    // Append embedding-cache counters when a cache is registered. We
    // render from here (not inside `Metrics::render`) because the
    // cache is a separate subsystem with its own counters and we
    // don't want `nebula-server::metrics` to depend on `nebula-cache`.
    if let Some(stats) = &s.cache_stats {
        let (hits, misses, evictions, inserts) = stats.snapshot();
        use std::fmt::Write as _;
        let _ = writeln!(
            body,
            "# HELP nebula_embed_cache_hits Cache hits on embedding lookup"
        );
        let _ = writeln!(body, "# TYPE nebula_embed_cache_hits counter");
        let _ = writeln!(body, "nebula_embed_cache_hits {hits}");
        let _ = writeln!(
            body,
            "# HELP nebula_embed_cache_misses Cache misses on embedding lookup"
        );
        let _ = writeln!(body, "# TYPE nebula_embed_cache_misses counter");
        let _ = writeln!(body, "nebula_embed_cache_misses {misses}");
        let _ = writeln!(
            body,
            "# HELP nebula_embed_cache_evictions Entries evicted from the LRU"
        );
        let _ = writeln!(body, "# TYPE nebula_embed_cache_evictions counter");
        let _ = writeln!(body, "nebula_embed_cache_evictions {evictions}");
        let _ = writeln!(
            body,
            "# HELP nebula_embed_cache_inserts New entries written into the cache"
        );
        let _ = writeln!(body, "# TYPE nebula_embed_cache_inserts counter");
        let _ = writeln!(body, "nebula_embed_cache_inserts {inserts}");
    }

    render_durability_metrics(&mut body, &s);
    render_memory_metrics(&mut body);
    render_reliability_metrics(&mut body, &s);

    // Replication catch-up gauges (design 0009 §6). On a follower this
    // probes the leader (≤500ms, gated to followers only); leaders /
    // standalones report caught_up=1 with no lag. Same source of truth
    // as GET /healthz/caught-up so dashboards and the rollout agree.
    {
        use std::fmt::Write as _;
        let status = compute_caught_up(&s).await;
        let _ = writeln!(
            body,
            "# HELP nebula_replication_caught_up 1 when this node is caught up to its leader (or is leader/standalone), else 0"
        );
        let _ = writeln!(body, "# TYPE nebula_replication_caught_up gauge");
        let _ = writeln!(
            body,
            "nebula_replication_caught_up {}",
            if status.caught_up { 1 } else { 0 }
        );
        if let Some(lag) = status.lag_bytes {
            let _ = writeln!(
                body,
                "# HELP nebula_replication_lag_bytes Follower byte lag behind the leader WAL tip (same-segment only)"
            );
            let _ = writeln!(body, "# TYPE nebula_replication_lag_bytes gauge");
            let _ = writeln!(body, "nebula_replication_lag_bytes {lag}");
        }
    }

    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
}

/// Append WAL / snapshot durability gauges to the `/metrics` body.
///
/// Reads ONLY from `AppState::durability_cache` — a set of atomics a
/// background sampler in `main.rs` refreshes every ~30s. This used to
/// walk the WAL (`wal_stats()` + per-segment `fs::metadata` over a
/// multi-GB WAL + two `latest_snapshot_header()` decodes) on every
/// scrape, which took ~8s of blocking disk I/O and timed out Prometheus
/// every 15s. Now it's a few atomic loads — no index calls, no disk.
///
/// Emission rules (kept consistent so alerts don't see bogus values):
/// - In-memory index / not yet sampled: the cache is all-zero, so we
///   emit `nebula_wal_bytes 0` and `nebula_wal_bytes_since_snapshot 0`,
///   and OMIT `nebula_snapshot_age_seconds` (there is no snapshot — a
///   huge bogus age would falsely trip `NebulaSnapshotAgeHigh`).
/// - Persistent but no snapshot yet: WAL gauges are real,
///   `snapshot_taken_at_ms == 0` so `nebula_snapshot_age_seconds` is
///   still omitted.
/// - `nebula_snapshot_scheduler_enabled` is always emitted (0/1); it's
///   the Bug C signal and must never silently disappear. Still read
///   directly off `s.snapshot_scheduler_enabled` (already cheap).
fn render_durability_metrics(body: &mut String, s: &AppState) {
    use std::fmt::Write as _;
    use std::time::SystemTime;

    let (wal_bytes, bytes_since, snapshot_taken_at_ms) = s.durability_cache.load();

    let _ = writeln!(
        body,
        "# HELP nebula_wal_bytes Total bytes in the WAL on disk (0 when in-memory only)"
    );
    let _ = writeln!(body, "# TYPE nebula_wal_bytes gauge");
    let _ = writeln!(body, "nebula_wal_bytes {wal_bytes}");

    // Bytes appended after the newest committed snapshot's segment seq.
    // Per-segment granularity (slight over-estimate bounded by one
    // segment) — see nebula_index::TextIndex::wal_bytes_since_snapshot.
    let _ = writeln!(
        body,
        "# HELP nebula_wal_bytes_since_snapshot Bytes in WAL segments at/after the newest snapshot's seq (per-segment granularity; 0 when in-memory)"
    );
    let _ = writeln!(body, "# TYPE nebula_wal_bytes_since_snapshot gauge");
    let _ = writeln!(body, "nebula_wal_bytes_since_snapshot {bytes_since}");

    // Snapshot age: only emitted when a snapshot actually exists
    // (taken_at_ms != 0), so a node with no snapshot doesn't report a
    // bogus multi-decade age. `now` is cheap, so it's computed here at
    // render time rather than cached.
    if snapshot_taken_at_ms != 0 {
        let now_secs = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let taken_secs = snapshot_taken_at_ms / 1000;
        let age = now_secs.saturating_sub(taken_secs);
        let _ = writeln!(
            body,
            "# HELP nebula_snapshot_age_seconds Wall time since the most recent committed snapshot"
        );
        let _ = writeln!(body, "# TYPE nebula_snapshot_age_seconds gauge");
        let _ = writeln!(body, "nebula_snapshot_age_seconds {age}");
    }

    let enabled = u8::from(s.snapshot_scheduler_enabled);
    let _ = writeln!(body, "# HELP nebula_snapshot_scheduler_enabled 1 if the snapshot scheduler has any trigger enabled, 0 if disabled (Bug C)");
    let _ = writeln!(body, "# TYPE nebula_snapshot_scheduler_enabled gauge");
    let _ = writeln!(body, "nebula_snapshot_scheduler_enabled {enabled}");
}

/// Operating-mode and resource-pressure gauges (design 0010 §9).
/// Everything here reads the resource manager's atomics / one short
/// mutex — no OS probing on the scrape path; the 5s background sampler
/// does that. Value mapping for `nebula_operating_mode` matches the
/// enum discriminants: 0=normal 1=cpu 2=memory 3=disk 4=disk_critical.
fn render_reliability_metrics(body: &mut String, s: &AppState) {
    use std::fmt::Write as _;
    let snap = s.resource.snapshot();

    let _ = writeln!(body, "# HELP nebula_operating_mode Node operating mode: 0=normal 1=cpu_pressure 2=memory_pressure 3=disk_pressure 4=disk_critical");
    let _ = writeln!(body, "# TYPE nebula_operating_mode gauge");
    let _ = writeln!(body, "nebula_operating_mode {}", snap.mode as u8);

    let _ = writeln!(body, "# HELP nebula_resource_pressure Per-resource pressure level: 0=ok 1=high 2=critical (absent when the source is unreadable)");
    let _ = writeln!(body, "# TYPE nebula_resource_pressure gauge");
    for (name, level) in [
        ("memory", snap.levels.memory),
        ("disk", snap.levels.disk),
        ("cpu", snap.levels.cpu),
    ] {
        if let Some(l) = level {
            let _ = writeln!(
                body,
                "nebula_resource_pressure{{resource=\"{name}\"}} {}",
                l as u8
            );
        }
    }

    if let (Some(free), Some(total)) = (snap.sample.disk_free, snap.sample.disk_total) {
        let _ = writeln!(
            body,
            "# HELP nebula_disk_free_bytes Free bytes on the data volume (unprivileged-available)"
        );
        let _ = writeln!(body, "# TYPE nebula_disk_free_bytes gauge");
        let _ = writeln!(body, "nebula_disk_free_bytes {free}");
        let _ = writeln!(
            body,
            "# HELP nebula_disk_total_bytes Total bytes on the data volume"
        );
        let _ = writeln!(body, "# TYPE nebula_disk_total_bytes gauge");
        let _ = writeln!(body, "nebula_disk_total_bytes {total}");
    }
    if let Some(cpu) = snap.sample.cpu_ratio {
        let _ = writeln!(body, "# HELP nebula_cpu_utilization_ratio Process CPU over the last sampling window as a fraction of all cores");
        let _ = writeln!(body, "# TYPE nebula_cpu_utilization_ratio gauge");
        let _ = writeln!(body, "nebula_cpu_utilization_ratio {cpu:.4}");
    }

    let _ = writeln!(
        body,
        "# HELP nebula_mode_transitions_total Committed operating-mode transitions since boot"
    );
    let _ = writeln!(body, "# TYPE nebula_mode_transitions_total counter");
    let _ = writeln!(
        body,
        "nebula_mode_transitions_total {}",
        snap.transitions_total
    );
}

/// cgroup memory accounting, exposed so an alert can fire BEFORE the
/// resident HNSW arena grows into the cgroup cap and the kernel OOM-kills
/// the process (which docker masks as a clean restart — see the
/// NebulaMemoryNearCap alert). These are cheap reads of two small cgroup
/// files (no lock, no disk-walk), so they stay on the hot scrape path.
/// cgroup v2 first, then v1; silently emit nothing if neither is present
/// (e.g. running outside a container in tests).
fn render_memory_metrics(body: &mut String) {
    use std::fmt::Write as _;
    let read_u64 =
        |p: &str| -> Option<u64> { std::fs::read_to_string(p).ok()?.trim().parse::<u64>().ok() };
    // current usage
    let current = read_u64("/sys/fs/cgroup/memory.current")
        .or_else(|| read_u64("/sys/fs/cgroup/memory/memory.usage_in_bytes"));
    // hard limit ("max" can be the literal "max" => unlimited => skip)
    let limit = read_u64("/sys/fs/cgroup/memory.max")
        .or_else(|| read_u64("/sys/fs/cgroup/memory/memory.limit_in_bytes"));
    if let Some(c) = current {
        let _ = writeln!(
            body,
            "# HELP nebula_memory_resident_bytes Process cgroup memory usage in bytes"
        );
        let _ = writeln!(body, "# TYPE nebula_memory_resident_bytes gauge");
        let _ = writeln!(body, "nebula_memory_resident_bytes {c}");
    }
    if let Some(l) = limit {
        let _ = writeln!(
            body,
            "# HELP nebula_memory_limit_bytes cgroup memory hard limit in bytes"
        );
        let _ = writeln!(body, "# TYPE nebula_memory_limit_bytes gauge");
        let _ = writeln!(body, "nebula_memory_limit_bytes {l}");
    }
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
            return Err(ApiError::BadRequest(
                "each item.text must be non-empty".into(),
            ));
        }
    }
    if bucket.is_empty() {
        return Err(ApiError::BadRequest("bucket must be non-empty".into()));
    }

    let requested = body.items.len();

    let inserted = if let Some(raft) = &s.raft {
        // Raft mode v1: one Raft submit per item. The standalone path
        // batches into a single embedder call + sequential HNSW
        // inserts with partial-success tolerance; we preserve the
        // batched embedder call here to keep upstream cost flat, then
        // submit one log entry per item.
        //
        // Trade-off: N round-trips through Raft consensus per request.
        // For N=100 in-DC that's ~100×election-RTT overhead vs the
        // single-fsync standalone path. Acceptable for v1; a future
        // ADR will introduce a `WalRecord::UpsertTextBulk` variant
        // for atomic bulk submit. That's a wire-format change with
        // bincode-variant-ordering discipline, hence not in this PR.
        // Tracked under "Phase 2.5d follow-up" — bulk-atomic submit.
        //
        // Partial-success semantics: if the Nth item fails (e.g. a
        // mid-batch leader change → NotLeader), we return the count
        // committed so far and surface the error. The standalone path
        // also tolerates partial success (one bad HNSW insert doesn't
        // fail the whole batch); this is the closest analogue we can
        // give without the bulk WAL variant.
        let inputs: Vec<String> = body.items.iter().map(|it| it.text.clone()).collect();
        let vectors = s
            .index
            .embedder()
            .embed(&inputs)
            .await
            .map_err(ApiError::Embed)?;
        if vectors.len() != body.items.len() {
            return Err(ApiError::Internal(format!(
                "embedder returned {} vectors for {} items",
                vectors.len(),
                body.items.len()
            )));
        }
        let mut committed = 0usize;
        for (item, vector) in body.items.into_iter().zip(vectors) {
            if vector.len() != s.index.dim() {
                return Err(ApiError::Embed(
                    nebula_embed::EmbedError::DimensionMismatch {
                        expected: s.index.dim(),
                        actual: vector.len(),
                    },
                ));
            }
            let payload = nebula_raft::LogPayload::Mutation(nebula_wal::WalRecord::UpsertText {
                bucket: bucket.clone(),
                external_id: item.id,
                text: item.text,
                vector,
                metadata_json: item.metadata.to_string(),
            });
            raft.submit_mutation(payload).await?;
            committed += 1;
        }
        committed
    } else {
        let prepared: Vec<(String, String, serde_json::Value)> = body
            .items
            .into_iter()
            .map(|d| (d.id, d.text, d.metadata))
            .collect();
        s.index.upsert_text_bulk(&bucket, &prepared).await?
    };

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
    if bucket.is_empty() || body.id.is_empty() {
        return Err(ApiError::BadRequest(
            "bucket and id must be non-empty".into(),
        ));
    }

    if let Some(raft) = &s.raft {
        // Raft mode: leader resolves text → vector, then submits the
        // log entry. Followers receive the entry with the resolved
        // vector and apply via the state-machine path. The embedder
        // is only called once per write — on the leader — keeping
        // followers embedder-free during apply (the property the
        // determinism test in nebula-raft/state_machine.rs locks in).
        let vector = s
            .index
            .embedder()
            .embed_one(&body.text)
            .await
            .map_err(ApiError::Embed)?;
        if vector.len() != s.index.dim() {
            return Err(ApiError::Embed(
                nebula_embed::EmbedError::DimensionMismatch {
                    expected: s.index.dim(),
                    actual: vector.len(),
                },
            ));
        }
        let payload = nebula_raft::LogPayload::Mutation(nebula_wal::WalRecord::UpsertText {
            bucket: bucket.clone(),
            external_id: body.id.clone(),
            text: body.text.clone(),
            vector,
            metadata_json: body.metadata.to_string(),
        });
        raft.submit_mutation(payload).await?;
    } else {
        s.index
            .upsert_text(&bucket, &body.id, &body.text, body.metadata)
            .await?;
    }
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
    if let Some(raft) = &s.raft {
        // Raft mode: deletes go through consensus the same as
        // upserts. The state-machine apply path tolerates a delete
        // for a missing id (replay no-op), which matches what
        // standalone does — see TextIndex::apply_wal_record.
        let payload = nebula_raft::LogPayload::Mutation(nebula_wal::WalRecord::Delete {
            bucket: bucket.clone(),
            external_id: id.clone(),
        });
        raft.submit_mutation(payload).await?;
    } else {
        // Standalone delete: WAL + inner.write + HNSW tombstone are
        // all synchronous. Hand off to the blocking pool so a burst
        // of deletes from a client app doesn't pin async workers.
        let idx = std::sync::Arc::clone(&s.index);
        let b = bucket.clone();
        let i = id.clone();
        tokio::task::spawn_blocking(move || idx.delete(&b, &i))
            .await
            .map_err(|e| ApiError::Internal(format!("delete task: {e}")))??;
    }
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
    /// Optional document kind. When set, the server selects a
    /// structure-aware chunking strategy for this document (design 0008
    /// §8) instead of the configured default chunker. The ingestion
    /// worker sets this from the detected file type.
    #[serde(default)]
    doc_type: Option<nebula_chunk::DocType>,
}

/// Resolve the chunker for one `/documents` request: a `doc_type`-driven
/// strategy when provided (sized from the server's chunk config), else
/// the server's default chunker. Returned as a value the caller borrows.
fn chunker_for(
    s: &AppState,
    doc_type: Option<nebula_chunk::DocType>,
) -> Result<std::sync::Arc<dyn nebula_chunk::Chunker>, ApiError> {
    match doc_type {
        Some(dt) => nebula_chunk::select_strategy(dt, s.config.chunk_chars, s.config.chunk_overlap)
            .map(std::sync::Arc::from)
            .map_err(|e| ApiError::BadRequest(format!("chunk strategy: {e}"))),
        None => Ok(std::sync::Arc::clone(&s.chunker)),
    }
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
    if bucket.is_empty() || body.doc_id.is_empty() {
        return Err(ApiError::BadRequest(
            "bucket and doc_id must be non-empty".into(),
        ));
    }

    let chunker = chunker_for(&s, body.doc_type)?;

    let chunk_count = if let Some(raft) = &s.raft {
        // Raft mode: chunk + batch-embed on the leader, then submit
        // a single UpsertDocument log entry. The state-machine apply
        // path handles the multi-chunk insert on every peer
        // identically — same code path standalone WAL replay uses.
        // One log entry covers the whole document so a partial apply
        // can't leave a doc half-indexed (this matches the standalone
        // contract documented on TextIndex::upsert_document).
        let chunks = chunker.chunk(&body.text);
        if chunks.is_empty() {
            return Err(ApiError::BadRequest("chunker produced no chunks".into()));
        }
        let inputs: Vec<String> = chunks.iter().map(|c| c.text.clone()).collect();
        let vectors = s
            .index
            .embedder()
            .embed(&inputs)
            .await
            .map_err(ApiError::Embed)?;
        if vectors.len() != chunks.len() {
            return Err(ApiError::Internal(format!(
                "embedder returned {} vectors for {} chunks",
                vectors.len(),
                chunks.len()
            )));
        }
        for v in &vectors {
            if v.len() != s.index.dim() {
                return Err(ApiError::Embed(
                    nebula_embed::EmbedError::DimensionMismatch {
                        expected: s.index.dim(),
                        actual: v.len(),
                    },
                ));
            }
        }
        let wal_chunks: Vec<nebula_wal::WalChunk> = chunks
            .iter()
            .zip(vectors.iter())
            .map(|(c, v)| nebula_wal::WalChunk {
                index: c.index,
                char_start: c.char_start,
                text: c.text.clone(),
                vector: v.clone(),
            })
            .collect();
        let count = wal_chunks.len();
        let payload = nebula_raft::LogPayload::Mutation(nebula_wal::WalRecord::UpsertDocument {
            bucket: bucket.clone(),
            doc_id: body.doc_id.clone(),
            chunks: wal_chunks,
            metadata_json: body.metadata.to_string(),
        });
        raft.submit_mutation(payload).await?;
        count
    } else {
        s.index
            .upsert_document(
                &bucket,
                &body.doc_id,
                &body.text,
                chunker.as_ref(),
                body.metadata,
            )
            .await?
    };
    s.metrics.inc_insert();
    Ok(Json(UpsertDocumentResponse {
        bucket,
        doc_id: body.doc_id,
        chunks: chunk_count,
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
    let n = if let Some(raft) = &s.raft {
        // Raft mode: peek the chunk count from the local index for
        // the response body, then submit. The peek is best-effort —
        // a follower's view may lag, so the count we return is the
        // count the *leader* saw at submit time. That's still the
        // most useful number for the client (it answers "did the
        // delete cover anything?"). The state-machine apply tolerates
        // a missing doc_id so a stale peek that under-counts is a
        // monitoring concern, not a correctness one.
        let local_count = s.index.count_chunks(&bucket, &doc_id);
        let payload = nebula_raft::LogPayload::Mutation(nebula_wal::WalRecord::DeleteDocument {
            bucket: bucket.clone(),
            doc_id: doc_id.clone(),
        });
        raft.submit_mutation(payload).await?;
        local_count
    } else {
        // Standalone path: delete_document tombstones every chunk
        // of the parent doc — linear in chunk count, fully sync.
        // Off to the blocking pool.
        let idx = std::sync::Arc::clone(&s.index);
        let b = bucket.clone();
        let d = doc_id.clone();
        tokio::task::spawn_blocking(move || idx.delete_document(&b, &d))
            .await
            .map_err(|e| ApiError::Internal(format!("delete_document task: {e}")))??
    };
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
    /// Opt into hybrid (BM25 + vector fusion) retrieval. Default false
    /// keeps vector-only behaviour until benchmarked (design 0008 §6).
    #[serde(default)]
    hybrid: bool,
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
    // search_vector_blocking moves the HNSW traversal onto tokio's
    // blocking pool. Calling the synchronous variant from an async
    // handler pins a worker for the duration of the search; a burst
    // of concurrent searches then starves /healthz and wedges the
    // runtime — the symptom that motivated this change.
    let hits = std::sync::Arc::clone(&s.index)
        .search_vector_blocking(req.vector, req.bucket, top_k, req.ef)
        .await?;
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
    let hits = if req.hybrid {
        let weights = s.hybrid_weights.resolve(req.bucket.as_deref());
        std::sync::Arc::clone(&s.index)
            .search_text_hybrid_blocking(req.query, req.bucket, top_k, req.ef, weights)
            .await?
    } else {
        std::sync::Arc::clone(&s.index)
            .search_text_blocking(req.query, req.bucket, top_k, req.ef)
            .await?
    };
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
        return Err(ApiError::BadRequest(format!("top_k exceeds max ({max})")));
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

    // search_text is async only because of the embedder call;
    // the HNSW lookup it dispatches to is synchronous. Use the
    // _blocking variant so retrieval doesn't pin the tokio worker
    // for the duration of the graph walk. This was the wedge
    // trigger observed via showcase RAG chat after PR #51 / #52
    // (which fixed search_vector but missed the search_text leak
    // for the RAG / SQL / gRPC paths).
    let hits = std::sync::Arc::clone(&s.index)
        .search_text_blocking(req.query.clone(), req.bucket.clone(), top_k, None)
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
        Ok(LlmChunk::Delta(t)) => {
            Ok::<_, Infallible>(Event::default().event("answer_delta").data(t))
        }
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

    let stream: std::pin::Pin<Box<dyn Stream<Item = Result<Event, Infallible>> + Send>> = Box::pin(
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

// ---------- RAG answer (batteries-included, design 0008 §5) ----------

#[derive(Deserialize)]
struct RagAnswerRequest {
    query: String,
    #[serde(default = "default_rag_top_k")]
    top_k: usize,
    #[serde(default)]
    bucket: Option<String>,
    /// Use hybrid (BM25 + vector fusion) retrieval for grounding rather
    /// than vector-only (design 0008 §6). Default false.
    #[serde(default)]
    hybrid: bool,
    /// Run the configured query expander before retrieval (design 0008
    /// §7). No effect when the server uses the default NoopQueryExpander.
    #[serde(default)]
    expand: bool,
    /// Run the configured reranker after retrieval (design 0008 §7).
    /// No effect when the server uses the default NoopReranker.
    #[serde(default)]
    rerank: bool,
}

/// One retrieved chunk, attributed back to its source document. `doc`
/// and `chunk` are derived by splitting the stored external id on the
/// `'#'` convention (`{doc_id}#{chunk_index}`); a row with no `'#'`
/// (a plain text upsert, not a chunked document) reports the whole id
/// as `doc` and a `chunk` of `None`.
#[derive(Serialize)]
struct AnswerSource {
    doc: String,
    chunk: Option<usize>,
    score: f32,
}

#[derive(Serialize)]
struct RagAnswerResponse {
    query: String,
    answer: String,
    sources: Vec<AnswerSource>,
    took_ms: u64,
}

fn attribute(hit: &nebula_index::Hit) -> AnswerSource {
    match hit.id.rsplit_once('#') {
        Some((doc, chunk)) => AnswerSource {
            doc: doc.to_string(),
            chunk: chunk.parse().ok(),
            score: hit.score,
        },
        None => AnswerSource {
            doc: hit.id.clone(),
            chunk: None,
            score: hit.score,
        },
    }
}

/// Shared retrieval pipeline for the answer path (design 0008 §7):
///
/// 1. **Query expansion** (opt-in): expand the query into variants and
///    retrieve for each, merging by id and keeping each chunk's best
///    score. Overfetches per variant so the merged set is rich.
/// 2. **Reranking** (opt-in): hand the merged candidates to the
///    configured reranker, which reorders by a more accurate model and
///    truncates to `top_k`. The result is re-projected back onto the
///    original `Hit`s (the reranker only returns ids + scores).
///
/// With both flags off and the default Noop stages, this collapses to a
/// single retrieval call — identical to the pre-§7 behaviour.
async fn retrieve_grounding(
    s: &AppState,
    query: &str,
    bucket: &Option<String>,
    top_k: usize,
    hybrid: bool,
    expand: bool,
    rerank: bool,
) -> Result<Vec<nebula_index::Hit>, ApiError> {
    // 1. Expansion. Default expander returns just the original query, so
    // the non-expand path is a single-element loop.
    let variants = if expand {
        s.query_expander.expand(query).await
    } else {
        vec![query.to_string()]
    };

    // Overfetch when we'll rerank/merge so the better stage has more to
    // work with; otherwise fetch exactly top_k.
    let fetch_k = if rerank || variants.len() > 1 {
        top_k.saturating_mul(3).max(top_k)
    } else {
        top_k
    };

    // Retrieve per variant, merge by id keeping the best (smallest
    // distance / largest fused) score. We dedup on id so the same chunk
    // surfaced by two variants isn't double-counted.
    let mut by_id: std::collections::HashMap<String, nebula_index::Hit> =
        std::collections::HashMap::new();
    let weights = s.hybrid_weights.resolve(bucket.as_deref());
    for v in &variants {
        let hits = if hybrid {
            std::sync::Arc::clone(&s.index)
                .search_text_hybrid_blocking(v.clone(), bucket.clone(), fetch_k, None, weights)
                .await?
        } else {
            std::sync::Arc::clone(&s.index)
                .search_text_blocking(v.clone(), bucket.clone(), fetch_k, None)
                .await?
        };
        for h in hits {
            by_id
                .entry(h.id.clone())
                .and_modify(|existing| {
                    // Hybrid scores are higher-is-better; vector
                    // distances lower-is-better. Keep whichever the
                    // current pipeline considers stronger.
                    let better = if hybrid {
                        h.score > existing.score
                    } else {
                        h.score < existing.score
                    };
                    if better {
                        *existing = h.clone();
                    }
                })
                .or_insert(h);
        }
    }
    let mut merged: Vec<nebula_index::Hit> = by_id.into_values().collect();

    // Stable pre-rerank ordering so the Noop reranker (order-preserving)
    // and the no-rerank path agree. Hybrid: descending; vector: ascending.
    merged.sort_by(|a, b| {
        if hybrid {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        } else {
            a.score
                .partial_cmp(&b.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        }
        .then(a.id.cmp(&b.id))
    });

    // 2. Rerank (opt-in). The default NoopReranker just truncates in the
    // order above, so the non-rerank path and default-config path match.
    if rerank {
        let candidates: Vec<nebula_rerank::Candidate> = merged
            .iter()
            .map(|h| nebula_rerank::Candidate {
                id: h.id.clone(),
                text: h.text.clone(),
            })
            .collect();
        let scored = s
            .reranker
            .rerank(query, &candidates, top_k)
            .await
            .map_err(|e| ApiError::Internal(format!("rerank: {e}")))?;
        // Re-project ids → Hits, applying the reranker's score.
        let mut index: std::collections::HashMap<String, nebula_index::Hit> =
            merged.into_iter().map(|h| (h.id.clone(), h)).collect();
        let reordered = scored
            .into_iter()
            .filter_map(|sc| {
                index.remove(&sc.id).map(|mut h| {
                    h.score = sc.score;
                    h
                })
            })
            .collect();
        Ok(reordered)
    } else {
        merged.truncate(top_k);
        Ok(merged)
    }
}

/// Non-streaming, one-call RAG: retrieve → prompt → drain LLM → return
/// `{ answer, sources }`. The streaming variant lives at `/ai/rag`;
/// this endpoint exists so SQL/ORM clients and the `ai_answer()` demo
/// get a single JSON object with structured source attribution rather
/// than raw `Hit`s.
async fn rag_answer(
    State(s): State<AppState>,
    Json(req): Json<RagAnswerRequest>,
) -> Result<Json<RagAnswerResponse>, ApiError> {
    if req.query.trim().is_empty() {
        return Err(ApiError::BadRequest("query must be non-empty".into()));
    }
    let top_k = validate_top_k(req.top_k, s.config.max_top_k)?;
    s.metrics.inc_rag();
    let started = std::time::Instant::now();

    let hits = retrieve_grounding(
        &s,
        &req.query,
        &req.bucket,
        top_k,
        req.hybrid,
        req.expand,
        req.rerank,
    )
    .await?;
    let snippets: Vec<&str> = hits.iter().map(|h| h.text.as_str()).collect();
    let prompt = build_rag_prompt(&req.query, &snippets);

    // Surface a provider error (bad key, dead Ollama) as an HTTP 5xx
    // before we commit to a 200 body.
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

    let sources = hits.iter().map(attribute).collect();
    Ok(Json(RagAnswerResponse {
        query: req.query,
        answer,
        sources,
        took_ms: started.elapsed().as_millis() as u64,
    }))
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

/// How long a cached `/admin/buckets` result is served before the
/// next request triggers a fresh scan. The Admin tab polls every ~3s,
/// so a 15s TTL turns an unbounded pile-up of corpus scans into at
/// most four polls per scan — stats lag by up to 15s, which is fine
/// for a dashboard panel.
const BUCKET_STATS_TTL: std::time::Duration = std::time::Duration::from_secs(15);

async fn admin_buckets(
    State(s): State<AppState>,
    axum::extract::Query(q): axum::extract::Query<BucketsQuery>,
) -> Result<Json<Vec<nebula_index::BucketStats>>, ApiError> {
    // `bucket_stats` is a full corpus scan — it iterates every doc
    // in every bucket and tallies metadata keys, holding the index
    // read lock throughout. At ~84k docs that was a multi-hundred-ms
    // walk; at millions of docs it outlives the Admin tab's ~3s poll
    // interval, so without a gate the scans stack without bound, the
    // read lock is held continuously, and a queued writer wedges
    // every new reader (SQL queries stop responding — incident
    // 2026-06-04). Two defenses:
    //   1. spawn_blocking keeps the scan off the async workers
    //      (the original fix — same wedge mechanism as sync search,
    //      see router.rs:628 et al).
    //   2. the cache mutex is held across the scan: concurrent polls
    //      wait for the in-flight scan instead of starting their
    //      own, then everyone inside the TTL gets the cached copy.
    let top_keys = q.top_keys;
    let mut cache = s.bucket_stats_cache.lock().await;
    if let Some(entry) = cache.as_ref() {
        if entry.top_keys == top_keys && entry.at.elapsed() < BUCKET_STATS_TTL {
            return Ok(Json(entry.stats.clone()));
        }
    }
    let idx = std::sync::Arc::clone(&s.index);
    let stats = tokio::task::spawn_blocking(move || idx.bucket_stats(top_keys))
        .await
        .map_err(|e| ApiError::Internal(format!("bucket_stats task: {e}")))?;
    *cache = Some(crate::state::BucketStatsCacheEntry {
        at: std::time::Instant::now(),
        top_keys,
        stats: stats.clone(),
    });
    Ok(Json(stats))
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
async fn admin_slow(State(s): State<AppState>) -> Json<Vec<crate::slow_log::SlowQueryEntry>> {
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

#[derive(Serialize)]
struct VersionInfo {
    /// Cargo package version — matches the git tag on releases.
    version: &'static str,
    /// Short git sha when baked in at build time, `"unknown"` otherwise.
    git_commit: &'static str,
    /// Build date when baked in at build time.
    build_date: &'static str,
    /// Target OS / architecture the binary was compiled for.
    os: &'static str,
    arch: &'static str,
}

/// Full build identity. Used by the Kubernetes operator to verify an
/// upgrade has taken effect inside the pod, and by `nebula-ctl`-style
/// tooling for version-compatibility checks before issuing admin calls.
async fn admin_version() -> Json<VersionInfo> {
    Json(VersionInfo {
        version: crate::build_info::VERSION,
        git_commit: crate::build_info::GIT_COMMIT,
        build_date: crate::build_info::BUILD_DATE,
        os: crate::build_info::OS,
        arch: crate::build_info::ARCH,
    })
}

async fn admin_snapshot(
    State(s): State<AppState>,
) -> Result<Json<nebula_index::SnapshotOutcome>, ApiError> {
    // `TextIndex::snapshot` holds the inner read lock through the
    // disk write (streaming bincode → zstd → file). At a few hundred
    // MB that's seconds of synchronous I/O — far too long to run on
    // a tokio worker thread, which would block every other axum
    // handler queued behind it. The background snapshot scheduler
    // already wraps the same call in `spawn_blocking`; do the same
    // for the admin-triggered path.
    let idx = s.index.clone();
    let out = tokio::task::spawn_blocking(move || idx.snapshot())
        .await
        .map_err(|e| ApiError::Internal(format!("snapshot task panicked: {e}")))??;
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
    /// This node's own region name, if multi-region is configured.
    /// Included in the top-level view (rather than under `remotes`)
    /// so dashboards can label the local entry without a second API
    /// call to `/healthz`.
    #[serde(skip_serializing_if = "Option::is_none")]
    region: Option<String>,
    /// Per-remote-region view. Empty for single-region deployments.
    /// Each entry is the consumer task's self-reported state for that
    /// remote's stream.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    remotes: Vec<crate::cross_region_status::RemoteRegionStatus>,
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
    let (leader_newest_probed, lag_bytes, behind) =
        match (s.cluster.is_follower(), &follower_applied) {
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

    let remotes = s.cross_region_status.snapshot();
    let region = s.cluster.region.clone();
    Json(ReplicationInfo {
        // Report the runtime-mutable role (design 0009 §5), not the
        // boot-time one — after a promote this node IS the leader.
        role: s.role.load(),
        local_newest,
        follower_applied,
        leader_newest_probed,
        lag_bytes,
        behind,
        region,
        remotes,
    })
}

// ---- promotion (design 0009 §5) ----

#[derive(Serialize)]
struct PromoteResponse {
    /// Role after the call. Always `"leader"` on success.
    role: crate::cluster::NodeRole,
    /// Whether this call performed the flip (`true`) or the node was
    /// already a leader (`false`). Lets the orchestrator treat promote
    /// as idempotent.
    promoted: bool,
}

/// Promote this node from Follower to Leader at runtime (design 0009 §5).
///
/// Operator/orchestrator-triggered (a 2-node cluster can't safely
/// auto-elect — design 0007 §6). The flip is observed by all three
/// protocol write-guards at once because they share one
/// [`nebula_core::AtomicNodeRole`]. The node also stops tailing its
/// former leader so it doesn't fight its own writes.
///
/// - Follower → Leader: flips the role, aborts the follower tail task,
///   returns `promoted: true`.
/// - Leader → Leader: idempotent no-op, `promoted: false` (so a retry
///   or a double-trigger from the orchestrator is safe).
/// - Standalone: 400 — there is no leader to promote toward, and a
///   standalone already accepts writes. Promoting it would be a
///   meaningless role change that breaks the standalone invariant.
async fn admin_promote(State(s): State<AppState>) -> Result<Json<PromoteResponse>, ApiError> {
    use crate::cluster::NodeRole;
    match s.role.load() {
        NodeRole::Leader => Ok(Json(PromoteResponse {
            role: NodeRole::Leader,
            promoted: false,
        })),
        NodeRole::Standalone => Err(ApiError::BadRequest(
            "node is standalone; nothing to promote (it already accepts writes)".into(),
        )),
        NodeRole::Follower => {
            // Stop tailing the former leader BEFORE accepting writes, so
            // a record we accept post-promotion can't race a late inbound
            // replicated record. abort() on an already-finished task is a
            // no-op, so this is safe even if the tail already exited.
            if let Some(abort) = s.follower_abort.get() {
                abort.abort();
                tracing::info!("promote: aborted follower replication task");
            }
            s.role.store(NodeRole::Leader);
            tracing::warn!("node promoted Follower -> Leader (design 0009 §5)");
            Ok(Json(PromoteResponse {
                role: NodeRole::Leader,
                promoted: true,
            }))
        }
    }
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
    let live = tokio_stream::wrappers::BroadcastStream::new(rx).filter_map(move |item| {
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
                    Some(Ok::<_, Infallible>(Event::default().event("lagged").data(
                        format!("missed {n} events; reconnect for a fresh snapshot"),
                    )))
                }
            }
        }
    });

    let stream: std::pin::Pin<Box<dyn Stream<Item = Result<Event, Infallible>> + Send>> = Box::pin(
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
/// Catch-up threshold in bytes (design 0009 §6). A follower within this
/// many bytes of the leader's WAL tip — in the same segment — counts as
/// "caught up", tolerating a record or two in flight. Override with
/// `NEBULA_CATCHUP_THRESHOLD_BYTES`.
fn catchup_threshold_bytes() -> u64 {
    std::env::var("NEBULA_CATCHUP_THRESHOLD_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4096)
}

/// Replication catch-up state for the orchestrator's between-step gate
/// (design 0009 §6). Shared by `GET /healthz/caught-up` and the
/// `/metrics` replication gauges so both report one source of truth.
struct CaughtUpStatus {
    /// True when this node is ready to be treated as "serving" for a
    /// rollout: a leader/standalone is always caught up; a follower is
    /// caught up when it is not behind the probed leader tip (or within
    /// the byte threshold in the same segment).
    caught_up: bool,
    /// Byte lag vs the leader tip when measurable in the same segment.
    /// `None` for leader/standalone, or when the lag spans segments, or
    /// when the leader probe is unavailable.
    lag_bytes: Option<u64>,
}

/// Compute catch-up status for this node. On a follower this performs
/// the same leader HTTP probe as `/admin/replication`; on a
/// leader/standalone it is a cheap trivially-caught-up result.
async fn compute_caught_up(s: &AppState) -> CaughtUpStatus {
    // Use the runtime role (post-promote a node is a leader).
    if !s.role.is_read_only() {
        return CaughtUpStatus {
            caught_up: true,
            lag_bytes: None,
        };
    }
    let applied = s.follower_cursor.as_ref().map(|fc| fc.load());
    let Some((applied_seg, applied_off)) = applied else {
        // Follower with no cursor yet — not caught up.
        return CaughtUpStatus {
            caught_up: false,
            lag_bytes: None,
        };
    };
    match fetch_leader_newest().await {
        Some(leader) => {
            let behind = leader.segment_seq > applied_seg
                || (leader.segment_seq == applied_seg && leader.byte_offset > applied_off);
            let lag = if leader.segment_seq == applied_seg {
                Some(leader.byte_offset.saturating_sub(applied_off))
            } else {
                None
            };
            // Caught up when not behind, OR within-threshold in the
            // same segment (a record or two may be mid-flight).
            let caught_up = !behind || lag.map(|l| l <= catchup_threshold_bytes()).unwrap_or(false);
            CaughtUpStatus {
                caught_up,
                lag_bytes: lag,
            }
        }
        // Can't probe the leader — report not-caught-up so the
        // orchestrator waits/aborts rather than proceeding blind.
        None => CaughtUpStatus {
            caught_up: false,
            lag_bytes: None,
        },
    }
}

#[derive(Serialize)]
struct CaughtUpResponse {
    caught_up: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    lag_bytes: Option<u64>,
    role: crate::cluster::NodeRole,
}

/// `GET /healthz/caught-up` (design 0009 §6). The rolling orchestrator
/// polls this between steps: it returns 200 when this node is caught up
/// (safe to treat as serving / safe to proceed to the next node) and
/// 503 when it is still trailing its leader. A leader/standalone is
/// trivially 200.
async fn healthz_caught_up(State(s): State<AppState>) -> (StatusCode, Json<CaughtUpResponse>) {
    let status = compute_caught_up(&s).await;
    let code = if status.caught_up {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        code,
        Json(CaughtUpResponse {
            caught_up: status.caught_up,
            lag_bytes: status.lag_bytes,
            role: s.role.load(),
        }),
    )
}

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
    let url = format!(
        "{}/api/v1/admin/replication",
        rest_base.trim_end_matches('/')
    );
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
    // empty_bucket tombstones every doc in the bucket — linear in
    // bucket size. On `leads` (~84k docs) that's a real wedge risk
    // if called from an admin script while writes are flowing.
    let idx = std::sync::Arc::clone(&s.index);
    let b = bucket.clone();
    let removed = tokio::task::spawn_blocking(move || idx.empty_bucket(&b))
        .await
        .map_err(|e| ApiError::Internal(format!("empty_bucket task: {e}")))?;
    s.metrics.inc_delete();
    Ok(Json(EmptyBucketResponse { bucket, removed }))
}

/// Response shape for bucket export. Carries the full pre-embedded
/// document payload so a target node can ingest without its own
/// embedder. The wire format is stable JSON — `nebula-ctl` and the
/// operator's rebalance coordinator both consume it.
#[derive(Serialize)]
struct ExportBucketResponse {
    bucket: String,
    dim: usize,
    model: String,
    count: usize,
    docs: Vec<nebula_index::ExportedDoc>,
}

/// Streaming-style (but in one JSON blob for now) bucket export.
/// Holding the whole payload in memory is fine at current scale
/// — a future revision should chunked-encode for very large buckets.
async fn admin_export_bucket(
    State(s): State<AppState>,
    axum::extract::Path(bucket): axum::extract::Path<String>,
) -> Result<Json<ExportBucketResponse>, ApiError> {
    if bucket.is_empty() {
        return Err(ApiError::BadRequest("bucket name required".into()));
    }
    // export_bucket walks every doc in the bucket and serializes
    // the full payload. Linear scan; same wedge risk as
    // empty_bucket. Hand off to the blocking pool.
    let idx = std::sync::Arc::clone(&s.index);
    let b = bucket.clone();
    let docs = tokio::task::spawn_blocking(move || idx.export_bucket(&b))
        .await
        .map_err(|e| ApiError::Internal(format!("export_bucket task: {e}")))?;
    Ok(Json(ExportBucketResponse {
        bucket,
        dim: s.index.dim(),
        model: s.index.embedder_model().to_string(),
        count: docs.len(),
        docs,
    }))
}

/// Request body for bucket import. The `dim` field is informational
/// (already implied by the vectors) but included so mistakes fail
/// loudly before we touch the WAL.
#[derive(Deserialize)]
struct ImportBucketRequest {
    #[serde(default)]
    dim: Option<usize>,
    docs: Vec<nebula_index::ExportedDoc>,
}

#[derive(Serialize)]
struct ImportBucketResponse {
    bucket: String,
    imported: usize,
    requested: usize,
}

/// Ingest a batch of pre-embedded documents into a bucket. The target
/// index's `dim()` must match every vector in the payload; mismatches
/// abort the batch up-front so the bucket never ends up in a mixed-dim
/// state. Designed to pair with `GET /admin/bucket/:b/export` —
/// together they form the rebalance-swap primitive.
/// Response body for `GET /admin/bucket/:b/home-region`.
///
/// Stable shape — consumed by the `nebula-client` SDK for routing
/// writes to the correct region, and by the operator's failover
/// controller to read the current epoch before bumping it.
///
/// `node_region` is included so a caller on the wrong region gets
/// both "where does this bucket live?" and "where am I?" in one hop,
/// avoiding a follow-up `/healthz` probe.
#[derive(Serialize)]
struct HomeRegionResponse {
    bucket: String,
    home_region: Option<String>,
    home_epoch: u64,
    replicated_to: Vec<String>,
    node_region: String,
    has_home: bool,
}

/// Report the cross-region coordination state for `bucket`. Reads the
/// seed doc's metadata — a cheap local lookup, no peer traffic. When
/// the seed doc doesn't exist or doesn't have a home set, returns
/// `has_home: false` so callers can fall back to local routing.
async fn admin_home_region(
    State(s): State<AppState>,
    axum::extract::Path(bucket): axum::extract::Path<String>,
) -> Result<Json<HomeRegionResponse>, ApiError> {
    if bucket.is_empty() {
        return Err(ApiError::BadRequest("bucket name required".into()));
    }
    let hr = s
        .index
        .get(&bucket, crate::home_region::SEED_DOC_ID)
        .map(|doc| crate::home_region::HomeRegion::from_metadata(&doc.metadata))
        .unwrap_or_default();
    Ok(Json(HomeRegionResponse {
        bucket,
        has_home: hr.has_home(),
        home_region: hr.region,
        home_epoch: hr.epoch,
        replicated_to: hr.replicated_to,
        node_region: s.cluster.region_or_default().to_string(),
    }))
}

async fn admin_import_bucket(
    State(s): State<AppState>,
    axum::extract::Path(bucket): axum::extract::Path<String>,
    Json(body): Json<ImportBucketRequest>,
) -> Result<Json<ImportBucketResponse>, ApiError> {
    if bucket.is_empty() {
        return Err(ApiError::BadRequest("bucket name required".into()));
    }
    if body.docs.is_empty() {
        return Err(ApiError::BadRequest("docs must be non-empty".into()));
    }
    if let Some(claimed) = body.dim {
        if claimed != s.index.dim() {
            return Err(ApiError::BadRequest(format!(
                "dim mismatch: payload claims {}, target index is {}",
                claimed,
                s.index.dim()
            )));
        }
    }
    let requested = body.docs.len();
    // import_bucket is bulk insert — linear in payload size. Move
    // it off the async worker so the upload doesn't pin the runtime.
    let idx = std::sync::Arc::clone(&s.index);
    let b = bucket.clone();
    let docs_in = body.docs;
    let imported = tokio::task::spawn_blocking(move || idx.import_bucket(&b, &docs_in))
        .await
        .map_err(|e| ApiError::Internal(format!("import_bucket task: {e}")))?
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    s.metrics.inc_insert();
    Ok(Json(ImportBucketResponse {
        bucket,
        imported,
        requested,
    }))
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
