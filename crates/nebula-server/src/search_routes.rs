//! Native Azure AI Search–compatible REST + unified search helpers.
//!
//! Mounted at `/api/v1/azure-search/*` (and optionally `/api/v1/indexes/*`
//! when `NEBULA_AZURE_SEARCH_ALIAS_INDEXES=1`).

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use serde_json::json;

use nebula_search::{
    DualSearchBackend, IndexBatch, IndexDefinition, SearchBackend, SearchBackendKind, SearchError,
    SearchMode, SearchRequest, SearchResponse as UnifiedSearchResponse, VectorQuery,
};

use crate::error::ApiError;
use crate::state::AppState;

fn map_search_err(e: SearchError) -> ApiError {
    match e {
        SearchError::BadRequest(m) => ApiError::BadRequest(m),
        SearchError::NotFound(m) => ApiError::NotFound(m),
        SearchError::Unsupported(m) => ApiError::BadRequest(format!(
            "unsupported (see GET /api/v1/compat/azure_ai_search): {m}"
        )),
        SearchError::Index(e) => ApiError::Index(e),
        SearchError::Azure(m) | SearchError::Other(m) => ApiError::BadRequest(m),
        SearchError::Http(e) => ApiError::BadRequest(format!("http: {e}")),
    }
}

fn backend_from_headers(headers: &HeaderMap, req: &mut SearchRequest) {
    if let Some(v) = headers
        .get("x-nebula-search-backend")
        .and_then(|h| h.to_str().ok())
    {
        match v.to_ascii_lowercase().as_str() {
            "azure" => req.backend = SearchBackendKind::Azure,
            "native" => req.backend = SearchBackendKind::Native,
            _ => {}
        }
    }
}

/// Run a unified search through the dual backend (legacy + Azure routes).
pub async fn run_unified_search(
    state: &AppState,
    mut req: SearchRequest,
    headers: &HeaderMap,
) -> Result<UnifiedSearchResponse, ApiError> {
    backend_from_headers(headers, &mut req);
    if req.top == 0 {
        return Err(ApiError::BadRequest("top/top_k must be > 0".into()));
    }
    if req.top > state.config.max_top_k {
        return Err(ApiError::BadRequest(format!(
            "top exceeds max ({})",
            state.config.max_top_k
        )));
    }
    state
        .search
        .search(req)
        .await
        .map_err(map_search_err)
}

#[derive(Deserialize)]
pub struct AzureSearchBody {
    #[serde(default)]
    pub search: String,
    #[serde(default = "default_top")]
    pub top: usize,
    #[serde(default)]
    pub skip: usize,
    #[serde(default)]
    pub filter: Option<String>,
    #[serde(default)]
    pub facets: Vec<String>,
    #[serde(default)]
    pub orderby: Option<String>,
    #[serde(default, rename = "searchFields")]
    pub search_fields: Option<String>,
    #[serde(default, rename = "vectorQueries")]
    pub vector_queries: Vec<AzureVectorQuery>,
    #[serde(default, rename = "queryType")]
    pub query_type: Option<String>,
    #[serde(default, rename = "semanticConfiguration")]
    pub semantic_configuration: Option<String>,
    #[serde(default)]
    pub explain: bool,
    #[serde(default)]
    pub tenant: Option<String>,
}

#[derive(Deserialize)]
pub struct AzureVectorQuery {
    #[serde(default)]
    pub fields: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub vector: Option<Vec<f32>>,
    #[serde(default = "default_top")]
    pub k: usize,
}

fn default_top() -> usize {
    10
}

fn mode_from_query_type(qt: Option<&str>, has_vectors: bool) -> SearchMode {
    match qt.map(|s| s.to_ascii_lowercase()).as_deref() {
        Some("semantic") => SearchMode::Semantic,
        Some("full") => SearchMode::FullText,
        _ if has_vectors => SearchMode::Hybrid,
        _ => SearchMode::Hybrid,
    }
}

fn azure_body_to_request(index: &str, body: AzureSearchBody) -> SearchRequest {
    let has_vectors = !body.vector_queries.is_empty();
    let mode = mode_from_query_type(body.query_type.as_deref(), has_vectors);
    let orderby = body
        .orderby
        .map(|s| {
            s.split(',')
                .map(|x| x.trim().to_string())
                .filter(|x| !x.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let search_fields = body
        .search_fields
        .map(|s| {
            s.split(',')
                .map(|x| x.trim().to_string())
                .filter(|x| !x.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let vector_queries = body
        .vector_queries
        .into_iter()
        .map(|vq| VectorQuery {
            fields: vq.fields.unwrap_or_else(|| "contentVector".into()),
            kind: vq.kind,
            vector: vq.vector,
            k: vq.k,
        })
        .collect();
    SearchRequest {
        query: body.search,
        mode,
        index: Some(index.to_string()),
        bucket: Some(index.to_string()),
        top: body.top,
        skip: body.skip,
        filter: body.filter,
        facets: body.facets,
        orderby,
        search_fields,
        vector_queries,
        semantic_configuration: body.semantic_configuration,
        ef: None,
        explain: body.explain,
        backend: SearchBackendKind::Native,
        tenant: body.tenant,
    }
}

pub async fn list_indexes(State(s): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let indexes = s.search.list_indexes().await.map_err(map_search_err)?;
    Ok(Json(json!({ "value": indexes, "@odata.count": indexes.len() })))
}

pub async fn put_index(
    State(s): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    Json(mut def): Json<IndexDefinition>,
) -> Result<impl IntoResponse, ApiError> {
    def.name = name;
    if let Some(v) = headers
        .get("x-nebula-search-backend")
        .and_then(|h| h.to_str().ok())
    {
        if v.eq_ignore_ascii_case("azure") {
            def.backend = SearchBackendKind::Azure;
        }
    }
    let created = s.search.create_index(def).await.map_err(map_search_err)?;
    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_index(
    State(s): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let def = s.search.get_index(&name).await.map_err(map_search_err)?;
    Ok(Json(def))
}

pub async fn delete_index(
    State(s): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    s.search.delete_index(&name).await.map_err(map_search_err)?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn index_docs(
    State(s): State<AppState>,
    Path(name): Path<String>,
    Json(batch): Json<IndexBatch>,
) -> Result<impl IntoResponse, ApiError> {
    let result = s
        .search
        .index_docs(&name, batch)
        .await
        .map_err(map_search_err)?;
    Ok(Json(result))
}

pub async fn search_docs(
    State(s): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    Json(body): Json<AzureSearchBody>,
) -> Result<impl IntoResponse, ApiError> {
    let req = azure_body_to_request(&name, body);
    let resp = run_unified_search(&s, req, &headers).await?;
    // Azure-shaped envelope
    let value: Vec<_> = resp
        .hits
        .iter()
        .map(|h| {
            json!({
                "id": h.id,
                "content": h.text,
                "@search.score": h.search_score.unwrap_or(h.score),
                "@search.rerankerScore": h.reranker_score,
                "metadata": h.metadata,
            })
        })
        .collect();
    Ok(Json(json!({
        "value": value,
        "@odata.count": resp.total_count,
        "@search.answers": resp.answers,
        "took_ms": resp.took_ms,
        "warnings": resp.warnings,
        "explain": resp.explain,
        "capability_id": resp.capability_id,
    })))
}

pub async fn compat_all(State(s): State<AppState>) -> impl IntoResponse {
    Json(s.compat_registry.as_ref().clone())
}

pub async fn compat_product(
    State(s): State<AppState>,
    Path(product): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    s.compat_registry
        .product(&product)
        .cloned()
        .map(Json)
        .ok_or_else(|| ApiError::NotFound(format!("compat product `{product}`")))
}

/// Per-bucket (bm25, vector) hybrid weight resolver.
pub type HybridWeightsFn = Arc<dyn Fn(Option<&str>) -> (f32, f32) + Send + Sync>;

/// Build the dual search backend for AppState.
pub fn build_search_backend(
    index: Arc<nebula_index::TextIndex>,
    registry: nebula_search::IndexRegistry,
    weights: HybridWeightsFn,
    reranker: Arc<dyn nebula_rerank::Reranker>,
) -> Arc<DualSearchBackend> {
    let native = Arc::new(
        nebula_search::NativeBackend::new(Arc::clone(&index), registry.clone())
            .with_weights(weights)
            .with_reranker(reranker),
    );
    let azure = nebula_search::AzureSearchBackend::from_env()
        .map(|b| Arc::new(b) as Arc<dyn SearchBackend>);
    Arc::new(DualSearchBackend {
        native: native as Arc<dyn SearchBackend>,
        azure,
        registry,
    })
}
