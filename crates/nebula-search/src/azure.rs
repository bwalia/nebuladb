//! External Azure AI Search HTTP client + [`SearchBackend`] adapter.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::backend::{Result, SearchBackend, SearchError};
use crate::types::{
    IndexActionResult, IndexBatch, IndexBatchResult, IndexDefinition, SearchBackendKind,
    SearchHit, SearchMode, SearchRequest, SearchResponse,
};

/// Connection settings for a real Azure AI Search service.
#[derive(Debug, Clone)]
pub struct AzureSearchConfig {
    pub endpoint: String,
    pub api_key: String,
    pub api_version: String,
}

impl AzureSearchConfig {
    /// Load from `NEBULA_AZURE_SEARCH_*` env vars. Returns `None` when
    /// endpoint or key is unset.
    pub fn from_env() -> Option<Self> {
        let endpoint = std::env::var("NEBULA_AZURE_SEARCH_ENDPOINT").ok()?;
        let api_key = std::env::var("NEBULA_AZURE_SEARCH_API_KEY").ok()?;
        if endpoint.trim().is_empty() || api_key.trim().is_empty() {
            return None;
        }
        let api_version = std::env::var("NEBULA_AZURE_SEARCH_API_VERSION")
            .unwrap_or_else(|_| "2024-07-01".into());
        Some(Self {
            endpoint: endpoint.trim_end_matches('/').to_string(),
            api_key,
            api_version,
        })
    }
}

/// Thin reqwest wrapper around Azure AI Search REST.
#[derive(Clone)]
pub struct AzureSearchClient {
    http: reqwest::Client,
    cfg: AzureSearchConfig,
}

impl AzureSearchClient {
    pub fn new(cfg: AzureSearchConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            cfg,
        }
    }

    fn url(&self, path: &str) -> String {
        format!(
            "{}/{}?api-version={}",
            self.cfg.endpoint,
            path.trim_start_matches('/'),
            self.cfg.api_version
        )
    }

    async fn send(&self, req: reqwest::RequestBuilder) -> Result<Value> {
        let resp = req
            .header("api-key", &self.cfg.api_key)
            .header("Content-Type", "application/json")
            .send()
            .await?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(SearchError::Azure(format!("HTTP {status}: {body}")));
        }
        if body.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&body).map_err(|e| SearchError::Azure(format!("decode: {e}: {body}")))
    }

    pub async fn create_or_update_index(&self, def: &IndexDefinition) -> Result<Value> {
        let fields: Vec<Value> = def
            .fields
            .iter()
            .map(|f| {
                let mut o = json!({
                    "name": f.name,
                    "type": f.field_type,
                    "key": f.key,
                    "searchable": f.searchable,
                    "filterable": f.filterable,
                    "facetable": f.facetable,
                    "sortable": f.sortable,
                    "retrievable": f.retrievable,
                });
                if let Some(d) = f.dimensions {
                    o["dimensions"] = json!(d);
                }
                if let Some(p) = &f.vector_search_profile {
                    o["vectorSearchProfile"] = json!(p);
                }
                o
            })
            .collect();
        let body = json!({ "name": def.name, "fields": fields });
        self.send(
            self.http
                .put(self.url(&format!("indexes/{}", def.name)))
                .json(&body),
        )
        .await
    }

    pub async fn get_index(&self, name: &str) -> Result<Value> {
        self.send(self.http.get(self.url(&format!("indexes/{name}"))))
            .await
    }

    pub async fn list_indexes(&self) -> Result<Value> {
        self.send(self.http.get(self.url("indexes"))).await
    }

    pub async fn delete_index(&self, name: &str) -> Result<()> {
        let resp = self
            .http
            .delete(self.url(&format!("indexes/{name}")))
            .header("api-key", &self.cfg.api_key)
            .send()
            .await?;
        if resp.status().as_u16() == 404 {
            return Err(SearchError::NotFound(format!("index `{name}`")));
        }
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(SearchError::Azure(body));
        }
        Ok(())
    }

    pub async fn index_documents(&self, index: &str, batch: &IndexBatch) -> Result<Value> {
        let value: Vec<Value> = batch
            .value
            .iter()
            .map(|a| {
                let mut doc = serde_json::Map::new();
                doc.insert("id".into(), json!(a.id));
                if let Some(t) = a.body_text() {
                    doc.insert("content".into(), json!(t));
                }
                for (k, v) in &a.extra {
                    if k != "id" && k != "content" && k != "text" {
                        doc.insert(k.clone(), v.clone());
                    }
                }
                doc.insert("@search.action".into(), json!(a.action));
                Value::Object(doc)
            })
            .collect();
        self.send(
            self.http
                .post(self.url(&format!("indexes/{index}/docs/index")))
                .json(&json!({ "value": value })),
        )
        .await
    }

    pub async fn search(&self, index: &str, req: &SearchRequest) -> Result<Value> {
        let query_type = match req.mode {
            SearchMode::Semantic => "semantic",
            _ => "simple",
        };
        let mut body = json!({
            "search": req.query,
            "top": req.top,
            "skip": req.skip,
            "queryType": query_type,
        });
        if let Some(f) = &req.filter {
            body["filter"] = json!(f);
        }
        if !req.facets.is_empty() {
            body["facets"] = json!(req.facets);
        }
        if !req.orderby.is_empty() {
            body["orderby"] = json!(req.orderby.join(", "));
        }
        if !req.search_fields.is_empty() {
            body["searchFields"] = json!(req.search_fields.join(","));
        }
        if let Some(sc) = &req.semantic_configuration {
            body["semanticConfiguration"] = json!(sc);
        }
        if !req.vector_queries.is_empty() {
            let vqs: Vec<Value> = req
                .vector_queries
                .iter()
                .map(|vq| {
                    let mut o = json!({
                        "kind": vq.kind.as_deref().unwrap_or("vector"),
                        "fields": vq.fields,
                        "k": vq.k,
                    });
                    if let Some(v) = &vq.vector {
                        o["vector"] = json!(v);
                    }
                    o
                })
                .collect();
            body["vectorQueries"] = json!(vqs);
        }
        self.send(
            self.http
                .post(self.url(&format!("indexes/{index}/docs/search")))
                .json(&body),
        )
        .await
    }
}

/// [`SearchBackend`] that forwards to a live Azure AI Search service.
pub struct AzureSearchBackend {
    client: AzureSearchClient,
}

impl AzureSearchBackend {
    pub fn new(cfg: AzureSearchConfig) -> Self {
        Self {
            client: AzureSearchClient::new(cfg),
        }
    }

    pub fn from_env() -> Option<Self> {
        AzureSearchConfig::from_env().map(Self::new)
    }

    pub fn client(&self) -> &AzureSearchClient {
        &self.client
    }
}

fn azure_value_to_def(v: &Value) -> IndexDefinition {
    let name = v
        .get("name")
        .and_then(|x| x.as_str())
        .unwrap_or("unknown")
        .to_string();
    let fields = v
        .get("fields")
        .and_then(|x| x.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|f| {
                    Some(crate::types::FieldDefinition {
                        name: f.get("name")?.as_str()?.to_string(),
                        field_type: f
                            .get("type")
                            .and_then(|t| t.as_str())
                            .unwrap_or("Edm.String")
                            .to_string(),
                        key: f.get("key").and_then(|x| x.as_bool()).unwrap_or(false),
                        searchable: f.get("searchable").and_then(|x| x.as_bool()).unwrap_or(true),
                        filterable: f.get("filterable").and_then(|x| x.as_bool()).unwrap_or(false),
                        facetable: f.get("facetable").and_then(|x| x.as_bool()).unwrap_or(false),
                        sortable: f.get("sortable").and_then(|x| x.as_bool()).unwrap_or(false),
                        retrievable: f.get("retrievable").and_then(|x| x.as_bool()).unwrap_or(true),
                        dimensions: f
                            .get("dimensions")
                            .and_then(|x| x.as_u64())
                            .map(|n| n as usize),
                        vector_search_profile: f
                            .get("vectorSearchProfile")
                            .and_then(|x| x.as_str())
                            .map(|s| s.to_string()),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    IndexDefinition {
        name,
        fields,
        backend: SearchBackendKind::Azure,
        semantic_configurations: Vec::new(),
        metadata: v.clone(),
    }
}

fn azure_search_to_response(v: &Value, took_ms: u64) -> SearchResponse {
    let hits = v
        .get("value")
        .and_then(|x| x.as_array())
        .map(|arr| {
            arr.iter()
                .map(|doc| {
                    let id = doc
                        .get("id")
                        .or_else(|| doc.get("Id"))
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string();
                    let text = doc
                        .get("content")
                        .or_else(|| doc.get("text"))
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string();
                    let score = doc
                        .get("@search.score")
                        .and_then(|x| x.as_f64())
                        .unwrap_or(0.0) as f32;
                    let rerank = doc
                        .get("@search.rerankerScore")
                        .and_then(|x| x.as_f64())
                        .map(|x| x as f32);
                    SearchHit {
                        bucket: String::new(),
                        id,
                        text,
                        score,
                        metadata: doc.clone(),
                        search_score: Some(score),
                        reranker_score: rerank,
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    let total = v
        .get("@odata.count")
        .and_then(|x| x.as_u64())
        .or(Some(hits.len() as u64));
    SearchResponse {
        hits,
        took_ms,
        total_count: total,
        facets: Vec::new(),
        answers: v
            .get("@search.answers")
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default(),
        explain: None,
        warnings: Vec::new(),
        capability_id: Some("azure_search.external".into()),
    }
}

#[async_trait]
impl SearchBackend for AzureSearchBackend {
    async fn search(&self, req: SearchRequest) -> Result<SearchResponse> {
        let index = req
            .effective_index()
            .ok_or_else(|| SearchError::BadRequest("index required for Azure backend".into()))?
            .to_string();
        let started = std::time::Instant::now();
        let v = self.client.search(&index, &req).await?;
        Ok(azure_search_to_response(
            &v,
            started.elapsed().as_millis() as u64,
        ))
    }

    async fn create_index(&self, mut def: IndexDefinition) -> Result<IndexDefinition> {
        def.backend = SearchBackendKind::Azure;
        self.client.create_or_update_index(&def).await?;
        Ok(def)
    }

    async fn get_index(&self, name: &str) -> Result<IndexDefinition> {
        let v = self.client.get_index(name).await?;
        Ok(azure_value_to_def(&v))
    }

    async fn list_indexes(&self) -> Result<Vec<IndexDefinition>> {
        let v = self.client.list_indexes().await?;
        let arr = v
            .get("value")
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default();
        Ok(arr.iter().map(azure_value_to_def).collect())
    }

    async fn delete_index(&self, name: &str) -> Result<()> {
        self.client.delete_index(name).await
    }

    async fn index_docs(&self, index: &str, batch: IndexBatch) -> Result<IndexBatchResult> {
        let v = self.client.index_documents(index, &batch).await?;
        let results = v
            .get("value")
            .and_then(|x| x.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|r| IndexActionResult {
                        key: r
                            .get("key")
                            .and_then(|x| x.as_str())
                            .unwrap_or("")
                            .to_string(),
                        status: r.get("status").and_then(|x| x.as_bool()).unwrap_or(false),
                        error_message: r
                            .get("errorMessage")
                            .and_then(|x| x.as_str())
                            .map(|s| s.to_string()),
                        status_code: r
                            .get("statusCode")
                            .and_then(|x| x.as_u64())
                            .unwrap_or(200) as u16,
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(IndexBatchResult { value: results })
    }

    async fn delete_docs(&self, index: &str, ids: &[String]) -> Result<IndexBatchResult> {
        let batch = IndexBatch {
            value: ids
                .iter()
                .map(|id| crate::types::IndexAction {
                    action: "delete".into(),
                    id: id.clone(),
                    content: None,
                    text: None,
                    extra: Default::default(),
                })
                .collect(),
        };
        self.index_docs(index, batch).await
    }
}

/// Router that dispatches to native or Azure based on request / index metadata.
pub struct DualSearchBackend {
    pub native: Arc<dyn SearchBackend>,
    pub azure: Option<Arc<dyn SearchBackend>>,
    pub registry: crate::backend::IndexRegistry,
}

impl DualSearchBackend {
    pub fn resolve(&self, req: &SearchRequest) -> Result<Arc<dyn SearchBackend>> {
        match req.backend {
            SearchBackendKind::Native => Ok(Arc::clone(&self.native)),
            SearchBackendKind::Azure => self
                .azure
                .clone()
                .ok_or_else(|| {
                    SearchError::BadRequest(
                        "Azure backend requested but NEBULA_AZURE_SEARCH_ENDPOINT/API_KEY unset"
                            .into(),
                    )
                }),
        }
    }

    pub fn for_index(&self, name: &str) -> Arc<dyn SearchBackend> {
        if let Some(def) = self.registry.get(name) {
            if def.backend == SearchBackendKind::Azure {
                if let Some(a) = &self.azure {
                    return Arc::clone(a);
                }
            }
        }
        Arc::clone(&self.native)
    }
}

#[async_trait]
impl SearchBackend for DualSearchBackend {
    async fn search(&self, req: SearchRequest) -> Result<SearchResponse> {
        let backend = if req.backend == SearchBackendKind::Azure {
            self.resolve(&req)?
        } else if let Some(name) = req.effective_index() {
            self.for_index(name)
        } else {
            Arc::clone(&self.native)
        };
        backend.search(req).await
    }

    async fn create_index(&self, def: IndexDefinition) -> Result<IndexDefinition> {
        let backend = if def.backend == SearchBackendKind::Azure {
            self.azure.clone().ok_or_else(|| {
                SearchError::BadRequest("Azure backend not configured".into())
            })?
        } else {
            Arc::clone(&self.native)
        };
        let created = backend.create_index(def.clone()).await?;
        // Always record in local registry for routing.
        self.registry.upsert(created.clone());
        Ok(created)
    }

    async fn get_index(&self, name: &str) -> Result<IndexDefinition> {
        self.for_index(name).get_index(name).await
    }

    async fn list_indexes(&self) -> Result<Vec<IndexDefinition>> {
        self.native.list_indexes().await
    }

    async fn delete_index(&self, name: &str) -> Result<()> {
        self.for_index(name).delete_index(name).await?;
        self.registry.remove(name);
        Ok(())
    }

    async fn index_docs(&self, index: &str, batch: IndexBatch) -> Result<IndexBatchResult> {
        self.for_index(index).index_docs(index, batch).await
    }

    async fn delete_docs(&self, index: &str, ids: &[String]) -> Result<IndexBatchResult> {
        self.for_index(index).delete_docs(index, ids).await
    }
}
