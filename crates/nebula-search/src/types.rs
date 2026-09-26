//! Unified enterprise search types shared by native NebulaDB retrieval
//! and Azure AI Search–compatible / external adapters.
//!
//! Capability classes (see [`crate::compat`]):
//! `native | compatible | translated | external | unsupported`.

use serde::{Deserialize, Serialize};

/// How retrieval should combine keyword / vector / semantic signals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SearchMode {
    Keyword,
    FullText,
    #[default]
    Vector,
    Hybrid,
    /// Hybrid + optional cross-encoder / semantic ranking stage.
    Semantic,
}

/// Backend that should execute the search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SearchBackendKind {
    #[default]
    Native,
    Azure,
}

/// One vector query (Azure `vectorQueries` shape, simplified).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorQuery {
    #[serde(default = "default_vector_field")]
    pub fields: String,
    #[serde(default)]
    pub kind: Option<String>,
    /// Precomputed vector. When absent, the text query is embedded.
    #[serde(default)]
    pub vector: Option<Vec<f32>>,
    #[serde(default = "default_k")]
    pub k: usize,
}

fn default_vector_field() -> String {
    "contentVector".into()
}

fn default_k() -> usize {
    10
}

/// Unified search request. Azure-compat routes translate into this.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchRequest {
    /// Free-text query (`search` in Azure).
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub mode: SearchMode,
    /// NebulaDB bucket / Azure index name.
    #[serde(default)]
    pub index: Option<String>,
    /// Alias for `index` used by legacy `/ai/search`.
    #[serde(default)]
    pub bucket: Option<String>,
    #[serde(default = "default_top")]
    pub top: usize,
    #[serde(default)]
    pub skip: usize,
    /// OData-lite filter expression (translated subset).
    #[serde(default)]
    pub filter: Option<String>,
    /// Facet field names.
    #[serde(default)]
    pub facets: Vec<String>,
    /// `orderby` field names (best-effort; unsupported → warning).
    #[serde(default)]
    pub orderby: Vec<String>,
    /// Restrict searchable text fields (ignored when only one content field).
    #[serde(default)]
    pub search_fields: Vec<String>,
    #[serde(default)]
    pub vector_queries: Vec<VectorQuery>,
    /// Azure `semanticConfiguration` name (recorded in explain).
    #[serde(default)]
    pub semantic_configuration: Option<String>,
    #[serde(default)]
    pub ef: Option<usize>,
    #[serde(default)]
    pub explain: bool,
    #[serde(default)]
    pub backend: SearchBackendKind,
    /// Tenant / isolation key mirrored into metadata filter when set.
    #[serde(default)]
    pub tenant: Option<String>,
}

fn default_top() -> usize {
    10
}

impl SearchRequest {
    pub fn effective_index(&self) -> Option<&str> {
        self.index
            .as_deref()
            .or(self.bucket.as_deref())
            .filter(|s| !s.is_empty())
    }

    pub fn from_legacy_ai(
        query: String,
        top_k: usize,
        bucket: Option<String>,
        ef: Option<usize>,
        hybrid: bool,
        explain: bool,
    ) -> Self {
        Self {
            query,
            mode: if hybrid {
                SearchMode::Hybrid
            } else {
                SearchMode::Vector
            },
            index: bucket.clone(),
            bucket,
            top: top_k,
            skip: 0,
            filter: None,
            facets: Vec::new(),
            orderby: Vec::new(),
            search_fields: Vec::new(),
            vector_queries: Vec::new(),
            semantic_configuration: None,
            ef,
            explain,
            backend: SearchBackendKind::Native,
            tenant: None,
        }
    }

    pub fn from_legacy_vector(
        vector: Vec<f32>,
        top_k: usize,
        bucket: Option<String>,
        ef: Option<usize>,
    ) -> Self {
        Self {
            query: String::new(),
            mode: SearchMode::Vector,
            index: bucket.clone(),
            bucket,
            top: top_k,
            skip: 0,
            filter: None,
            facets: Vec::new(),
            orderby: Vec::new(),
            search_fields: Vec::new(),
            vector_queries: vec![VectorQuery {
                fields: default_vector_field(),
                kind: Some("vector".into()),
                vector: Some(vector),
                k: top_k,
            }],
            semantic_configuration: None,
            ef,
            explain: false,
            backend: SearchBackendKind::Native,
            tenant: None,
        }
    }
}

/// One hit in a unified response. Optional Azure-style aliases are
/// filled when serializing Azure-compat responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchHit {
    pub bucket: String,
    pub id: String,
    pub text: String,
    pub score: f32,
    pub metadata: serde_json::Value,
    /// Azure `@search.score` mirror.
    #[serde(rename = "@search.score", skip_serializing_if = "Option::is_none")]
    pub search_score: Option<f32>,
    #[serde(rename = "@search.rerankerScore", skip_serializing_if = "Option::is_none")]
    pub reranker_score: Option<f32>,
}

impl From<nebula_index::Hit> for SearchHit {
    fn from(h: nebula_index::Hit) -> Self {
        let score = h.score;
        Self {
            bucket: h.bucket,
            id: h.id,
            text: h.text,
            score,
            metadata: h.metadata,
            search_score: Some(score),
            reranker_score: None,
        }
    }
}

impl From<SearchHit> for nebula_index::Hit {
    fn from(h: SearchHit) -> Self {
        nebula_index::Hit {
            bucket: h.bucket,
            id: h.id,
            text: h.text,
            score: h.score,
            metadata: h.metadata,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FacetResult {
    pub field: String,
    pub values: Vec<FacetValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FacetValue {
    pub value: String,
    pub count: u64,
}

/// Unified search response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResponse {
    pub hits: Vec<SearchHit>,
    pub took_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub facets: Vec<FacetResult>,
    /// Azure `@search.answers` stub (empty unless a future answerer fills it).
    #[serde(
        rename = "@search.answers",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub answers: Vec<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub explain: Option<nebula_index::explain::Explain>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability_id: Option<String>,
}

impl SearchResponse {
    /// Legacy wire shape used by `/ai/search` and `/vector/search`.
    pub fn to_legacy_hits(&self) -> Vec<nebula_index::Hit> {
        self.hits.iter().cloned().map(Into::into).collect()
    }
}

/// Field flags matching Azure AI Search index field properties.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FieldDefinition {
    pub name: String,
    #[serde(rename = "type", default = "default_field_type")]
    pub field_type: String,
    #[serde(default)]
    pub key: bool,
    #[serde(default = "default_true")]
    pub searchable: bool,
    #[serde(default)]
    pub filterable: bool,
    #[serde(default)]
    pub facetable: bool,
    #[serde(default)]
    pub sortable: bool,
    #[serde(default = "default_true")]
    pub retrievable: bool,
    #[serde(default)]
    pub dimensions: Option<usize>,
    #[serde(default, rename = "vectorSearchProfile")]
    pub vector_search_profile: Option<String>,
}

fn default_field_type() -> String {
    "Edm.String".into()
}

fn default_true() -> bool {
    true
}

/// Index / collection schema stored beside the NebulaDB bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinition {
    pub name: String,
    #[serde(default)]
    pub fields: Vec<FieldDefinition>,
    /// `native` (default) or `azure` — which backend owns this index.
    #[serde(default)]
    pub backend: SearchBackendKind,
    #[serde(default)]
    pub semantic_configurations: Vec<String>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

impl IndexDefinition {
    pub fn default_for(name: &str) -> Self {
        Self {
            name: name.to_string(),
            fields: vec![
                FieldDefinition {
                    name: "id".into(),
                    field_type: "Edm.String".into(),
                    key: true,
                    searchable: false,
                    filterable: true,
                    facetable: false,
                    sortable: false,
                    retrievable: true,
                    dimensions: None,
                    vector_search_profile: None,
                },
                FieldDefinition {
                    name: "content".into(),
                    field_type: "Edm.String".into(),
                    key: false,
                    searchable: true,
                    filterable: false,
                    facetable: false,
                    sortable: false,
                    retrievable: true,
                    dimensions: None,
                    vector_search_profile: None,
                },
                FieldDefinition {
                    name: "contentVector".into(),
                    field_type: "Collection(Edm.Single)".into(),
                    key: false,
                    searchable: false,
                    filterable: false,
                    facetable: false,
                    sortable: false,
                    retrievable: false,
                    dimensions: None,
                    vector_search_profile: Some("default".into()),
                },
            ],
            backend: SearchBackendKind::Native,
            semantic_configurations: vec!["default".into()],
            metadata: serde_json::Value::Null,
        }
    }
}

/// Azure-shaped document batch action.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexAction {
    #[serde(rename = "@search.action", default = "default_action")]
    pub action: String,
    /// Document key.
    pub id: String,
    /// Primary searchable text. Falls back to `content` / `text` keys.
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub text: Option<String>,
    /// Extra fields merged into NebulaDB metadata.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

fn default_action() -> String {
    "upload".into()
}

impl IndexAction {
    pub fn body_text(&self) -> Option<String> {
        self.content
            .clone()
            .or_else(|| self.text.clone())
            .or_else(|| {
                self.extra
                    .get("content")
                    .or_else(|| self.extra.get("text"))
                    .and_then(|v| v.as_str().map(|s| s.to_string()))
            })
    }

    pub fn metadata_value(&self) -> serde_json::Value {
        let mut m = self.extra.clone();
        m.remove("content");
        m.remove("text");
        m.remove("id");
        serde_json::Value::Object(m)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexBatch {
    pub value: Vec<IndexAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexBatchResult {
    pub value: Vec<IndexActionResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexActionResult {
    pub key: String,
    pub status: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    #[serde(default = "default_status_code")]
    pub status_code: u16,
}

fn default_status_code() -> u16 {
    200
}
