//! Search backend trait + in-memory index definition registry.

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use thiserror::Error;

use crate::types::{
    IndexActionResult, IndexBatch, IndexBatchResult, IndexDefinition, SearchRequest, SearchResponse,
};

#[derive(Debug, Error)]
pub enum SearchError {
    #[error("{0}")]
    BadRequest(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error(transparent)]
    Index(#[from] nebula_index::IndexError),
    #[error("azure: {0}")]
    Azure(String),
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, SearchError>;

/// Pluggable search + index administration backend.
#[async_trait]
pub trait SearchBackend: Send + Sync {
    async fn search(&self, req: SearchRequest) -> Result<SearchResponse>;

    async fn create_index(&self, def: IndexDefinition) -> Result<IndexDefinition>;

    async fn get_index(&self, name: &str) -> Result<IndexDefinition>;

    async fn list_indexes(&self) -> Result<Vec<IndexDefinition>>;

    async fn delete_index(&self, name: &str) -> Result<()>;

    async fn index_docs(&self, index: &str, batch: IndexBatch) -> Result<IndexBatchResult>;

    async fn delete_docs(&self, index: &str, ids: &[String]) -> Result<IndexBatchResult>;
}

/// Process-local store for [`IndexDefinition`]s (bucket schemas).
#[derive(Clone, Default)]
pub struct IndexRegistry {
    inner: Arc<DashMap<String, IndexDefinition>>,
}

impl IndexRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert(&self, def: IndexDefinition) -> IndexDefinition {
        self.inner.insert(def.name.clone(), def.clone());
        def
    }

    pub fn get(&self, name: &str) -> Option<IndexDefinition> {
        self.inner.get(name).map(|e| e.clone())
    }

    pub fn list(&self) -> Vec<IndexDefinition> {
        let mut v: Vec<_> = self.inner.iter().map(|e| e.value().clone()).collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }

    pub fn remove(&self, name: &str) -> bool {
        self.inner.remove(name).is_some()
    }

    pub fn get_or_default(&self, name: &str) -> IndexDefinition {
        self.get(name)
            .unwrap_or_else(|| IndexDefinition::default_for(name))
    }
}
