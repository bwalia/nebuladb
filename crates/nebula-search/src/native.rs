//! Native NebulaDB search backend (HNSW + BM25 + optional rerank).

use std::sync::Arc;

use async_trait::async_trait;
use nebula_index::explain::{Explain, ExplainKind};
use nebula_index::TextIndex;
use nebula_rerank::{Candidate, NoopReranker, Reranker};

use crate::backend::{IndexRegistry, Result, SearchBackend, SearchError};
use crate::filter::parse_filter;
use crate::types::{
    FacetResult, FacetValue, IndexActionResult, IndexBatch, IndexBatchResult, IndexDefinition,
    SearchHit, SearchMode, SearchRequest, SearchResponse,
};

/// Resolve hybrid fusion weights for a bucket.
pub type WeightFn = Arc<dyn Fn(Option<&str>) -> (f32, f32) + Send + Sync>;

pub struct NativeBackend {
    index: Arc<TextIndex>,
    registry: IndexRegistry,
    weights: WeightFn,
    reranker: Arc<dyn Reranker>,
}

impl NativeBackend {
    pub fn new(index: Arc<TextIndex>, registry: IndexRegistry) -> Self {
        Self {
            index,
            registry,
            weights: Arc::new(|_| (0.5, 0.5)),
            reranker: Arc::new(NoopReranker),
        }
    }

    pub fn with_weights(mut self, weights: WeightFn) -> Self {
        self.weights = weights;
        self
    }

    pub fn with_reranker(mut self, reranker: Arc<dyn Reranker>) -> Self {
        self.reranker = reranker;
        self
    }

    pub fn registry(&self) -> &IndexRegistry {
        &self.registry
    }

    async fn retrieve(&self, req: &SearchRequest) -> Result<(Vec<SearchHit>, Option<Explain>, Vec<String>)> {
        let mut warnings = Vec::new();
        let bucket = req.effective_index().map(|s| s.to_string());
        let top = req.top.max(1);
        let fetch_k = top + req.skip;

        if !req.orderby.is_empty() {
            warnings.push(format!(
                "orderby not applied natively (capability azure_search.orderby): {:?}",
                req.orderby
            ));
        }
        if !req.search_fields.is_empty() {
            warnings.push(
                "searchFields restricted to primary content field (translated)".into(),
            );
        }

        let filter = match &req.filter {
            Some(f) if !f.trim().is_empty() => Some(parse_filter(f).map_err(|e| match e {
                crate::filter::FilterError::Unsupported(msg) => SearchError::Unsupported(msg),
                crate::filter::FilterError::Invalid(msg) => SearchError::BadRequest(msg),
            })?),
            _ => None,
        };

        // Tenant isolation: inject as metadata eq filter.
        let filter = {
            let mut f = filter.unwrap_or_default();
            if let Some(t) = &req.tenant {
                f.preds.push(crate::filter::FilterPred::Eq {
                    field: "tenant".into(),
                    value: t.clone(),
                });
            }
            if f.preds.is_empty() {
                None
            } else {
                Some(f)
            }
        };

        let started = std::time::Instant::now();
        let mut explain = req.explain.then(|| Explain::new(ExplainKind::Search, true));

        let raw_hits = match req.mode {
            SearchMode::Keyword | SearchMode::FullText => {
                let hits = self
                    .index
                    .search_bm25(&req.query, bucket.as_deref(), fetch_k.max(top * 3));
                if let Some(ex) = explain.as_mut() {
                    ex.finish(
                        format!("BM25 keyword search returned {} hits", hits.len()),
                        started.elapsed(),
                    );
                }
                hits
            }
            SearchMode::Vector => {
                if let Some(vq) = req.vector_queries.first().and_then(|q| q.vector.as_ref()) {
                    Arc::clone(&self.index)
                        .search_vector_blocking(vq.clone(), bucket.clone(), fetch_k, req.ef)
                        .await?
                } else {
                    if req.query.trim().is_empty() {
                        return Err(SearchError::BadRequest(
                            "query or vectorQueries required for vector mode".into(),
                        ));
                    }
                    if req.explain {
                        let (hits, trace) = Arc::clone(&self.index)
                            .search_text_explained(
                                req.query.clone(),
                                bucket.clone(),
                                fetch_k,
                                req.ef,
                                None,
                            )
                            .await?;
                        if let Some(ex) = explain.as_mut() {
                            ex.absorb(trace);
                            ex.finish(
                                format!("Vector search returned {} hits", hits.len()),
                                started.elapsed(),
                            );
                        }
                        hits
                    } else {
                        Arc::clone(&self.index)
                            .search_text_blocking(
                                req.query.clone(),
                                bucket.clone(),
                                fetch_k,
                                req.ef,
                            )
                            .await?
                    }
                }
            }
            SearchMode::Hybrid | SearchMode::Semantic => {
                let weights = (self.weights)(bucket.as_deref());
                if req.query.trim().is_empty()
                    && req
                        .vector_queries
                        .first()
                        .and_then(|q| q.vector.as_ref())
                        .is_none()
                {
                    return Err(SearchError::BadRequest(
                        "query required for hybrid/semantic mode".into(),
                    ));
                }
                if req.explain {
                    let (hits, trace) = Arc::clone(&self.index)
                        .search_text_explained(
                            req.query.clone(),
                            bucket.clone(),
                            fetch_k,
                            req.ef,
                            Some(weights),
                        )
                        .await?;
                    if let Some(ex) = explain.as_mut() {
                        ex.absorb(trace);
                        ex.finish(
                            format!(
                                "Hybrid search (vector×{} + BM25×{}) returned {} hits",
                                weights.0,
                                weights.1,
                                hits.len()
                            ),
                            started.elapsed(),
                        );
                    }
                    hits
                } else if let Some(vq) = req.vector_queries.first().and_then(|q| q.vector.as_ref()) {
                    self.index.search_vector_hybrid(
                        vq,
                        &req.query,
                        bucket.as_deref(),
                        fetch_k,
                        req.ef,
                        weights,
                    )?
                } else {
                    Arc::clone(&self.index)
                        .search_text_hybrid_blocking(
                            req.query.clone(),
                            bucket.clone(),
                            fetch_k,
                            req.ef,
                            weights,
                        )
                        .await?
                }
            }
        };

        let mut hits: Vec<SearchHit> = raw_hits.into_iter().map(Into::into).collect();

        if let Some(f) = &filter {
            hits.retain(|h| f.matches(&h.metadata));
        }

        // Semantic: optional rerank over hybrid candidates.
        if matches!(req.mode, SearchMode::Semantic) && !hits.is_empty() {
            let cands: Vec<Candidate> = hits
                .iter()
                .map(|h| Candidate {
                    id: format!("{}/{}", h.bucket, h.id),
                    text: h.text.clone(),
                })
                .collect();
            match self.reranker.rerank(&req.query, &cands, top).await {
                Ok(scored) => {
                    let mut by_id: std::collections::HashMap<String, SearchHit> =
                        hits.drain(..).map(|h| (format!("{}/{}", h.bucket, h.id), h)).collect();
                    let mut reranked = Vec::new();
                    for s in scored {
                        if let Some(mut h) = by_id.remove(&s.id) {
                            h.reranker_score = Some(s.score);
                            h.score = s.score;
                            h.search_score = Some(s.score);
                            reranked.push(h);
                        }
                    }
                    hits = reranked;
                }
                Err(e) => warnings.push(format!("rerank skipped: {e}")),
            }
        }

        if req.skip > 0 {
            if req.skip >= hits.len() {
                hits.clear();
            } else {
                hits = hits.split_off(req.skip);
            }
        }
        hits.truncate(top);

        Ok((hits, explain, warnings))
    }

    fn compute_facets(&self, index: &str, fields: &[String], sample: &[SearchHit]) -> Vec<FacetResult> {
        // Facet over the returned hits (translated / approximate). Full
        // corpus facets are unsupported without a secondary inverted index.
        let _ = index;
        fields
            .iter()
            .map(|field| {
                let mut counts: std::collections::BTreeMap<String, u64> =
                    std::collections::BTreeMap::new();
                for h in sample {
                    if let Some(v) = h.metadata.get(field).and_then(|x| match x {
                        serde_json::Value::String(s) => Some(s.clone()),
                        serde_json::Value::Number(n) => Some(n.to_string()),
                        serde_json::Value::Bool(b) => Some(b.to_string()),
                        _ => None,
                    }) {
                        *counts.entry(v).or_default() += 1;
                    }
                }
                FacetResult {
                    field: field.clone(),
                    values: counts
                        .into_iter()
                        .map(|(value, count)| FacetValue { value, count })
                        .collect(),
                }
            })
            .collect()
    }
}

#[async_trait]
impl SearchBackend for NativeBackend {
    async fn search(&self, req: SearchRequest) -> Result<SearchResponse> {
        let started = std::time::Instant::now();
        let (hits, explain, warnings) = self.retrieve(&req).await?;
        let facets = if req.facets.is_empty() {
            Vec::new()
        } else {
            self.compute_facets(req.effective_index().unwrap_or(""), &req.facets, &hits)
        };
        let total = hits.len() as u64;
        Ok(SearchResponse {
            hits,
            took_ms: started.elapsed().as_millis() as u64,
            total_count: Some(total),
            facets,
            answers: Vec::new(),
            explain,
            warnings,
            capability_id: Some("azure_search.docs_search".into()),
        })
    }

    async fn create_index(&self, def: IndexDefinition) -> Result<IndexDefinition> {
        if def.name.trim().is_empty() {
            return Err(SearchError::BadRequest("index name required".into()));
        }
        Ok(self.registry.upsert(def))
    }

    async fn get_index(&self, name: &str) -> Result<IndexDefinition> {
        self.registry
            .get(name)
            .ok_or_else(|| SearchError::NotFound(format!("index `{name}`")))
    }

    async fn list_indexes(&self) -> Result<Vec<IndexDefinition>> {
        let mut listed = self.registry.list();
        // Also surface buckets that have docs but no explicit schema.
        for b in self.index.bucket_stats(0) {
            if !listed.iter().any(|d| d.name == b.bucket) {
                listed.push(IndexDefinition::default_for(&b.bucket));
            }
        }
        listed.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(listed)
    }

    async fn delete_index(&self, name: &str) -> Result<()> {
        if !self.registry.remove(name) {
            return Err(SearchError::NotFound(format!("index `{name}`")));
        }
        Ok(())
    }

    async fn index_docs(&self, index: &str, batch: IndexBatch) -> Result<IndexBatchResult> {
        // Ensure schema exists.
        if self.registry.get(index).is_none() {
            self.registry
                .upsert(IndexDefinition::default_for(index));
        }
        let mut results = Vec::new();
        for action in batch.value {
            let act = action.action.to_ascii_lowercase();
            if act == "delete" {
                match self.index.delete_document(index, &action.id) {
                    Ok(_) => results.push(IndexActionResult {
                        key: action.id,
                        status: true,
                        error_message: None,
                        status_code: 200,
                    }),
                    Err(e) => results.push(IndexActionResult {
                        key: action.id,
                        status: false,
                        error_message: Some(e.to_string()),
                        status_code: 400,
                    }),
                }
                continue;
            }
            // upload / merge / mergeOrUpload
            let text = match action.body_text() {
                Some(t) if !t.is_empty() => t,
                _ => {
                    results.push(IndexActionResult {
                        key: action.id.clone(),
                        status: false,
                        error_message: Some("content/text required for upload".into()),
                        status_code: 400,
                    });
                    continue;
                }
            };
            let meta = action.metadata_value();
            match Arc::clone(&self.index)
                .upsert_text(index, &action.id, &text, meta)
                .await
            {
                Ok(_) => results.push(IndexActionResult {
                    key: action.id,
                    status: true,
                    error_message: None,
                    status_code: 201,
                }),
                Err(e) => results.push(IndexActionResult {
                    key: action.id,
                    status: false,
                    error_message: Some(e.to_string()),
                    status_code: 400,
                }),
            }
        }
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
