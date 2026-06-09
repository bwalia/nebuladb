//! HTTP cross-encoder reranker.
//!
//! Calls an out-of-process reranking service over a small JSON contract,
//! so the cross-encoder model (and its ONNX/torch runtime) never gets
//! linked into the DB binary. The contract is intentionally generic so
//! it fits common rerank servers (e.g. a TEI / text-embeddings-inference
//! rerank endpoint, or a thin custom wrapper):
//!
//! Request:
//! ```json
//! { "query": "...", "documents": ["text a", "text b", ...] }
//! ```
//!
//! Response (either shape is accepted):
//! ```json
//! { "scores": [0.91, 0.12, ...] }            // aligned to input order
//! // or
//! { "results": [ {"index": 0, "score": 0.91}, ... ] }  // any order
//! ```
//!
//! We map scores back onto the caller's [`Candidate`] ids, sort
//! descending, and truncate to `top_k`.

use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;

use crate::{Candidate, RerankError, Reranker, Result, Scored};

#[derive(Debug, Clone)]
pub struct HttpReranker {
    client: reqwest::Client,
    url: String,
}

impl HttpReranker {
    /// `url` is the full endpoint, e.g. `http://reranker:8080/rerank`.
    /// `connect_timeout` caps the dial; the request as a whole is
    /// bounded by `request_timeout` since rerank is non-streaming and a
    /// hung model must not pin a NebulaDB request forever.
    pub fn new(
        url: impl Into<String>,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self> {
        let client = reqwest::Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .build()?;
        Ok(Self {
            client,
            url: url.into(),
        })
    }
}

#[derive(Deserialize)]
struct ResultItem {
    index: usize,
    score: f32,
}

#[derive(Deserialize)]
struct RerankResponse {
    #[serde(default)]
    scores: Option<Vec<f32>>,
    #[serde(default)]
    results: Option<Vec<ResultItem>>,
}

#[async_trait]
impl Reranker for HttpReranker {
    async fn rerank(
        &self,
        query: &str,
        candidates: &[Candidate],
        top_k: usize,
    ) -> Result<Vec<Scored>> {
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let documents: Vec<&str> = candidates.iter().map(|c| c.text.as_str()).collect();
        let body = serde_json::json!({ "query": query, "documents": documents });

        let resp = self.client.post(&self.url).json(&body).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(RerankError::Provider {
                status: status.as_u16(),
                body,
            });
        }
        let parsed: RerankResponse = resp
            .json()
            .await
            .map_err(|e| RerankError::Decode(e.to_string()))?;

        // Build (candidate index, score) pairs from whichever response
        // shape the service used.
        let mut scored: Vec<(usize, f32)> = if let Some(scores) = parsed.scores {
            if scores.len() != candidates.len() {
                return Err(RerankError::CountMismatch {
                    expected: candidates.len(),
                    got: scores.len(),
                });
            }
            scores.into_iter().enumerate().collect()
        } else if let Some(results) = parsed.results {
            // Validate indices are in range before trusting them.
            for r in &results {
                if r.index >= candidates.len() {
                    return Err(RerankError::Decode(format!(
                        "result index {} out of range for {} candidates",
                        r.index,
                        candidates.len()
                    )));
                }
            }
            results.into_iter().map(|r| (r.index, r.score)).collect()
        } else {
            return Err(RerankError::Decode(
                "response had neither `scores` nor `results`".into(),
            ));
        };

        // Descending score; stable on ties via original index.
        scored.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });
        scored.truncate(top_k);

        Ok(scored
            .into_iter()
            .map(|(i, score)| Scored {
                id: candidates[i].id.clone(),
                score,
            })
            .collect())
    }
}
