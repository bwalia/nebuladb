//! Optional retrieval-quality stages for NebulaDB: **reranking** and
//! **query expansion** (design 0008 §7).
//!
//! Both follow the same posture as [`nebula_embed`] and `nebula_llm`:
//! a trait with a zero-dependency default impl plus an HTTP-backed impl
//! that calls a sidecar. Nothing heavy or native is linked into the DB
//! binary — a cross-encoder reranker runs as its own service and is
//! reached over HTTP, exactly like the embedding and LLM providers.
//!
//! # Why these are separate from retrieval
//!
//! Retrieval (vector + BM25 fusion) lives in `nebula-index`. Reranking
//! is a *post*-retrieval refinement: take the top-N candidates and
//! reorder them with a more expensive, more accurate model. Query
//! expansion is a *pre*-retrieval step: broaden the query so recall
//! improves before retrieval runs. Keeping both here, decoupled from
//! the index, means the index crate has no opinion about — and no
//! dependency on — an optional model server.
//!
//! # Decoupling
//!
//! This crate deliberately does **not** depend on `nebula-index`. A
//! reranker operates on opaque [`Candidate`]s (`id` + `text`); the
//! caller maps its own hits to candidates and back. That keeps the
//! dependency graph a DAG and lets these stages be unit-tested without
//! standing up an index.

mod expand;
mod http;

pub use expand::{MapQueryExpander, NoopQueryExpander, QueryExpander};
pub use http::HttpReranker;

use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
pub enum RerankError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("rerank service returned error: {status}: {body}")]
    Provider { status: u16, body: String },
    #[error("response shape invalid: {0}")]
    Decode(String),
    #[error("service returned {got} scores for {expected} candidates")]
    CountMismatch { expected: usize, got: usize },
}

pub type Result<T> = std::result::Result<T, RerankError>;

/// One thing to be reranked. `id` is opaque to this crate — the caller
/// uses it to map a reranked result back to its original hit. `text` is
/// what the reranker scores against the query.
#[derive(Debug, Clone)]
pub struct Candidate {
    pub id: String,
    pub text: String,
}

/// A candidate paired with its relevance score against the query.
/// Larger `score` = more relevant. Returned in **descending** score
/// order by [`Reranker::rerank`].
#[derive(Debug, Clone, PartialEq)]
pub struct Scored {
    pub id: String,
    pub score: f32,
}

/// Reorders retrieval candidates by relevance to the query. Implementors
/// must be cheap to clone (wrap shared state in `Arc`) because one
/// reranker is shared across many requests.
#[async_trait]
pub trait Reranker: Send + Sync {
    /// Score `candidates` against `query` and return them in descending
    /// relevance order, truncated to `top_k`. Implementations must
    /// return only ids drawn from the input set.
    async fn rerank(
        &self,
        query: &str,
        candidates: &[Candidate],
        top_k: usize,
    ) -> Result<Vec<Scored>>;
}

/// The default reranker: a no-op pass-through. It preserves the input
/// order (which is already the fusion ranking from the index) and
/// assigns a descending synthetic score so downstream code can treat
/// every path uniformly. Choosing this means "no rerank stage" without
/// the caller needing an `Option<Reranker>` branch everywhere.
#[derive(Debug, Default, Clone)]
pub struct NoopReranker;

#[async_trait]
impl Reranker for NoopReranker {
    async fn rerank(
        &self,
        _query: &str,
        candidates: &[Candidate],
        top_k: usize,
    ) -> Result<Vec<Scored>> {
        Ok(candidates
            .iter()
            .take(top_k)
            .enumerate()
            .map(|(i, c)| Scored {
                id: c.id.clone(),
                // Strictly descending, order-preserving: the i-th input
                // keeps rank i. Value is synthetic — callers that need a
                // real relevance score use a non-noop reranker.
                score: (candidates.len() - i) as f32,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cands(items: &[(&str, &str)]) -> Vec<Candidate> {
        items
            .iter()
            .map(|(id, text)| Candidate {
                id: id.to_string(),
                text: text.to_string(),
            })
            .collect()
    }

    #[tokio::test]
    async fn noop_preserves_order_and_truncates() {
        let r = NoopReranker;
        let c = cands(&[("a", "x"), ("b", "y"), ("c", "z")]);
        let out = r.rerank("q", &c, 2).await.unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].id, "a");
        assert_eq!(out[1].id, "b");
        // Descending synthetic scores.
        assert!(out[0].score > out[1].score);
    }

    #[tokio::test]
    async fn noop_top_k_larger_than_input_is_safe() {
        let r = NoopReranker;
        let c = cands(&[("a", "x")]);
        let out = r.rerank("q", &c, 10).await.unwrap();
        assert_eq!(out.len(), 1);
    }
}
