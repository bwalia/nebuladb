// `tonic::Status` is ~176 bytes and shows up in every RPC return
// signature. Clippy's `result_large_err` fires on every such signature
// in this crate; boxing would add allocations to the hot path and
// obscure generated code. Acknowledge once and move on.
#![allow(clippy::result_large_err)]

//! gRPC surface for NebulaDB.
//!
//! Exposes three services — [`pb::document_service_server::DocumentService`],
//! [`pb::search_service_server::SearchService`], and
//! [`pb::ai_service_server::AIService`] — all backed by the same
//! `nebula-index` state that powers the REST server. Keeping the same
//! core means the two transports can't diverge on behavior: bug fixes
//! and feature additions in the index crate surface through both.
//!
//! No auth or rate-limit layer here yet. In-cluster gRPC commonly
//! terminates behind an envoy/nginx sidecar that already handles
//! those concerns; wiring Tonic interceptors for JWT verification
//! mirrors the REST layer and is a logical follow-up.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use nebula_chunk::Chunker;
use nebula_index::TextIndex;
use nebula_llm::{build_rag_prompt, LlmChunk, LlmClient};

/// Generated prost + tonic types.
///
/// `tonic-build` emits a module matching the proto package
/// (`nebula.v1`). We re-expose it under a shorter alias for call sites.
pub mod pb {
    tonic::include_proto!("nebula.v1");
}

/// Shared runtime state. Identical shape to the REST server's view —
/// no cloning of data, just `Arc`s.
#[derive(Clone)]
pub struct GrpcState {
    pub index: Arc<TextIndex>,
    pub llm: Arc<dyn LlmClient>,
    pub chunker: Arc<dyn Chunker>,
}

impl GrpcState {
    pub fn new(
        index: Arc<TextIndex>,
        llm: Arc<dyn LlmClient>,
        chunker: Arc<dyn Chunker>,
    ) -> Self {
        Self {
            index,
            llm,
            chunker,
        }
    }
}

// ---- Document ----

#[derive(Clone)]
pub struct DocumentSvc {
    state: GrpcState,
}

impl DocumentSvc {
    pub fn new(state: GrpcState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl pb::document_service_server::DocumentService for DocumentSvc {
    async fn upsert_document(
        &self,
        req: Request<pb::UpsertDocumentRequest>,
    ) -> Result<Response<pb::UpsertDocumentResponse>, Status> {
        let r = req.into_inner();
        if r.bucket.is_empty() || r.doc_id.is_empty() {
            return Err(Status::invalid_argument("bucket and doc_id required"));
        }
        if r.text.trim().is_empty() {
            return Err(Status::invalid_argument("text required"));
        }
        let metadata = parse_metadata(&r.metadata_json)?;
        let chunks = self
            .state
            .index
            .upsert_document(
                &r.bucket,
                &r.doc_id,
                &r.text,
                self.state.chunker.as_ref(),
                metadata,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(pb::UpsertDocumentResponse {
            bucket: r.bucket,
            doc_id: r.doc_id,
            chunks: chunks as u32,
        }))
    }

    async fn delete_document(
        &self,
        req: Request<pb::DeleteDocumentRequest>,
    ) -> Result<Response<pb::DeleteDocumentResponse>, Status> {
        let r = req.into_inner();
        let removed = self
            .state
            .index
            .delete_document(&r.bucket, &r.doc_id)
            .map_err(|e| match e {
                nebula_index::IndexError::DocNotFound { .. } => Status::not_found(e.to_string()),
                other => Status::internal(other.to_string()),
            })?;
        Ok(Response::new(pb::DeleteDocumentResponse {
            bucket: r.bucket,
            doc_id: r.doc_id,
            chunks_removed: removed as u32,
        }))
    }
}

// ---- Search ----

#[derive(Clone)]
pub struct SearchSvc {
    state: GrpcState,
}

impl SearchSvc {
    pub fn new(state: GrpcState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl pb::search_service_server::SearchService for SearchSvc {
    async fn semantic_search(
        &self,
        req: Request<pb::SemanticSearchRequest>,
    ) -> Result<Response<pb::SearchResponse>, Status> {
        let r = req.into_inner();
        if r.query.trim().is_empty() {
            return Err(Status::invalid_argument("query required"));
        }
        let k = validate_top_k(r.top_k)?;
        let bucket = optional(&r.bucket);
        let ef = optional_ef(r.ef);
        let started = std::time::Instant::now();
        let hits = self
            .state
            .index
            .search_text(&r.query, bucket, k, ef)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(pb::SearchResponse {
            hits: hits.into_iter().map(hit_to_proto).collect(),
            took_ms: started.elapsed().as_millis() as u64,
        }))
    }

    async fn vector_search(
        &self,
        req: Request<pb::VectorSearchRequest>,
    ) -> Result<Response<pb::SearchResponse>, Status> {
        let r = req.into_inner();
        if r.vector.len() != self.state.index.dim() {
            return Err(Status::invalid_argument(format!(
                "vector dim {} != index dim {}",
                r.vector.len(),
                self.state.index.dim()
            )));
        }
        let k = validate_top_k(r.top_k)?;
        let bucket = optional(&r.bucket);
        let ef = optional_ef(r.ef);
        let started = std::time::Instant::now();
        let hits = self
            .state
            .index
            .search_vector(&r.vector, bucket, k, ef)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(pb::SearchResponse {
            hits: hits.into_iter().map(hit_to_proto).collect(),
            took_ms: started.elapsed().as_millis() as u64,
        }))
    }
}

// ---- AI / RAG ----

#[derive(Clone)]
pub struct AiSvc {
    state: GrpcState,
}

impl AiSvc {
    pub fn new(state: GrpcState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl pb::ai_service_server::AiService for AiSvc {
    type RagStream = ReceiverStream<Result<pb::RagChunk, Status>>;

    async fn rag(
        &self,
        req: Request<pb::RagRequest>,
    ) -> Result<Response<Self::RagStream>, Status> {
        let r = req.into_inner();
        if r.query.trim().is_empty() {
            return Err(Status::invalid_argument("query required"));
        }
        let k = validate_top_k(r.top_k)?;
        let bucket = optional(&r.bucket).map(|s| s.to_string());

        // Retrieve synchronously so retrieval errors surface as a proper
        // gRPC Status before we hand the client a stream.
        let hits = self
            .state
            .index
            .search_text(&r.query, bucket.as_deref(), k, None)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let snippets: Vec<&str> = hits.iter().map(|h| h.text.as_str()).collect();
        let prompt = build_rag_prompt(&r.query, &snippets);

        // Kick off the LLM before returning so a provider-level failure
        // (bad key, dead Ollama) is a pre-stream error.
        let mut llm_stream = self
            .state
            .llm
            .generate(prompt)
            .await
            .map_err(|e| Status::internal(format!("llm: {e}")))?;

        // Buffer size 64 picked to hold a handful of context events +
        // a typical per-stream token burst without blocking the LLM
        // task. A backed-up client will see back-pressure via Tonic's
        // flow control either way.
        let (tx, rx) = mpsc::channel::<Result<pb::RagChunk, Status>>(64);

        // Emit every context hit up front. If the client drops before
        // draining, the send fails and we bail — nothing leaks.
        for h in hits {
            let chunk = pb::RagChunk {
                kind: Some(pb::rag_chunk::Kind::Context(hit_to_proto(h))),
            };
            if tx.send(Ok(chunk)).await.is_err() {
                return Ok(Response::new(ReceiverStream::new(rx)));
            }
        }

        // Forward LLM tokens in a background task so this handler can
        // return immediately; the client starts streaming without
        // waiting for the first token.
        tokio::spawn(async move {
            while let Some(item) = llm_stream.next().await {
                let chunk = match item {
                    Ok(LlmChunk::Delta(t)) => pb::RagChunk {
                        kind: Some(pb::rag_chunk::Kind::AnswerDelta(t)),
                    },
                    Ok(LlmChunk::Done) => pb::RagChunk {
                        kind: Some(pb::rag_chunk::Kind::Done(pb::Done {})),
                    },
                    Err(e) => pb::RagChunk {
                        kind: Some(pb::rag_chunk::Kind::Error(e.to_string())),
                    },
                };
                if tx.send(Ok(chunk)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

// ---- helpers ----

fn validate_top_k(top_k: u32) -> Result<usize, Status> {
    if top_k == 0 {
        return Err(Status::invalid_argument("top_k must be > 0"));
    }
    Ok(top_k as usize)
}

fn optional(s: &str) -> Option<&str> {
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

fn optional_ef(ef: u32) -> Option<usize> {
    if ef == 0 {
        None
    } else {
        Some(ef as usize)
    }
}

fn parse_metadata(json: &str) -> Result<serde_json::Value, Status> {
    if json.is_empty() {
        return Ok(serde_json::Value::Null);
    }
    serde_json::from_str(json).map_err(|e| Status::invalid_argument(format!("metadata_json: {e}")))
}

fn hit_to_proto(h: nebula_index::Hit) -> pb::Hit {
    pb::Hit {
        bucket: h.bucket,
        id: h.id,
        text: h.text,
        score: h.score,
        metadata_json: h.metadata.to_string(),
    }
}

/// Serve all three services on a single Tonic server bound to
/// `addr`. Returns when the server stops (graceful shutdown or
/// fatal error). Callers wanting finer control should mount the
/// server wrappers themselves — they're public above.
pub async fn serve(
    state: GrpcState,
    addr: std::net::SocketAddr,
) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
        .add_service(pb::document_service_server::DocumentServiceServer::new(
            DocumentSvc::new(state.clone()),
        ))
        .add_service(pb::search_service_server::SearchServiceServer::new(
            SearchSvc::new(state.clone()),
        ))
        .add_service(pb::ai_service_server::AiServiceServer::new(AiSvc::new(state)))
        .serve(addr)
        .await
}
