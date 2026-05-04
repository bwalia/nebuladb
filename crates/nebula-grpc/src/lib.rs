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
use nebula_core::NodeRole;
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
    /// Role the process booted with. Reads are always allowed; writes
    /// are rejected with [`Status::failed_precondition`] when the
    /// role is [`NodeRole::Follower`]. Parity with the REST
    /// `guard_writes_on_follower` middleware.
    pub role: NodeRole,
}

impl GrpcState {
    pub fn new(
        index: Arc<TextIndex>,
        llm: Arc<dyn LlmClient>,
        chunker: Arc<dyn Chunker>,
    ) -> Self {
        Self::with_role(index, llm, chunker, NodeRole::default())
    }

    pub fn with_role(
        index: Arc<TextIndex>,
        llm: Arc<dyn LlmClient>,
        chunker: Arc<dyn Chunker>,
        role: NodeRole,
    ) -> Self {
        Self {
            index,
            llm,
            chunker,
            role,
        }
    }

    /// The WAL handle this state will replicate from, if any.
    /// Derived from the index rather than stored separately so the
    /// two can't disagree about persistence mode.
    pub fn wal(&self) -> Option<Arc<nebula_wal::Wal>> {
        self.index.wal()
    }
}

/// Stable error detail string emitted when a follower refuses a write.
/// REST returns `409 read_only_follower`; gRPC returns
/// `FAILED_PRECONDITION` with this message so clients can pattern-match
/// without parsing prose.
pub const READ_ONLY_FOLLOWER: &str = "read_only_follower";

fn follower_write_error() -> Status {
    Status::failed_precondition(READ_ONLY_FOLLOWER)
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
        if self.state.role.is_read_only() {
            return Err(follower_write_error());
        }
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
        if self.state.role.is_read_only() {
            return Err(follower_write_error());
        }
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

pub mod follower;

// ---- Replication (follower-mode WAL tail) ----

/// gRPC service that streams the leader's WAL to follower replicas.
///
/// Internally this is a thin wrapper over [`nebula_wal::Wal::subscribe`]:
/// every entry the subscriber yields is serialized and forwarded
/// over the stream. The subscriber survives catch-up + live tail
/// transparently, so the follower just drains one ordered stream.
///
/// # Failure modes
///
/// - Subscriber `Lagged` → close the stream with
///   `FAILED_PRECONDITION`. The follower must reconnect with a
///   cursor that's still in the retained WAL, or bootstrap from a
///   fresh snapshot if it's fallen too far behind.
/// - Client disconnect → mpsc send fails, the forwarder task exits,
///   the subscriber drops, its ack slot is released (freeing up
///   any compaction the leader was holding back on behalf of the
///   dead follower).
#[derive(Clone)]
pub struct ReplicationSvc {
    /// The leader's WAL handle. `None` means the leader is running
    /// in-memory (no durability, nothing to replicate from) — every
    /// TailWal call returns `FAILED_PRECONDITION` in that case.
    wal: Option<std::sync::Arc<nebula_wal::Wal>>,
}

impl ReplicationSvc {
    pub fn new(wal: Option<std::sync::Arc<nebula_wal::Wal>>) -> Self {
        Self { wal }
    }
}

#[async_trait]
impl pb::replication_service_server::ReplicationService for ReplicationSvc {
    type TailWalStream = ReceiverStream<Result<pb::WalTailEntry, Status>>;

    async fn tail_wal(
        &self,
        req: Request<pb::TailWalRequest>,
    ) -> Result<Response<Self::TailWalStream>, Status> {
        let wal = self.wal.clone().ok_or_else(|| {
            Status::failed_precondition(
                "leader has no WAL (running in-memory); enable NEBULA_DATA_DIR to serve followers",
            )
        })?;

        // Translate the proto cursor into the WAL's native one.
        // Missing proto message => BEGIN; this matches the "give me
        // everything" case from a fresh follower.
        let start = match req.into_inner().start {
            Some(c) => nebula_wal::WalCursor {
                segment_seq: c.segment_seq,
                byte_offset: c.byte_offset,
            },
            None => nebula_wal::WalCursor::BEGIN,
        };

        let mut sub = wal
            .subscribe(start)
            .map_err(|e| Status::internal(format!("wal subscribe: {e}")))?;

        // Same buffer size as the RAG stream — handful of entries to
        // smooth over client-side hiccups, small enough that a
        // stalled client creates immediate backpressure.
        let (tx, rx) = mpsc::channel::<Result<pb::WalTailEntry, Status>>(64);

        tokio::spawn(async move {
            while let Some(item) = sub.next().await {
                let send_result = match item {
                    Ok(entry) => {
                        let body = match bincode::serialize(&entry.record) {
                            Ok(b) => b,
                            Err(e) => {
                                // Encoding our own record should never
                                // fail; surface it loudly if it does.
                                let _ = tx
                                    .send(Err(Status::internal(format!(
                                        "bincode encode: {e}"
                                    ))))
                                    .await;
                                break;
                            }
                        };
                        tx.send(Ok(pb::WalTailEntry {
                            cursor: Some(pb::WalCursor {
                                segment_seq: entry.cursor.segment_seq,
                                byte_offset: entry.cursor.byte_offset,
                            }),
                            next_cursor: Some(pb::WalCursor {
                                segment_seq: entry.next_cursor.segment_seq,
                                byte_offset: entry.next_cursor.byte_offset,
                            }),
                            record_bincode: body,
                        }))
                        .await
                    }
                    Err(e) => {
                        // Lagged / format error. Close the stream
                        // with FAILED_PRECONDITION so the follower
                        // knows to resubscribe (or bootstrap).
                        tx.send(Err(Status::failed_precondition(e.to_string()))).await
                    }
                };
                if send_result.is_err() {
                    // Client dropped. Fall out; subscriber drop
                    // releases the ack slot.
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
    let wal = state.wal();
    tonic::transport::Server::builder()
        .add_service(pb::document_service_server::DocumentServiceServer::new(
            DocumentSvc::new(state.clone()),
        ))
        .add_service(pb::search_service_server::SearchServiceServer::new(
            SearchSvc::new(state.clone()),
        ))
        .add_service(pb::ai_service_server::AiServiceServer::new(AiSvc::new(state)))
        // ReplicationService is always mounted. In-memory leaders
        // respond with FAILED_PRECONDITION, which is the right
        // signal for a follower ("this node can't serve me").
        .add_service(pb::replication_service_server::ReplicationServiceServer::new(
            ReplicationSvc::new(wal),
        ))
        .serve(addr)
        .await
}
