//! End-to-end gRPC test.
//!
//! Spins up a real Tonic server on an ephemeral port and drives it
//! with the generated client. This is slower than unit-level
//! exercises but is the only way to verify that the generated code,
//! the transport, our service impls, and the streaming semantics
//! actually wire together.

use std::net::SocketAddr;
use std::sync::Arc;

use nebula_chunk::{Chunker, FixedSizeChunker};
use nebula_embed::{Embedder, MockEmbedder};
use nebula_grpc::pb::{
    ai_service_client::AiServiceClient, document_service_client::DocumentServiceClient,
    rag_chunk, search_service_client::SearchServiceClient, RagRequest, SemanticSearchRequest,
    UpsertDocumentRequest,
};
use nebula_grpc::{serve, GrpcState};
use nebula_index::TextIndex;
use nebula_llm::{LlmClient, MockLlm};
use nebula_vector::{HnswConfig, Metric};
use tokio::time::{timeout, Duration};
use tonic::transport::Channel;

async fn spawn_server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(32));
    let index = Arc::new(
        TextIndex::new(emb, Metric::Cosine, HnswConfig::default()).unwrap(),
    );
    let chunker: Arc<dyn Chunker> = Arc::new(FixedSizeChunker::new(20, 0).unwrap());
    let llm: Arc<dyn LlmClient> = Arc::new(MockLlm::default());

    // Bind a TcpListener to get a free port, then hand the address to
    // the Tonic server. Tonic doesn't expose "bind 0 and report" so
    // we use the OS to pick and drop the listener before Tonic binds.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let state = GrpcState::new(index, llm, chunker);
    let handle = tokio::spawn(async move {
        serve(state, addr).await.unwrap();
    });

    // Give the server a moment to bind. Polling the port is tidier
    // than `sleep`, but for a local test a short sleep is fine.
    tokio::time::sleep(Duration::from_millis(100)).await;
    (addr, handle)
}

async fn channel(addr: SocketAddr) -> Channel {
    Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .expect("connect")
}

#[tokio::test]
async fn grpc_upsert_and_semantic_search() {
    let (addr, _srv) = spawn_server().await;
    let ch = channel(addr).await;

    let mut docs = DocumentServiceClient::new(ch.clone());
    let resp = docs
        .upsert_document(UpsertDocumentRequest {
            bucket: "docs".into(),
            doc_id: "d1".into(),
            text: "zero trust networking is a security model".into(),
            metadata_json: "".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.bucket, "docs");
    assert!(resp.chunks >= 1);

    let mut search = SearchServiceClient::new(ch);
    let hits = search
        .semantic_search(SemanticSearchRequest {
            query: "zero trust".into(),
            top_k: 3,
            bucket: "docs".into(),
            ef: 0,
        })
        .await
        .unwrap()
        .into_inner()
        .hits;
    assert!(!hits.is_empty(), "expected at least one hit");
    assert!(hits.iter().all(|h| h.bucket == "docs"));
}

#[tokio::test]
async fn grpc_rag_stream_delivers_context_then_deltas_then_done() {
    let (addr, _srv) = spawn_server().await;
    let ch = channel(addr).await;

    // Seed a doc so retrieval has something to return.
    DocumentServiceClient::new(ch.clone())
        .upsert_document(UpsertDocumentRequest {
            bucket: "docs".into(),
            doc_id: "d1".into(),
            text: "alpha bravo charlie".into(),
            metadata_json: "".into(),
        })
        .await
        .unwrap();

    let mut ai = AiServiceClient::new(ch);
    let mut stream = ai
        .rag(RagRequest {
            query: "alpha".into(),
            top_k: 2,
            bucket: "docs".into(),
        })
        .await
        .unwrap()
        .into_inner();

    let mut context_count = 0;
    let mut delta_count = 0;
    let mut saw_done = false;

    // Cap each read with a timeout — a broken stream should fail
    // loud, not hang CI for 60s. `stream.message()` returns
    // `Result<Option<T>, Status>`: `Ok(None)` = clean end of stream,
    // `Err` = transport/server error.
    loop {
        let next = timeout(Duration::from_secs(5), stream.message())
            .await
            .expect("stream read timed out")
            .expect("stream returned error");
        let Some(msg) = next else { break };
        match msg.kind {
            Some(rag_chunk::Kind::Context(_)) => context_count += 1,
            Some(rag_chunk::Kind::AnswerDelta(_)) => delta_count += 1,
            Some(rag_chunk::Kind::Done(_)) => {
                saw_done = true;
                break;
            }
            Some(rag_chunk::Kind::Error(e)) => panic!("unexpected error: {e}"),
            None => {}
        }
    }
    assert!(context_count >= 1, "expected at least one context event");
    assert!(delta_count >= 2, "expected at least two deltas, got {delta_count}");
    assert!(saw_done, "stream ended without a Done marker");
}

#[tokio::test]
async fn grpc_semantic_search_validates_empty_query() {
    let (addr, _srv) = spawn_server().await;
    let ch = channel(addr).await;

    let mut search = SearchServiceClient::new(ch);
    let err = search
        .semantic_search(SemanticSearchRequest {
            query: "   ".into(),
            top_k: 1,
            bucket: "".into(),
            ef: 0,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}
