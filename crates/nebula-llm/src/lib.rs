//! LLM client abstractions.
//!
//! One trait — [`LlmClient`] — returns a token stream. Two built-in
//! backends:
//!
//! - [`MockLlm`]: deterministic, offline. Produces a fixed
//!   "context: ...\nquery: ..." summary tokenized on whitespace.
//! - [`OllamaLlm`]: `POST /api/generate` with `stream:true`. Ollama
//!   emits newline-delimited JSON chunks; we parse them lazily.
//! - [`OpenAiChatLlm`]: `POST /v1/chat/completions` with `stream:true`.
//!   OpenAI emits Server-Sent Events (`data: {...}\n\n`); we parse the
//!   SSE frames into deltas.
//!
//! All streams yield [`LlmChunk::Delta(String)`] tokens then terminate
//! with [`LlmChunk::Done`]. Callers forward deltas onto their own SSE
//! stream, so a single `/ai/rag` handler works unchanged for any backend.

mod mock;
mod ollama;
mod openai_chat;

use async_trait::async_trait;
use futures::stream::BoxStream;

pub use mock::MockLlm;
pub use ollama::{OllamaLlm, OllamaConfig};
pub use openai_chat::{OpenAiChatLlm, OpenAiChatConfig};

#[derive(Debug, thiserror::Error)]
pub enum LlmError {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("provider: {status}: {body}")]
    Provider { status: u16, body: String },
    #[error("decode: {0}")]
    Decode(String),
    #[error("empty prompt")]
    Empty,
}

pub type Result<T> = std::result::Result<T, LlmError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LlmChunk {
    /// A partial token / token group. Forward as-is to the consumer.
    Delta(String),
    /// Terminal marker. Consumers should stop reading the stream.
    Done,
}

/// A prompt bundle. `system` is optional (Ollama ignores it unless set
/// at model creation; OpenAI puts it in the first message).
#[derive(Debug, Clone)]
pub struct Prompt {
    pub system: Option<String>,
    pub user: String,
}

impl Prompt {
    pub fn user(text: impl Into<String>) -> Self {
        Self {
            system: None,
            user: text.into(),
        }
    }
}

#[async_trait]
pub trait LlmClient: Send + Sync {
    /// Backend identity for telemetry (e.g. "ollama/llama3").
    fn model(&self) -> &str;

    /// Produce a streaming response. Returning a boxed stream keeps
    /// the trait object-safe; the cost is one allocation per call,
    /// which is fine next to a network round-trip.
    async fn generate(
        &self,
        prompt: Prompt,
    ) -> Result<BoxStream<'static, Result<LlmChunk>>>;
}

/// Build a default RAG prompt. Public so callers can override formatting
/// without re-implementing the handler. Keep this deliberately plain —
/// elaborate prompt engineering belongs in the caller, not a lib crate.
pub fn build_rag_prompt(query: &str, context_snippets: &[&str]) -> Prompt {
    let mut user = String::new();
    if !context_snippets.is_empty() {
        user.push_str("Context:\n");
        for (i, c) in context_snippets.iter().enumerate() {
            user.push_str(&format!("[{i}] {c}\n"));
        }
        user.push('\n');
    }
    user.push_str("Question: ");
    user.push_str(query);
    user.push_str("\nAnswer concisely using only the context above.");
    Prompt {
        system: Some("You are NebulaDB's retrieval assistant. Cite chunks by [n].".into()),
        user,
    }
}
