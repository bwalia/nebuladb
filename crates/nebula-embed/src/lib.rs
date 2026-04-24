//! Embedding providers for NebulaDB.
//!
//! Exposes an async [`Embedder`] trait and two implementations:
//!
//! - [`MockEmbedder`]: deterministic, offline, dependency-free.
//!   The default; also what tests use.
//! - [`OpenAiEmbedder`]: HTTP client for any OpenAI-compatible
//!   `/v1/embeddings` endpoint (OpenAI, Azure OpenAI, vLLM, Ollama
//!   with the compat shim, local LiteLLM, etc.).
//!
//! Keeping this behind a trait means the rest of the system never talks
//! to a vendor SDK, and swapping providers is a one-line change at
//! construction time.

mod mock;
mod openai;

use async_trait::async_trait;

pub use mock::MockEmbedder;
pub use openai::{OpenAiEmbedder, OpenAiEmbedderConfig};

#[derive(Debug, thiserror::Error)]
pub enum EmbedError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("provider returned error: {status}: {body}")]
    Provider { status: u16, body: String },
    #[error("response shape invalid: {0}")]
    Decode(String),
    #[error("input batch was empty")]
    EmptyBatch,
    #[error("dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },
}

pub type Result<T> = std::result::Result<T, EmbedError>;

/// A provider that turns text into dense vectors. Implementations must
/// be cheap to clone (typically `Arc` inside) because callers will share
/// one embedder across many tasks.
#[async_trait]
pub trait Embedder: Send + Sync {
    /// Vector dimensionality this embedder produces. Callers use this
    /// to size the vector index at startup.
    fn dim(&self) -> usize;

    /// Model identifier. Used in telemetry and to key semantic caches —
    /// two different models may both be 768-dim but produce incompatible
    /// vectors, so the model name is the real identity.
    fn model(&self) -> &str;

    /// Embed a batch. Returning one `Vec<f32>` per input, in the same
    /// order. A batched API matters: every provider charges round-trip
    /// latency per call, and local models amortize GPU launches.
    async fn embed(&self, inputs: &[String]) -> Result<Vec<Vec<f32>>>;

    /// Convenience wrapper for a single string. The default impl
    /// delegates to `embed`, so providers only implement the batched
    /// path.
    async fn embed_one(&self, input: &str) -> Result<Vec<f32>> {
        let mut out = self.embed(&[input.to_string()]).await?;
        out.pop().ok_or(EmbedError::EmptyBatch)
    }
}
