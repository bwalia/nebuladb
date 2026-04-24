//! OpenAI-compatible embeddings client.
//!
//! Targets the `/v1/embeddings` shape that OpenAI, Azure OpenAI, vLLM,
//! LiteLLM, and Ollama's compat shim all speak. Keeping the surface to
//! this one endpoint is deliberate — vendor-specific SDKs pull in heavy
//! dependency trees and drag tokenizer downloads into the build.

use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::{EmbedError, Embedder, Result};

#[derive(Debug, Clone)]
pub struct OpenAiEmbedderConfig {
    /// Base URL up to but not including `/embeddings`. For OpenAI,
    /// `https://api.openai.com/v1`. For a local vLLM, something like
    /// `http://localhost:8000/v1`.
    pub base_url: String,
    /// API key. `None` is valid for local servers that don't auth.
    pub api_key: Option<String>,
    pub model: String,
    /// Expected output dimensionality. Required because `dim()` is
    /// synchronous and we can't probe the server at construction time.
    pub dim: usize,
    pub timeout: Duration,
}

impl OpenAiEmbedderConfig {
    pub fn openai(api_key: impl Into<String>, model: impl Into<String>, dim: usize) -> Self {
        Self {
            base_url: "https://api.openai.com/v1".into(),
            api_key: Some(api_key.into()),
            model: model.into(),
            dim,
            timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug)]
pub struct OpenAiEmbedder {
    http: reqwest::Client,
    config: OpenAiEmbedderConfig,
}

impl OpenAiEmbedder {
    pub fn new(config: OpenAiEmbedderConfig) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if let Some(key) = &config.api_key {
            let v = HeaderValue::from_str(&format!("Bearer {key}"))
                .map_err(|e| EmbedError::Decode(format!("invalid api key: {e}")))?;
            headers.insert(AUTHORIZATION, v);
        }
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(config.timeout)
            .build()?;
        Ok(Self { http, config })
    }
}

#[derive(Serialize)]
struct EmbedRequest<'a> {
    model: &'a str,
    input: &'a [String],
}

#[derive(Deserialize)]
struct EmbedResponse {
    data: Vec<EmbeddingEntry>,
}

#[derive(Deserialize)]
struct EmbeddingEntry {
    embedding: Vec<f32>,
    /// Present in OpenAI responses. We sort by it to guarantee the
    /// output order matches the input order — the spec says yes, but
    /// belt-and-braces costs nothing and saves debugging later.
    #[serde(default)]
    index: usize,
}

#[async_trait]
impl Embedder for OpenAiEmbedder {
    fn dim(&self) -> usize {
        self.config.dim
    }

    fn model(&self) -> &str {
        &self.config.model
    }

    async fn embed(&self, inputs: &[String]) -> Result<Vec<Vec<f32>>> {
        if inputs.is_empty() {
            return Err(EmbedError::EmptyBatch);
        }
        let url = format!("{}/embeddings", self.config.base_url.trim_end_matches('/'));
        let body = EmbedRequest {
            model: &self.config.model,
            input: inputs,
        };
        let resp = self.http.post(&url).json(&body).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(EmbedError::Provider {
                status: status.as_u16(),
                body,
            });
        }
        let mut parsed: EmbedResponse = resp
            .json()
            .await
            .map_err(|e| EmbedError::Decode(e.to_string()))?;
        parsed.data.sort_by_key(|e| e.index);

        if parsed.data.len() != inputs.len() {
            return Err(EmbedError::Decode(format!(
                "expected {} embeddings, got {}",
                inputs.len(),
                parsed.data.len()
            )));
        }
        for (i, entry) in parsed.data.iter().enumerate() {
            if entry.embedding.len() != self.config.dim {
                return Err(EmbedError::DimensionMismatch {
                    expected: self.config.dim,
                    actual: entry.embedding.len(),
                });
            }
            let _ = i;
        }
        Ok(parsed.data.into_iter().map(|e| e.embedding).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_key_header() {
        let cfg = OpenAiEmbedderConfig {
            base_url: "http://localhost".into(),
            api_key: Some("bad\nkey".into()),
            model: "m".into(),
            dim: 4,
            timeout: Duration::from_secs(1),
        };
        let err = OpenAiEmbedder::new(cfg).unwrap_err();
        assert!(matches!(err, EmbedError::Decode(_)));
    }

    #[test]
    fn constructs_without_api_key() {
        let cfg = OpenAiEmbedderConfig {
            base_url: "http://localhost:8000/v1".into(),
            api_key: None,
            model: "nomic-embed-text".into(),
            dim: 768,
            timeout: Duration::from_secs(1),
        };
        assert!(OpenAiEmbedder::new(cfg).is_ok());
    }
}
