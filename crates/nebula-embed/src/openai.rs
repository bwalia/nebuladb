//! OpenAI-compatible embeddings client.
//!
//! Targets the `/v1/embeddings` shape that OpenAI, Azure OpenAI, vLLM,
//! LiteLLM, and Ollama's compat shim all speak. Keeping the surface to
//! this one endpoint is deliberate — vendor-specific SDKs pull in heavy
//! dependency trees and drag tokenizer downloads into the build.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};

use crate::{EmbedError, Embedder, Result};

/// How to send `api_key` on outbound embedding requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OpenAiAuthStyle {
    /// `Authorization: Bearer <key>` — OpenAI, vLLM, most local shims.
    #[default]
    Bearer,
    /// `x-api-key: <JWT>` — wslproxy / ollama.workstation.co.uk gateway.
    /// When `api_key` is an HMAC secret (not already a JWT), a short-lived
    /// HS256 token is minted per request (same scheme as nebula-llm).
    XApiKey,
}

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
    pub auth_style: OpenAiAuthStyle,
}

impl OpenAiEmbedderConfig {
    pub fn openai(api_key: impl Into<String>, model: impl Into<String>, dim: usize) -> Self {
        Self {
            base_url: "https://api.openai.com/v1".into(),
            api_key: Some(api_key.into()),
            model: model.into(),
            dim,
            timeout: Duration::from_secs(30),
            auth_style: OpenAiAuthStyle::Bearer,
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
        // Bearer can be set as a default header; x-api-key is minted
        // per request (short TTL) so it is attached in `embed`.
        if config.auth_style == OpenAiAuthStyle::Bearer {
            if let Some(key) = &config.api_key {
                let v = HeaderValue::from_str(&format!("Bearer {key}"))
                    .map_err(|e| EmbedError::Decode(format!("invalid api key: {e}")))?;
                headers.insert(AUTHORIZATION, v);
            }
        }
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(config.timeout)
            .build()?;
        Ok(Self { http, config })
    }

    fn auth_x_api_key(&self) -> Result<Option<String>> {
        let Some(raw) = self
            .config
            .api_key
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        else {
            return Ok(None);
        };
        if looks_like_jwt(raw) {
            return Ok(Some(raw.to_string()));
        }
        Ok(Some(mint_gateway_jwt(raw)?))
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

const GATEWAY_JWT_TTL_SECS: u64 = 300;

#[derive(Debug, Serialize, Deserialize)]
struct GatewayJwtClaims {
    app: String,
    iat: u64,
    exp: u64,
}

fn mint_gateway_jwt(secret: &str) -> Result<String> {
    let hmac_secret = resolve_hmac_secret(secret);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| EmbedError::Decode(format!("system clock before epoch: {e}")))?
        .as_secs();
    let claims = GatewayJwtClaims {
        app: "nebuladb".into(),
        iat: now,
        exp: now.saturating_add(GATEWAY_JWT_TTL_SECS),
    };
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(hmac_secret.as_bytes()),
    )
    .map_err(|e| EmbedError::Decode(format!("embed gateway jwt mint failed: {e}")))
}

fn looks_like_jwt(s: &str) -> bool {
    let mut parts = s.split('.');
    matches!(
        (parts.next(), parts.next(), parts.next(), parts.next()),
        (Some(a), Some(b), Some(c), None)
            if !a.is_empty() && !b.is_empty() && !c.is_empty() && a.starts_with("eyJ")
    )
}

fn resolve_hmac_secret(raw: &str) -> String {
    let mut padded = raw.to_string();
    let rem = padded.len() % 4;
    if rem != 0 {
        padded.push_str(&"=".repeat(4 - rem));
    }
    if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(&padded) {
        if let Ok(decoded) = std::str::from_utf8(&bytes) {
            let ok = !decoded.is_empty()
                && decoded.len() < 128
                && decoded.chars().all(|c| c.is_ascii_graphic());
            if ok {
                return decoded.to_string();
            }
        }
    }
    raw.to_string()
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
        let mut req = self.http.post(&url).json(&body);
        if self.config.auth_style == OpenAiAuthStyle::XApiKey {
            if let Some(token) = self.auth_x_api_key()? {
                req = req.header("x-api-key", token);
            }
        }
        let resp = req.send().await?;
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
        for entry in &parsed.data {
            if entry.embedding.len() != self.config.dim {
                return Err(EmbedError::DimensionMismatch {
                    expected: self.config.dim,
                    actual: entry.embedding.len(),
                });
            }
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
            auth_style: OpenAiAuthStyle::Bearer,
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
            auth_style: OpenAiAuthStyle::Bearer,
        };
        assert!(OpenAiEmbedder::new(cfg).is_ok());
    }

    #[test]
    fn mints_gateway_jwt() {
        let token = mint_gateway_jwt("test-secret").expect("mint");
        assert!(looks_like_jwt(&token));
    }
}
