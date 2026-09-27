//! Ollama `/api/generate` streaming client.
//!
//! Ollama's streaming format is newline-delimited JSON (NDJSON), not
//! SSE. Each chunk looks like:
//!
//! ```json
//! {"model":"llama3","response":"Hello","done":false}
//! {"model":"llama3","response":" world","done":false}
//! {"model":"llama3","response":"","done":true,"total_duration":...}
//! ```
//!
//! We iterate the byte stream, buffer until newlines, and emit one
//! [`LlmChunk::Delta`] per non-empty `response`, terminating on `done`.
//!
//! Authenticated gateways (wslproxy in front of
//! `https://ollama.workstation.co.uk`) expect an `x-api-key` header
//! carrying a short-lived HS256 JWT. `OllamaConfig::api_key` is the
//! shared signing secret (or a pre-minted JWT). A fresh token is minted
//! on every request so it never outlives `exp`.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine;
use futures::stream::{BoxStream, StreamExt};
use futures::TryStreamExt;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::{Deserialize, Serialize};

use crate::{
    GenerateOptions, LlmChunk, LlmClient, LlmError, ModelCapabilities, ModelInfo, Prompt, Result,
};

/// Default JWT lifetime — short enough that a leaked token is useless,
/// long enough for a slow generate + stream.
const OLLAMA_JWT_TTL_SECS: u64 = 600;

#[derive(Debug, Clone)]
pub struct OllamaConfig {
    /// e.g. `http://localhost:11434` or `https://ollama.workstation.co.uk`.
    pub base_url: String,
    pub model: String,
    /// Shared HMAC secret used to mint the gateway `x-api-key` JWT, or
    /// a pre-minted JWT (three base64url segments). Empty / `None` ⇒
    /// no auth header (local unauthenticated Ollama).
    ///
    /// Kept as `bearer_token` in older call sites via the alias below —
    /// the wire format is `x-api-key`, not `Authorization: Bearer`.
    pub api_key: Option<String>,
    /// **Connect** timeout for the initial TCP/TLS handshake. Does
    /// NOT cap the streaming body — see the field below.
    ///
    /// Kept named `timeout` so existing callers don't break, but the
    /// semantics are connect-only since the streaming-body bug fix.
    pub timeout: Duration,
    /// Idle-byte timeout: if `read_timeout` elapses without any new
    /// bytes from the upstream during streaming, the request fails.
    /// `None` ⇒ no idle timeout (rely on TCP keepalive + the watchdog).
    ///
    /// We split this from `timeout` because the prior single-knob
    /// design applied `reqwest::ClientBuilder::timeout` to the whole
    /// response body, which silently aborts a long generation
    /// mid-stream — a model that takes >timeout seconds to finish
    /// (legitimately) had its SSE response cut. Use
    /// `read_timeout` to detect a stuck Ollama (no bytes for 60s)
    /// without truncating a healthy long answer.
    pub read_timeout: Option<Duration>,
}

impl Default for OllamaConfig {
    fn default() -> Self {
        Self {
            base_url: "http://localhost:11434".into(),
            model: "llama3".into(),
            api_key: None,
            // 10s is plenty to dial localhost; raise via env if you
            // point at a remote Ollama over a slow link.
            timeout: Duration::from_secs(10),
            // 60s without a single byte ⇒ assume Ollama is stuck. A
            // healthy generation emits tokens far more often than this.
            // None disables the check — useful in tests with a
            // deliberately slow mock.
            read_timeout: Some(Duration::from_secs(60)),
        }
    }
}

impl OllamaConfig {
    /// Back-compat setter used by call sites that still say `bearer_token`.
    pub fn with_bearer_token(mut self, token: Option<String>) -> Self {
        self.api_key = token;
        self
    }
}

#[derive(Debug)]
pub struct OllamaLlm {
    http: reqwest::Client,
    config: OllamaConfig,
    model_label: String,
}

impl OllamaLlm {
    pub fn new(config: OllamaConfig) -> Result<Self> {
        // Critical: do NOT set `.timeout(config.timeout)` here. That
        // applies to the whole response body — including the
        // streaming generate response — so a long-running model gets
        // its stream cut at exactly that wall clock, regardless of
        // whether bytes are still flowing. Use `connect_timeout` for
        // the dial, `read_timeout` for byte-idle, leave the body
        // duration uncapped. See the comment on
        // `OllamaConfig::read_timeout` for the rationale.
        //
        // Auth is attached per-request (fresh JWT) — not as default
        // headers — so a long-lived client never presents an expired
        // token.
        let mut builder = reqwest::Client::builder().connect_timeout(config.timeout);
        if let Some(rt) = config.read_timeout {
            builder = builder.read_timeout(rt);
        }
        let http = builder.build()?;
        let model_label = format!("ollama/{}", config.model);
        Ok(Self {
            http,
            config,
            model_label,
        })
    }

    /// Build the `x-api-key` value for one outbound request.
    fn auth_header_value(&self) -> Result<Option<String>> {
        let Some(raw) = self.config.api_key.as_deref().map(str::trim).filter(|s| !s.is_empty()) else {
            return Ok(None);
        };
        if looks_like_jwt(raw) {
            return Ok(Some(raw.to_string()));
        }
        Ok(Some(mint_ollama_jwt(raw)?))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct OllamaJwtClaims {
    app: String,
    iat: u64,
    exp: u64,
}

/// Mint an HS256 JWT for the wslproxy Ollama gateway.
///
/// `secret` may be either the plaintext HMAC passphrase or the
/// Base64 form stored in the gateway rule's `jwt_token_validation_key`
/// (gateway Base64-decodes before verify). We accept both.
fn mint_ollama_jwt(secret: &str) -> Result<String> {
    let hmac_secret = resolve_hmac_secret(secret);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| LlmError::Decode(format!("system clock before epoch: {e}")))?
        .as_secs();
    let claims = OllamaJwtClaims {
        app: "nebuladb".into(),
        iat: now,
        exp: now.saturating_add(OLLAMA_JWT_TTL_SECS),
    };
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(hmac_secret.as_bytes()),
    )
    .map_err(|e| LlmError::Decode(format!("ollama jwt mint failed: {e}")))
}

fn looks_like_jwt(s: &str) -> bool {
    let mut parts = s.split('.');
    matches!(
        (parts.next(), parts.next(), parts.next(), parts.next()),
        (Some(a), Some(b), Some(c), None)
            if !a.is_empty() && !b.is_empty() && !c.is_empty() && a.starts_with("eyJ")
    )
}

/// If `raw` is standard Base64 of a printable passphrase, return the
/// decoded passphrase (matches wslproxy rule storage). Otherwise use
/// `raw` as-is (DIY Vault stores the already-decoded secret).
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

#[derive(serde::Serialize)]
struct GenRequest<'a> {
    model: &'a str,
    prompt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<&'a str>,
    stream: bool,
}

#[derive(Deserialize)]
struct GenChunk {
    #[serde(default)]
    response: String,
    #[serde(default)]
    done: bool,
}

#[async_trait]
impl LlmClient for OllamaLlm {
    fn model(&self) -> &str {
        &self.model_label
    }

    fn info(&self) -> ModelInfo {
        ModelInfo {
            id: self.config.model.clone(),
            provider: "ollama".into(),
            display_name: self.config.model.clone(),
            capabilities: ModelCapabilities::OLLAMA,
            context_window: Some(8_192),
            is_mock: false,
        }
    }

    async fn generate_with_options(
        &self,
        prompt: Prompt,
        opts: GenerateOptions,
    ) -> Result<BoxStream<'static, Result<LlmChunk>>> {
        if !opts.tools.is_empty() {
            return Err(LlmError::Unsupported("tool_calling"));
        }
        if opts.response_format.is_some() {
            return Err(LlmError::Unsupported("structured_output"));
        }
        if prompt.user.trim().is_empty() {
            return Err(LlmError::Empty);
        }
        let url = format!(
            "{}/api/generate",
            self.config.base_url.trim_end_matches('/')
        );
        let body = GenRequest {
            model: &self.config.model,
            prompt: prompt.user,
            system: prompt.system.as_deref(),
            stream: true,
        };
        let mut req = self.http.post(&url).json(&body);
        if let Some(token) = self.auth_header_value()? {
            req = req.header("x-api-key", token);
        }
        let resp = req.send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(LlmError::Provider {
                status: status.as_u16(),
                body,
            });
        }

        let byte_stream = resp.bytes_stream().map_err(LlmError::from);
        let stream = parse_ndjson_stream(byte_stream);
        Ok(stream.boxed())
    }
}

/// Turn a stream of byte chunks into a stream of [`LlmChunk`]. Buffers
/// incomplete JSON lines across reads, which is the only subtle part —
/// reqwest does not guarantee chunk boundaries align with application
/// frame boundaries.
fn parse_ndjson_stream<S>(byte_stream: S) -> BoxStream<'static, Result<LlmChunk>>
where
    S: futures::Stream<Item = Result<bytes::Bytes>> + Send + 'static,
{
    let mut buf = Vec::<u8>::new();
    let mut done = false;
    byte_stream
        .flat_map(move |chunk| {
            let mut out: Vec<Result<LlmChunk>> = Vec::new();
            let bytes = match chunk {
                Ok(b) => b,
                Err(e) => {
                    out.push(Err(e));
                    return futures::stream::iter(out);
                }
            };
            if done {
                return futures::stream::iter(out);
            }
            buf.extend_from_slice(&bytes);
            // Drain every complete line currently in the buffer.
            while let Some(nl) = buf.iter().position(|b| *b == b'\n') {
                let line: Vec<u8> = buf.drain(..=nl).collect();
                let trimmed = std::str::from_utf8(&line[..line.len() - 1])
                    .unwrap_or("")
                    .trim();
                if trimmed.is_empty() {
                    continue;
                }
                match serde_json::from_str::<GenChunk>(trimmed) {
                    Ok(c) => {
                        if !c.response.is_empty() {
                            out.push(Ok(LlmChunk::Delta(c.response)));
                        }
                        if c.done {
                            out.push(Ok(LlmChunk::Done));
                            done = true;
                            break;
                        }
                    }
                    Err(e) => {
                        out.push(Err(LlmError::Decode(format!("{e}: {trimmed}"))));
                    }
                }
            }
            futures::stream::iter(out)
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_are_sane() {
        let c = OllamaConfig::default();
        assert!(c.base_url.contains("11434"));
        assert_eq!(c.model, "llama3");
    }

    #[test]
    fn resolve_hmac_secret_decodes_rule_style_base64() {
        // Base64 of `m7Q9vX2pL8zR4nT6cH1aK5sW0dF3uJ9B` (no padding).
        let b64 = "bTdROXZYMnBMOHpSNG5UNmNIMWFLNXNXMGRGM3VKOUI";
        assert_eq!(
            resolve_hmac_secret(b64),
            "m7Q9vX2pL8zR4nT6cH1aK5sW0dF3uJ9B"
        );
        // Already-decoded vault form passes through.
        assert_eq!(
            resolve_hmac_secret("m7Q9vX2pL8zR4nT6cH1aK5sW0dF3uJ9B"),
            "m7Q9vX2pL8zR4nT6cH1aK5sW0dF3uJ9B"
        );
    }

    #[test]
    fn mint_ollama_jwt_is_verifiable_hs256() {
        let token = mint_ollama_jwt("test-secret").expect("mint");
        assert!(looks_like_jwt(&token));
        let data = jsonwebtoken::decode::<OllamaJwtClaims>(
            &token,
            &jsonwebtoken::DecodingKey::from_secret(b"test-secret"),
            &jsonwebtoken::Validation::new(Algorithm::HS256),
        )
        .expect("verify");
        assert_eq!(data.claims.app, "nebuladb");
    }

    /// Streaming-body timeout regression guard.
    ///
    /// Stand up a mock Ollama server that emits NDJSON tokens slowly:
    /// 5 frames total, with **1.5s between frames**. Configure the
    /// client with `connect_timeout = 2s` (well below the total
    /// generation time of ~7.5s). With the bug present —
    /// `reqwest::ClientBuilder::timeout(connect_timeout)` — the
    /// stream would be aborted at exactly 2s and we'd see fewer
    /// than 5 deltas. With the fix (connect_timeout only applies
    /// to the dial, no whole-body timeout), all 5 deltas arrive.
    ///
    /// This is the test that would have caught the wedge-trigger
    /// described in the showcase RAG chat report. Without it, a
    /// future refactor that re-introduces `.timeout()` on the
    /// streaming client trips here before users see truncated chats.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn streaming_response_survives_long_generation_past_connect_timeout() {
        use axum::body::Body;
        use axum::routing::post;
        use axum::Router;
        // Two `StreamExt`s collide when both are in scope (the
        // axum/tokio mock uses `tokio_stream::StreamExt::then` and
        // `chain`, the LLM stream uses `futures::StreamExt::next`).
        // Import them under disambiguated names so call sites can
        // pick explicitly.
        use futures::StreamExt as FuturesStreamExt;
        use std::time::Duration;
        use tokio::net::TcpListener;
        use tokio_stream::StreamExt as TokioStreamExt;

        // Mock Ollama: emit 5 NDJSON tokens, 1.5s apart, then `done`.
        // Total wall time ~7.5s. Client's connect_timeout is 2s — if
        // that timeout incorrectly applies to the body, we'll see the
        // stream cut around 2s and `done` never arrive.
        async fn slow_generate() -> axum::response::Response {
            let s = TokioStreamExt::then(tokio_stream::iter([0u32, 1, 2, 3, 4]), |i| async move {
                tokio::time::sleep(Duration::from_millis(1500)).await;
                let line = format!(
                    r#"{{"response":"tok{i} ","done":false}}{newline}"#,
                    newline = "\n",
                );
                Ok::<_, std::io::Error>(bytes::Bytes::from(line))
            });
            // Trailing `done` frame.
            let done = tokio_stream::once(Ok::<_, std::io::Error>(bytes::Bytes::from_static(
                b"{\"response\":\"\",\"done\":true}\n",
            )));
            let combined = TokioStreamExt::chain(s, done);
            axum::response::Response::builder()
                .header("content-type", "application/x-ndjson")
                .body(Body::from_stream(combined))
                .unwrap()
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new().route("/api/generate", post(slow_generate));
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let cfg = OllamaConfig {
            base_url: format!("http://{addr}"),
            model: "test".into(),
            api_key: None,
            // Connect-only timeout: 2 seconds. The body lasts ~7.5s.
            // With the old code (`timeout(2s)` on ClientBuilder), the
            // stream gets cut around 2s. The fix keeps the body uncapped.
            timeout: Duration::from_secs(2),
            // Read timeout 5s — much longer than the 1.5s inter-token
            // gap, so a healthy stream won't trip it. Sets the bound
            // for "is the upstream actually feeding us bytes?"
            read_timeout: Some(Duration::from_secs(5)),
        };
        let client = OllamaLlm::new(cfg).unwrap();

        let prompt = Prompt {
            user: "hello".into(),
            system: None,
        };
        let mut stream = client.generate(prompt).await.unwrap();
        let mut deltas = Vec::new();
        let mut saw_done = false;
        while let Some(item) = FuturesStreamExt::next(&mut stream).await {
            match item.unwrap() {
                LlmChunk::Delta(t) => deltas.push(t),
                LlmChunk::Done => {
                    saw_done = true;
                    break;
                }
                _ => {}
            }
        }
        assert_eq!(
            deltas.len(),
            5,
            "expected 5 tokens (one per frame), got {} — likely body timeout regression: {deltas:?}",
            deltas.len()
        );
        assert!(saw_done, "stream truncated before `done` frame arrived");
    }

    #[tokio::test]
    async fn ndjson_parser_handles_split_frames() {
        // Simulate a byte stream that breaks in the middle of a JSON
        // frame. The parser must buffer until the newline.
        let frames: Vec<Result<bytes::Bytes>> = vec![
            Ok(bytes::Bytes::from_static(b"{\"response\":\"hel")),
            Ok(bytes::Bytes::from_static(b"lo\",\"done\":false}\n")),
            Ok(bytes::Bytes::from_static(
                b"{\"response\":\" world\",\"done\":true}\n",
            )),
        ];
        let s = futures::stream::iter(frames);
        let mut out = parse_ndjson_stream(s);
        let mut tokens = Vec::new();
        let mut saw_done = false;
        while let Some(item) = out.next().await {
            match item.unwrap() {
                LlmChunk::Delta(t) => tokens.push(t),
                LlmChunk::Done => {
                    saw_done = true;
                    break;
                }
                _ => {}
            }
        }
        assert_eq!(tokens, vec!["hello".to_string(), " world".to_string()]);
        assert!(saw_done);
    }
}
