//! Anthropic Claude streaming client (`POST /v1/messages`, SSE).
//!
//! There is no official Anthropic Rust SDK, so this speaks the wire
//! protocol directly. Anthropic's stream framing differs from the
//! OpenAI-compat shape the sibling module parses: each SSE frame is a
//! typed event —
//!
//! ```text
//! event: content_block_delta
//! data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"Hel"}}
//!
//! event: message_stop
//! data: {"type":"message_stop"}
//! ```
//!
//! We only need `data:` lines (the JSON carries its own `type`), text
//! deltas become [`LlmChunk::Delta`], `message_stop` becomes
//! [`LlmChunk::Done`].
//!
//! Claude Fable 5 specifics baked in here:
//! - **No `thinking` parameter.** Thinking is always on for Fable 5;
//!   sending any explicit config is a 400. Thinking deltas stream as
//!   `thinking_delta` events, which we ignore — only answer text is
//!   forwarded to the RAG consumer.
//! - **No sampling params** (`temperature`/`top_p`/`top_k` are 400s on
//!   Fable 5 / Opus 4.8) — steering happens in the prompt.
//! - **Refusal fallbacks on by default.** Fable 5's safety classifiers
//!   can decline a request (HTTP 200, `stop_reason: "refusal"`); with
//!   the `server-side-fallback` beta + `fallbacks` parameter the API
//!   transparently re-serves the request on the fallback model inside
//!   the same call. A refusal on the *final* response (whole chain
//!   declined) surfaces as an error chunk so the RAG handler reports
//!   it instead of returning an empty answer.

use std::time::Duration;

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use futures::TryStreamExt;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde::Deserialize;

use crate::{LlmChunk, LlmClient, LlmError, Prompt, Result};

#[derive(Debug, Clone)]
pub struct ClaudeConfig {
    /// API origin, up to but not including `/v1`. Default
    /// `https://api.anthropic.com`.
    pub base_url: String,
    pub api_key: String,
    /// Model ID, e.g. `claude-fable-5` or `claude-opus-4-8`.
    pub model: String,
    /// Hard cap on generated tokens per answer. RAG answers are short;
    /// 4096 is generous without letting a runaway generation bill
    /// unbounded output.
    pub max_tokens: u32,
    /// Fallback model for safety-classifier declines (design note
    /// above). `None` disables the fallback parameter and beta header.
    pub fallback_model: Option<String>,
    /// Connect timeout for the TCP/TLS handshake. NOT a body timeout —
    /// same streaming discipline as the sibling clients.
    pub timeout: Duration,
    /// Idle-byte timeout during streaming. `None` disables.
    pub read_timeout: Option<Duration>,
}

impl ClaudeConfig {
    pub fn new(api_key: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            base_url: "https://api.anthropic.com".into(),
            api_key: api_key.into(),
            model: model.into(),
            max_tokens: 4096,
            fallback_model: Some("claude-opus-4-8".into()),
            timeout: Duration::from_secs(10),
            read_timeout: Some(Duration::from_secs(60)),
        }
    }
}

#[derive(Debug)]
pub struct ClaudeLlm {
    http: reqwest::Client,
    config: ClaudeConfig,
    model_label: String,
}

impl ClaudeLlm {
    pub fn new(config: ClaudeConfig) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let key = HeaderValue::from_str(&config.api_key)
            .map_err(|e| LlmError::Decode(format!("invalid api key: {e}")))?;
        headers.insert("x-api-key", key);
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        if config.fallback_model.is_some() {
            headers.insert(
                "anthropic-beta",
                HeaderValue::from_static("server-side-fallback-2026-06-01"),
            );
        }
        // No whole-response `.timeout(...)` — it would abort a long
        // generation mid-stream. Connect + idle-byte only, matching
        // the Ollama/OpenAI clients.
        let mut builder = reqwest::Client::builder()
            .default_headers(headers)
            .connect_timeout(config.timeout);
        if let Some(rt) = config.read_timeout {
            builder = builder.read_timeout(rt);
        }
        let http = builder.build()?;
        let model_label = format!("anthropic/{}", config.model);
        Ok(Self {
            http,
            config,
            model_label,
        })
    }
}

#[derive(serde::Serialize)]
struct MessagesRequest<'a> {
    model: &'a str,
    max_tokens: u32,
    stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<&'a str>,
    messages: Vec<ClaudeMsg<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fallbacks: Option<Vec<FallbackEntry<'a>>>,
}

#[derive(serde::Serialize)]
struct ClaudeMsg<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(serde::Serialize)]
struct FallbackEntry<'a> {
    model: &'a str,
}

/// One `data:` payload from the Anthropic event stream. Every event
/// carries its own `type`; unknown fields/events are ignored so new
/// server-side event types can't break the parser.
#[derive(Deserialize)]
struct StreamEvent {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    delta: Option<EventDelta>,
}

#[derive(Deserialize, Default)]
struct EventDelta {
    #[serde(rename = "type", default)]
    kind: Option<String>,
    #[serde(default)]
    text: Option<String>,
    /// Present on `message_delta` events; carries the terminal
    /// stop_reason ("end_turn", "max_tokens", "refusal", ...).
    #[serde(default)]
    stop_reason: Option<String>,
}

#[async_trait]
impl LlmClient for ClaudeLlm {
    fn model(&self) -> &str {
        &self.model_label
    }

    async fn generate(&self, prompt: Prompt) -> Result<BoxStream<'static, Result<LlmChunk>>> {
        if prompt.user.trim().is_empty() {
            return Err(LlmError::Empty);
        }
        let url = format!("{}/v1/messages", self.config.base_url.trim_end_matches('/'));
        let fallbacks = self
            .config
            .fallback_model
            .as_deref()
            .map(|m| vec![FallbackEntry { model: m }]);
        let body = MessagesRequest {
            model: &self.config.model,
            max_tokens: self.config.max_tokens,
            stream: true,
            system: prompt.system.as_deref(),
            messages: vec![ClaudeMsg {
                role: "user",
                content: &prompt.user,
            }],
            fallbacks,
        };
        let resp = self.http.post(&url).json(&body).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(LlmError::Provider {
                status: status.as_u16(),
                body,
            });
        }
        let byte_stream = resp.bytes_stream().map_err(LlmError::from);
        Ok(parse_anthropic_sse(byte_stream).boxed())
    }
}

/// Parse Anthropic's typed SSE events into the crate's chunk stream.
/// Only `data:` lines matter — the JSON's own `type` field identifies
/// the event, so `event:` lines are skipped just like in the OpenAI
/// parser.
fn parse_anthropic_sse<S>(byte_stream: S) -> BoxStream<'static, Result<LlmChunk>>
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
            while let Some(nl) = buf.iter().position(|b| *b == b'\n') {
                let line: Vec<u8> = buf.drain(..=nl).collect();
                let text = std::str::from_utf8(&line[..line.len() - 1])
                    .unwrap_or("")
                    .trim_end_matches('\r')
                    .trim();
                let Some(payload) = text.strip_prefix("data:") else {
                    continue;
                };
                let payload = payload.trim();
                if payload.is_empty() {
                    continue;
                }
                let event: StreamEvent = match serde_json::from_str(payload) {
                    Ok(ev) => ev,
                    Err(e) => {
                        out.push(Err(LlmError::Decode(format!("{e}: {payload}"))));
                        continue;
                    }
                };
                match event.kind.as_str() {
                    "content_block_delta" => {
                        if let Some(delta) = event.delta {
                            // Forward answer text only. `thinking_delta`
                            // (empty under the default display) and
                            // other delta kinds are internal.
                            if delta.kind.as_deref() == Some("text_delta") {
                                if let Some(t) = delta.text {
                                    if !t.is_empty() {
                                        out.push(Ok(LlmChunk::Delta(t)));
                                    }
                                }
                            }
                        }
                    }
                    "message_delta" => {
                        // Terminal metadata. A refusal here means the
                        // whole chain (requested model + fallbacks)
                        // declined — surface it, don't emit an empty
                        // answer.
                        if let Some(delta) = event.delta {
                            if delta.stop_reason.as_deref() == Some("refusal") {
                                out.push(Err(LlmError::Provider {
                                    status: 200,
                                    body: "model declined the request (stop_reason: refusal)"
                                        .into(),
                                }));
                                done = true;
                                break;
                            }
                        }
                    }
                    "message_stop" => {
                        out.push(Ok(LlmChunk::Done));
                        done = true;
                        break;
                    }
                    // message_start, content_block_start/stop, ping,
                    // fallback markers, error frames we don't model:
                    // ignore.
                    _ => {}
                }
            }
            futures::stream::iter(out)
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(frames: Vec<&'static [u8]>) -> (Vec<String>, bool, Vec<String>) {
        let frames: Vec<Result<bytes::Bytes>> = frames
            .into_iter()
            .map(|f| Ok(bytes::Bytes::from_static(f)))
            .collect();
        let s = futures::stream::iter(frames);
        let mut out = parse_anthropic_sse(s);
        let (mut tokens, mut errors, mut saw_done) = (Vec::new(), Vec::new(), false);
        futures::executor::block_on(async {
            while let Some(item) = out.next().await {
                match item {
                    Ok(LlmChunk::Delta(t)) => tokens.push(t),
                    Ok(LlmChunk::Done) => {
                        saw_done = true;
                        break;
                    }
                    Err(e) => errors.push(e.to_string()),
                }
            }
        });
        (tokens, saw_done, errors)
    }

    #[tokio::test]
    async fn parses_text_deltas_and_stop() {
        let (tokens, saw_done, errors) = collect(vec![
            b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\"}}\n\n",
            b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hel\"}}\n\n",
            b"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"lo\"}}\n\n",
            b"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n",
            b"data: {\"type\":\"message_stop\"}\n\n",
        ]);
        assert_eq!(tokens, vec!["Hel".to_string(), "lo".to_string()]);
        assert!(saw_done);
        assert!(errors.is_empty());
    }

    #[tokio::test]
    async fn thinking_deltas_are_not_forwarded() {
        let (tokens, saw_done, errors) = collect(vec![
            b"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"...\"}}\n\n",
            b"data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"Answer\"}}\n\n",
            b"data: {\"type\":\"message_stop\"}\n\n",
        ]);
        assert_eq!(tokens, vec!["Answer".to_string()]);
        assert!(saw_done);
        assert!(errors.is_empty());
    }

    #[tokio::test]
    async fn refusal_surfaces_as_error() {
        let (tokens, saw_done, errors) = collect(vec![
            b"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"refusal\"}}\n\n",
        ]);
        assert!(tokens.is_empty());
        assert!(!saw_done);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("refusal"), "got {errors:?}");
    }

    #[tokio::test]
    async fn frames_split_across_chunks_reassemble() {
        let (tokens, saw_done, _) = collect(vec![
            b"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_del",
            b"ta\",\"text\":\"Hi\"}}\n\ndata: {\"type\":\"message_stop\"}\n\n",
        ]);
        assert_eq!(tokens, vec!["Hi".to_string()]);
        assert!(saw_done);
    }
}
