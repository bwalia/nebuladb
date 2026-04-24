//! OpenAI-compatible `/v1/chat/completions` streaming client.
//!
//! OpenAI (and every compat server — vLLM, LiteLLM, Together, Azure,
//! Ollama's `/v1` shim) emits SSE frames:
//!
//! ```text
//! data: {"choices":[{"delta":{"content":"Hel"}}]}
//!
//! data: {"choices":[{"delta":{"content":"lo"}}]}
//!
//! data: [DONE]
//! ```
//!
//! We parse SSE frames out of the byte stream, extract
//! `choices[0].delta.content`, and forward as deltas.

use std::time::Duration;

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use futures::TryStreamExt;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::Deserialize;

use crate::{LlmChunk, LlmClient, LlmError, Prompt, Result};

#[derive(Debug, Clone)]
pub struct OpenAiChatConfig {
    pub base_url: String,
    pub api_key: Option<String>,
    pub model: String,
    pub timeout: Duration,
}

impl OpenAiChatConfig {
    pub fn openai(api_key: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            base_url: "https://api.openai.com/v1".into(),
            api_key: Some(api_key.into()),
            model: model.into(),
            timeout: Duration::from_secs(120),
        }
    }
}

#[derive(Debug)]
pub struct OpenAiChatLlm {
    http: reqwest::Client,
    config: OpenAiChatConfig,
    model_label: String,
}

impl OpenAiChatLlm {
    pub fn new(config: OpenAiChatConfig) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if let Some(key) = &config.api_key {
            let v = HeaderValue::from_str(&format!("Bearer {key}"))
                .map_err(|e| LlmError::Decode(format!("invalid api key: {e}")))?;
            headers.insert(AUTHORIZATION, v);
        }
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(config.timeout)
            .build()?;
        let model_label = format!("openai/{}", config.model);
        Ok(Self {
            http,
            config,
            model_label,
        })
    }
}

#[derive(serde::Serialize)]
struct ChatRequest<'a> {
    model: &'a str,
    messages: Vec<ChatMsg<'a>>,
    stream: bool,
}

#[derive(serde::Serialize)]
struct ChatMsg<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(Deserialize)]
struct ChatFrame {
    choices: Vec<ChatChoice>,
}

#[derive(Deserialize)]
struct ChatChoice {
    #[serde(default)]
    delta: ChatDelta,
}

#[derive(Deserialize, Default)]
struct ChatDelta {
    #[serde(default)]
    content: Option<String>,
}

#[async_trait]
impl LlmClient for OpenAiChatLlm {
    fn model(&self) -> &str {
        &self.model_label
    }

    async fn generate(
        &self,
        prompt: Prompt,
    ) -> Result<BoxStream<'static, Result<LlmChunk>>> {
        if prompt.user.trim().is_empty() {
            return Err(LlmError::Empty);
        }
        let url = format!(
            "{}/chat/completions",
            self.config.base_url.trim_end_matches('/')
        );

        let mut messages = Vec::with_capacity(2);
        if let Some(sys) = prompt.system.as_deref() {
            messages.push(ChatMsg {
                role: "system",
                content: sys,
            });
        }
        messages.push(ChatMsg {
            role: "user",
            content: &prompt.user,
        });

        let body = ChatRequest {
            model: &self.config.model,
            messages,
            stream: true,
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
        Ok(parse_sse_stream(byte_stream).boxed())
    }
}

/// Minimal SSE parser: we only care about `data:` lines. Event-type
/// lines are ignored (OpenAI doesn't use them). A frame ends at a
/// blank line, but since each data line is already complete JSON we
/// parse per-line and skip the blank-line dispatch — simpler, same
/// result.
fn parse_sse_stream<S>(byte_stream: S) -> BoxStream<'static, Result<LlmChunk>>
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
                if payload == "[DONE]" {
                    out.push(Ok(LlmChunk::Done));
                    done = true;
                    break;
                }
                if payload.is_empty() {
                    continue;
                }
                match serde_json::from_str::<ChatFrame>(payload) {
                    Ok(frame) => {
                        if let Some(choice) = frame.choices.into_iter().next() {
                            if let Some(content) = choice.delta.content {
                                if !content.is_empty() {
                                    out.push(Ok(LlmChunk::Delta(content)));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        out.push(Err(LlmError::Decode(format!("{e}: {payload}"))));
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

    #[tokio::test]
    async fn sse_parser_yields_content_deltas() {
        let frames: Vec<Result<bytes::Bytes>> = vec![
            Ok(bytes::Bytes::from_static(
                b"data: {\"choices\":[{\"delta\":{\"content\":\"Hel\"}}]}\n\n",
            )),
            Ok(bytes::Bytes::from_static(
                b"data: {\"choices\":[{\"delta\":{\"content\":\"lo\"}}]}\n\n",
            )),
            Ok(bytes::Bytes::from_static(b"data: [DONE]\n\n")),
        ];
        let s = futures::stream::iter(frames);
        let mut out = parse_sse_stream(s);
        let mut tokens = Vec::new();
        let mut saw_done = false;
        while let Some(item) = out.next().await {
            match item.unwrap() {
                LlmChunk::Delta(t) => tokens.push(t),
                LlmChunk::Done => {
                    saw_done = true;
                    break;
                }
            }
        }
        assert_eq!(tokens, vec!["Hel".to_string(), "lo".to_string()]);
        assert!(saw_done);
    }
}
