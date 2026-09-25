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

use crate::{
    GenerateOptions, LlmChunk, LlmClient, LlmError, ModelCapabilities, ModelInfo, Prompt, Result,
    TokenUsage, ToolSpec,
};

#[derive(Debug, Clone)]
pub struct OpenAiChatConfig {
    pub base_url: String,
    pub api_key: Option<String>,
    pub model: String,
    /// **Connect** timeout for the initial TCP/TLS handshake. Does
    /// NOT cap the streaming body — see `read_timeout`. Same fix as
    /// in `OllamaConfig`; rationale documented there.
    pub timeout: Duration,
    /// Idle-byte timeout during streaming. `None` disables.
    pub read_timeout: Option<Duration>,
}

impl OpenAiChatConfig {
    pub fn openai(api_key: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            base_url: "https://api.openai.com/v1".into(),
            api_key: Some(api_key.into()),
            model: model.into(),
            // 10s for connect to api.openai.com is plenty.
            timeout: Duration::from_secs(10),
            // 60s without a token from OpenAI ⇒ something's wrong;
            // surface it instead of pretending the request is fine.
            read_timeout: Some(Duration::from_secs(60)),
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
        // Critical: do NOT set `.timeout(config.timeout)` — see the
        // matching comment in `nebula-llm::ollama::OllamaLlm::new`.
        // The `reqwest::ClientBuilder::timeout` applies to the whole
        // response body, which truncates streaming chat completions.
        let mut builder = reqwest::Client::builder()
            .default_headers(headers)
            .connect_timeout(config.timeout);
        if let Some(rt) = config.read_timeout {
            builder = builder.read_timeout(rt);
        }
        let http = builder.build()?;
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
    messages: Vec<serde_json::Value>,
    stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_format: Option<serde_json::Value>,
}

fn tools_to_openai(tools: &[ToolSpec]) -> Vec<serde_json::Value> {
    tools
        .iter()
        .map(|t| {
            serde_json::json!({
                "type": "function",
                "function": {
                    "name": t.name,
                    "description": t.description,
                    "parameters": t.input_schema,
                }
            })
        })
        .collect()
}

#[derive(Deserialize)]
struct ChatFrame {
    #[serde(default)]
    choices: Vec<ChatChoice>,
    #[serde(default)]
    usage: Option<OpenAiUsage>,
}

#[derive(Deserialize, Default)]
struct OpenAiUsage {
    #[serde(default)]
    prompt_tokens: Option<u32>,
    #[serde(default)]
    completion_tokens: Option<u32>,
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
    #[serde(default)]
    reasoning_content: Option<String>,
    #[serde(default)]
    tool_calls: Vec<OpenAiToolCallDelta>,
}

#[derive(Deserialize, Default)]
struct OpenAiToolCallDelta {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    index: Option<usize>,
    #[serde(default)]
    function: Option<OpenAiFnDelta>,
}

#[derive(Deserialize, Default)]
struct OpenAiFnDelta {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
}

#[async_trait]
impl LlmClient for OpenAiChatLlm {
    fn model(&self) -> &str {
        &self.model_label
    }

    fn info(&self) -> ModelInfo {
        let vision = self.config.model.contains("gpt-4o")
            || self.config.model.contains("vision")
            || self.config.model.contains("gemini");
        ModelInfo {
            id: self.config.model.clone(),
            provider: "openai".into(),
            display_name: self.config.model.clone(),
            capabilities: if vision {
                ModelCapabilities::OPENAI_VISION
            } else {
                ModelCapabilities::OPENAI_CHAT
            },
            context_window: Some(128_000),
            is_mock: false,
        }
    }

    async fn generate_with_options(
        &self,
        prompt: Prompt,
        opts: GenerateOptions,
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
            messages.push(serde_json::json!({"role": "system", "content": sys}));
        }
        messages.push(serde_json::json!({"role": "user", "content": prompt.user}));

        let response_format = match &opts.response_format {
            Some(crate::ResponseFormat::JsonObject) => {
                Some(serde_json::json!({"type": "json_object"}))
            }
            Some(crate::ResponseFormat::JsonSchema { schema }) => Some(serde_json::json!({
                "type": "json_schema",
                "json_schema": {"name": "nebula_structured", "schema": schema}
            })),
            None => None,
        };

        let body = ChatRequest {
            model: &self.config.model,
            messages,
            stream: true,
            temperature: opts.temperature,
            max_tokens: opts.max_tokens,
            tools: if opts.tools.is_empty() {
                None
            } else {
                Some(tools_to_openai(&opts.tools))
            },
            response_format,
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
                        if let Some(u) = frame.usage {
                            out.push(Ok(LlmChunk::Usage(TokenUsage {
                                prompt_tokens: u.prompt_tokens,
                                completion_tokens: u.completion_tokens,
                            })));
                        }
                        if let Some(choice) = frame.choices.into_iter().next() {
                            if let Some(content) = choice.delta.content {
                                if !content.is_empty() {
                                    out.push(Ok(LlmChunk::Delta(content)));
                                }
                            }
                            if let Some(reasoning) = choice.delta.reasoning_content {
                                if !reasoning.is_empty() {
                                    out.push(Ok(LlmChunk::Reasoning(reasoning)));
                                }
                            }
                            for tc in choice.delta.tool_calls {
                                let id = tc.id.unwrap_or_else(|| {
                                    format!("call_{}", tc.index.unwrap_or(0))
                                });
                                let name = tc
                                    .function
                                    .as_ref()
                                    .and_then(|f| f.name.clone())
                                    .unwrap_or_default();
                                let arguments = tc
                                    .function
                                    .as_ref()
                                    .and_then(|f| f.arguments.clone())
                                    .unwrap_or_default();
                                if !name.is_empty() || !arguments.is_empty() {
                                    out.push(Ok(LlmChunk::ToolCall {
                                        id,
                                        name,
                                        arguments,
                                    }));
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
                _ => {}
            }
        }
        assert_eq!(tokens, vec!["Hel".to_string(), "lo".to_string()]);
        assert!(saw_done);
    }
}
