//! Anthropic Messages API streaming client (`/v1/messages`).
//!
//! Capability flags: chat, streaming, vision, tool_calling, reasoning.
//! Structured JSON schema mode is not advertised (Anthropic has no
//! first-class json_schema response_format equivalent here).

use std::time::Duration;

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use futures::TryStreamExt;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde::Deserialize;
use serde_json::json;

use crate::{
    GenerateOptions, LlmChunk, LlmClient, LlmError, ModelCapabilities, ModelInfo, Prompt, Result,
    TokenUsage, ToolSpec,
};

#[derive(Debug, Clone)]
pub struct AnthropicConfig {
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    pub timeout: Duration,
    pub read_timeout: Option<Duration>,
}

impl AnthropicConfig {
    pub fn new(api_key: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            base_url: "https://api.anthropic.com".into(),
            api_key: api_key.into(),
            model: model.into(),
            timeout: Duration::from_secs(10),
            read_timeout: Some(Duration::from_secs(120)),
        }
    }
}

#[derive(Debug)]
pub struct AnthropicLlm {
    http: reqwest::Client,
    config: AnthropicConfig,
    model_label: String,
}

impl AnthropicLlm {
    pub fn new(config: AnthropicConfig) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(
            "x-api-key",
            HeaderValue::from_str(&config.api_key)
                .map_err(|e| LlmError::Decode(format!("invalid api key: {e}")))?,
        );
        headers.insert(
            "anthropic-version",
            HeaderValue::from_static("2023-06-01"),
        );
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

#[derive(Deserialize)]
struct AnthropicSse {
    #[serde(rename = "type")]
    event_type: String,
    #[serde(default)]
    delta: Option<AnthropicDelta>,
    #[serde(default)]
    usage: Option<AnthropicUsage>,
    #[serde(default)]
    message: Option<AnthropicMessageMeta>,
    #[serde(default)]
    content_block: Option<AnthropicContentBlock>,
}

#[derive(Deserialize, Default)]
struct AnthropicDelta {
    #[serde(rename = "type")]
    #[serde(default)]
    delta_type: Option<String>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    thinking: Option<String>,
    #[serde(default)]
    partial_json: Option<String>,
}

#[derive(Deserialize, Default)]
struct AnthropicUsage {
    #[serde(default)]
    input_tokens: Option<u32>,
    #[serde(default)]
    output_tokens: Option<u32>,
}

#[derive(Deserialize, Default)]
struct AnthropicMessageMeta {
    #[serde(default)]
    usage: Option<AnthropicUsage>,
}

#[derive(Deserialize, Default)]
struct AnthropicContentBlock {
    #[serde(rename = "type")]
    #[serde(default)]
    block_type: Option<String>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    name: Option<String>,
}

fn tools_to_anthropic(tools: &[ToolSpec]) -> Vec<serde_json::Value> {
    tools
        .iter()
        .map(|t| {
            json!({
                "name": t.name,
                "description": t.description,
                "input_schema": t.input_schema,
            })
        })
        .collect()
}

#[async_trait]
impl LlmClient for AnthropicLlm {
    fn model(&self) -> &str {
        &self.model_label
    }

    fn info(&self) -> ModelInfo {
        ModelInfo {
            id: self.config.model.clone(),
            provider: "anthropic".into(),
            display_name: self.config.model.clone(),
            capabilities: ModelCapabilities::ANTHROPIC,
            context_window: Some(200_000),
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
            "{}/v1/messages",
            self.config.base_url.trim_end_matches('/')
        );

        let mut body = json!({
            "model": self.config.model,
            "max_tokens": opts.max_tokens.unwrap_or(4096),
            "stream": true,
            "messages": [{"role": "user", "content": prompt.user}],
        });
        if let Some(sys) = prompt.system {
            body["system"] = json!(sys);
        }
        if let Some(t) = opts.temperature {
            body["temperature"] = json!(t);
        }
        if !opts.tools.is_empty() {
            body["tools"] = json!(tools_to_anthropic(&opts.tools));
        }

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

fn parse_anthropic_sse<S>(byte_stream: S) -> BoxStream<'static, Result<LlmChunk>>
where
    S: futures::Stream<Item = Result<bytes::Bytes>> + Send + 'static,
{
    let mut buf = Vec::<u8>::new();
    let mut done = false;
    let mut pending_tool: Option<(String, String, String)> = None;
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
                if payload.is_empty() || payload == "[DONE]" {
                    continue;
                }
                let frame: AnthropicSse = match serde_json::from_str(payload) {
                    Ok(f) => f,
                    Err(_) => continue,
                };
                match frame.event_type.as_str() {
                    "content_block_start" => {
                        if let Some(block) = frame.content_block {
                            if block.block_type.as_deref() == Some("tool_use") {
                                pending_tool = Some((
                                    block.id.unwrap_or_default(),
                                    block.name.unwrap_or_default(),
                                    String::new(),
                                ));
                            }
                        }
                    }
                    "content_block_delta" => {
                        if let Some(delta) = frame.delta {
                            if let Some(text) = delta.text {
                                if !text.is_empty() {
                                    out.push(Ok(LlmChunk::Delta(text)));
                                }
                            }
                            if let Some(thinking) = delta.thinking {
                                if !thinking.is_empty() {
                                    out.push(Ok(LlmChunk::Reasoning(thinking)));
                                }
                            }
                            if let Some(partial) = delta.partial_json {
                                if let Some((_, _, ref mut args)) = pending_tool {
                                    args.push_str(&partial);
                                }
                            }
                        }
                    }
                    "content_block_stop" => {
                        if let Some((id, name, arguments)) = pending_tool.take() {
                            out.push(Ok(LlmChunk::ToolCall {
                                id,
                                name,
                                arguments,
                            }));
                        }
                    }
                    "message_delta" | "message_start" => {
                        let usage = frame
                            .usage
                            .or_else(|| frame.message.and_then(|m| m.usage));
                        if let Some(u) = usage {
                            out.push(Ok(LlmChunk::Usage(TokenUsage {
                                prompt_tokens: u.input_tokens,
                                completion_tokens: u.output_tokens,
                            })));
                        }
                    }
                    "message_stop" => {
                        out.push(Ok(LlmChunk::Done));
                        done = true;
                    }
                    _ => {}
                }
            }
            futures::stream::iter(out)
        })
        .boxed()
}
