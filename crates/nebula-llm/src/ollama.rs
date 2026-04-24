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

use std::time::Duration;

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use futures::TryStreamExt;
use serde::Deserialize;

use crate::{LlmChunk, LlmClient, LlmError, Prompt, Result};

#[derive(Debug, Clone)]
pub struct OllamaConfig {
    /// e.g. `http://localhost:11434`.
    pub base_url: String,
    pub model: String,
    pub timeout: Duration,
}

impl Default for OllamaConfig {
    fn default() -> Self {
        Self {
            base_url: "http://localhost:11434".into(),
            model: "llama3".into(),
            timeout: Duration::from_secs(120),
        }
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
        let http = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()?;
        let model_label = format!("ollama/{}", config.model);
        Ok(Self {
            http,
            config,
            model_label,
        })
    }
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

    async fn generate(
        &self,
        prompt: Prompt,
    ) -> Result<BoxStream<'static, Result<LlmChunk>>> {
        if prompt.user.trim().is_empty() {
            return Err(LlmError::Empty);
        }
        let url = format!("{}/api/generate", self.config.base_url.trim_end_matches('/'));
        let body = GenRequest {
            model: &self.config.model,
            prompt: prompt.user,
            system: prompt.system.as_deref(),
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

        // Stream the body byte-by-byte, buffer until newline, parse.
        // We use `TryStreamExt` to turn reqwest's error-prone byte
        // stream into a `Stream<Item=Result<Bytes>>`.
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
                let trimmed = std::str::from_utf8(&line[..line.len() - 1]).unwrap_or("").trim();
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

    #[tokio::test]
    async fn ndjson_parser_handles_split_frames() {
        // Simulate a byte stream that breaks in the middle of a JSON
        // frame. The parser must buffer until the newline.
        let frames: Vec<Result<bytes::Bytes>> = vec![
            Ok(bytes::Bytes::from_static(b"{\"response\":\"hel")),
            Ok(bytes::Bytes::from_static(b"lo\",\"done\":false}\n")),
            Ok(bytes::Bytes::from_static(b"{\"response\":\" world\",\"done\":true}\n")),
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
            }
        }
        assert_eq!(tokens, vec!["hello".to_string(), " world".to_string()]);
        assert!(saw_done);
    }
}
