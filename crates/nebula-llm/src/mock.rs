//! Deterministic offline LLM. Useful for tests and for dev runs where
//! you don't want to burn tokens. Advertised as `is_mock: true` so the
//! showcase never presents it as a frontier model.

use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};

use crate::{
    GenerateOptions, LlmChunk, LlmClient, LlmError, ModelCapabilities, ModelInfo, Prompt, Result,
};

#[derive(Debug, Clone)]
pub struct MockLlm {
    model: String,
}

impl Default for MockLlm {
    fn default() -> Self {
        Self {
            model: "mock-llm".into(),
        }
    }
}

#[async_trait]
impl LlmClient for MockLlm {
    fn model(&self) -> &str {
        &self.model
    }

    fn info(&self) -> ModelInfo {
        ModelInfo {
            id: self.model.clone(),
            provider: "mock".into(),
            display_name: "Mock LLM (dev only)".into(),
            capabilities: ModelCapabilities::MOCK,
            context_window: Some(8_192),
            is_mock: true,
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

        // Deterministic tool-call demo: if tools include semantic_search
        // and the user asks to search, emit one tool call then stop.
        if !opts.tools.is_empty()
            && prompt.user.to_lowercase().contains("search")
            && opts.tools.iter().any(|t| t.name == "semantic_search")
        {
            let args = serde_json::json!({
                "query": prompt.user,
                "top_k": 5
            })
            .to_string();
            let s = stream::iter(vec![
                Ok(LlmChunk::ToolCall {
                    id: "mock_call_1".into(),
                    name: "semantic_search".into(),
                    arguments: args,
                }),
                Ok(LlmChunk::Done),
            ]);
            return Ok(s.boxed());
        }

        if matches!(
            opts.response_format,
            Some(crate::ResponseFormat::JsonObject | crate::ResponseFormat::JsonSchema { .. })
        ) {
            let json = serde_json::json!({
                "answer": prompt.user,
                "mock": true
            })
            .to_string();
            let s = stream::iter(vec![Ok(LlmChunk::Delta(json)), Ok(LlmChunk::Done)]);
            return Ok(s.boxed());
        }

        let reply = format!("Answer: {}", prompt.user);
        let tokens: Vec<String> = reply.split_inclusive(' ').map(|s| s.to_string()).collect();
        let s = stream::iter(tokens)
            .map(|t| Ok(LlmChunk::Delta(t)))
            .chain(stream::iter(std::iter::once(Ok(LlmChunk::Done))));
        Ok(s.boxed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn emits_deltas_then_done() {
        let llm = MockLlm::default();
        let mut s = llm.generate(Prompt::user("hello world")).await.unwrap();
        let mut tokens = Vec::new();
        let mut saw_done = false;
        while let Some(item) = s.next().await {
            match item.unwrap() {
                LlmChunk::Delta(t) => tokens.push(t),
                LlmChunk::Done => {
                    saw_done = true;
                    break;
                }
                _ => {}
            }
        }
        assert!(saw_done);
        assert!(!tokens.is_empty());
        assert!(tokens.concat().contains("hello world"));
    }

    #[tokio::test]
    async fn empty_prompt_errors() {
        let llm = MockLlm::default();
        match llm.generate(Prompt::user("   ")).await {
            Err(LlmError::Empty) => {}
            Err(e) => panic!("unexpected error: {e}"),
            Ok(_) => panic!("expected error for empty prompt"),
        }
    }

    #[tokio::test]
    async fn capabilities_are_honest() {
        let info = MockLlm::default().info();
        assert!(info.is_mock);
        assert!(info.capabilities.chat);
        assert!(info.capabilities.streaming);
    }
}
