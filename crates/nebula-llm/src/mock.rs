//! Deterministic offline LLM. Useful for tests and for dev runs where
//! you don't want to burn tokens.

use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};

use crate::{LlmChunk, LlmClient, LlmError, Prompt, Result};

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

    async fn generate(
        &self,
        prompt: Prompt,
    ) -> Result<BoxStream<'static, Result<LlmChunk>>> {
        if prompt.user.trim().is_empty() {
            return Err(LlmError::Empty);
        }
        // Produce a deterministic echo. Tests can assert on the exact
        // output without a live backend. Streaming a word at a time
        // exercises the multi-event SSE path.
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
}
