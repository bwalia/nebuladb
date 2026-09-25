//! Google Gemini via the OpenAI-compatible endpoint
//! (`…/v1beta/openai/chat/completions`) or any Gemini OpenAI-compat proxy.
//!
//! We reuse the OpenAI SSE parser; capabilities are advertised as Gemini.

use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;

use crate::openai_chat::{OpenAiChatConfig, OpenAiChatLlm};
use crate::{
    GenerateOptions, LlmChunk, LlmClient, LlmError, ModelCapabilities, ModelInfo, Prompt, Result,
};

#[derive(Debug, Clone)]
pub struct GeminiConfig {
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    pub timeout: Duration,
    pub read_timeout: Option<Duration>,
}

impl GeminiConfig {
    pub fn new(api_key: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            // Google AI Studio OpenAI-compat surface.
            base_url: "https://generativelanguage.googleapis.com/v1beta/openai".into(),
            api_key: api_key.into(),
            model: model.into(),
            timeout: Duration::from_secs(10),
            read_timeout: Some(Duration::from_secs(120)),
        }
    }
}

#[derive(Debug)]
pub struct GeminiLlm {
    inner: OpenAiChatLlm,
    model: String,
    model_label: String,
}

impl GeminiLlm {
    pub fn new(config: GeminiConfig) -> Result<Self> {
        let inner = OpenAiChatLlm::new(OpenAiChatConfig {
            base_url: config.base_url,
            api_key: Some(config.api_key),
            model: config.model.clone(),
            timeout: config.timeout,
            read_timeout: config.read_timeout,
        })?;
        Ok(Self {
            inner,
            model_label: format!("gemini/{}", config.model),
            model: config.model,
        })
    }
}

#[async_trait]
impl LlmClient for GeminiLlm {
    fn model(&self) -> &str {
        &self.model_label
    }

    fn info(&self) -> ModelInfo {
        ModelInfo {
            id: self.model.clone(),
            provider: "gemini".into(),
            display_name: self.model.clone(),
            capabilities: ModelCapabilities::GEMINI,
            context_window: Some(1_000_000),
            is_mock: false,
        }
    }

    async fn generate(&self, prompt: Prompt) -> Result<BoxStream<'static, Result<LlmChunk>>> {
        self.inner.generate(prompt).await
    }

    async fn generate_with_options(
        &self,
        prompt: Prompt,
        opts: GenerateOptions,
    ) -> Result<BoxStream<'static, Result<LlmChunk>>> {
        self.inner.generate_with_options(prompt, opts).await
    }
}
