//! Model capability discovery and request options for frontier providers.
//!
//! Adapters must report only what they actually support. The showcase
//! UI and gateway refuse to expose unsupported operations.

use serde::{Deserialize, Serialize};

/// Machine-readable capability flags for a configured model.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCapabilities {
    pub chat: bool,
    pub streaming: bool,
    pub vision: bool,
    pub tool_calling: bool,
    pub structured_output: bool,
    /// Chat providers never embed; embeddings use a separate stack.
    pub embeddings: bool,
    pub reasoning: bool,
}

impl ModelCapabilities {
    pub const OPENAI_CHAT: Self = Self {
        chat: true,
        streaming: true,
        vision: false,
        tool_calling: true,
        structured_output: true,
        embeddings: false,
        reasoning: false,
    };

    pub const OPENAI_VISION: Self = Self {
        chat: true,
        streaming: true,
        vision: true,
        tool_calling: true,
        structured_output: true,
        embeddings: false,
        reasoning: false,
    };

    pub const ANTHROPIC: Self = Self {
        chat: true,
        streaming: true,
        vision: true,
        tool_calling: true,
        structured_output: false,
        embeddings: false,
        reasoning: true,
    };

    pub const GEMINI: Self = Self {
        chat: true,
        streaming: true,
        vision: true,
        tool_calling: true,
        structured_output: true,
        embeddings: false,
        reasoning: false,
    };

    pub const OLLAMA: Self = Self {
        chat: true,
        streaming: true,
        vision: false,
        tool_calling: false,
        structured_output: false,
        embeddings: false,
        reasoning: false,
    };

    pub const MOCK: Self = Self {
        chat: true,
        streaming: true,
        vision: false,
        tool_calling: true,
        structured_output: true,
        embeddings: false,
        reasoning: false,
    };
}

/// Public model card returned by `GET /api/v1/ai/models`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelInfo {
    pub id: String,
    pub provider: String,
    pub display_name: String,
    pub capabilities: ModelCapabilities,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_window: Option<u32>,
    /// True when this is the deterministic offline mock (dev only).
    #[serde(default)]
    pub is_mock: bool,
}

/// JSON-schema style tool definition for tool-calling models.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
}

/// Optional structured-output request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponseFormat {
    JsonObject,
    JsonSchema { schema: serde_json::Value },
}

/// Per-call generation options. Unsupported fields are ignored by
/// adapters that lack the capability (they must not invent results).
#[derive(Debug, Clone, Default)]
pub struct GenerateOptions {
    pub temperature: Option<f32>,
    pub max_tokens: Option<u32>,
    pub tools: Vec<ToolSpec>,
    pub response_format: Option<ResponseFormat>,
    /// Hint for providers that expose extended thinking / reasoning.
    pub reasoning: bool,
}

/// Token accounting when the provider reports it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_tokens: Option<u32>,
}
