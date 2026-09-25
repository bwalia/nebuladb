//! Provider registry, routing policy, and fallback.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use nebula_llm::{
    AnthropicConfig, AnthropicLlm, GeminiConfig, GeminiLlm, LlmClient, MockLlm, ModelInfo,
    OllamaConfig, OllamaLlm, OpenAiChatConfig, OpenAiChatLlm,
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// Logical task used by the routing policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    Chat,
    Reason,
    Classify,
    Summarise,
    Vision,
    Embed,
}

#[derive(Clone)]
pub struct ProviderEntry {
    pub name: String,
    pub client: Arc<dyn LlmClient>,
    pub info: ModelInfo,
    /// Optional fallback provider name on failure.
    pub fallback: Option<String>,
}

/// Multi-provider AI gateway. Secrets stay in process env; never returned.
pub struct AiGateway {
    providers: HashMap<String, ProviderEntry>,
    /// Default provider for chat when the request omits one.
    default_chat: String,
    /// Task → preferred provider name.
    routes: HashMap<TaskKind, String>,
    /// Connect/read timeouts already applied inside clients; stored for config API.
    pub connect_timeout_secs: u64,
}

impl AiGateway {
    /// Boot from env + the process-default LLM (existing NEBULA_LLM_* path).
    pub fn from_env(default_llm: Arc<dyn LlmClient>) -> Self {
        let mut providers = HashMap::new();
        let default_info = default_llm.info();
        let default_name = if default_info.is_mock {
            "mock".to_string()
        } else {
            default_info.provider.clone()
        };
        providers.insert(
            default_name.clone(),
            ProviderEntry {
                name: default_name.clone(),
                client: Arc::clone(&default_llm),
                info: default_info,
                fallback: None,
            },
        );

        // Optional additional providers (do not override default slot if same name).
        if let Ok(key) = std::env::var("NEBULA_AI_ANTHROPIC_KEY") {
            let model = std::env::var("NEBULA_AI_ANTHROPIC_MODEL")
                .unwrap_or_else(|_| "claude-sonnet-4-20250514".into());
            match AnthropicLlm::new(AnthropicConfig::new(key, model)) {
                Ok(llm) => {
                    let info = llm.info();
                    providers.insert(
                        "anthropic".into(),
                        ProviderEntry {
                            name: "anthropic".into(),
                            client: Arc::new(llm),
                            info,
                            fallback: Some(default_name.clone()),
                        },
                    );
                    info!("ai gateway: registered anthropic provider");
                }
                Err(e) => warn!(error = %e, "ai gateway: anthropic init failed"),
            }
        }
        if let Ok(key) = std::env::var("NEBULA_AI_GEMINI_KEY") {
            let model =
                std::env::var("NEBULA_AI_GEMINI_MODEL").unwrap_or_else(|_| "gemini-2.0-flash".into());
            let mut cfg = GeminiConfig::new(key, model);
            if let Ok(base) = std::env::var("NEBULA_AI_GEMINI_BASE") {
                cfg.base_url = base;
            }
            match GeminiLlm::new(cfg) {
                Ok(llm) => {
                    let info = llm.info();
                    providers.insert(
                        "gemini".into(),
                        ProviderEntry {
                            name: "gemini".into(),
                            client: Arc::new(llm),
                            info,
                            fallback: Some(default_name.clone()),
                        },
                    );
                    info!("ai gateway: registered gemini provider");
                }
                Err(e) => warn!(error = %e, "ai gateway: gemini init failed"),
            }
        }
        if let Ok(key) = std::env::var("NEBULA_AI_OPENAI_KEY") {
            let model =
                std::env::var("NEBULA_AI_OPENAI_MODEL").unwrap_or_else(|_| "gpt-4o-mini".into());
            let base = std::env::var("NEBULA_AI_OPENAI_BASE")
                .unwrap_or_else(|_| "https://api.openai.com/v1".into());
            let cfg = OpenAiChatConfig {
                base_url: base,
                api_key: Some(key),
                model,
                timeout: Duration::from_secs(10),
                read_timeout: Some(Duration::from_secs(120)),
            };
            match OpenAiChatLlm::new(cfg) {
                Ok(llm) => {
                    let info = llm.info();
                    providers.insert(
                        "openai".into(),
                        ProviderEntry {
                            name: "openai".into(),
                            client: Arc::new(llm),
                            info,
                            fallback: Some(default_name.clone()),
                        },
                    );
                    info!("ai gateway: registered openai provider");
                }
                Err(e) => warn!(error = %e, "ai gateway: openai init failed"),
            }
        }
        if let Ok(base) = std::env::var("NEBULA_AI_OLLAMA_URL") {
            let model =
                std::env::var("NEBULA_AI_OLLAMA_MODEL").unwrap_or_else(|_| "llama3.1:8b".into());
            let cfg = OllamaConfig {
                base_url: base,
                model,
                timeout: Duration::from_secs(10),
                read_timeout: Some(Duration::from_secs(120)),
            };
            match OllamaLlm::new(cfg) {
                Ok(llm) => {
                    let info = llm.info();
                    providers.insert(
                        "ollama".into(),
                        ProviderEntry {
                            name: "ollama".into(),
                            client: Arc::new(llm),
                            info,
                            fallback: Some(default_name.clone()),
                        },
                    );
                    info!("ai gateway: registered ollama provider");
                }
                Err(e) => warn!(error = %e, "ai gateway: ollama init failed"),
            }
        }

        // Ensure mock is always available for local demos when nothing else registered.
        if providers.is_empty() {
            let mock = MockLlm::default();
            let info = mock.info();
            providers.insert(
                "mock".into(),
                ProviderEntry {
                    name: "mock".into(),
                    client: Arc::new(mock),
                    info,
                    fallback: None,
                },
            );
        }

        let mut routes = HashMap::new();
        routes.insert(TaskKind::Chat, default_name.clone());
        routes.insert(TaskKind::Reason, default_name.clone());
        routes.insert(TaskKind::Classify, default_name.clone());
        routes.insert(TaskKind::Summarise, default_name.clone());
        routes.insert(TaskKind::Vision, default_name.clone());
        if providers.contains_key("anthropic") {
            routes.insert(TaskKind::Reason, "anthropic".into());
        }
        if providers.contains_key("gemini") {
            routes.insert(TaskKind::Vision, "gemini".into());
        }

        // Optional JSON override: {"chat":"openai","reason":"anthropic",...}
        if let Ok(raw) = std::env::var("NEBULA_AI_ROUTES") {
            if let Ok(map) = serde_json::from_str::<HashMap<String, String>>(&raw) {
                for (k, v) in map {
                    if let Ok(kind) = serde_json::from_value::<TaskKind>(serde_json::json!(k)) {
                        if providers.contains_key(&v) {
                            routes.insert(kind, v);
                        }
                    }
                }
            }
        }

        Self {
            providers,
            default_chat: default_name,
            routes,
            connect_timeout_secs: 10,
        }
    }

    pub fn list_models(&self) -> Vec<ModelInfo> {
        let mut out: Vec<_> = self.providers.values().map(|p| p.info.clone()).collect();
        out.sort_by(|a, b| a.provider.cmp(&b.provider).then(a.id.cmp(&b.id)));
        out
    }

    pub fn resolve(
        &self,
        provider: Option<&str>,
        model: Option<&str>,
        task: TaskKind,
    ) -> Result<&ProviderEntry, String> {
        if let Some(p) = provider {
            return self
                .providers
                .get(p)
                .ok_or_else(|| format!("unknown provider '{p}'"));
        }
        if let Some(m) = model {
            if let Some(entry) = self.providers.values().find(|e| e.info.id == m || e.name == m)
            {
                return Ok(entry);
            }
            return Err(format!("unknown model '{m}'"));
        }
        let name = self
            .routes
            .get(&task)
            .cloned()
            .unwrap_or_else(|| self.default_chat.clone());
        self.providers
            .get(&name)
            .ok_or_else(|| format!("no provider for task {task:?}"))
    }

    pub fn client_with_fallback(
        &self,
        provider: Option<&str>,
        model: Option<&str>,
        task: TaskKind,
    ) -> Result<(Arc<dyn LlmClient>, ModelInfo, Option<String>), String> {
        let primary = self.resolve(provider, model, task)?;
        let fallback_name = primary.fallback.clone();
        Ok((
            Arc::clone(&primary.client),
            primary.info.clone(),
            fallback_name,
        ))
    }

    pub fn get_fallback(&self, name: &str) -> Option<&ProviderEntry> {
        self.providers.get(name)
    }

    pub fn routes_public(&self) -> HashMap<String, String> {
        self.routes
            .iter()
            .map(|(k, v)| {
                (
                    serde_json::to_value(k)
                        .ok()
                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                        .unwrap_or_else(|| format!("{k:?}")),
                    v.clone(),
                )
            })
            .collect()
    }

    pub fn default_provider(&self) -> &str {
        &self.default_chat
    }
}
