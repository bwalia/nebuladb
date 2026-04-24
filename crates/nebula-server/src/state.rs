//! Shared server state.
//!
//! Cloned cheaply by Axum into every handler — everything inside is
//! already wrapped in `Arc` so clone is a refcount bump, not a deep copy.

use std::sync::Arc;

use ahash::AHashSet;

use nebula_cache::CacheStats;
use nebula_chunk::{Chunker, FixedSizeChunker};
use nebula_index::TextIndex;
use nebula_llm::{LlmClient, MockLlm};
use nebula_sql::SqlEngine;

use crate::audit::AuditLog;
use crate::jwt::JwtConfig;
use crate::metrics::Metrics;
use crate::ratelimit::{RateLimitConfig, RateLimiter};

#[derive(Clone, Debug)]
pub struct AppConfig {
    /// Max body size in bytes. 1 MiB default; adjust per deployment.
    pub max_body_bytes: usize,
    /// Accepted bearer tokens. Empty set = allowlist disabled; JWT
    /// may still be accepted depending on `jwt` below.
    pub api_keys: AHashSet<String>,
    /// Optional JWT verification. Requests bearing a JWT that passes
    /// verification are authorized even if `api_keys` is empty.
    pub jwt: Option<JwtConfig>,
    /// Default `ef` for search when the request doesn't specify one.
    pub default_ef_search: usize,
    /// Hard ceiling on `top_k` a caller can ask for. Prevents a client
    /// from pinning a huge priority queue.
    pub max_top_k: usize,
    /// Token-bucket parameters applied by the rate-limit layer when
    /// `AppState::rate_limiter` is `Some`.
    pub rate_limit: RateLimitConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            max_body_bytes: 1024 * 1024,
            api_keys: AHashSet::new(),
            jwt: None,
            default_ef_search: 64,
            max_top_k: 100,
            rate_limit: RateLimitConfig::default(),
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub index: Arc<TextIndex>,
    pub llm: Arc<dyn LlmClient>,
    pub chunker: Arc<dyn Chunker>,
    pub metrics: Arc<Metrics>,
    /// Optional embedding cache stats. `None` when the embedder isn't
    /// wrapped in a [`nebula_cache::CachingEmbedder`]; the metrics
    /// endpoint then omits the cache lines entirely rather than
    /// reporting misleading zeros.
    pub cache_stats: Option<Arc<CacheStats>>,
    /// Optional rate-limit state. `None` disables the middleware;
    /// helpful for tests and tools that hammer the server.
    pub rate_limiter: Option<RateLimiter>,
    /// SQL engine. Shares the same underlying `TextIndex` as the
    /// vector / AI search routes — the SQL layer is a parser +
    /// planner on top, not a separate data store.
    pub sql: Arc<SqlEngine>,
    /// Audit ring buffer. Populated by the audit middleware on every
    /// mutating request; surfaced via `GET /api/v1/admin/audit`.
    pub audit: Arc<AuditLog>,
    pub config: Arc<AppConfig>,
}

impl AppState {
    /// Default setup: mock LLM, 500/50 fixed-size chunker, no embedding
    /// cache. Tests and dev runs use this; production swaps the LLM
    /// via [`Self::with_llm`], the chunker via [`Self::with_chunker`],
    /// and registers cache stats via [`Self::with_cache_stats`].
    pub fn new(index: Arc<TextIndex>, config: AppConfig) -> Self {
        // Default SQL engine wraps the same index; no result cache
        // unless a caller wires one in via `with_sql`.
        let sql = Arc::new(SqlEngine::new(Arc::clone(&index)));
        Self {
            index,
            llm: Arc::new(MockLlm::default()),
            chunker: Arc::new(FixedSizeChunker::default()),
            metrics: Arc::new(Metrics::default()),
            cache_stats: None,
            rate_limiter: None,
            sql,
            audit: AuditLog::new(1024),
            config: Arc::new(config),
        }
    }

    pub fn with_audit(mut self, audit: Arc<AuditLog>) -> Self {
        self.audit = audit;
        self
    }

    pub fn with_sql(mut self, sql: Arc<SqlEngine>) -> Self {
        self.sql = sql;
        self
    }

    pub fn with_llm(mut self, llm: Arc<dyn LlmClient>) -> Self {
        self.llm = llm;
        self
    }

    pub fn with_chunker(mut self, chunker: Arc<dyn Chunker>) -> Self {
        self.chunker = chunker;
        self
    }

    pub fn with_cache_stats(mut self, stats: Arc<CacheStats>) -> Self {
        self.cache_stats = Some(stats);
        self
    }

    pub fn with_rate_limiter(mut self, limiter: RateLimiter) -> Self {
        self.rate_limiter = Some(limiter);
        self
    }
}
