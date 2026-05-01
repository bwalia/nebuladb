//! `nebula-server` binary.
//!
//! Defaults to `MockEmbedder` (deterministic, no network). Set
//! `NEBULA_OPENAI_API_KEY` to wire in an OpenAI-compatible backend; the
//! base URL and model come from `NEBULA_OPENAI_BASE_URL` /
//! `NEBULA_OPENAI_MODEL` so the same binary can point at OpenAI, vLLM,
//! or Ollama without a rebuild.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use ahash::AHashSet;
use nebula_cache::{CacheStats, CachingEmbedder};
use nebula_redis_cache::{RedisCacheConfig, RedisEmbedCache};
use nebula_chunk::{Chunker, FixedSizeChunker};
use nebula_embed::{Embedder, MockEmbedder, OpenAiEmbedder, OpenAiEmbedderConfig};
use nebula_index::TextIndex;
use nebula_llm::{
    LlmClient, MockLlm, OllamaConfig, OllamaLlm, OpenAiChatConfig, OpenAiChatLlm,
};
use nebula_grpc::GrpcState;
use nebula_server::{
    build_router, AppConfig, AppState, JwtConfig, RateLimitConfig, RateLimiter, SlowQueryLog,
};
use nebula_sql::{cache::SemanticCacheConfig, SemanticCache, SqlEngine};
use nebula_vector::{HnswConfig, Metric};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber_init();

    let bind: SocketAddr = std::env::var("NEBULA_BIND")
        .unwrap_or_else(|_| "127.0.0.1:8080".into())
        .parse()?;

    let raw_embedder: Arc<dyn Embedder> = match std::env::var("NEBULA_OPENAI_API_KEY").ok() {
        Some(key) => {
            let base_url = std::env::var("NEBULA_OPENAI_BASE_URL")
                .unwrap_or_else(|_| "https://api.openai.com/v1".into());
            let model = std::env::var("NEBULA_OPENAI_MODEL")
                .unwrap_or_else(|_| "text-embedding-3-small".into());
            let dim: usize = std::env::var("NEBULA_EMBED_DIM")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1536);
            let cfg = OpenAiEmbedderConfig {
                base_url,
                api_key: Some(key),
                model,
                dim,
                timeout: Duration::from_secs(30),
            };
            Arc::new(OpenAiEmbedder::new(cfg)?)
        }
        None => {
            let dim: usize = std::env::var("NEBULA_EMBED_DIM")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(384);
            Arc::new(MockEmbedder::new(dim))
        }
    };

    // Optional Redis layer goes *below* the in-process LRU. Why:
    //   - in-proc hits are nanoseconds; Redis hits are ~ms.
    //   - a miss on the in-proc layer is still cheap to check in
    //     Redis, and if Redis has it we save the upstream call.
    //   - Redis failure is transparent (see nebula-redis-cache docs).
    let redis_layer: Arc<dyn Embedder> = match std::env::var("NEBULA_REDIS_URL").ok() {
        Some(url) if !url.is_empty() => {
            let cfg = RedisCacheConfig {
                url,
                prefix: std::env::var("NEBULA_REDIS_PREFIX")
                    .unwrap_or_else(|_| "nebula:embed:".into()),
                ttl: std::env::var("NEBULA_REDIS_TTL_SECS")
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .filter(|s| *s > 0)
                    .map(Duration::from_secs),
                op_timeout: Duration::from_millis(
                    std::env::var("NEBULA_REDIS_TIMEOUT_MS")
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(150),
                ),
            };
            match RedisEmbedCache::new(Arc::clone(&raw_embedder), cfg) {
                Ok(c) => Arc::new(c),
                Err(e) => {
                    tracing::warn!(error = %e, "redis cache init failed; continuing without");
                    raw_embedder
                }
            }
        }
        _ => raw_embedder,
    };

    // Wrap the embedder in an LRU cache. Size is env-overridable;
    // 0 disables the cache entirely (useful for debugging "is the
    // cache lying?" scenarios).
    let cache_size: usize = std::env::var("NEBULA_EMBED_CACHE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
    let (embedder, cache_stats): (Arc<dyn Embedder>, Option<Arc<CacheStats>>) = if cache_size > 0 {
        let cache = Arc::new(CachingEmbedder::new(redis_layer, cache_size));
        let stats = cache.stats();
        (cache, Some(stats))
    } else {
        (redis_layer, None)
    };

    let index = Arc::new(TextIndex::new(
        embedder,
        Metric::Cosine,
        HnswConfig::default(),
    )?);

    let api_keys: AHashSet<String> = std::env::var("NEBULA_API_KEYS")
        .unwrap_or_default()
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    // LLM selection is driven by env so one binary covers all
    // backends. Precedence: explicit OpenAI key > Ollama base URL >
    // mock default.
    let llm: Arc<dyn LlmClient> = if let Ok(key) = std::env::var("NEBULA_LLM_OPENAI_KEY") {
        let base = std::env::var("NEBULA_LLM_OPENAI_BASE")
            .unwrap_or_else(|_| "https://api.openai.com/v1".into());
        let model = std::env::var("NEBULA_LLM_MODEL").unwrap_or_else(|_| "gpt-4o-mini".into());
        let cfg = OpenAiChatConfig {
            base_url: base,
            api_key: Some(key),
            model,
            timeout: Duration::from_secs(120),
        };
        Arc::new(OpenAiChatLlm::new(cfg)?)
    } else if let Ok(base) = std::env::var("NEBULA_LLM_OLLAMA_URL") {
        let model = std::env::var("NEBULA_LLM_MODEL").unwrap_or_else(|_| "llama3".into());
        let cfg = OllamaConfig {
            base_url: base,
            model,
            timeout: Duration::from_secs(120),
        };
        Arc::new(OllamaLlm::new(cfg)?)
    } else {
        Arc::new(MockLlm::default())
    };

    // Chunker: fixed-size with env-overridable window/overlap.
    let chunk_chars: usize = std::env::var("NEBULA_CHUNK_CHARS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500);
    let chunk_overlap: usize = std::env::var("NEBULA_CHUNK_OVERLAP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);
    let chunker: Arc<dyn Chunker> = Arc::new(FixedSizeChunker::new(chunk_chars, chunk_overlap)?);

    // Optional JWT. Silently off when NEBULA_JWT_SECRET is unset.
    let jwt = std::env::var("NEBULA_JWT_SECRET")
        .ok()
        .filter(|s| !s.is_empty())
        .map(|secret| {
            let mut cfg = JwtConfig::hs256(secret.into_bytes());
            cfg.expected_issuer = std::env::var("NEBULA_JWT_ISS").ok();
            cfg.expected_audience = std::env::var("NEBULA_JWT_AUD").ok();
            cfg
        });

    // Rate limit: on by default with generous dev limits.
    // NEBULA_RATE_LIMIT=off disables; otherwise capacity / rate come
    // from two env vars.
    let rl_mode = std::env::var("NEBULA_RATE_LIMIT").unwrap_or_else(|_| "on".into());
    let (rate_limit_cfg, rate_limiter) = if rl_mode == "off" {
        (RateLimitConfig::default(), None)
    } else {
        let capacity: f64 = std::env::var("NEBULA_RATE_LIMIT_BURST")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(120.0);
        let refill: f64 = std::env::var("NEBULA_RATE_LIMIT_RPS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(20.0);
        (
            RateLimitConfig {
                capacity,
                refill_per_sec: refill,
            },
            Some(RateLimiter::new()),
        )
    };

    // Semantic SQL cache. NEBULA_SQL_CACHE=off disables; otherwise
    // capacity and TTL are env-tuned. The cache shares the same index
    // handle as the rest of the server — consistent mutations (upsert,
    // delete) bypass the cache but TTL will naturally expire stale
    // query results within seconds.
    let sql_cache_mode = std::env::var("NEBULA_SQL_CACHE").unwrap_or_else(|_| "on".into());
    let sql_cache = if sql_cache_mode == "off" {
        None
    } else {
        let capacity: usize = std::env::var("NEBULA_SQL_CACHE_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(512);
        let ttl_secs: u64 = std::env::var("NEBULA_SQL_CACHE_TTL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);
        Some(SemanticCache::new(SemanticCacheConfig {
            capacity,
            ttl: Duration::from_secs(ttl_secs),
        }))
    };
    let sql_engine = Arc::new({
        let mut eng = SqlEngine::new(Arc::clone(&index));
        if let Some(cache) = sql_cache.as_ref() {
            eng = eng.with_cache(Arc::clone(cache));
        }
        eng
    });

    let mut state = AppState::new(
        index,
        AppConfig {
            api_keys,
            jwt,
            rate_limit: rate_limit_cfg,
            ..AppConfig::default()
        },
    )
    .with_llm(llm)
    .with_chunker(chunker)
    .with_sql(Arc::clone(&sql_engine));
    if let Some(stats) = cache_stats {
        state = state.with_cache_stats(stats);
    }
    if let Some(rl) = rate_limiter {
        state = state.with_rate_limiter(rl);
    }

    // Slow-query log: default capacity 32, threshold 10 ms. Dev
    // stacks on MockEmbedder are fast enough that nothing crosses
    // 10ms, so the panel stays empty — set threshold to 0 for a
    // demo, raise it for a noisy production (50 or 100 ms).
    let slow_cap: usize = std::env::var("NEBULA_SLOW_QUERY_CAPACITY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(32);
    let slow_threshold: u64 = std::env::var("NEBULA_SLOW_QUERY_THRESHOLD_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    state = state.with_slow_log(SlowQueryLog::new(slow_cap, slow_threshold));

    // Snapshot the Arc handles we need for the gRPC server before
    // moving `state` into the REST router. Both transports see the
    // same underlying index and LLM — this is how that guarantee is
    // enforced.
    let grpc_index = Arc::clone(&state.index);
    let grpc_llm = Arc::clone(&state.llm);
    let grpc_chunker = Arc::clone(&state.chunker);

    let app = build_router(state);

    // Optional gRPC transport. Off when NEBULA_GRPC_BIND is unset —
    // the binary still runs REST only, preserving existing behavior.
    // Using the same AppState pieces means both transports see the
    // same index and the same LLM client.
    let grpc_fut: Option<_> = match std::env::var("NEBULA_GRPC_BIND").ok() {
        Some(addr) => {
            let grpc_addr: SocketAddr = addr.parse()?;
            let grpc_state = GrpcState::new(grpc_index, grpc_llm, grpc_chunker);
            tracing::info!("nebula-grpc listening on {grpc_addr}");
            Some(tokio::spawn(async move {
                if let Err(e) = nebula_grpc::serve(grpc_state, grpc_addr).await {
                    tracing::error!(error = %e, "grpc server exited");
                }
            }))
        }
        None => None,
    };

    // Postgres wire protocol: simple-query only, trust auth. Off by
    // default. The SQL engine is shared with REST, so a query issued
    // via psql sees identical state to the same query hitting
    // `POST /api/v1/query`.
    let pg_fut: Option<_> = match std::env::var("NEBULA_PG_BIND").ok() {
        Some(addr) => {
            let pg_addr: SocketAddr = addr.parse()?;
            let engine = Arc::clone(&sql_engine);
            tracing::info!("nebula-pgwire listening on {pg_addr}");
            Some(tokio::spawn(async move {
                if let Err(e) = nebula_pgwire::serve(engine, pg_addr).await {
                    tracing::error!(error = %e, "pgwire server exited");
                }
            }))
        }
        None => None,
    };

    tracing::info!("nebula-server listening on {bind}");
    let listener = tokio::net::TcpListener::bind(bind).await?;
    // `into_make_service_with_connect_info` is what lets the
    // `ConnectInfo<SocketAddr>` extractor in the rate-limit middleware
    // see the peer address. Without it, the middleware still runs —
    // it just falls back to "anon" keys, which is fine but loses IP
    // discrimination for unauthenticated requests.
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    // If gRPC or pgwire were spawned, abort them on REST shutdown so
    // `docker stop` sees a clean exit. Neither server surfaces its
    // listener to us, so abort is the simplest way to tear down.
    if let Some(h) = grpc_fut {
        h.abort();
    }
    if let Some(h) = pg_fut {
        h.abort();
    }
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
}

fn tracing_subscriber_init() {
    // Best-effort: swallow errors so a missing feature (e.g. no
    // env_logger) never stops the server from booting.
    let _ = tracing::subscriber::set_global_default(
        tracing_subscriber_stub::subscriber(),
    );
}

// The binary pulls in `tracing` but not `tracing-subscriber` — avoiding
// another heavy dep for the vertical slice. A no-op subscriber keeps
// `tracing::info!` calls from panicking when no collector is set.
mod tracing_subscriber_stub {
    use tracing::{span, Metadata, Subscriber};

    pub fn subscriber() -> impl Subscriber + Send + Sync {
        NoopSubscriber
    }

    struct NoopSubscriber;
    impl Subscriber for NoopSubscriber {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            false
        }
        fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
            span::Id::from_u64(1)
        }
        fn record(&self, _: &span::Id, _: &span::Record<'_>) {}
        fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}
        fn event(&self, _: &tracing::Event<'_>) {}
        fn enter(&self, _: &span::Id) {}
        fn exit(&self, _: &span::Id) {}
    }
}
