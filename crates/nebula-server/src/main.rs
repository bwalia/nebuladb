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
use nebula_server::state::{FollowerCursor, TeeCursorStore};
use nebula_server::{
    build_router, AppConfig, AppState, CapturingSubscriber, ClusterConfig, JwtConfig, LogBus,
    LogLevel, RateLimitConfig, RateLimiter, SlowQueryLog,
};
use nebula_sql::{cache::SemanticCacheConfig, SemanticCache, SqlEngine};
use nebula_vector::{HnswConfig, Metric};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // LogBus created before the subscriber so the same Arc winds up
    // on AppState later. Anything logged from this point on is
    // captured and served via /admin/logs/stream.
    let min_level = std::env::var("NEBULA_LOG_LEVEL")
        .ok()
        .and_then(|s| LogLevel::parse(&s))
        .unwrap_or(LogLevel::Info);
    let log_bus = Arc::new(LogBus::new(256, min_level));
    tracing_subscriber_init(Arc::clone(&log_bus));

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

    // Durability: NEBULA_DATA_DIR turns on WAL + snapshots. When
    // unset the server stays in-memory (legacy/demo default).
    // The persistent path also runs replay-on-boot, so startup
    // latency is proportional to WAL size — on a fresh dir it's
    // ~instant.
    let index = match std::env::var("NEBULA_DATA_DIR").ok() {
        Some(dir) if !dir.is_empty() => {
            let start = std::time::Instant::now();
            let idx = Arc::new(TextIndex::open_persistent(
                embedder,
                Metric::Cosine,
                HnswConfig::default(),
                &dir,
            )?);
            tracing::info!(
                data_dir = %dir,
                docs = idx.len(),
                took_ms = start.elapsed().as_millis() as u64,
                "durability: recovered from disk"
            );
            idx
        }
        _ => Arc::new(TextIndex::new(
            embedder,
            Metric::Cosine,
            HnswConfig::default(),
        )?),
    };

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

    // Cluster role + peers parse up front so a typo fails boot
    // rather than silently degrading. The follower_cursor atomic
    // is only created when we're actually a follower — leaders
    // and standalones never need it.
    let cluster = Arc::new(ClusterConfig::from_env().map_err(|e| -> Box<dyn std::error::Error> {
        format!("cluster config: {e}").into()
    })?);
    let follower_cursor: Option<Arc<FollowerCursor>> = if cluster.is_follower() {
        Some(Arc::new(FollowerCursor::default()))
    } else {
        None
    };

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
    .with_sql(Arc::clone(&sql_engine))
    .with_cluster(Arc::clone(&cluster))
    .with_log_bus(Arc::clone(&log_bus));
    if let Some(fc) = follower_cursor.as_ref() {
        state = state.with_follower_cursor(Arc::clone(fc));
    }
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
            // Pass the cluster role through so DocumentService rejects
            // writes on followers — symmetric with the REST guard.
            let grpc_state = GrpcState::with_role(
                grpc_index.clone(),
                grpc_llm,
                grpc_chunker,
                cluster.role,
            );
            tracing::info!("nebula-grpc listening on {grpc_addr} (role={:?})", cluster.role);
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
            let pg_role = cluster.role;
            tracing::info!("nebula-pgwire listening on {pg_addr} (role={pg_role:?})");
            Some(tokio::spawn(async move {
                if let Err(e) = nebula_pgwire::serve_with_role(engine, pg_addr, pg_role).await {
                    tracing::error!(error = %e, "pgwire server exited");
                }
            }))
        }
        None => None,
    };

    // Follower mode. When NEBULA_FOLLOW_LEADER is set to a gRPC
    // endpoint like "http://leader:50051", the server also spawns
    // a background task that tails the leader's WAL and applies
    // records into this local index. The server itself still
    // accepts reads on REST/gRPC/pgwire as normal — writes to
    // mutating endpoints should not be made on a follower (they'd
    // diverge from the leader), but we don't yet enforce this at
    // the router layer. Wire protection is a follow-up.
    //
    // NEBULA_FOLLOWER_CURSOR_PATH controls where the follower
    // persists its last-applied cursor. Defaults to
    // "$NEBULA_DATA_DIR/follower.cursor" when a data dir is set,
    // otherwise a per-process temp file (cursor doesn't survive
    // restart — acceptable for an in-memory follower that will
    // replay from BEGIN anyway).
    let follower_handle: Option<nebula_grpc::follower::FollowerHandle> =
        match std::env::var("NEBULA_FOLLOW_LEADER").ok() {
            Some(leader) if !leader.is_empty() => {
                tracing::info!(leader = %leader, "follower mode: connecting to leader");
                let channel = tonic::transport::Channel::from_shared(leader.clone())?
                    .connect()
                    .await?;
                // Durable (file-backed) half of the cursor store.
                // Optional — without NEBULA_DATA_DIR the follower
                // has nowhere on disk to persist, so it restarts
                // from BEGIN (RAM-only state is fine with that).
                let durable: Option<Arc<dyn nebula_grpc::follower::CursorStore>> =
                    match std::env::var("NEBULA_FOLLOWER_CURSOR_PATH")
                        .ok()
                        .or_else(|| {
                            std::env::var("NEBULA_DATA_DIR")
                                .ok()
                                .filter(|d| !d.is_empty())
                                .map(|d| format!("{d}/follower.cursor"))
                        }) {
                        Some(path) => {
                            tracing::info!(path = %path, "follower cursor persistence on");
                            Some(Arc::new(nebula_grpc::follower::FileCursorStore::new(path)))
                        }
                        None => {
                            tracing::warn!(
                                "no cursor persistence — follower will replay from BEGIN on restart"
                            );
                            None
                        }
                    };

                // Wrap the durable store in a tee that also
                // publishes to the AppState follower_cursor so
                // /admin/replication can report lag without
                // touching the file system.
                let store: Option<Arc<dyn nebula_grpc::follower::CursorStore>> =
                    match (durable, follower_cursor.as_ref()) {
                        (Some(durable), Some(snapshot)) => {
                            Some(Arc::new(TeeCursorStore {
                                durable,
                                snapshot: Arc::clone(snapshot),
                            }))
                        }
                        // Snapshot-only (RAM) or durable-only
                        // (role misconfigured but data dir set) —
                        // both are edge cases; keep whichever we
                        // have so lag is still visible in the
                        // snapshot-only case.
                        (None, Some(snapshot)) => {
                            struct SnapshotOnly(Arc<FollowerCursor>);
                            impl nebula_grpc::follower::CursorStore for SnapshotOnly {
                                fn load(
                                    &self,
                                ) -> Result<
                                    Option<nebula_wal::WalCursor>,
                                    nebula_grpc::follower::CursorStoreError,
                                > {
                                    Ok(None)
                                }
                                fn save(
                                    &self,
                                    c: nebula_wal::WalCursor,
                                ) -> Result<
                                    (),
                                    nebula_grpc::follower::CursorStoreError,
                                > {
                                    self.0.store(c.segment_seq, c.byte_offset);
                                    Ok(())
                                }
                            }
                            Some(Arc::new(SnapshotOnly(Arc::clone(snapshot))))
                        }
                        (Some(durable), None) => Some(durable),
                        (None, None) => None,
                    };
                Some(nebula_grpc::follower::spawn_with_store(
                    channel,
                    Arc::clone(&grpc_index),
                    nebula_wal::WalCursor::BEGIN,
                    store,
                ))
            }
            _ => None,
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
    if let Some(h) = follower_handle {
        h.abort();
    }
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
}

fn tracing_subscriber_init(bus: Arc<LogBus>) {
    // Capturing subscriber replaces the former noop. Every
    // `tracing::info!` / `warn!` / `error!` on a server task
    // flows through `bus` and out to SSE subscribers.
    //
    // Best-effort: if another subscriber was already installed
    // (tests can do this), swallow the error — the library has
    // always been tolerant of that and we keep booting.
    let _ = tracing::subscriber::set_global_default(CapturingSubscriber::new(bus));
}
