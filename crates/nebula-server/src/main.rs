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
use nebula_chunk::{Chunker, FixedSizeChunker};
use nebula_embed::{
    BreakerConfig, CircuitBreakerEmbedder, Embedder, MockEmbedder, OpenAiEmbedder,
    OpenAiEmbedderConfig, RetryConfig, RetryingEmbedder,
};
use nebula_grpc::GrpcState;
use nebula_index::TextIndex;
use nebula_llm::{LlmClient, MockLlm, OllamaConfig, OllamaLlm, OpenAiChatConfig, OpenAiChatLlm};
use nebula_redis_cache::{RedisCacheConfig, RedisEmbedCache};
use nebula_server::snapshot_scheduler::{self, SnapshotSchedulerConfig};
use nebula_server::state::{FollowerCursor, TeeCursorStore};
use nebula_server::{
    build_router, AppConfig, AppState, CapturingSubscriber, ClusterConfig, JwtConfig, LogBus,
    LogLevel, RateLimitConfig, RateLimiter, SlowQueryLog, TrustedProxies,
};
use nebula_sql::{cache::SemanticCacheConfig, SemanticCache, SqlEngine};
use nebula_vector::{HnswConfig, Metric};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Runtime is built explicitly so worker_threads is operator-tunable.
    // Default = `available_parallelism` (respects cgroup CPU caps).
    // The previous `#[tokio::main]` default was supposed to do the
    // same but in some container topologies came up with only 2
    // workers, which let a handful of slow tasks pin the entire
    // runtime — the symptom that motivated this change.
    let workers: usize = std::env::var("NEBULA_WORKER_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .or_else(|| std::thread::available_parallelism().ok().map(|n| n.get()))
        .unwrap_or(4)
        .max(2);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .thread_name("tokio-rt-worker")
        .build()?;
    runtime.block_on(async_main(workers))
}

async fn async_main(workers: usize) -> Result<(), Box<dyn std::error::Error>> {
    // LogBus created before the subscriber so the same Arc winds up
    // on AppState later. Anything logged from this point on is
    // captured and served via /admin/logs/stream — and, when
    // `NEBULA_LOG_STDERR=on` (the default), also to stderr so
    // `docker logs` has breadcrumbs even if the runtime is wedged.
    let min_level = std::env::var("NEBULA_LOG_LEVEL")
        .ok()
        .and_then(|s| LogLevel::parse(&s))
        .unwrap_or(LogLevel::Info);
    let log_bus = Arc::new(LogBus::new(256, min_level));
    let stderr = std::env::var("NEBULA_LOG_STDERR")
        .map(|v| v != "off")
        .unwrap_or(true);
    tracing_subscriber_init(Arc::clone(&log_bus), stderr);
    tracing::info!(workers, stderr, "nebula-server starting");

    // Watchdog: convert silent tokio-runtime wedges into clean
    // restarts. A heartbeat tokio task increments a counter every
    // ~5s; a *separate* OS thread (which keeps running even if the
    // tokio runtime is fully wedged) polls that counter and aborts
    // the process if it stalls for `NEBULA_WATCHDOG_STALL_SECS`
    // (default 300). Aborting fires SIGABRT (exit 134); compose's
    // `restart: unless-stopped` then brings us back up.
    //
    // Why this is necessary even with PR #25 + PR #49 landed:
    // *some* wedge mechanism is still present (~13h silent hang
    // observed on 192.168.1.193 after the first scheduler-driven
    // snapshot completed). Until the actual cause is fully fixed,
    // this bounds the blast radius to a 5-minute outage instead of
    // "undefined — call ops."
    //
    // Disable with `NEBULA_WATCHDOG_DISABLE=1` (tests, debugging,
    // single-shot smoke jobs).
    if std::env::var("NEBULA_WATCHDOG_DISABLE").ok().as_deref() != Some("1") {
        spawn_runtime_watchdog();
    }

    let bind: SocketAddr = std::env::var("NEBULA_BIND")
        .unwrap_or_else(|_| "127.0.0.1:8080".into())
        .parse()?;

    // Stand up a minimal "boot" HTTP listener immediately, well
    // before WAL recovery starts. Recovery on a multi-GB WAL with no
    // snapshot can take minutes; without an early listener the port
    // is closed during that window and every probe hits "connection
    // refused". Both docker's `wget /healthz` healthcheck and the
    // showcase nginx pool then see the leader as down and start the
    // unhealthy → kill loop just as recovery is nearly finished.
    //
    // The stub serves three things:
    //   * GET /healthz/live → 200 "alive" (process is up)
    //   * GET /healthz      → 503 { status: "recovering", elapsed_ms }
    //   * everything else   → 503 (so writes/reads fail fast and the
    //                         nginx read pool fails over to the
    //                         follower instead of waiting on timeout)
    //
    // The docker healthcheck uses /healthz/live so the container stays
    // "healthy" through recovery, while /healthz returns 503
    // {"status":"recovering"} (legible to humans and curl smoke tests)
    // until the real router takes over below.
    let boot_started = std::time::Instant::now();
    let boot_listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(
        "nebula-server boot listener bound on {bind} (serving 503 until recovery completes)"
    );
    let boot_handle = tokio::spawn(async move {
        let boot_router = axum::Router::new()
            .route(
                "/healthz/live",
                axum::routing::get(|| async { (axum::http::StatusCode::OK, "alive") }),
            )
            .route(
                "/healthz",
                axum::routing::get(move || async move {
                    let elapsed_ms = boot_started.elapsed().as_millis() as u64;
                    (
                        axum::http::StatusCode::SERVICE_UNAVAILABLE,
                        axum::Json(serde_json::json!({
                            "status": "recovering",
                            "elapsed_ms": elapsed_ms,
                        })),
                    )
                }),
            )
            .fallback(|| async {
                (
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    axum::Json(serde_json::json!({
                        "code": "recovering",
                        "error": "server is still in WAL recovery; retry shortly",
                    })),
                )
            })
            // Send `Connection: close` on every boot-stub response. axum 0.7
            // spawns each accepted connection as a DETACHED task, so aborting
            // the serve future at handoff drops the listener but NOT the
            // already-open connection handlers — a client holding a keepalive
            // connection (nginx pool, Prometheus) keeps being served the stub's
            // stale 503 forever after the real router has bound. Closing the
            // connection after each response forces every client to reconnect,
            // and the reconnect lands on the real router. Observed in prod:
            // Prometheus stuck on `503` post-recovery until its sidecar was
            // restarted; this is the durable fix.
            .layer(tower_http::set_header::SetResponseHeaderLayer::overriding(
                axum::http::header::CONNECTION,
                axum::http::HeaderValue::from_static("close"),
            ));
        // Runs until the task is aborted at handoff time (below). We do
        // NOT use `with_graceful_shutdown`: axum 0.7 waits for every open
        // connection to drain, and nginx's upstream keepalive pool keeps
        // idle connections to :8080 open indefinitely, so a graceful drain
        // would hang forever and the real listener would never bind.
        let _ = axum::serve(boot_listener, boot_router).await;
    });

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

    // Resilience wrappers (design 0010 §5), innermost-first:
    //   provider → retry → circuit breaker → redis cache → LRU cache
    // Retries sit closest to the provider so a transient 5xx is
    // absorbed before it counts toward the breaker. The caches sit
    // OUTSIDE the breaker deliberately: a cache hit must keep
    // succeeding while the circuit is open — repeated queries stay
    // servable through an outage. The AiHealth handle is the node's
    // `ai_degraded` signal on /healthz, /metrics, /admin/reliability.
    let raw_embedder: Arc<dyn Embedder> =
        Arc::new(RetryingEmbedder::new(raw_embedder, RetryConfig::default()));
    let breaker = Arc::new(CircuitBreakerEmbedder::new(
        raw_embedder,
        BreakerConfig::default(),
    ));
    let ai_health = breaker.health();
    let raw_embedder: Arc<dyn Embedder> = breaker;

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
    // Keep a concrete handle to the LRU wrapper so the resource
    // manager's actuator can resize it under memory pressure
    // (design 0010 §6).
    let mut embed_cache_handle: Option<Arc<CachingEmbedder>> = None;
    let (embedder, cache_stats): (Arc<dyn Embedder>, Option<Arc<CacheStats>>) = if cache_size > 0 {
        let cache = Arc::new(CachingEmbedder::new(redis_layer, cache_size));
        let stats = cache.stats();
        embed_cache_handle = Some(Arc::clone(&cache));
        (cache, Some(stats))
    } else {
        (redis_layer, None)
    };

    // Embedding-space identity guard (design 0010 §5). A persistent
    // corpus is tied to the (model, dim) that produced its vectors —
    // booting the same data dir with a different embedder would
    // silently mix incompatible vector spaces and quietly ruin every
    // similarity score. Stamp the identity on first boot; refuse to
    // start on mismatch (the operator either restores the old config
    // or re-ingests; NEBULA_EMBEDDER_IDENTITY_OVERRIDE=1 accepts the
    // new identity for an intentional migration wipe).
    if let Some(dir) = std::env::var("NEBULA_DATA_DIR")
        .ok()
        .filter(|d| !d.is_empty())
    {
        nebula_server::embedder_identity::check_or_stamp(
            std::path::Path::new(&dir),
            embedder.model(),
            embedder.dim(),
            std::env::var("NEBULA_EMBEDDER_IDENTITY_OVERRIDE").as_deref() == Ok("1"),
        )?;
    }

    // Durability: NEBULA_DATA_DIR turns on WAL + snapshots. When
    // unset the server stays in-memory (legacy/demo default).
    // The persistent path also runs replay-on-boot, so startup
    // latency is proportional to WAL size — on a fresh dir it's
    // ~instant.
    let index = match std::env::var("NEBULA_DATA_DIR").ok() {
        Some(dir) if !dir.is_empty() => {
            let start = std::time::Instant::now();
            // open_persistent is synchronous and CPU-heavy: it
            // deserializes the WAL and inserts every record into
            // HNSW. With a large WAL (tens of MB) and a tight CPU
            // cap that can take minutes. Running it on a tokio
            // worker stalls every other future on the runtime —
            // including the healthcheck handler — so docker marks
            // the container unhealthy and tears it down before
            // recovery completes. spawn_blocking moves it to the
            // dedicated blocking pool where stalling is fine.
            let dir_owned = dir.clone();
            let embedder_for_open = Arc::clone(&embedder);
            let idx_inner = tokio::task::spawn_blocking(move || {
                TextIndex::open_persistent(
                    embedder_for_open,
                    Metric::Cosine,
                    HnswConfig::default(),
                    &dir_owned,
                )
            })
            .await
            .map_err(|e| -> Box<dyn std::error::Error> {
                format!("recovery task panicked: {e}").into()
            })??;
            let idx = Arc::new(idx_inner);
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

    // Raft bootstrap: opt-in via NEBULA_RAFT_PEERS. When set, this
    // node joins (or starts) a Raft cluster and the gRPC raft transport
    // is bound on NEBULA_RAFT_BIND (default 0.0.0.0:50052). When unset,
    // the standalone WAL path runs unchanged — every existing
    // single-node deployment keeps working without env changes.
    //
    // The handle is held in AppState so future PRs (2.5c) can route
    // writes through `RaftHandle::client_write` instead of going
    // straight to TextIndex. Phase 2.5b — this PR — only stands up the
    // raft instance + transport; the write path doesn't change yet.
    //
    // Snapshot scheduler is intentionally NOT spawned in raft mode:
    // openraft drives its own snapshot triggers from log growth,
    // tracked by ADR §6. Two snapshot schedulers fighting over the
    // same nsnap dir would race on the .ok marker. We pick one path.
    let raft_handle: Option<Arc<nebula_raft::RaftHandle>> =
        match nebula_raft::RaftConfig::from_env() {
            Ok(Some(cfg)) => {
                tracing::info!(
                    node_id = cfg.node_id,
                    bind = %cfg.bind,
                    peers = cfg.peers.len(),
                    "raft mode: bootstrapping"
                );
                let bind_addr: SocketAddr = cfg.bind.parse()?;
                let handle = Arc::new(nebula_raft::bootstrap(cfg, Arc::clone(&index)).await?);

                // Stand up the Raft gRPC service so peers can reach us.
                // Server runs for the process lifetime; we don't await
                // its JoinHandle here because that would block the boot.
                let svc = nebula_raft::RaftRpcServer::new(Arc::clone(handle.raft()));
                tracing::info!(addr = %bind_addr, "nebula-raft transport listening");
                tokio::spawn(async move {
                    if let Err(e) = tonic::transport::Server::builder()
                        .add_service(nebula_raft::NebulaRaftServer::new(svc))
                        .serve(bind_addr)
                        .await
                    {
                        tracing::error!(error = %e, "raft gRPC server exited");
                    }
                });
                Some(handle)
            }
            Ok(None) => None,
            Err(e) => return Err(format!("raft config: {e}").into()),
        };

    // Spawn the background snapshot scheduler. No-op for in-memory
    // mode (the scheduler self-exits when wal_stats() returns None).
    // In raft mode openraft drives its own snapshot lifecycle, so we
    // skip our scheduler — no point running two.
    // Without this, the WAL grows unbounded between operator-triggered
    // snapshots — which in practice means forever, leaving the next
    // cold recovery to replay everything since the dawn of the data
    // dir. Tunables are env-driven; see snapshot_scheduler.rs.
    let snapshot_cfg = SnapshotSchedulerConfig::from_env();
    // Mirror the exact conditions under which `run()` stays alive:
    // not in raft mode (raft drives its own snapshots), at least one
    // trigger configured, and a real WAL to snapshot. Computed from
    // the SAME config object we hand to `spawn` so the metric can't
    // drift from the scheduler's actual state.
    let snapshot_scheduler_enabled =
        raft_handle.is_none() && snapshot_cfg.any_trigger_enabled() && index.wal_stats().is_some();

    // Cluster role + peers parse up front so a typo fails boot rather
    // than silently degrading. The follower_cursor atomic is only
    // created when we're actually a follower — leaders and standalones
    // never need it. Parsed here (before the scheduler spawn) because
    // the role selects the scheduler's SnapshotMode (design 0007 §4)
    // and the follower hands its cursor to the FollowerSnapshot mode.
    let cluster = Arc::new(
        ClusterConfig::from_env()
            .map_err(|e| -> Box<dyn std::error::Error> { format!("cluster config: {e}").into() })?,
    );
    let follower_cursor: Option<Arc<FollowerCursor>> = if cluster.is_follower() {
        Some(Arc::new(FollowerCursor::default()))
    } else {
        None
    };

    // Select the snapshot strategy (design 0007 §4):
    //   * Follower → FollowerSnapshot: it owns snapshotting, stamping
    //     each snapshot with the leader-WAL seq it has applied
    //     (`follower_cursor`) into the shared dir.
    //   * Leader + NEBULA_SNAPSHOT_OFFLOAD on → LeaderCompactOnly: it
    //     never snapshots (never holds the index lock for minutes),
    //     only compacts its WAL against the follower's snapshots.
    //   * else (Standalone, or a leader with offload OFF) → Standalone:
    //     snapshot locally as today. A leader with offload OFF falls
    //     back to self-snapshotting so a misconfigured pair is still
    //     durable — the safe, current-behavior default.
    let snapshot_offload_on = std::env::var("NEBULA_SNAPSHOT_OFFLOAD")
        .map(|v| matches!(v.as_str(), "on" | "1" | "true" | "yes"))
        .unwrap_or(false);
    let snapshot_mode = match (cluster.role, follower_cursor.as_ref()) {
        (nebula_server::NodeRole::Follower, Some(cursor)) => {
            snapshot_scheduler::SnapshotMode::FollowerSnapshot {
                cursor: Arc::clone(cursor),
            }
        }
        (nebula_server::NodeRole::Leader, _) if snapshot_offload_on => {
            snapshot_scheduler::SnapshotMode::LeaderCompactOnly
        }
        _ => snapshot_scheduler::SnapshotMode::Standalone,
    };

    // Resource manager created BEFORE the snapshot scheduler so the
    // scheduler can defer triggers under memory pressure (design 0010
    // §6). The sampler task itself is spawned further down.
    let resource_manager = Arc::new(nebula_resource::ResourceManager::new(
        nebula_resource::Thresholds::default(),
    ));
    // Adaptive components: the embed cache shrinks 4x under
    // memory/disk pressure and restores on recovery.
    if let Some(cache) = embed_cache_handle {
        resource_manager.register_actuator(Arc::new(
            nebula_server::actuators::EmbedCacheActuator::new(cache),
        ));
    }

    let snapshot_handle = if raft_handle.is_some() {
        None
    } else {
        Some(snapshot_scheduler::spawn_with_resource(
            Arc::clone(&index),
            snapshot_cfg,
            snapshot_mode,
            Arc::clone(&resource_manager),
        ))
    };

    // Durability-metrics sampler (#69 hardening). The WAL/snapshot
    // gauges used to be computed synchronously on every `/metrics`
    // scrape — ~8s of blocking disk I/O over a multi-GB WAL (wal_stats,
    // two snapshot-header decodes, a per-segment fs::metadata walk) —
    // which timed out Prometheus and hammered the disk every 15s. Move
    // it off the hot path: a background task refreshes a shared atomic
    // cache every 30s; `/metrics` just loads the atomics.
    //
    // Only spawned for a persistent index (in-memory has no WAL, so the
    // cache stays at its all-zero default, which renders correctly).
    // The reads are blocking, so each refresh runs on `spawn_blocking`
    // — never blocking the async runtime, and the underlying index
    // calls only take short read locks / do disk I/O. An initial sample
    // runs immediately so the gauges aren't zero for the first 30s.
    let durability_cache = Arc::new(nebula_server::state::DurabilityMetricsCache::default());
    if index.is_persistent() {
        let cache = Arc::clone(&durability_cache);
        let idx = Arc::clone(&index);
        // Initial sample, off the runtime, before serving traffic.
        {
            let cache = Arc::clone(&cache);
            let idx = Arc::clone(&idx);
            let _ = tokio::task::spawn_blocking(move || cache.sample_from_index(&idx)).await;
        }
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(30));
            // Skip the immediate first tick; we already sampled above.
            tick.tick().await;
            loop {
                tick.tick().await;
                let cache = Arc::clone(&cache);
                let idx = Arc::clone(&idx);
                if let Err(e) =
                    tokio::task::spawn_blocking(move || cache.sample_from_index(&idx)).await
                {
                    tracing::warn!("durability-metrics sampler task panicked: {e}");
                }
            }
        });
    }

    // Resource manager + operating-mode sampler (design 0010 §4).
    // Probes cgroup memory, data-volume free space, and process CPU
    // every 5s from the blocking pool (statvfs/cgroup reads are file
    // I/O), feeding the hysteresis/debounce state machine that the
    // disk-critical write gate and /admin/reliability read lock-free.
    // The disk probe is only armed when a data dir exists — an
    // in-memory node has no WAL volume to protect.
    {
        let mgr = Arc::clone(&resource_manager);
        let data_dir = std::env::var("NEBULA_DATA_DIR")
            .ok()
            .filter(|d| !d.is_empty())
            .map(std::path::PathBuf::from);
        tokio::spawn(async move {
            let mut prober = nebula_resource::Prober::new(data_dir);
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                // The prober owns CPU-delta state, so it round-trips
                // through the blocking task each iteration.
                let mgr2 = Arc::clone(&mgr);
                match tokio::task::spawn_blocking(move || {
                    let s = prober.sample();
                    mgr2.observe(s);
                    prober
                })
                .await
                {
                    Ok(p) => prober = p,
                    Err(e) => {
                        tracing::warn!("resource sampler task panicked: {e}");
                        break;
                    }
                }
            }
        });
    }

    // Embedding worker (design 0010 §5): drains vector-pending
    // documents (deferred writes + circuit-open fallbacks) in batches.
    // Polls; embed_pending_batch is a no-op when nothing is pending.
    // Provider failures leave the batch pending for the next tick —
    // the embedder stack retries transients and fails fast when the
    // circuit is open, so a dead provider costs one cheap error per
    // tick, not a hung worker.
    {
        let idx = Arc::clone(&index);
        let mgr = Arc::clone(&resource_manager);
        let batch: usize = std::env::var("NEBULA_EMBED_WORKER_BATCH")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(64);
        let interval_secs: u64 = std::env::var("NEBULA_EMBED_WORKER_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));
            loop {
                tick.tick().await;
                if idx.pending_embedding_count() == 0 {
                    continue;
                }
                // Mode-aware pacing (design 0010 §6): completing a
                // pending doc APPENDS to the WAL, so DiskCritical
                // pauses the worker entirely (the write gate already
                // refuses client mutations; background work must not
                // sneak past it). Under other pressure modes the
                // batch shrinks 4x — embedding is background work and
                // yields to interactive traffic first.
                let batch = match mgr.mode() {
                    nebula_resource::OperatingMode::DiskCritical => continue,
                    nebula_resource::OperatingMode::Normal => batch,
                    _ => (batch / 4).max(1),
                };
                match idx.embed_pending_batch(batch).await {
                    Ok(0) => {}
                    Ok(n) => {
                        tracing::info!(
                            completed = n,
                            remaining = idx.pending_embedding_count(),
                            "embedding worker drained batch"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, pending = idx.pending_embedding_count(), "embedding worker batch failed; will retry");
                    }
                }
            }
        });
    }

    let api_keys: AHashSet<String> = std::env::var("NEBULA_API_KEYS")
        .unwrap_or_default()
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    // LLM selection is driven by env so one binary covers all
    // backends. Precedence: explicit OpenAI key > Ollama base URL >
    // mock default.
    // Streaming-body discipline: `timeout` is the connect timeout
    // only; `read_timeout` is byte-idle (60s without a token ⇒ fail
    // fast). The whole-response timeout that `reqwest::Client::timeout`
    // sets is intentionally absent — it would silently abort a long
    // generation mid-stream. Operators tune via env if needed.
    let llm_connect_secs: u64 = std::env::var("NEBULA_LLM_CONNECT_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let llm_read_secs: Option<u64> = std::env::var("NEBULA_LLM_READ_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .or(Some(60));
    let connect_timeout = Duration::from_secs(llm_connect_secs);
    let read_timeout = llm_read_secs.map(Duration::from_secs);

    let llm: Arc<dyn LlmClient> = if let Ok(key) = std::env::var("NEBULA_LLM_OPENAI_KEY") {
        let base = std::env::var("NEBULA_LLM_OPENAI_BASE")
            .unwrap_or_else(|_| "https://api.openai.com/v1".into());
        let model = std::env::var("NEBULA_LLM_MODEL").unwrap_or_else(|_| "gpt-4o-mini".into());
        let cfg = OpenAiChatConfig {
            base_url: base,
            api_key: Some(key),
            model,
            timeout: connect_timeout,
            read_timeout,
        };
        Arc::new(OpenAiChatLlm::new(cfg)?)
    } else if let Ok(base) = std::env::var("NEBULA_LLM_OLLAMA_URL") {
        let model = std::env::var("NEBULA_LLM_MODEL").unwrap_or_else(|_| "llama3".into());
        let cfg = OllamaConfig {
            base_url: base,
            model,
            timeout: connect_timeout,
            read_timeout,
        };
        Arc::new(OllamaLlm::new(cfg)?)
    } else {
        Arc::new(MockLlm::default())
    };

    // Reranker (design 0008 §7). Off unless NEBULA_RERANK_URL points at
    // a cross-encoder sidecar; the model never links into this binary.
    let reranker: Arc<dyn nebula_rerank::Reranker> =
        if let Ok(url) = std::env::var("NEBULA_RERANK_URL") {
            let req_secs: u64 = std::env::var("NEBULA_RERANK_TIMEOUT_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10);
            tracing::info!("rerank stage enabled → {url}");
            Arc::new(nebula_rerank::HttpReranker::new(
                url,
                connect_timeout,
                Duration::from_secs(req_secs),
            )?)
        } else {
            Arc::new(nebula_rerank::NoopReranker)
        };

    // Query expander (design 0008 §7). The built-in abbreviation
    // dictionary is enabled with NEBULA_QUERY_EXPAND=1; otherwise the
    // no-op expander leaves queries untouched.
    let query_expander: Arc<dyn nebula_rerank::QueryExpander> =
        if std::env::var("NEBULA_QUERY_EXPAND").as_deref() == Ok("1") {
            tracing::info!("query expansion enabled (built-in abbreviation map)");
            Arc::new(nebula_rerank::MapQueryExpander::with_defaults())
        } else {
            Arc::new(nebula_rerank::NoopQueryExpander)
        };

    // Per-collection hybrid fusion weights (design 0008 §9). The global
    // default is overridable via NEBULA_HYBRID_WEIGHTS="vector,bm25"
    // (e.g. "0.6,0.4"). Per-bucket overrides use
    // NEBULA_HYBRID_WEIGHTS_<BUCKET>="vector,bm25". Invalid values fall
    // back to the built-in 0.5/0.5 default rather than failing boot.
    let hybrid_weights = {
        let parse_pair = |s: &str| -> Option<(f32, f32)> {
            let (a, b) = s.split_once(',')?;
            Some((a.trim().parse().ok()?, b.trim().parse().ok()?))
        };
        let default = std::env::var("NEBULA_HYBRID_WEIGHTS")
            .ok()
            .and_then(|s| parse_pair(&s))
            .unwrap_or((0.5, 0.5));
        let mut hw = nebula_server::HybridWeights::new(default);
        // Scan env for per-bucket overrides.
        for (k, v) in std::env::vars() {
            if let Some(bucket) = k.strip_prefix("NEBULA_HYBRID_WEIGHTS_") {
                if let Some(pair) = parse_pair(&v) {
                    tracing::info!("hybrid weights for bucket {bucket}: {pair:?}");
                    hw = hw.with_bucket(bucket.to_lowercase(), pair);
                }
            }
        }
        Arc::new(hw)
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
    // from two env vars. NEBULA_TRUSTED_PROXIES enables honoring
    // X-Forwarded-For from listed peers (or "*" for all) so the
    // showcase nginx proxy doesn't lump every browser into one bucket.
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
        let trusted_proxies = match std::env::var("NEBULA_TRUSTED_PROXIES").ok().as_deref() {
            None | Some("") => TrustedProxies::None,
            Some("*") => TrustedProxies::All,
            Some(list) => {
                let ips: Vec<_> = list
                    .split(',')
                    .filter_map(|s| s.trim().parse().ok())
                    .collect();
                if ips.is_empty() {
                    TrustedProxies::None
                } else {
                    TrustedProxies::Only(ips)
                }
            }
        };
        (
            RateLimitConfig {
                capacity,
                refill_per_sec: refill,
                trusted_proxies,
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
        let mut eng = SqlEngine::new(Arc::clone(&index)).with_llm(Arc::clone(&llm));
        if let Some(cache) = sql_cache.as_ref() {
            eng = eng.with_cache(Arc::clone(cache));
        }
        eng
    });

    // Per-request timeout for non-streaming routes. Off by default
    // so existing slow ingestions (bulk upsert with cold embedder)
    // aren't cut short. Set NEBULA_REQUEST_TIMEOUT_SECS=30 in dev to
    // surface a stuck handler quickly.
    let request_timeout = std::env::var("NEBULA_REQUEST_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|n| *n > 0)
        .map(Duration::from_secs);

    let mut state = AppState::new(
        index,
        AppConfig {
            api_keys,
            jwt,
            rate_limit: rate_limit_cfg,
            request_timeout,
            chunk_chars,
            chunk_overlap,
            ..AppConfig::default()
        },
    )
    .with_llm(llm)
    .with_chunker(chunker)
    .with_reranker(reranker)
    .with_query_expander(query_expander)
    .with_hybrid_weights(hybrid_weights)
    .with_sql(Arc::clone(&sql_engine))
    .with_cluster(Arc::clone(&cluster))
    .with_log_bus(Arc::clone(&log_bus))
    .with_snapshot_scheduler_enabled(snapshot_scheduler_enabled)
    .with_durability_cache(Arc::clone(&durability_cache))
    .with_resource_manager(Arc::clone(&resource_manager))
    .with_ai_health(Arc::clone(&ai_health))
    // Class-aware admission (design 0010 §6). Budgets scale with the
    // worker count and shrink under resource pressure. Disable with
    // NEBULA_ADMISSION_CONTROL=off for load tests that want raw
    // saturation behavior.
    .with_admission(Arc::new(nebula_server::workload::AdmissionController::new(
        workers,
        Arc::clone(&resource_manager),
    )));
    if std::env::var("NEBULA_ADMISSION_CONTROL").as_deref() == Ok("off") {
        state.admission = None;
        tracing::info!("admission control disabled via NEBULA_ADMISSION_CONTROL=off");
    }
    if let Some(fc) = follower_cursor.as_ref() {
        state = state.with_follower_cursor(Arc::clone(fc));
    }
    if let Some(stats) = cache_stats {
        state = state.with_cache_stats(stats);
    }
    if let Some(rl) = rate_limiter {
        state = state.with_rate_limiter(rl);
    }
    if let Some(rh) = raft_handle.as_ref() {
        state = state.with_raft(Arc::clone(rh));
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
    // The single runtime-mutable role atomic, seeded from the boot role
    // by `with_cluster`. Cloned into the gRPC and pgwire servers so a
    // `POST /admin/promote` (which flips this atomic via the REST
    // AppState) lifts the write guard on all three protocols at once
    // (design 0009 §5).
    let shared_role = Arc::clone(&state.role);
    // Write-once cell the promote handler reads to abort the follower
    // tail task. Filled below once the follower is spawned (design 0009
    // §5). Captured here because `state` is moved into the router next.
    let follower_abort_cell = state.follower_abort_cell();
    // Status hub for cross-region: need a handle outside state before
    // we move state into the router, so the consumer task can report
    // progress into the same hub that /admin/replication reads.
    let cross_region_status = state.cross_region_status.clone();

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
            // When raft is configured, also hand the handle through so
            // DocumentService writes route through Raft consensus
            // (Phase 2.5f) — same model AppState uses on the REST side.
            let mut grpc_state = GrpcState::with_shared_role(
                grpc_index.clone(),
                grpc_llm,
                grpc_chunker,
                Arc::clone(&shared_role),
            );
            if let Some(rh) = raft_handle.as_ref() {
                grpc_state = grpc_state.with_raft(Arc::clone(rh));
            }
            let source_region = cluster.region.clone();
            tracing::info!(
                "nebula-grpc listening on {grpc_addr} (role={:?}, region={:?})",
                cluster.role,
                source_region,
            );
            Some(tokio::spawn(async move {
                if let Err(e) =
                    nebula_grpc::serve_with_region(grpc_state, grpc_addr, source_region).await
                {
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
            let pg_role = Arc::clone(&shared_role);
            let pg_region = cluster.region.clone();
            tracing::info!(
                "nebula-pgwire listening on {pg_addr} (role={:?}, region={pg_region:?})",
                pg_role.load(),
            );
            Some(tokio::spawn(async move {
                if let Err(e) = nebula_pgwire::serve_with_shared_role_and_region(
                    engine, pg_addr, pg_role, pg_region,
                )
                .await
                {
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
                // `connect_lazy` builds the channel without doing any
                // I/O. The first RPC drives the actual TCP/HTTP2
                // handshake; if it fails (leader still in WAL
                // recovery, network blip, restart in progress) the
                // follower's existing reconnect loop catches the
                // error, backs off, and tries again.
                //
                // Critically this avoids `.connect().await?` — the
                // eager variant — which would propagate the error to
                // `main`, exit the process, and combine with
                // `restart: unless-stopped` into a crashloop. We
                // observed exactly that on 192.168.1.193: 5-10
                // restarts/sec while the leader was recovering,
                // making the follower unavailable as a read backup
                // during the window where it mattered most.
                let channel =
                    tonic::transport::Channel::from_shared(leader.clone())?.connect_lazy();
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
                        (Some(durable), Some(snapshot)) => Some(Arc::new(TeeCursorStore {
                            durable,
                            snapshot: Arc::clone(snapshot),
                        })),
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
                                ) -> Result<(), nebula_grpc::follower::CursorStoreError>
                                {
                                    self.0.store(c.segment_seq, c.byte_offset);
                                    Ok(())
                                }
                            }
                            Some(Arc::new(SnapshotOnly(Arc::clone(snapshot))))
                        }
                        (Some(durable), None) => Some(durable),
                        (None, None) => None,
                    };
                let handle = nebula_grpc::follower::spawn_with_store(
                    channel,
                    Arc::clone(&grpc_index),
                    nebula_wal::WalCursor::BEGIN,
                    store,
                );
                // Publish the abort handle so `POST /admin/promote` can
                // stop tailing on promotion (design 0009 §5). set() only
                // fails if already set, which can't happen here (single
                // spawn) — ignore the result.
                let _ = follower_abort_cell.set(handle.abort_handle());
                Some(handle)
            }
            _ => None,
        };

    // Cross-region consumers. Only leader-role nodes subscribe to
    // remote regions — followers already mirror their home region's
    // leader and would double-apply if they tailed directly.
    // Populated only when NEBULA_REGION + NEBULA_CROSS_REGION_PEERS
    // are both set and role != follower.
    let cross_region_handles: Vec<tokio::task::JoinHandle<()>> =
        if matches!(
            cluster.role,
            nebula_server::NodeRole::Leader | nebula_server::NodeRole::Standalone
        ) && !cluster.cross_region_peers.is_empty()
        {
            let my_region = cluster.region_or_default().to_string();
            let peers: Vec<_> = cluster
                .cross_region_peers
                .iter()
                .map(|p| nebula_grpc::cross_region::RemotePeer {
                    region: p.region.clone(),
                    grpc_url: p.grpc_url.clone(),
                })
                .collect();
            let owned: Arc<dyn nebula_grpc::cross_region::OwnedBuckets> =
                Arc::new(nebula_grpc::cross_region::IndexOwnedBuckets {
                    index: Arc::clone(&grpc_index),
                    my_region: my_region.clone(),
                    seed_doc_id: nebula_server::SEED_DOC_ID,
                });
            let status: Arc<dyn nebula_grpc::cross_region::StatusSink> =
                Arc::new(cross_region_status.clone());
            tracing::info!(
                "cross-region: tailing {} peers from {}",
                peers.len(),
                my_region,
            );
            nebula_grpc::cross_region::spawn_all(
                peers,
                Arc::clone(&grpc_index),
                owned,
                status,
                my_region,
            )
        } else {
            Vec::new()
        };

    // Hand off from the boot stub to the real router. We ABORT the stub
    // task rather than gracefully draining it: axum 0.7's
    // `with_graceful_shutdown` blocks until every open connection closes,
    // and nginx's upstream keepalive pool keeps idle connections to :8080
    // open indefinitely — so a graceful drain hangs forever, the real
    // listener never binds, and the leader stays stuck serving 503
    // "recovering" even though recovery already finished. Aborting drops
    // the serve future, releasing the listener immediately; we await the
    // handle so the listener is fully dropped before we rebind. tokio's
    // `TcpListener::bind` sets SO_REUSEADDR so the rebind is immediate.
    boot_handle.abort();
    if let Err(e) = boot_handle.await {
        // Cancellation is expected (we just aborted it); only surface a
        // genuine panic in the stub task.
        if !e.is_cancelled() {
            tracing::warn!(error = ?e, "boot listener task panicked during handoff");
        }
    }
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
    // The snapshot scheduler's loop is `loop { sleep; ... }` — abort
    // cancels the sleep and drops the task. No in-flight work is at
    // risk: a snapshot in progress runs on spawn_blocking and will
    // finish on its own, just without us awaiting it.
    // None in raft mode (openraft drives its own snapshot lifecycle).
    if let Some(h) = snapshot_handle {
        h.abort();
    }
    for h in cross_region_handles {
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

fn tracing_subscriber_init(bus: Arc<LogBus>, stderr: bool) {
    // Capturing subscriber replaces the former noop. Every
    // `tracing::info!` / `warn!` / `error!` on a server task
    // flows through `bus` and out to SSE subscribers.
    //
    // When `stderr` is true the same events also go to the process's
    // stderr, so `docker logs` / journald operators see the recent
    // history without going through `/admin/logs/stream`. That
    // matters during runtime wedges — when the SSE endpoint isn't
    // reachable, the in-memory log is unhelpful by definition.
    //
    // Best-effort: if another subscriber was already installed
    // (tests can do this), swallow the error — the library has
    // always been tolerant of that and we keep booting.
    let _ =
        tracing::subscriber::set_global_default(CapturingSubscriber::new(bus).with_stderr(stderr));
}

/// Last-line-of-defense watchdog against silent tokio runtime wedges.
///
/// Two coupled actors:
/// 1. A tokio task that ticks every 5s and bumps a shared atomic
///    counter. Lives on the runtime, so if the runtime stops making
///    progress for any reason — parked workers, IO driver wedge,
///    blocking-pool deadlock — this tick stops.
/// 2. A *dedicated OS thread* that polls that counter every 15s.
///    This thread is **not** a tokio worker; it uses `std::thread::sleep`,
///    which is a kernel-level wait that runs regardless of what the
///    async runtime is doing. If the counter doesn't advance for
///    `NEBULA_WATCHDOG_STALL_SECS` seconds (default 300), it calls
///    `std::process::abort()`. SIGABRT (exit 134) lands in docker;
///    `restart: unless-stopped` picks the container back up.
///
/// This is NOT a fix for the underlying wedge — it bounds its blast
/// radius. The proper fix is to identify and remove the wedge source.
/// Tracked separately.
fn spawn_runtime_watchdog() {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    let stall_secs: u64 = std::env::var("NEBULA_WATCHDOG_STALL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|v: &u64| *v >= 30)
        .unwrap_or(300);

    let beat = Arc::new(AtomicU64::new(0));

    // (1) Heartbeat — on the tokio runtime. If the runtime wedges,
    // this future stops being polled and the counter stops advancing.
    let writer = Arc::clone(&beat);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        loop {
            tick.tick().await;
            writer.fetch_add(1, Ordering::Relaxed);
        }
    });

    // (2) Watcher — OS thread, NOT a tokio task. Survives any kind of
    // runtime wedge because it uses kernel sleep, not tokio sleep.
    let reader = beat;
    let stall_limit = Duration::from_secs(stall_secs);
    std::thread::Builder::new()
        .name("nebula-watchdog".into())
        .spawn(move || {
            let poll = Duration::from_secs(15);
            // Allow one grace window before declaring stall so a slow
            // boot (WAL recovery) doesn't trip us during startup.
            let mut last = 0u64;
            let mut stalled = Duration::ZERO;
            loop {
                std::thread::sleep(poll);
                let cur = reader.load(Ordering::Relaxed);
                if cur != last {
                    last = cur;
                    stalled = Duration::ZERO;
                    continue;
                }
                stalled += poll;
                if stalled >= stall_limit {
                    // Best-effort message to stderr — the tokio
                    // logging pipeline is by definition wedged here.
                    eprintln!(
                        "[WATCHDOG] tokio runtime hasn't beat in {}s — dumping forensics then aborting",
                        stalled.as_secs()
                    );
                    dump_forensics_to_stderr();
                    std::process::abort();
                }
            }
        })
        .expect("failed to spawn watchdog thread");
}

/// Dump every thread's state to stderr just before the watchdog
/// aborts. Two layers:
///
/// 1. Per-thread `/proc/self/task/*/{comm,wchan,status,syscall}`.
///    Always readable (no caps, no ptrace) and tells us:
///      - which thread is `R` (running) vs `S` (sleeping)
///      - which syscall every sleeper is parked in
///      - which kernel function each is waiting on (wchan)
///
/// 2. `gdb -batch -ex "thread apply all bt"` against our own PID.
///    Gives the user-space Rust backtrace for every thread, which is
///    what we actually need to identify the busy-spinning worker
///    that's wedging the runtime (the `/proc/.../stack` kernel
///    backtrace only shows scheduler ticks, not the Rust frame
///    actually running). Requires gdb in the image (added in the
///    runtime Dockerfile) and `cap_add: [SYS_PTRACE]` on the compose
///    service. If gdb isn't installed or ptrace is denied, this
///    silently fails and we still get the /proc dump above.
fn dump_forensics_to_stderr() {
    eprintln!("[WATCHDOG-DUMP] ==================== begin ====================");
    eprintln!(
        "[WATCHDOG-DUMP] pid={} time={:?}",
        std::process::id(),
        std::time::SystemTime::now()
    );

    // (1) /proc/self/task/* — works without root for own threads.
    if let Ok(entries) = std::fs::read_dir("/proc/self/task") {
        for entry in entries.flatten() {
            let tid = entry.file_name().to_string_lossy().to_string();
            let p = entry.path();
            let comm = std::fs::read_to_string(p.join("comm"))
                .unwrap_or_default()
                .trim()
                .to_string();
            let wchan = std::fs::read_to_string(p.join("wchan"))
                .unwrap_or_default()
                .trim()
                .to_string();
            let state = std::fs::read_to_string(p.join("status"))
                .unwrap_or_default()
                .lines()
                .find(|l| l.starts_with("State:"))
                .unwrap_or("")
                .trim()
                .to_string();
            let syscall = std::fs::read_to_string(p.join("syscall"))
                .unwrap_or_default()
                .trim()
                .to_string();
            eprintln!(
                "[WATCHDOG-DUMP] tid={tid} comm={comm} {state} wchan={wchan} syscall={syscall}"
            );
        }
    } else {
        eprintln!("[WATCHDOG-DUMP] (could not read /proc/self/task)");
    }

    // (2) gdb thread apply all bt — gives user-space Rust frames.
    // 30s timeout: prior 5s budget timed out after gdb finished
    // enumerating threads but before it could walk any stack
    // (observed in wedge at 2026-05-15T20:09:39Z, exit status 124).
    // 30s comfortably covers a 17-thread backtrace at 12 frames each
    // even on a slow box; if gdb genuinely can't attach the call
    // returns much faster than that, so the budget is mostly slack.
    let pid = std::process::id().to_string();
    let gdb = std::process::Command::new("timeout")
        .args([
            "30",
            "gdb",
            "-batch",
            "-nx",
            "-p",
            &pid,
            "-ex",
            "set pagination off",
            "-ex",
            "set print thread-events off",
            // Shorter per-thread bt (12 vs 30) so the total dump
            // fits in the timeout. The top 12 frames are enough
            // to identify the call path on every stack we care
            // about (axum/tokio/hyper paths cap out well below 12).
            "-ex",
            "thread apply all bt 12",
            "-ex",
            "detach",
            "-ex",
            "quit",
        ])
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status();
    match gdb {
        Ok(s) if s.success() => {
            eprintln!("[WATCHDOG-DUMP] gdb completed");
        }
        Ok(s) => {
            eprintln!(
                "[WATCHDOG-DUMP] gdb exited {} (likely missing or ptrace denied)",
                s
            );
        }
        Err(e) => {
            eprintln!(
                "[WATCHDOG-DUMP] gdb spawn failed: {} — install `gdb` + cap_add SYS_PTRACE",
                e
            );
        }
    }
    eprintln!("[WATCHDOG-DUMP] ===================== end =====================");
}
