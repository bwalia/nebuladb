//! Shared server state.
//!
//! Cloned cheaply by Axum into every handler — everything inside is
//! already wrapped in `Arc` so clone is a refcount bump, not a deep copy.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ahash::AHashSet;

use nebula_cache::CacheStats;
use nebula_chunk::{Chunker, FixedSizeChunker};
use nebula_index::TextIndex;
use nebula_llm::{LlmClient, MockLlm};
use nebula_rerank::{NoopQueryExpander, NoopReranker, QueryExpander, Reranker};
use nebula_sql::SqlEngine;

use crate::audit::AuditLog;
use crate::cluster::ClusterConfig;
use crate::jwt::JwtConfig;
use crate::log_stream::LogBus;
use crate::metrics::Metrics;
use crate::ratelimit::{RateLimitConfig, RateLimiter};
use crate::slow_log::SlowQueryLog;

/// Published position of the follower's last-applied cursor, so the
/// admin endpoint can compute lag without going through the running
/// follower task. Two atomics rather than a Mutex<WalCursor> because
/// this gets read on every /admin/replication hit and written on
/// every applied record — same hot path as cursor persistence.
///
/// Reads aren't consistent across the two atomics in isolation (a
/// reader could see a new segment_seq paired with an old
/// byte_offset for a single instant), but the consumer only uses
/// (seg, off) to compute "how far behind am I" and a temporary
/// under/over-estimate by one record is noise.
#[derive(Default, Debug)]
pub struct FollowerCursor {
    pub segment_seq: AtomicU64,
    pub byte_offset: AtomicU64,
}

impl FollowerCursor {
    pub fn load(&self) -> (u64, u64) {
        (
            self.segment_seq.load(Ordering::Relaxed),
            self.byte_offset.load(Ordering::Relaxed),
        )
    }

    pub fn store(&self, segment_seq: u64, byte_offset: u64) {
        self.segment_seq.store(segment_seq, Ordering::Relaxed);
        self.byte_offset.store(byte_offset, Ordering::Relaxed);
    }
}

/// Cached snapshot of the WAL/snapshot durability gauges (#69), kept
/// off the `/metrics` hot path. A background sampler in `main.rs`
/// recomputes these every ~30s via `spawn_blocking` (each refresh does
/// ~seconds of blocking disk I/O over a multi-GB WAL); `/metrics` then
/// just loads these atomics instead of walking the WAL on every scrape.
///
/// All-zero defaults are the correct "in-memory / not yet sampled"
/// state: `wal_bytes` / `wal_bytes_since_snapshot` of 0 render as the
/// real in-memory values, and `snapshot_taken_at_ms == 0` means "no
/// snapshot exists" so the `nebula_snapshot_age_seconds` line is
/// omitted (a bogus multi-decade age would falsely trip the alert).
#[derive(Default, Debug)]
pub struct DurabilityMetricsCache {
    /// Total bytes in the WAL on disk. Mirrors `wal_stats().total_bytes`.
    pub wal_bytes: AtomicU64,
    /// Bytes in WAL segments at/after the newest snapshot's seq.
    /// Mirrors `index.wal_bytes_since_snapshot()`.
    pub wal_bytes_since_snapshot: AtomicU64,
    /// Unix millis the newest committed snapshot was taken, or `0` when
    /// no snapshot exists. Used as the "has snapshot" sentinel.
    pub snapshot_taken_at_ms: AtomicU64,
}

impl DurabilityMetricsCache {
    /// Recompute the three cached values from a (persistent) index and
    /// store them. Does the blocking disk I/O inline — callers must run
    /// it off the async runtime (`spawn_blocking` / a std thread). The
    /// underlying index calls take only short read locks / do disk I/O.
    ///
    /// `latest_snapshot_header()` is called exactly ONCE here (the old
    /// inline render path called it twice).
    pub fn sample_from_index(&self, index: &nebula_index::TextIndex) {
        let wal_bytes = index.wal_stats().map(|w| w.total_bytes).unwrap_or(0);
        let bytes_since = index.wal_bytes_since_snapshot().unwrap_or(0);
        let taken_at_ms = index
            .latest_snapshot_header()
            .map(|h| h.taken_at_ms)
            .unwrap_or(0);
        self.wal_bytes.store(wal_bytes, Ordering::Relaxed);
        self.wal_bytes_since_snapshot
            .store(bytes_since, Ordering::Relaxed);
        self.snapshot_taken_at_ms
            .store(taken_at_ms, Ordering::Relaxed);
    }

    /// Load all three cached values: `(wal_bytes, wal_bytes_since_snapshot,
    /// snapshot_taken_at_ms)`. `snapshot_taken_at_ms == 0` means none.
    pub fn load(&self) -> (u64, u64, u64) {
        (
            self.wal_bytes.load(Ordering::Relaxed),
            self.wal_bytes_since_snapshot.load(Ordering::Relaxed),
            self.snapshot_taken_at_ms.load(Ordering::Relaxed),
        )
    }
}

/// Tee adapter: splits a `CursorStore::save` into the real durable
/// store + an atomic snapshot read by /admin/replication. Load
/// comes only from the durable half — the atomic is a pure mirror
/// and starts empty across restarts.
pub struct TeeCursorStore {
    pub durable: Arc<dyn nebula_grpc::follower::CursorStore>,
    pub snapshot: Arc<FollowerCursor>,
}

impl nebula_grpc::follower::CursorStore for TeeCursorStore {
    fn load(
        &self,
    ) -> Result<Option<nebula_wal::WalCursor>, nebula_grpc::follower::CursorStoreError> {
        self.durable.load()
    }

    fn save(
        &self,
        cursor: nebula_wal::WalCursor,
    ) -> Result<(), nebula_grpc::follower::CursorStoreError> {
        // Write to the snapshot first — it's infallible and we
        // want the /admin/replication view updated even if the
        // durable store is temporarily flaky.
        self.snapshot.store(cursor.segment_seq, cursor.byte_offset);
        self.durable.save(cursor)
    }
}

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
    /// Optional per-request timeout applied to non-streaming routes.
    /// `None` keeps the historical "let it run" behavior. SSE streams
    /// (`/ai/rag`, `/admin/logs/stream`) bypass this regardless.
    pub request_timeout: Option<Duration>,
    /// Target chunk size (Unicode scalars) used when a `/documents`
    /// request asks for a `doc_type`-specific chunking strategy
    /// (design 0008 §8). Mirrors the default chunker's window so
    /// per-request strategy selection stays size-consistent.
    pub chunk_chars: usize,
    /// Overlap (scalars) for the same per-request strategy selection.
    pub chunk_overlap: usize,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            // 16 MiB. Picks up bulk-upsert batches (up to 1000 items
            // × a few KB of text each) without being large enough for
            // an abusive client to OOM the server.
            max_body_bytes: 16 * 1024 * 1024,
            api_keys: AHashSet::new(),
            jwt: None,
            default_ef_search: 64,
            max_top_k: 100,
            rate_limit: RateLimitConfig::default(),
            request_timeout: None,
            chunk_chars: 500,
            chunk_overlap: 50,
        }
    }
}

/// Per-collection (per-bucket) hybrid fusion weights `(vector, bm25)`
/// for hybrid retrieval (design 0008 §9). A global default applies to
/// every bucket unless a per-bucket override is registered. Weights are
/// applied as-is by the fusion stage and need not sum to 1.
#[derive(Debug, Clone)]
pub struct HybridWeights {
    default: (f32, f32),
    per_bucket: std::collections::HashMap<String, (f32, f32)>,
}

impl Default for HybridWeights {
    fn default() -> Self {
        // Even split between dense and lexical signals — a sane
        // out-of-the-box choice; operators tune per bucket below.
        Self {
            default: (0.5, 0.5),
            per_bucket: std::collections::HashMap::new(),
        }
    }
}

impl HybridWeights {
    pub fn new(default: (f32, f32)) -> Self {
        Self {
            default,
            per_bucket: std::collections::HashMap::new(),
        }
    }

    /// Register an override for one bucket. Chainable.
    pub fn with_bucket(mut self, bucket: impl Into<String>, weights: (f32, f32)) -> Self {
        self.per_bucket.insert(bucket.into(), weights);
        self
    }

    /// Resolve the weights for a query. A `None` bucket (search-all) or
    /// a bucket with no override falls back to the default.
    pub fn resolve(&self, bucket: Option<&str>) -> (f32, f32) {
        bucket
            .and_then(|b| self.per_bucket.get(b).copied())
            .unwrap_or(self.default)
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
    /// Top-N slowest SQL queries seen since boot. Unlike `audit`,
    /// this is a priority queue (hall of fame) rather than a
    /// rolling window — operators want to find the worst offenders,
    /// not the most recent ones.
    pub slow_log: Arc<SlowQueryLog>,
    pub config: Arc<AppConfig>,
    /// Cluster membership + role. Standalone by default; every
    /// multi-node feature reads this instead of sniffing envs
    /// on its own. The `role` field here is the boot-time *initial*
    /// role; the runtime-authoritative role is [`Self::role`] (an
    /// atomic shared with the gRPC and pgwire write-guards) so a
    /// `POST /admin/promote` flips all three protocols at once.
    pub cluster: Arc<ClusterConfig>,
    /// Runtime-mutable node role, shared (same `Arc`) with the gRPC
    /// services and pgwire handler so promotion (design 0009 §5) is
    /// observed on every protocol's write path simultaneously. Seeded
    /// from `cluster.role` at construction.
    pub role: Arc<nebula_core::AtomicNodeRole>,
    /// Aborts the background follower replication task. `POST
    /// /admin/promote` uses it so a promoted node stops tailing its
    /// former leader before it starts accepting writes (design 0009 §5).
    /// A shared, write-once cell because the follower task is spawned
    /// *after* this state is moved into the router — `main.rs` fills it
    /// once the handle exists. `None` inside the cell on a node that
    /// never started as a follower.
    pub follower_abort: Arc<std::sync::OnceLock<tokio::task::AbortHandle>>,
    /// Present when this node is a follower. The background
    /// replication task writes its latest applied cursor here;
    /// `/admin/replication` reads it to compute lag vs. the
    /// leader. `None` on leader / standalone.
    pub follower_cursor: Option<Arc<FollowerCursor>>,
    /// Live log fan-out + recent-events ring. The `tracing`
    /// subscriber pushes events here; `/admin/logs/stream` reads
    /// them out as SSE. Always present so the endpoint doesn't
    /// need to worry about None.
    pub log_bus: Arc<LogBus>,
    /// Cross-region consumer status keyed by remote region name.
    /// The background task (Phase 2.2) updates this; admin endpoints
    /// read it. Always present — an empty hub means single-region.
    pub cross_region_status: crate::cross_region_status::CrossRegionStatusHub,
    /// In-process status ring for /admin/backup and /admin/restore
    /// jobs. Capacity is fixed at 50 in `JobRing`; production
    /// historical view comes from object storage, not this ring.
    pub backup_jobs: Arc<crate::backup_routes::JobRing>,
    /// Raft handle when this node booted in raft mode (NEBULA_RAFT_PEERS
    /// configured). `None` for standalone / legacy single-leader mode.
    /// Phase 2.5c will gate write-path routing on this — when present,
    /// REST/pgwire/gRPC writes go through `Raft::client_write`.
    pub raft: Option<Arc<nebula_raft::RaftHandle>>,
    /// Whether the background snapshot scheduler has at least one
    /// trigger enabled. Computed from the SAME `SnapshotSchedulerConfig`
    /// that's used to spawn the scheduler (see `main.rs`), so it can't
    /// drift from reality. Surfaced as `nebula_snapshot_scheduler_enabled`
    /// on `/metrics` to catch the "Bug C" footgun where an obsolete
    /// `.env` silently disables snapshots and a later cold recovery
    /// takes hours. Defaults to `false` (tests / in-memory dev runs
    /// that never spawn a scheduler).
    pub snapshot_scheduler_enabled: bool,
    /// Cached WAL/snapshot durability gauges (#69). A background
    /// sampler in `main.rs` refreshes these every ~30s off the hot
    /// path; `/metrics` reads them instantly. Defaults to all-zero
    /// (the correct "in-memory / not yet sampled" state). Shared via
    /// `Arc` so the sampler task and the handler see the same atomics.
    pub durability_cache: Arc<DurabilityMetricsCache>,
    /// Post-retrieval reranker (design 0008 §7). Defaults to
    /// [`NoopReranker`] (pass-through); production swaps in an
    /// [`HttpReranker`] pointed at a cross-encoder sidecar via
    /// [`Self::with_reranker`]. Always present so handlers don't branch
    /// on `Option`.
    pub reranker: Arc<dyn Reranker>,
    /// Pre-retrieval query expander (design 0008 §7). Defaults to
    /// [`NoopQueryExpander`]; [`Self::with_query_expander`] swaps in a
    /// dictionary or LLM-backed expander.
    pub query_expander: Arc<dyn QueryExpander>,
    /// Per-collection hybrid fusion weights (design 0008 §9). Defaults
    /// to an even 0.5/0.5 split for every bucket; override globally or
    /// per-bucket via [`Self::with_hybrid_weights`].
    pub hybrid_weights: Arc<HybridWeights>,
    /// Circuit-breaker health for the embedding provider (design 0010
    /// §5): the node's `ai_degraded` flag plus open/reject counters.
    /// `None` when no breaker is wired (tests, or a bare MockEmbedder
    /// state) — the observability endpoints then omit the AI-health
    /// lines rather than reporting a misleading healthy-forever zero.
    pub ai_health: Option<Arc<nebula_embed::AiHealth>>,
    /// Class-aware admission controller (design 0010 §6). `None`
    /// (tests / dev default) admits everything; `main.rs` wires one
    /// sized to the worker count. Reads the resource manager's mode,
    /// so budgets shrink automatically under pressure.
    pub admission: Option<Arc<crate::workload::AdmissionController>>,
    /// Resource manager + operating-mode state machine (design 0010).
    /// A background sampler in `main.rs` feeds it every ~5s; the
    /// disk-critical write gate and the reliability endpoints read it
    /// lock-free. Always present — with no sampler running it stays
    /// in `Normal` forever, which is the correct in-memory/test
    /// behavior.
    pub resource: Arc<nebula_resource::ResourceManager>,
    /// TTL cache + single-flight gate for `/admin/buckets`.
    /// `bucket_stats` is a full corpus scan under the index read
    /// lock; the showcase Admin tab polls every ~3s, and at
    /// multi-million-doc scale each scan outlives the poll interval —
    /// scans stack without bound and the lock is held read
    /// continuously, wedging writers (and, via writer priority,
    /// every new reader: SQL stops responding). The handler holds
    /// this async mutex across the scan so at most ONE scan runs at
    /// a time, and serves the cached result inside the TTL. Keyed by
    /// the `top_keys` request param so a non-default introspection
    /// call can't poison the UI's cache.
    pub bucket_stats_cache: Arc<tokio::sync::Mutex<Option<BucketStatsCacheEntry>>>,
}

/// One cached `/admin/buckets` response. See
/// [`AppState::bucket_stats_cache`].
pub struct BucketStatsCacheEntry {
    pub at: std::time::Instant,
    pub top_keys: usize,
    pub stats: Vec<nebula_index::BucketStats>,
}

impl AppState {
    /// Default setup: mock LLM, 500/50 fixed-size chunker, no embedding
    /// cache. Tests and dev runs use this; production swaps the LLM
    /// via [`Self::with_llm`], the chunker via [`Self::with_chunker`],
    /// and registers cache stats via [`Self::with_cache_stats`].
    pub fn new(index: Arc<TextIndex>, config: AppConfig) -> Self {
        // Default SQL engine wraps the same index; no result cache
        // unless a caller wires one in via `with_sql`. The LLM is shared
        // with the SQL engine so `ai_answer(...)` works out of the box.
        let llm: Arc<dyn LlmClient> = Arc::new(MockLlm::default());
        let sql = Arc::new(SqlEngine::new(Arc::clone(&index)).with_llm(Arc::clone(&llm)));
        Self {
            index,
            llm,
            chunker: Arc::new(FixedSizeChunker::default()),
            metrics: Arc::new(Metrics::default()),
            cache_stats: None,
            rate_limiter: None,
            sql,
            audit: AuditLog::new(1024),
            // Top 32 slowest since boot, threshold 10ms so MockEmbedder
            // noise stays out. Override via with_slow_log() for bespoke
            // sizing in tests or production tuning.
            slow_log: SlowQueryLog::new(32, 10),
            config: Arc::new(config),
            cluster: Arc::new(ClusterConfig::default()),
            role: Arc::new(nebula_core::AtomicNodeRole::default()),
            follower_abort: Arc::new(std::sync::OnceLock::new()),
            follower_cursor: None,
            log_bus: Arc::new(LogBus::default()),
            cross_region_status: crate::cross_region_status::CrossRegionStatusHub::new(),
            backup_jobs: Arc::new(crate::backup_routes::JobRing::new()),
            raft: None,
            snapshot_scheduler_enabled: false,
            durability_cache: Arc::new(DurabilityMetricsCache::default()),
            reranker: Arc::new(NoopReranker),
            query_expander: Arc::new(NoopQueryExpander),
            hybrid_weights: Arc::new(HybridWeights::default()),
            ai_health: None,
            admission: None,
            resource: Arc::new(nebula_resource::ResourceManager::new(
                nebula_resource::Thresholds::default(),
            )),
            bucket_stats_cache: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    pub fn with_snapshot_scheduler_enabled(mut self, enabled: bool) -> Self {
        self.snapshot_scheduler_enabled = enabled;
        self
    }

    /// Swap in a post-retrieval reranker (e.g. an HTTP cross-encoder).
    pub fn with_reranker(mut self, reranker: Arc<dyn Reranker>) -> Self {
        self.reranker = reranker;
        self
    }

    /// Swap in a pre-retrieval query expander.
    pub fn with_query_expander(mut self, expander: Arc<dyn QueryExpander>) -> Self {
        self.query_expander = expander;
        self
    }

    /// Set per-collection hybrid fusion weights (design 0008 §9).
    pub fn with_hybrid_weights(mut self, weights: Arc<HybridWeights>) -> Self {
        self.hybrid_weights = weights;
        self
    }

    /// Wire in a shared durability-metrics cache (the one the
    /// background sampler in `main.rs` updates). Tests can also use
    /// this to seed the cache before scraping `/metrics`.
    pub fn with_durability_cache(mut self, cache: Arc<DurabilityMetricsCache>) -> Self {
        self.durability_cache = cache;
        self
    }

    pub fn with_raft(mut self, raft: Arc<nebula_raft::RaftHandle>) -> Self {
        self.raft = Some(raft);
        self
    }

    pub fn with_log_bus(mut self, bus: Arc<LogBus>) -> Self {
        self.log_bus = bus;
        self
    }

    pub fn with_cluster(mut self, cluster: Arc<ClusterConfig>) -> Self {
        // Seed the runtime-mutable role from the boot-time role so the
        // atomic and the cluster config agree at startup. Promotion
        // later flips only the atomic (design 0009 §5).
        self.role = Arc::new(nebula_core::AtomicNodeRole::new(cluster.role));
        self.cluster = cluster;
        self
    }

    /// Share the runtime role atomic with the gRPC / pgwire servers so
    /// a promotion flips every protocol's write-guard at once. Call
    /// AFTER [`Self::with_cluster`] (which creates the atomic seeded
    /// from the boot role); `main.rs` clones this `Arc` into the other
    /// servers.
    pub fn with_role(mut self, role: Arc<nebula_core::AtomicNodeRole>) -> Self {
        self.role = role;
        self
    }

    pub fn with_follower_cursor(mut self, cursor: Arc<FollowerCursor>) -> Self {
        self.follower_cursor = Some(cursor);
        self
    }

    /// Share the write-once follower-abort cell. `main.rs` clones this
    /// before moving the state into the router, then fills it once the
    /// follower task is spawned, so `POST /admin/promote` can stop
    /// tailing before accepting writes (design 0009 §5).
    pub fn follower_abort_cell(&self) -> Arc<std::sync::OnceLock<tokio::task::AbortHandle>> {
        Arc::clone(&self.follower_abort)
    }

    pub fn with_slow_log(mut self, log: Arc<SlowQueryLog>) -> Self {
        self.slow_log = log;
        self
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

    /// Wire in the class-aware admission controller (design 0010 §6).
    pub fn with_admission(mut self, controller: Arc<crate::workload::AdmissionController>) -> Self {
        self.admission = Some(controller);
        self
    }

    /// Wire in the embedder circuit-breaker health handle so
    /// `/healthz`, `/metrics`, and `/admin/reliability` report AI
    /// degradation (design 0010 §5).
    pub fn with_ai_health(mut self, health: Arc<nebula_embed::AiHealth>) -> Self {
        self.ai_health = Some(health);
        self
    }

    /// Share the resource manager the background sampler feeds, so
    /// the write gate and `/admin/reliability` observe live pressure.
    pub fn with_resource_manager(mut self, manager: Arc<nebula_resource::ResourceManager>) -> Self {
        self.resource = manager;
        self
    }
}
