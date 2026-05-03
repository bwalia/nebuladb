//! Shared server state.
//!
//! Cloned cheaply by Axum into every handler — everything inside is
//! already wrapped in `Arc` so clone is a refcount bump, not a deep copy.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ahash::AHashSet;

use nebula_cache::CacheStats;
use nebula_chunk::{Chunker, FixedSizeChunker};
use nebula_index::TextIndex;
use nebula_llm::{LlmClient, MockLlm};
use nebula_sql::SqlEngine;

use crate::audit::AuditLog;
use crate::cluster::ClusterConfig;
use crate::jwt::JwtConfig;
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
    /// Top-N slowest SQL queries seen since boot. Unlike `audit`,
    /// this is a priority queue (hall of fame) rather than a
    /// rolling window — operators want to find the worst offenders,
    /// not the most recent ones.
    pub slow_log: Arc<SlowQueryLog>,
    pub config: Arc<AppConfig>,
    /// Cluster membership + role. Standalone by default; every
    /// multi-node feature reads this instead of sniffing envs
    /// on its own.
    pub cluster: Arc<ClusterConfig>,
    /// Present when this node is a follower. The background
    /// replication task writes its latest applied cursor here;
    /// `/admin/replication` reads it to compute lag vs. the
    /// leader. `None` on leader / standalone.
    pub follower_cursor: Option<Arc<FollowerCursor>>,
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
            // Top 32 slowest since boot, threshold 10ms so MockEmbedder
            // noise stays out. Override via with_slow_log() for bespoke
            // sizing in tests or production tuning.
            slow_log: SlowQueryLog::new(32, 10),
            config: Arc::new(config),
            cluster: Arc::new(ClusterConfig::default()),
            follower_cursor: None,
        }
    }

    pub fn with_cluster(mut self, cluster: Arc<ClusterConfig>) -> Self {
        self.cluster = cluster;
        self
    }

    pub fn with_follower_cursor(mut self, cursor: Arc<FollowerCursor>) -> Self {
        self.follower_cursor = Some(cursor);
        self
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
}
