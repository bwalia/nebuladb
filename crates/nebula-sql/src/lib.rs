//! SQL surface for NebulaDB with native AI extensions.
//!
//! This crate does one thing: turn a subset of SQL extended with two
//! AI-native functions — `semantic_match(column, 'query')` and
//! `vector_distance(column, [vector])` — into calls against the
//! existing [`nebula_index::TextIndex`].
//!
//! # Scope
//!
//! Intentionally narrow so the code stays honest:
//!
//! - One table per query (the bucket name).
//! - `SELECT [cols] FROM <bucket> WHERE [semantic_match|filters] [ORDER BY score|...] [LIMIT n]`.
//! - No JOINs, no aggregates, no DML, no subqueries. Those belong in
//!   a real query engine and are huge on their own; layering them on
//!   top of a broken foundation is worse than not having them.
//!
//! # Grammar extensions
//!
//! We use `sqlparser-rs` in GenericDialect. It already accepts
//! arbitrary function calls — we just interpret them specially:
//!
//! - `semantic_match(column_name, 'query text')` — produces a
//!   retrieval step seeded by embedding the literal. Required when
//!   the query is to run against the vector index at all.
//! - `vector_distance(column_name, [f1, f2, ...])` — same idea but
//!   with a raw vector literal; skips the embedder.
//!
//! Any other WHERE predicates become residual filters applied to the
//! retrieved hits' metadata. That's how `WHERE semantic_match(...)
//! AND region = 'eu'` works end-to-end.
//!
//! # Layering
//!
//! ```text
//! POST /api/v1/query (JSON {sql})
//!           │
//!           ▼
//!     SqlEngine::run   ── semantic result cache ──┐
//!           │                                     │
//!     parser::parse   (sqlparser-rs)              │
//!           │                                     │
//!     plan::build     (our AST walker)            │
//!           │                                     │
//!     executor::run   (embed → HNSW → filter)     │
//!           │                                     │
//!           └───────── QueryResult ───────────────┘
//! ```

pub mod cache;
pub mod error;
pub mod executor;
pub mod parser;
pub mod plan;
pub mod plan_tree;

use std::sync::Arc;

use nebula_index::TextIndex;
use nebula_llm::LlmClient;

pub use cache::SemanticCache;
pub use error::SqlError;
pub use executor::{Executor, QueryResult};
pub use plan::{Plan, SemanticClause};
pub use plan_tree::{AggregateFn, AggregateSpec, AnswerPlan, JoinPlan, JoinPredicate, QueryPlan};

pub type Result<T> = std::result::Result<T, SqlError>;

/// High-level entry point. A thin facade that glues parser, planner,
/// executor, and optional result cache.
pub struct SqlEngine {
    index: Arc<TextIndex>,
    cache: Option<Arc<SemanticCache>>,
    /// Optional LLM for `ai_answer(...)`. Wired in by the server which
    /// already owns an `LlmClient`; absent in lightweight/test setups.
    llm: Option<Arc<dyn LlmClient>>,
}

impl SqlEngine {
    pub fn new(index: Arc<TextIndex>) -> Self {
        Self {
            index,
            cache: None,
            llm: None,
        }
    }

    pub fn with_cache(mut self, cache: Arc<SemanticCache>) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Attach the LLM used to synthesize `ai_answer(...)` responses.
    pub fn with_llm(mut self, llm: Arc<dyn LlmClient>) -> Self {
        self.llm = Some(llm);
        self
    }

    /// Parse → plan → execute a single SQL statement.
    ///
    /// The cache (if configured) is consulted before planning. We key
    /// on `(sql, embedder_model)` because two different models would
    /// produce different embeddings for the same text, so cached
    /// results under one model are invalid under another.
    pub async fn run(&self, sql: &str) -> Result<QueryResult> {
        let stmt = parser::parse(sql)?;
        let plan = plan_tree::build(stmt)?;

        // The result cache keys on `(sql, embedder_model)` — valid for
        // retrieval, but an `ai_answer` result also depends on the LLM,
        // which the key doesn't capture. Skip the cache entirely for
        // the answer path rather than risk serving an answer generated
        // by a since-swapped model.
        let cacheable = !matches!(plan, QueryPlan::Answer(_));

        if cacheable {
            if let Some(cache) = &self.cache {
                let key = cache.key(sql, self.index.embedder_model());
                if let Some(hit) = cache.get(&key) {
                    return Ok(hit);
                }
            }
        }

        let mut exec = Executor::new(Arc::clone(&self.index));
        if let Some(llm) = &self.llm {
            exec = exec.with_llm(Arc::clone(llm));
        }
        let out = exec.run(plan).await?;

        if cacheable {
            if let Some(cache) = &self.cache {
                let key = cache.key(sql, self.index.embedder_model());
                cache.put(key, out.clone());
            }
        }
        Ok(out)
    }

    /// Parse + plan a statement without running it. Returns the typed
    /// plan tree so operators can inspect the shape the executor
    /// would see — which semantic clause is picked, how WHERE splits
    /// across a join, whether overfetch kicks in, etc. Consciously
    /// does NOT consult or update the result cache: EXPLAIN is a
    /// debugging tool and mustn't warm caches for queries that never
    /// run.
    pub fn explain(&self, sql: &str) -> Result<QueryPlan> {
        let stmt = parser::parse(sql)?;
        plan_tree::build(stmt)
    }
}
