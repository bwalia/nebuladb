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

pub use cache::SemanticCache;
pub use error::SqlError;
pub use executor::{Executor, QueryResult};
pub use plan::{Plan, SemanticClause};
pub use plan_tree::{AggregateFn, AggregateSpec, JoinPlan, JoinPredicate, QueryPlan};

pub type Result<T> = std::result::Result<T, SqlError>;

/// High-level entry point. A thin facade that glues parser, planner,
/// executor, and optional result cache.
pub struct SqlEngine {
    index: Arc<TextIndex>,
    cache: Option<Arc<SemanticCache>>,
}

impl SqlEngine {
    pub fn new(index: Arc<TextIndex>) -> Self {
        Self { index, cache: None }
    }

    pub fn with_cache(mut self, cache: Arc<SemanticCache>) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Parse → plan → execute a single SQL statement.
    ///
    /// The cache (if configured) is consulted before planning. We key
    /// on `(sql, embedder_model)` because two different models would
    /// produce different embeddings for the same text, so cached
    /// results under one model are invalid under another.
    pub async fn run(&self, sql: &str) -> Result<QueryResult> {
        if let Some(cache) = &self.cache {
            let key = cache.key(sql, self.index.embedder_model());
            if let Some(hit) = cache.get(&key) {
                return Ok(hit);
            }
        }

        let stmt = parser::parse(sql)?;
        let plan = plan_tree::build(stmt)?;
        let exec = Executor::new(Arc::clone(&self.index));
        let out = exec.run(plan).await?;

        if let Some(cache) = &self.cache {
            let key = cache.key(sql, self.index.embedder_model());
            cache.put(key, out.clone());
        }
        Ok(out)
    }
}
