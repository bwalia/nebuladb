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
mod explain;
pub mod parser;
pub mod plan;
pub mod plan_tree;

use std::sync::Arc;
use std::time::{Duration, Instant};

use nebula_index::explain::{Explain, ExplainKind, Note, Stage};
use nebula_index::TextIndex;
use nebula_llm::LlmClient;
use sqlparser::ast;

pub use cache::SemanticCache;
pub use error::SqlError;
pub use executor::{Executor, QueryResult};
pub use plan::{Plan, SemanticClause};
pub use plan_tree::{AggregateFn, AggregateSpec, AnswerPlan, JoinPlan, JoinPredicate, QueryPlan};

pub type Result<T> = std::result::Result<T, SqlError>;

/// The filtering stage that removed the largest share of its input,
/// if any removed anything. Retrieval stages are skipped: HNSW's
/// "input" is the whole corpus, which would always win.
fn biggest_drop(stages: &[Stage]) -> Option<&Stage> {
    stages
        .iter()
        .filter(|s| matches!(s.name.as_str(), "bucket_filter" | "filter" | "limit" | "join"))
        .filter_map(|s| match (s.rows_in, s.rows_out) {
            (Some(i), Some(o)) if i > o => Some((s, (i - o) as f64 / i as f64)),
            _ => None,
        })
        .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(s, _)| s)
}

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
        Ok(self.execute(sql, false).await?.0)
    }

    /// [`Self::run`] with EXPLAIN support:
    ///
    /// - `EXPLAIN ANALYZE <select>` runs the query and returns its
    ///   explanation as `QUERY PLAN` text rows (Postgres parity — works
    ///   over pgwire too), plus the structured [`Explain`].
    /// - `EXPLAIN <select>` plans without executing anything.
    /// - `explain = true` runs `<select>` normally but also returns an
    ///   [`Explain`] alongside its real rows.
    ///
    /// Explained runs bypass the result cache: a cached answer would
    /// explain nothing about how the result is produced.
    pub async fn execute(&self, sql: &str, explain: bool) -> Result<(QueryResult, Option<Explain>)> {
        let started = Instant::now();
        let stmt = parser::parse(sql)?;
        let parse_took = started.elapsed();

        if let ast::Statement::Explain { describe_alias, analyze, statement, .. } = stmt {
            if describe_alias != ast::DescribeAlias::Explain {
                return Err(SqlError::Unsupported(format!("{describe_alias}")));
            }
            let ex = if analyze {
                self.analyze(*statement, parse_took, started).await?.1
            } else {
                Self::plan_only(*statement)?
            };
            let rows = ex
                .text
                .iter()
                .map(|line| executor::Row {
                    id: String::new(),
                    bucket: String::new(),
                    score: 0.0,
                    fields: serde_json::json!({ "QUERY PLAN": line }),
                })
                .collect();
            let result = QueryResult { took_ms: started.elapsed().as_millis() as u64, rows };
            return Ok((result, Some(ex)));
        }
        if explain {
            let (result, ex) = self.analyze(stmt, parse_took, started).await?;
            return Ok((result, Some(ex)));
        }

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
                    return Ok((hit, None));
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
        Ok((out, None))
    }

    /// EXPLAIN ANALYZE: plan, run with a recorder, summarize.
    async fn analyze(
        &self,
        stmt: ast::Statement,
        parse_took: Duration,
        started: Instant,
    ) -> Result<(QueryResult, Explain)> {
        let t = Instant::now();
        let plan = plan_tree::build(stmt)?;
        let describe = explain::describe(&plan);
        let mut ex = Explain::new(ExplainKind::Sql, true);
        ex.plan = serde_json::to_value(&plan).ok();
        ex.stages.push(Stage::new("parse", "Parse", "Parsed the statement.").took(parse_took));
        ex.stages.push(Stage::new("plan", "Plan", format!("{describe}.")).took(t.elapsed()));
        if self.cache.is_some() && !matches!(plan, QueryPlan::Answer(_)) {
            ex.notes.push(Note::info(
                "The result cache was bypassed so every stage actually ran; a normal run of this \
                 query may be served from cache.",
            ));
        }

        let mut exec = Executor::new(Arc::clone(&self.index));
        if let Some(llm) = &self.llm {
            exec = exec.with_llm(Arc::clone(llm));
        }
        let mut rec = Some(explain::Recorder::new(ex));
        let out = exec.run_recorded(plan, &mut rec).await?;
        let mut ex = rec.map(|r| r.ex).expect("recorder is always Some here");

        let n = out.rows.len();
        let mut summary = format!("{describe}. Returned {n} row{}.", if n == 1 { "" } else { "s" });
        if let Some(st) = biggest_drop(&ex.stages) {
            summary.push_str(&format!(
                " Most rows were dropped by {} ({} → {}).",
                st.label,
                st.rows_in.unwrap_or(0),
                st.rows_out.unwrap_or(0)
            ));
        }
        ex.finish(summary, started.elapsed());
        Ok((out, ex))
    }

    /// Plain EXPLAIN: describe the stages without running anything.
    fn plan_only(stmt: ast::Statement) -> Result<Explain> {
        let plan = plan_tree::build(stmt)?;
        let mut ex = Explain::new(ExplainKind::Sql, false);
        ex.plan = serde_json::to_value(&plan).ok();
        explain::plan_only(&plan, &mut ex);
        let summary = format!(
            "{}. Nothing was executed — use EXPLAIN ANALYZE to run it and see row counts and \
             timings.",
            explain::describe(&plan)
        );
        ex.finish(summary, Duration::ZERO);
        Ok(ex)
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
