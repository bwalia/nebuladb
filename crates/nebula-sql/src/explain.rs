//! `EXPLAIN` / `EXPLAIN ANALYZE` for the SQL surface.
//!
//! The executor threads an optional [`Recorder`] through the same code
//! path it always runs; when present, each step appends a
//! [`Stage`] saying what it did to the rows. That is what makes the
//! explanation trustworthy: there is no separate "explain executor" to
//! drift from the real one. [`plan_only`] describes a plan without
//! running it (`EXPLAIN` without `ANALYZE`).

use std::collections::HashMap;
use std::time::Duration;

use nebula_index::explain::{Explain, FilterCheck, HitExplain, Note, SearchTrace, Stage};
use nebula_index::Hit;

use crate::executor::{eval_filter, path_lookup};
use crate::plan::{Filter, OrderBy, OrderDir, OrderKey, Plan, Projection, SemanticClause};
use crate::plan_tree::{AggregateFn, AggregatePlan, AnswerPlan, JoinPlan, QueryPlan};

/// Distinct values listed when a filter matched nothing.
const SEEN_VALUES_SHOWN: usize = 5;

pub(crate) struct Recorder {
    pub ex: Explain,
    /// Retrieval-stage explanations by hit id, joined to final rows.
    hits: HashMap<String, HitExplain>,
}

impl Recorder {
    pub fn new(ex: Explain) -> Self {
        Self { ex, hits: HashMap::new() }
    }

    /// Fold an index retrieval trace in. `side` prefixes stage labels
    /// for joins ("left", "right"); empty for single scans.
    pub fn absorb(&mut self, trace: SearchTrace, side: &str) {
        for mut st in trace.stages {
            if !side.is_empty() {
                st.label = format!("{side}: {}", st.label);
            }
            self.ex.stages.push(st);
        }
        // The same note (e.g. the mock-embedder warning) from both sides
        // of a join is noise.
        for n in trace.notes {
            if !self.ex.notes.iter().any(|m| m.message == n.message) {
                self.ex.notes.push(n);
            }
        }
        for h in trace.hits {
            self.hits.insert(h.id.clone(), h);
        }
    }

    pub fn stage(&mut self, st: Stage) {
        self.ex.stages.push(st);
    }

    pub fn note(&mut self, n: Note) {
        self.ex.notes.push(n);
    }

    /// Residual WHERE filters: per-predicate pass counts, plus the
    /// diagnostics people actually need when a filter starves results —
    /// the field doesn't exist, or its values are spelled differently.
    pub fn filters(&mut self, plan: &Plan, hits_in: &[Hit], kept: usize, took: Duration, side: &str) {
        if plan.filters.is_empty() {
            return;
        }
        let counts: Vec<(String, usize)> = plan
            .filters
            .iter()
            .map(|f| {
                let n = hits_in.iter().filter(|h| eval_filter(f, &h.metadata)).count();
                (predicate_sql(f), n)
            })
            .collect();
        let per = counts
            .iter()
            .map(|(p, n)| format!("{p}: {n}"))
            .collect::<Vec<_>>()
            .join(", ");
        let label = prefixed(side, "Filter (WHERE)");
        self.stage(
            Stage::new(
                "filter",
                &label,
                format!(
                    "Kept retrieved rows satisfying every residual predicate — {kept} of {} \
                     passed ({per}). These run after the semantic search, on its candidates only.",
                    hits_in.len()
                ),
            )
            .rows(Some(hits_in.len()), Some(kept))
            .took(took)
            .attr(
                "predicates",
                counts
                    .iter()
                    .map(|(p, n)| serde_json::json!({ "predicate": p, "passed": n }))
                    .collect::<Vec<_>>(),
            ),
        );

        for (f, (pred, n)) in plan.filters.iter().zip(&counts) {
            if *n > 0 || hits_in.is_empty() {
                continue;
            }
            let path = filter_path(f);
            let mut seen: HashMap<String, usize> = HashMap::new();
            for h in hits_in {
                let v = path_lookup(&h.metadata, path);
                if !v.is_null() {
                    *seen.entry(sql_literal(v)).or_insert(0) += 1;
                }
            }
            if seen.is_empty() {
                let mut keys: Vec<String> = hits_in
                    .iter()
                    .filter_map(|h| h.metadata.as_object())
                    .flat_map(|m| m.keys().cloned())
                    .collect();
                keys.sort();
                keys.dedup();
                self.note(Note::warn(format!(
                    "No retrieved row has a metadata field '{}', so `{pred}` can never match. \
                     Fields present: {}.",
                    path.join("."),
                    if keys.is_empty() { "none".to_string() } else { keys.join(", ") }
                )));
            } else {
                let mut seen: Vec<(String, usize)> = seen.into_iter().collect();
                seen.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
                let shown = seen
                    .iter()
                    .take(SEEN_VALUES_SHOWN)
                    .map(|(v, c)| format!("{v} ×{c}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                self.note(Note::warn(format!(
                    "None of the {} retrieved rows satisfies `{pred}`. Values seen for {}: {shown}. \
                     Comparisons are exact and case-sensitive.",
                    hits_in.len(),
                    path.join(".")
                )));
            }
        }

        if let Some(limit) = plan.limit {
            if kept < limit && hits_in.len() >= plan.top_k() {
                self.note(Note::warn(format!(
                    "Filter starvation: only {kept} of the {} candidates the semantic search \
                     returned pass the WHERE filters, short of LIMIT {limit}. Filters can only see \
                     those candidates — raise LIMIT (retrieval fetches 4× LIMIT when filters are \
                     present) or make the semantic_match text more specific.",
                    hits_in.len()
                )));
            }
        }
    }

    pub fn sort(&mut self, order: Option<&OrderBy>, rows: usize, took: Duration) {
        let (what, explicit) = match order {
            Some(ob) => (order_sql(ob), true),
            None => ("score ASC".to_string(), false),
        };
        self.stage(
            Stage::new(
                "sort",
                "Sort",
                format!(
                    "Ordered by {what}{}.",
                    if explicit {
                        score_hint(order)
                    } else {
                        " — the default: vector distance, closest first".to_string()
                    }
                ),
            )
            .rows(Some(rows), Some(rows))
            .took(took)
            .attr("order_by", what),
        );
    }

    pub fn limit(&mut self, limit: Option<usize>, rows_in: usize, rows_out: usize) {
        if let Some(n) = limit {
            self.stage(
                Stage::new("limit", "Limit", format!("Kept the first {n} rows."))
                    .rows(Some(rows_in), Some(rows_out))
                    .attr("limit", n),
            );
        }
    }

    /// Projection, final per-row explanations, and a note for columns
    /// that are null in every row (almost always a misspelt field).
    pub fn project(&mut self, plan: &Plan, kept_hits: &[Hit], took: Duration) {
        let detail = match &plan.projection {
            Projection::All => "Returned every field: text plus the full metadata object.".to_string(),
            Projection::Columns(cols) => format!("Returned columns {}.", cols.join(", ")),
        };
        self.stage(
            Stage::new("project", "Project (SELECT list)", detail)
                .rows(Some(kept_hits.len()), Some(kept_hits.len()))
                .took(took),
        );
        if let Projection::Columns(cols) = &plan.projection {
            if !kept_hits.is_empty() {
                for col in cols {
                    if matches!(col.as_str(), "id" | "bucket" | "text" | "score") {
                        continue;
                    }
                    let path: Vec<String> = col.split('.').map(str::to_string).collect();
                    if kept_hits.iter().all(|h| path_lookup(&h.metadata, &path).is_null()) {
                        self.note(Note::info(format!(
                            "Column '{col}' is null in every returned row — it isn't a metadata \
                             field of these documents."
                        )));
                    }
                }
            }
        }
        self.ex.hits = kept_hits
            .iter()
            .enumerate()
            .map(|(i, h)| {
                let mut e = self.hits.get(&h.id).cloned().unwrap_or_else(|| HitExplain {
                    id: h.id.clone(),
                    rank: 0,
                    final_score: h.score,
                    score_kind: nebula_index::explain::ScoreKind::Distance,
                    vector: None,
                    bm25: None,
                    filters: None,
                });
                e.rank = i + 1;
                e.final_score = h.score;
                if !plan.filters.is_empty() {
                    e.filters = Some(
                        plan.filters
                            .iter()
                            .map(|f| FilterCheck {
                                predicate: predicate_sql(f),
                                actual: path_lookup(&h.metadata, filter_path(f)).clone(),
                                passed: eval_filter(f, &h.metadata),
                            })
                            .collect(),
                    );
                }
                e
            })
            .collect();
    }
}

fn prefixed(side: &str, label: &str) -> String {
    if side.is_empty() {
        label.to_string()
    } else {
        format!("{side}: {label}")
    }
}

fn score_hint(order: Option<&OrderBy>) -> String {
    match order {
        Some(OrderBy { key: OrderKey::Score, dir: OrderDir::Asc }) => {
            " — vector distance, closest first".to_string()
        }
        Some(OrderBy { key: OrderKey::Score, dir: OrderDir::Desc }) => {
            " — vector distance, farthest first".to_string()
        }
        _ => String::new(),
    }
}

pub(crate) fn order_sql(ob: &OrderBy) -> String {
    let key = match &ob.key {
        OrderKey::Score => "score".to_string(),
        OrderKey::Metadata(p) => p.join("."),
    };
    let dir = match ob.dir {
        OrderDir::Asc => "ASC",
        OrderDir::Desc => "DESC",
    };
    format!("{key} {dir}")
}

fn filter_path(f: &Filter) -> &[String] {
    match f {
        Filter::Eq { path, .. } | Filter::In { path, .. } => path,
    }
}

pub(crate) fn predicate_sql(f: &Filter) -> String {
    match f {
        Filter::Eq { path, value } => format!("{} = {}", path.join("."), sql_literal(value)),
        Filter::In { path, values } => format!(
            "{} IN ({})",
            path.join("."),
            values.iter().map(sql_literal).collect::<Vec<_>>().join(", ")
        ),
    }
}

fn sql_literal(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        other => other.to_string(),
    }
}

fn retrieval_sql(s: &SemanticClause) -> String {
    match s {
        SemanticClause::Match { column, query } => {
            format!("semantic_match({column}, {})", sql_literal(&serde_json::Value::String(query.clone())))
        }
        SemanticClause::Distance { column, vector } => {
            format!("vector_distance({column}, <{}-d vector>)", vector.len())
        }
    }
}

/// One-line description of a plan, used for the "plan" stage and as the
/// start of the summary.
pub(crate) fn describe(plan: &QueryPlan) -> String {
    match plan {
        QueryPlan::Scan(p) => describe_scan(p),
        QueryPlan::Aggregate(a) => format!(
            "{}, grouped by ({}) computing {}",
            describe_scan(&a.input),
            a.group_keys.join(", "),
            aggs_sql(a)
        ),
        QueryPlan::Join(j) => format!(
            "Inner hash join {} ⋈ {} on {}.{} = {}.{}",
            j.left.bucket, j.right.bucket, j.left_alias, j.predicate.left_column, j.right_alias,
            j.predicate.right_column
        ),
        QueryPlan::Answer(a) => format!(
            "ai_answer: retrieve {} chunk{} {} and ask the LLM",
            a.top_k,
            if a.top_k == 1 { "" } else { "s" },
            a.bucket.as_deref().map_or("from every bucket".to_string(), |b| format!("from '{b}'"))
        ),
    }
}

fn describe_scan(p: &Plan) -> String {
    let mut s = format!("Scan '{}' by {}", p.bucket, retrieval_sql(&p.semantic));
    if !p.filters.is_empty() {
        s.push_str(&format!(
            ", then filter {}",
            p.filters.iter().map(predicate_sql).collect::<Vec<_>>().join(" AND ")
        ));
    }
    if let Some(ob) = &p.order_by {
        s.push_str(&format!(", order by {}", order_sql(ob)));
    }
    if let Some(n) = p.limit {
        s.push_str(&format!(", limit {n}"));
    }
    s
}

fn aggs_sql(a: &AggregatePlan) -> String {
    a.aggs
        .iter()
        .map(|s| {
            let f = match &s.func {
                AggregateFn::CountStar => "COUNT(*)".to_string(),
                AggregateFn::Count(c) => format!("COUNT({c})"),
                AggregateFn::Sum(c) => format!("SUM({c})"),
                AggregateFn::Avg(c) => format!("AVG({c})"),
                AggregateFn::Min(c) => format!("MIN({c})"),
                AggregateFn::Max(c) => format!("MAX({c})"),
            };
            format!("{f} AS {}", s.alias)
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Why the scan asks the index for `top_k` candidates.
pub(crate) fn top_k_reason(p: &Plan) -> String {
    let base = p.limit.unwrap_or(10);
    let base_why = if p.limit.is_some() { format!("LIMIT {base}") } else { "the default 10 (no LIMIT)".to_string() };
    if p.filters.is_empty() {
        format!("{} candidates — {base_why}", p.top_k())
    } else {
        format!(
            "{} candidates — {base_why} × 4, at least 32, because {} WHERE filter{} run after retrieval",
            p.top_k(),
            p.filters.len(),
            if p.filters.len() == 1 { "" } else { "s" }
        )
    }
}

/// Plan-only EXPLAIN: the stages a plan *would* run, nothing executed.
pub(crate) fn plan_only(plan: &QueryPlan, ex: &mut Explain) {
    match plan {
        QueryPlan::Scan(p) => scan_plan_stages(p, "", ex),
        QueryPlan::Aggregate(a) => {
            scan_plan_stages(&a.input, "", ex);
            ex.stages.push(Stage::new(
                "aggregate",
                "Aggregate (GROUP BY)",
                format!(
                    "Group the filtered rows by ({}) and compute {} — over the retrieved \
                     candidates only, not the whole bucket.",
                    a.group_keys.join(", "),
                    aggs_sql(a)
                ),
            ));
            if let Some(ob) = &a.order_by {
                let dir = if ob.dir == OrderDir::Desc { "DESC" } else { "ASC" };
                ex.stages.push(Stage::new("sort", "Sort", format!("Order groups by {} {dir}.", ob.key)));
            }
            if let Some(n) = a.limit {
                ex.stages.push(Stage::new("limit", "Limit", format!("Keep the first {n} groups.")));
            }
        }
        QueryPlan::Join(j) => join_plan_stages(j, ex),
        QueryPlan::Answer(a) => answer_plan_stages(a, ex),
    }
}

fn scan_plan_stages(p: &Plan, side: &str, ex: &mut Explain) {
    let (label, detail) = match &p.semantic {
        SemanticClause::Match { query, .. } => (
            "Embed + vector search (HNSW)",
            format!(
                "Embed '{query}' and fetch the nearest {} from the HNSW graph.",
                top_k_reason(p)
            ),
        ),
        SemanticClause::Distance { vector, .. } => (
            "Vector search (HNSW)",
            format!("Search with the given {}-d vector for {}.", vector.len(), top_k_reason(p)),
        ),
    };
    ex.stages.push(Stage::new("hnsw", &prefixed(side, label), detail).attr("top_k", p.top_k()));
    ex.stages.push(Stage::new(
        "bucket_filter",
        &prefixed(side, "Bucket filter"),
        format!(
            "Keep candidates from bucket '{}'. The index over-fetches 4× (at least 32) to leave \
             room for this post-filter.",
            p.bucket
        ),
    ));
    if !p.filters.is_empty() {
        ex.stages.push(Stage::new(
            "filter",
            &prefixed(side, "Filter (WHERE)"),
            format!(
                "Keep rows satisfying {} — evaluated on the retrieved candidates only.",
                p.filters.iter().map(predicate_sql).collect::<Vec<_>>().join(" AND ")
            ),
        ));
    }
    if side.is_empty() {
        let order = p.order_by.as_ref().map_or("score ASC (default)".to_string(), order_sql);
        ex.stages.push(Stage::new("sort", "Sort", format!("Order by {order}.")));
        if let Some(n) = p.limit {
            ex.stages.push(Stage::new("limit", "Limit", format!("Keep the first {n} rows.")));
        }
        let cols = match &p.projection {
            Projection::All => "every field (text + metadata)".to_string(),
            Projection::Columns(c) => c.join(", "),
        };
        ex.stages.push(Stage::new("project", "Project (SELECT list)", format!("Return {cols}.")));
    }
}

fn join_plan_stages(j: &JoinPlan, ex: &mut Explain) {
    scan_plan_stages(&j.left, "left", ex);
    scan_plan_stages(&j.right, "right", ex);
    ex.stages.push(Stage::new(
        "join",
        "Hash join",
        format!(
            "Hash the right rows on {}.{}, probe with each left row's {}.{}; emit one row per match.",
            j.right_alias, j.predicate.right_column, j.left_alias, j.predicate.left_column
        ),
    ));
    if let Some(ob) = &j.order_by {
        ex.stages.push(Stage::new("sort", "Sort", format!("Order by {}.", order_sql(ob))));
    }
    if let Some(n) = j.limit {
        ex.stages.push(Stage::new("limit", "Limit", format!("Keep the first {n} rows.")));
    }
}

fn answer_plan_stages(a: &AnswerPlan, ex: &mut Explain) {
    ex.stages.push(Stage::new(
        "hnsw",
        "Embed + vector search (HNSW)",
        format!(
            "Embed '{}' and fetch the {} nearest chunk{} {}.",
            a.query,
            a.top_k,
            if a.top_k == 1 { "" } else { "s" },
            a.bucket.as_deref().map_or("from every bucket".to_string(), |b| format!("from '{b}'"))
        ),
    ));
    ex.stages.push(Stage::new("prompt", "Build prompt", "Number the chunks as context ahead of the question."));
    ex.stages.push(Stage::new("llm", "Generate answer (LLM)", "Stream the answer from the configured LLM."));
}
