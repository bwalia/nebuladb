//! Typed execution plan.
//!
//! Building a typed plan — instead of walking raw SQL AST inside the
//! executor — keeps the parser/executor boundary narrow. The executor
//! doesn't care how `LIMIT 10` got there; only that `Plan::limit` is
//! `Some(10)`. This also makes plan-level tests trivially easy.

use serde::Serialize;

/// One retrieval clause. Only one is allowed per query today — mixing
/// semantic and vector retrieval would require a merge strategy
/// (reciprocal rank fusion, weighted sum, etc.) that we punt on until
/// there's a real use case driving the choice.
#[derive(Debug, Clone, PartialEq)]
pub enum SemanticClause {
    /// `semantic_match(column, 'query text')`.
    /// The column isn't used by the executor yet — every column of a
    /// text document maps to the same underlying vector — but we
    /// preserve it for a future multi-column corpus where different
    /// fields get indexed separately.
    Match {
        column: String,
        query: String,
    },
    /// `vector_distance(column, [f1, f2, ...])`.
    Distance {
        column: String,
        vector: Vec<f32>,
    },
}

/// A residual WHERE predicate applied after retrieval against each
/// hit's `metadata` JSON. Intentionally tiny: equality and IN, no
/// boolean tree. Complex predicates can be added without changing
/// the planner shape — only this enum and the filter evaluator grow.
#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    /// `metadata_path = literal`. `path` uses `.`-separated JSON
    /// pointer notation; a one-segment path is just a top-level key.
    Eq { path: Vec<String>, value: serde_json::Value },
    /// `metadata_path IN (literals)`.
    In { path: Vec<String>, values: Vec<serde_json::Value> },
}

/// Column projection. `*` and specific fields; we don't do renames,
/// arithmetic, or function calls. Use the raw metadata/text verbatim.
#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    /// `SELECT *` — return full hits.
    All,
    /// `SELECT col1, col2, ...` — return an object with just those
    /// keys pulled from the hit. A missing key produces `null`.
    Columns(Vec<String>),
}

/// Ordering key. `score` targets the ANN distance returned by the
/// index (ascending = most similar first — distance semantics). Other
/// keys resolve against metadata.
#[derive(Debug, Clone, PartialEq)]
pub enum OrderKey {
    Score,
    Metadata(Vec<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum OrderDir {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
    pub key: OrderKey,
    pub dir: OrderDir,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PlanSerializable {
    pub bucket: String,
    pub top_k: usize,
}

/// The whole query plan. One retrieval clause + zero or more filters
/// + projection + optional ORDER BY + optional LIMIT.
#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    /// FROM target — maps to a NebulaDB bucket.
    pub bucket: String,
    pub semantic: SemanticClause,
    pub filters: Vec<Filter>,
    pub projection: Projection,
    pub order_by: Option<OrderBy>,
    pub limit: Option<usize>,
}

impl Plan {
    /// `top_k` passed to the vector index. Always at least the final
    /// LIMIT — we used to over-fetch automatically, but that hid
    /// pagination bugs. If LIMIT is absent, pick a sensible default.
    pub fn top_k(&self) -> usize {
        // Overfetch a bit when filters are present so post-filtering
        // has enough candidates to hit the limit. This is cheap
        // compared to a second round-trip to the index.
        let base = self.limit.unwrap_or(10);
        if self.filters.is_empty() {
            base
        } else {
            base.saturating_mul(4).max(32)
        }
    }
}

pub use crate::parser::parse;
use crate::{Result, SqlError};
use sqlparser::ast;

/// Lower a parsed SQL AST into a [`Plan`]. Rejects anything outside
/// the supported subset with a clear error.
pub fn build(stmt: ast::Statement) -> Result<Plan> {
    let ast::Statement::Query(q) = stmt else {
        return Err(SqlError::Unsupported(
            "only SELECT is supported".into(),
        ));
    };
    if q.with.is_some() {
        return Err(SqlError::Unsupported("WITH / CTE".into()));
    }
    let ast::SetExpr::Select(sel) = *q.body else {
        return Err(SqlError::Unsupported(
            "set operations (UNION, INTERSECT, EXCEPT)".into(),
        ));
    };

    // FROM <bucket>. Exactly one table, no joins.
    if sel.from.len() != 1 {
        return Err(SqlError::Unsupported(
            "exactly one table in FROM is required".into(),
        ));
    }
    let twj = &sel.from[0];
    if !twj.joins.is_empty() {
        return Err(SqlError::Unsupported("JOIN".into()));
    }
    let bucket = table_name(&twj.relation)?;

    // Projection.
    let projection = build_projection(&sel.projection)?;

    // WHERE — pull semantic clause + residual filters.
    let (semantic, filters) = match &sel.selection {
        Some(expr) => split_where(expr)?,
        None => {
            return Err(SqlError::InvalidPlan(
                "WHERE must include semantic_match(...) or vector_distance(...)".into(),
            ));
        }
    };

    // ORDER BY. Optional; defaults to score ascending (HNSW returns
    // distance, smaller = closer).
    let order_by = build_order_by(&q.order_by)?;

    // LIMIT.
    let limit = match q.limit.as_ref() {
        None => None,
        Some(ast::Expr::Value(ast::Value::Number(n, _))) => Some(
            n.parse::<usize>()
                .map_err(|_| SqlError::TypeError(format!("LIMIT must be a positive integer: {n}")))?,
        ),
        Some(other) => {
            return Err(SqlError::Unsupported(format!(
                "non-literal LIMIT: {other}"
            )));
        }
    };

    Ok(Plan {
        bucket,
        semantic,
        filters,
        projection,
        order_by,
        limit,
    })
}

/// Build a single-bucket scan where the caller has already chosen
/// which WHERE predicates / projection / order-by / limit apply to
/// this side. Used by JOIN and aggregate planners, which split the
/// original WHERE and ORDER BY across multiple scans.
///
/// `inherited_where` must already be an AND-chain that includes one
/// semantic clause and any residual filters for this bucket only.
pub(crate) fn build_scan_from_parts(
    rel: &ast::TableFactor,
    projection: Projection,
    inherited_where: Option<&ast::Expr>,
    order_by: Option<OrderBy>,
    limit: Option<usize>,
) -> Result<Plan> {
    let bucket = table_name(rel)?;
    let (semantic, filters) = match inherited_where {
        Some(expr) => split_where(expr)?,
        None => {
            return Err(SqlError::InvalidPlan(
                "scan requires WHERE with semantic_match(...) or vector_distance(...)".into(),
            ));
        }
    };
    Ok(Plan {
        bucket,
        semantic,
        filters,
        projection,
        order_by,
        limit,
    })
}

pub(crate) fn table_name(rel: &ast::TableFactor) -> Result<String> {
    match rel {
        ast::TableFactor::Table { name, .. } => {
            let parts: Vec<String> = name.0.iter().map(|i| i.value.clone()).collect();
            if parts.len() > 1 {
                return Err(SqlError::Unsupported(
                    "schema-qualified table names".into(),
                ));
            }
            Ok(parts.into_iter().next().unwrap())
        }
        other => Err(SqlError::Unsupported(format!(
            "unsupported FROM source: {other}"
        ))),
    }
}

pub(crate) fn build_projection(items: &[ast::SelectItem]) -> Result<Projection> {
    let mut cols = Vec::new();
    for item in items {
        match item {
            ast::SelectItem::Wildcard(_) => return Ok(Projection::All),
            ast::SelectItem::UnnamedExpr(ast::Expr::Identifier(id)) => {
                cols.push(id.value.clone());
            }
            ast::SelectItem::UnnamedExpr(ast::Expr::CompoundIdentifier(idents)) => {
                // Join compound identifiers with '.' to match our
                // metadata-path notation.
                cols.push(
                    idents
                        .iter()
                        .map(|i| i.value.clone())
                        .collect::<Vec<_>>()
                        .join("."),
                );
            }
            other => {
                return Err(SqlError::Unsupported(format!(
                    "projection item: {other}"
                )));
            }
        }
    }
    Ok(Projection::Columns(cols))
}

/// Partition a WHERE expression into (semantic clause, residual
/// filters). The only legal top-level shape is a chain of ANDs — any
/// OR in the WHERE clause is rejected because evaluating OR against
/// ANN retrieval requires a merge strategy we don't have yet.
pub(crate) fn split_where(expr: &ast::Expr) -> Result<(SemanticClause, Vec<Filter>)> {
    let mut leaves: Vec<&ast::Expr> = Vec::new();
    collect_and_leaves(expr, &mut leaves)?;

    let mut semantic: Option<SemanticClause> = None;
    let mut filters: Vec<Filter> = Vec::new();

    for leaf in leaves {
        if let Some(clause) = try_semantic(leaf)? {
            if semantic.is_some() {
                return Err(SqlError::InvalidPlan(
                    "only one semantic_match / vector_distance allowed per query".into(),
                ));
            }
            semantic = Some(clause);
        } else if let Some(f) = try_filter(leaf)? {
            filters.push(f);
        } else {
            return Err(SqlError::Unsupported(format!(
                "unsupported predicate in WHERE: {leaf}"
            )));
        }
    }

    let semantic = semantic.ok_or_else(|| {
        SqlError::InvalidPlan(
            "WHERE must contain exactly one semantic_match(...) or vector_distance(...)".into(),
        )
    })?;

    Ok((semantic, filters))
}

fn collect_and_leaves<'a>(expr: &'a ast::Expr, out: &mut Vec<&'a ast::Expr>) -> Result<()> {
    match expr {
        ast::Expr::BinaryOp {
            op: ast::BinaryOperator::And,
            left,
            right,
        } => {
            collect_and_leaves(left, out)?;
            collect_and_leaves(right, out)?;
        }
        ast::Expr::BinaryOp {
            op: ast::BinaryOperator::Or,
            ..
        } => {
            return Err(SqlError::Unsupported(
                "OR in WHERE is not supported (ANN retrieval has no well-defined disjunction)"
                    .into(),
            ));
        }
        ast::Expr::Nested(inner) => collect_and_leaves(inner, out)?,
        _ => out.push(expr),
    }
    Ok(())
}

fn try_semantic(expr: &ast::Expr) -> Result<Option<SemanticClause>> {
    let ast::Expr::Function(f) = expr else {
        return Ok(None);
    };
    let name = f.name.to_string().to_lowercase();
    let args: Vec<&ast::Expr> = match &f.args {
        ast::FunctionArguments::List(list) => list
            .args
            .iter()
            .map(|a| match a {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Ok(e),
                _ => Err(SqlError::Unsupported(
                    "named / wildcard function args".into(),
                )),
            })
            .collect::<Result<Vec<_>>>()?,
        _ => return Ok(None),
    };

    match name.as_str() {
        "semantic_match" => {
            if args.len() != 2 {
                return Err(SqlError::TypeError(format!(
                    "semantic_match expects (column, 'text'); got {} args",
                    args.len()
                )));
            }
            let column = ident_name(args[0])?;
            let query = string_literal(args[1])
                .ok_or_else(|| SqlError::TypeError("semantic_match: 2nd arg must be a string literal".into()))?;
            Ok(Some(SemanticClause::Match { column, query }))
        }
        "vector_distance" => {
            if args.len() != 2 {
                return Err(SqlError::TypeError(format!(
                    "vector_distance expects (column, [f32,...]); got {} args",
                    args.len()
                )));
            }
            let column = ident_name(args[0])?;
            let vector = vector_literal(args[1])?;
            Ok(Some(SemanticClause::Distance { column, vector }))
        }
        _ => Ok(None),
    }
}

fn try_filter(expr: &ast::Expr) -> Result<Option<Filter>> {
    match expr {
        ast::Expr::BinaryOp {
            op: ast::BinaryOperator::Eq,
            left,
            right,
        } => {
            // column = literal. We accept either side as the column.
            let (col, val) = match (path(left), literal(right)) {
                (Some(p), Some(v)) => (p, v),
                _ => match (path(right), literal(left)) {
                    (Some(p), Some(v)) => (p, v),
                    _ => return Ok(None),
                },
            };
            Ok(Some(Filter::Eq { path: col, value: val }))
        }
        ast::Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            let Some(col) = path(expr) else { return Ok(None) };
            let values = list
                .iter()
                .map(|e| {
                    literal(e).ok_or_else(|| {
                        SqlError::Unsupported("IN list: non-literal value".into())
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Some(Filter::In { path: col, values }))
        }
        _ => Ok(None),
    }
}

pub(crate) fn ident_name(expr: &ast::Expr) -> Result<String> {
    match expr {
        ast::Expr::Identifier(id) => Ok(id.value.clone()),
        ast::Expr::CompoundIdentifier(ids) => Ok(ids
            .iter()
            .map(|i| i.value.clone())
            .collect::<Vec<_>>()
            .join(".")),
        other => Err(SqlError::TypeError(format!(
            "expected column name, got: {other}"
        ))),
    }
}

pub(crate) fn path(expr: &ast::Expr) -> Option<Vec<String>> {
    match expr {
        ast::Expr::Identifier(id) => Some(vec![id.value.clone()]),
        ast::Expr::CompoundIdentifier(ids) => Some(ids.iter().map(|i| i.value.clone()).collect()),
        _ => None,
    }
}

fn string_literal(expr: &ast::Expr) -> Option<String> {
    match expr {
        ast::Expr::Value(ast::Value::SingleQuotedString(s)) => Some(s.clone()),
        ast::Expr::Value(ast::Value::DoubleQuotedString(s)) => Some(s.clone()),
        _ => None,
    }
}

fn literal(expr: &ast::Expr) -> Option<serde_json::Value> {
    match expr {
        ast::Expr::Value(ast::Value::SingleQuotedString(s))
        | ast::Expr::Value(ast::Value::DoubleQuotedString(s)) => Some(serde_json::Value::String(s.clone())),
        ast::Expr::Value(ast::Value::Number(n, _)) => {
            // Try i64 first, fall back to f64. If both fail, treat as
            // a string — a user writing `region = 1e3` probably wants
            // the numeric semantics but we prefer to pass the raw
            // token through than silently lose precision.
            if let Ok(i) = n.parse::<i64>() {
                Some(serde_json::Value::Number(i.into()))
            } else if let Ok(f) = n.parse::<f64>() {
                serde_json::Number::from_f64(f).map(serde_json::Value::Number)
            } else {
                Some(serde_json::Value::String(n.clone()))
            }
        }
        ast::Expr::Value(ast::Value::Boolean(b)) => Some(serde_json::Value::Bool(*b)),
        ast::Expr::Value(ast::Value::Null) => Some(serde_json::Value::Null),
        _ => None,
    }
}

fn vector_literal(expr: &ast::Expr) -> Result<Vec<f32>> {
    // Two shapes: a JSON-ish array literal `[0.1, 0.2, ...]` (not
    // natively supported by sqlparser) or a single-quoted string
    // holding a JSON array. We accept the string form — it's the
    // shape an HTTP client actually produces — and parse it as JSON.
    let s = string_literal(expr).ok_or_else(|| {
        SqlError::TypeError(
            "vector_distance expects a string literal holding a JSON array, e.g. '[0.1,0.2]'".into(),
        )
    })?;
    let parsed: serde_json::Value = serde_json::from_str(&s)
        .map_err(|e| SqlError::TypeError(format!("vector literal JSON: {e}")))?;
    let arr = parsed.as_array().ok_or_else(|| {
        SqlError::TypeError("vector literal must be a JSON array".into())
    })?;
    arr.iter()
        .map(|v| {
            v.as_f64()
                .map(|x| x as f32)
                .ok_or_else(|| SqlError::TypeError(format!("vector element not numeric: {v}")))
        })
        .collect()
}

pub(crate) fn build_order_by(order_by: &Option<ast::OrderBy>) -> Result<Option<OrderBy>> {
    let Some(ob) = order_by else { return Ok(None) };
    if ob.exprs.len() != 1 {
        return Err(SqlError::Unsupported(
            "multi-key ORDER BY not supported".into(),
        ));
    }
    let sort = &ob.exprs[0];
    let key = match &sort.expr {
        ast::Expr::Identifier(id) if id.value.eq_ignore_ascii_case("score") => OrderKey::Score,
        other => {
            let p = path(other).ok_or_else(|| {
                SqlError::Unsupported(format!("ORDER BY expression: {other}"))
            })?;
            OrderKey::Metadata(p)
        }
    };
    let dir = match sort.asc {
        Some(true) | None => OrderDir::Asc,
        Some(false) => OrderDir::Desc,
    };
    Ok(Some(OrderBy { key, dir }))
}
