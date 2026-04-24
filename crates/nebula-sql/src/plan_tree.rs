//! Top-level query plan tree.
//!
//! The original `plan::Plan` handled one-bucket scans. Real SQL needs
//! aggregation and joins; rather than bolt both into a single struct,
//! we wrap it in a sum type:
//!
//! - [`QueryPlan::Scan`] — unchanged, still produced for most queries.
//! - [`QueryPlan::Aggregate`] — a [`Plan`] input + GROUP BY + aggs.
//! - [`QueryPlan::Join`] — two [`Plan`] inputs + ON-predicate.
//!
//! The executor pattern-matches on this top-level. Keeping `Plan`
//! intact means every existing test still exercises the exact code
//! path it used to; only new queries take the new branches.
//!
//! # Scope boundaries
//!
//! - Joins are INNER only and must be equi-joins on a single
//!   metadata column from each side. Every other shape (cross joins,
//!   OUTER, USING, multi-column ON) is an explicit `Unsupported`
//!   error.
//! - Both sides of a join must carry their own `semantic_match` /
//!   `vector_distance` in the WHERE clause — there's no "table scan"
//!   retrieval path, so without a semantic clause we have nothing to
//!   feed the join from.
//! - Aggregates supported: `COUNT(*)`, `COUNT(col)`, `SUM(col)`,
//!   `AVG(col)`, `MIN(col)`, `MAX(col)`. No HAVING this pass.

use serde::Serialize;
use sqlparser::ast;

use crate::plan::{
    build_order_by, build_projection, build_scan_from_parts, ident_name, path, Plan, Projection,
};
use crate::{Result, SqlError};

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "fn", rename_all = "snake_case")]
pub enum AggregateFn {
    CountStar,
    Count(String),
    Sum(String),
    Avg(String),
    Min(String),
    Max(String),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AggregateSpec {
    pub alias: String,
    pub func: AggregateFn,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AggregatePlan {
    /// The scan that produces rows to aggregate. The scan's own
    /// projection is ignored — we project after grouping.
    pub input: Plan,
    pub group_keys: Vec<String>,
    pub aggs: Vec<AggregateSpec>,
    /// ORDER BY applies to the aggregated rows, not the input. Keys
    /// reference either a group-by column name or an aggregate alias.
    pub order_by: Option<AggOrderBy>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AggOrderBy {
    pub key: String,
    pub dir: crate::plan::OrderDir,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct JoinPlan {
    pub left: Plan,
    pub right: Plan,
    /// ON `left_alias.col = right_alias.col`. We keep the table
    /// qualifier so the executor can tell which side each key comes
    /// from.
    pub predicate: JoinPredicate,
    pub projection: Projection,
    pub order_by: Option<crate::plan::OrderBy>,
    pub limit: Option<usize>,
    pub left_alias: String,
    pub right_alias: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct JoinPredicate {
    pub left_column: String,
    pub right_column: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "node", rename_all = "snake_case")]
pub enum QueryPlan {
    // `Aggregate` and `Join` are much larger than `Scan` (they each
    // carry at least one full `Plan` plus extra bookkeeping). Boxing
    // keeps every `QueryPlan` value fixed-size and avoids the clippy
    // `large_enum_variant` warning. The extra allocation is
    // irrelevant next to the work downstream.
    Scan(Plan),
    Aggregate(Box<AggregatePlan>),
    Join(Box<JoinPlan>),
}

/// Top-level planner. Dispatches based on shape:
/// - `GROUP BY` present → [`AggregatePlan`].
/// - exactly one `JOIN` in FROM → [`JoinPlan`].
/// - neither → [`crate::plan::build`] (legacy path).
pub fn build(stmt: ast::Statement) -> Result<QueryPlan> {
    let q = match stmt {
        ast::Statement::Query(q) => q,
        _ => return Err(SqlError::Unsupported("only SELECT is supported".into())),
    };
    if q.with.is_some() {
        return Err(SqlError::Unsupported("WITH / CTE".into()));
    }
    let ast::SetExpr::Select(boxed_sel) = *q.body.clone() else {
        return Err(SqlError::Unsupported(
            "set operations (UNION, INTERSECT, EXCEPT)".into(),
        ));
    };
    // `boxed_sel` is `Box<Select>` in sqlparser 0.52; unbox once.
    let sel: ast::Select = *boxed_sel;

    if sel.from.len() != 1 {
        return Err(SqlError::Unsupported(
            "exactly one table or one JOIN is required in FROM".into(),
        ));
    }
    let twj = &sel.from[0];

    // Dispatch by shape. A GROUP BY always wins — even if there's
    // technically a join, we'd need HAVING + multi-stage agg which is
    // out of scope.
    if has_group_by(&sel.group_by) {
        if !twj.joins.is_empty() {
            return Err(SqlError::Unsupported(
                "GROUP BY combined with JOIN is not yet supported".into(),
            ));
        }
        return build_aggregate(*q, sel).map(|p| QueryPlan::Aggregate(Box::new(p)));
    }
    if !twj.joins.is_empty() {
        return build_join(*q, sel).map(|p| QueryPlan::Join(Box::new(p)));
    }
    let plan = crate::plan::build(ast::Statement::Query(q))?;
    Ok(QueryPlan::Scan(plan))
}

fn has_group_by(g: &ast::GroupByExpr) -> bool {
    match g {
        ast::GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
        _ => false,
    }
}

fn build_aggregate(q: ast::Query, sel: ast::Select) -> Result<AggregatePlan> {
    // 1. Group keys.
    let group_keys: Vec<String> = match &sel.group_by {
        ast::GroupByExpr::Expressions(exprs, _) => exprs
            .iter()
            .map(|e| {
                path(e)
                    .map(|p| p.join("."))
                    .ok_or_else(|| SqlError::Unsupported(format!("GROUP BY expression: {e}")))
            })
            .collect::<Result<Vec<_>>>()?,
        other => {
            return Err(SqlError::Unsupported(format!(
                "unsupported GROUP BY shape: {other:?}"
            )));
        }
    };

    // 2. Projection must be (group_keys)* followed by (agg_fn)* only.
    // We split the SELECT list into the two camps; anything else is
    // an error because the SQL semantics are ambiguous without HAVING.
    let mut scan_projected_keys: Vec<String> = Vec::new();
    let mut aggs: Vec<AggregateSpec> = Vec::new();
    for item in &sel.projection {
        match item {
            ast::SelectItem::UnnamedExpr(expr)
            | ast::SelectItem::ExprWithAlias { expr, .. } => {
                // Try to read an aggregate first; fall back to a
                // plain column (which must appear in GROUP BY).
                if let Some(spec) = try_aggregate(item)? {
                    aggs.push(spec);
                } else if let Some(p) = path(expr) {
                    let name = p.join(".");
                    if !group_keys.contains(&name) {
                        return Err(SqlError::InvalidPlan(format!(
                            "column `{name}` must be in GROUP BY or aggregated"
                        )));
                    }
                    scan_projected_keys.push(name);
                } else {
                    return Err(SqlError::Unsupported(format!(
                        "projection expression in GROUP BY query: {expr}"
                    )));
                }
            }
            ast::SelectItem::Wildcard(_) => {
                return Err(SqlError::InvalidPlan(
                    "SELECT * is not allowed with GROUP BY; list columns explicitly".into(),
                ));
            }
            other => {
                return Err(SqlError::Unsupported(format!(
                    "projection item: {other}"
                )));
            }
        }
    }

    // 3. Feeder scan. Strip ORDER BY / LIMIT from the scan; those
    // apply to the aggregate output. Projection on the scan side is
    // `*` — we pull group keys + agg inputs out of metadata, and we
    // don't know yet which are which.
    let twj = &sel.from[0];
    let scan = build_scan_from_parts(
        &twj.relation,
        Projection::All,
        sel.selection.as_ref(),
        None,
        None,
    )?;

    // 4. ORDER BY / LIMIT on the aggregate output.
    let order_by = build_agg_order_by(&q.order_by, &group_keys, &aggs)?;
    let limit = literal_limit(q.limit.as_ref())?;

    let _ = scan_projected_keys; // acknowledge variable: shape already validated above
    Ok(AggregatePlan {
        input: scan,
        group_keys,
        aggs,
        order_by,
        limit,
    })
}

fn try_aggregate(item: &ast::SelectItem) -> Result<Option<AggregateSpec>> {
    let (expr, alias) = match item {
        ast::SelectItem::UnnamedExpr(expr) => (expr, None),
        ast::SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
        _ => return Ok(None),
    };
    let ast::Expr::Function(f) = expr else {
        return Ok(None);
    };
    let name = f.name.to_string().to_uppercase();
    // Pull the one arg if present. COUNT(*) is special — the arg is a
    // wildcard.
    let args: Vec<&ast::FunctionArg> = match &f.args {
        ast::FunctionArguments::List(list) => list.args.iter().collect(),
        _ => return Ok(None),
    };

    fn single_col(args: &[&ast::FunctionArg]) -> Result<String> {
        if args.len() != 1 {
            return Err(SqlError::TypeError(format!(
                "aggregate expects 1 arg, got {}",
                args.len()
            )));
        }
        match args[0] {
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => ident_name(e),
            _ => Err(SqlError::Unsupported(
                "named aggregate arguments / wildcards".into(),
            )),
        }
    }

    let func = match name.as_str() {
        "COUNT" => {
            // COUNT(*) vs COUNT(col).
            if matches!(
                args.as_slice(),
                [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard)]
            ) {
                AggregateFn::CountStar
            } else {
                AggregateFn::Count(single_col(&args)?)
            }
        }
        "SUM" => AggregateFn::Sum(single_col(&args)?),
        "AVG" => AggregateFn::Avg(single_col(&args)?),
        "MIN" => AggregateFn::Min(single_col(&args)?),
        "MAX" => AggregateFn::Max(single_col(&args)?),
        _ => return Ok(None),
    };

    let alias = alias.unwrap_or_else(|| default_agg_alias(&func));
    Ok(Some(AggregateSpec { alias, func }))
}

fn default_agg_alias(func: &AggregateFn) -> String {
    match func {
        AggregateFn::CountStar => "count".into(),
        AggregateFn::Count(c) => format!("count_{c}"),
        AggregateFn::Sum(c) => format!("sum_{c}"),
        AggregateFn::Avg(c) => format!("avg_{c}"),
        AggregateFn::Min(c) => format!("min_{c}"),
        AggregateFn::Max(c) => format!("max_{c}"),
    }
}

fn build_agg_order_by(
    order_by: &Option<ast::OrderBy>,
    group_keys: &[String],
    aggs: &[AggregateSpec],
) -> Result<Option<AggOrderBy>> {
    let Some(ob) = order_by else { return Ok(None) };
    if ob.exprs.len() != 1 {
        return Err(SqlError::Unsupported(
            "multi-key ORDER BY on aggregate".into(),
        ));
    }
    let sort = &ob.exprs[0];
    let p = path(&sort.expr)
        .ok_or_else(|| SqlError::Unsupported(format!("ORDER BY expression: {}", sort.expr)))?;
    let key = p.join(".");
    let valid = group_keys.iter().any(|g| g == &key)
        || aggs.iter().any(|a| a.alias == key);
    if !valid {
        return Err(SqlError::InvalidPlan(format!(
            "ORDER BY `{key}` is not a group-by column or aggregate alias"
        )));
    }
    let dir = match sort.asc {
        Some(true) | None => crate::plan::OrderDir::Asc,
        Some(false) => crate::plan::OrderDir::Desc,
    };
    Ok(Some(AggOrderBy { key, dir }))
}

fn literal_limit(limit: Option<&ast::Expr>) -> Result<Option<usize>> {
    match limit {
        None => Ok(None),
        Some(ast::Expr::Value(ast::Value::Number(n, _))) => n
            .parse::<usize>()
            .map(Some)
            .map_err(|_| SqlError::TypeError(format!("LIMIT must be a positive integer: {n}"))),
        Some(other) => Err(SqlError::Unsupported(format!("non-literal LIMIT: {other}"))),
    }
}

fn build_join(q: ast::Query, sel: ast::Select) -> Result<JoinPlan> {
    let twj = &sel.from[0];
    if twj.joins.len() != 1 {
        return Err(SqlError::Unsupported(
            "multi-way joins are not supported (one JOIN per query)".into(),
        ));
    }
    let join = &twj.joins[0];
    // sqlparser 0.52 parses both `INNER JOIN` and plain `JOIN` as
    // `JoinOperator::Inner`. We reject everything else — LEFT/RIGHT
    // OUTER, SEMI, ANTI, CROSS, ASOF — because their semantics don't
    // map onto ANN retrieval without more thought.
    let on_expr = match &join.join_operator {
        ast::JoinOperator::Inner(ast::JoinConstraint::On(e)) => e,
        other => {
            return Err(SqlError::Unsupported(format!(
                "only INNER JOIN ... ON is supported, got: {other:?}"
            )));
        }
    };

    let (left_alias, left_rel) = rel_with_alias(&twj.relation)?;
    let (right_alias, right_rel) = rel_with_alias(&join.relation)?;
    if left_alias == right_alias {
        return Err(SqlError::InvalidPlan(
            "JOIN requires distinct table aliases on each side".into(),
        ));
    }
    let predicate = extract_equi_join(on_expr, &left_alias, &right_alias)?;

    // Split WHERE across left/right. A predicate is assigned to a
    // side if all of its qualified column references resolve through
    // that side's alias, or if it is unqualified and uses a known
    // column on exactly one side. Simpler rule for now: require
    // qualified column names in WHERE for JOIN queries, so we can
    // route deterministically.
    let (left_where, right_where) = split_where_by_alias(
        sel.selection.as_ref(),
        &left_alias,
        &right_alias,
    )?;

    let left = build_scan_from_parts(
        &left_rel,
        Projection::All,
        left_where.as_ref(),
        None,
        None,
    )?;
    let right = build_scan_from_parts(
        &right_rel,
        Projection::All,
        right_where.as_ref(),
        None,
        None,
    )?;

    let projection = build_projection(&sel.projection)?;
    let order_by = build_order_by(&q.order_by)?;
    let limit = literal_limit(q.limit.as_ref())?;

    Ok(JoinPlan {
        left,
        right,
        predicate,
        projection,
        order_by,
        limit,
        left_alias,
        right_alias,
    })
}

fn rel_with_alias(rel: &ast::TableFactor) -> Result<(String, ast::TableFactor)> {
    match rel {
        ast::TableFactor::Table { name, alias, .. } => {
            let alias_str = alias
                .as_ref()
                .map(|a| a.name.value.clone())
                .unwrap_or_else(|| {
                    name.0
                        .last()
                        .map(|i| i.value.clone())
                        .unwrap_or_default()
                });
            // Strip the alias from the clone we return so the scan
            // builder works against a plain table reference.
            let bare = ast::TableFactor::Table {
                name: name.clone(),
                alias: None,
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
                with_ordinality: false,
            };
            Ok((alias_str, bare))
        }
        other => Err(SqlError::Unsupported(format!(
            "unsupported JOIN source: {other}"
        ))),
    }
}

fn extract_equi_join(
    expr: &ast::Expr,
    left_alias: &str,
    right_alias: &str,
) -> Result<JoinPredicate> {
    let ast::Expr::BinaryOp {
        op: ast::BinaryOperator::Eq,
        left,
        right,
    } = expr
    else {
        return Err(SqlError::Unsupported(
            "JOIN ON must be `left.col = right.col`".into(),
        ));
    };
    let (lq, lc) = qualified_column(left)?;
    let (rq, rc) = qualified_column(right)?;
    // Accept either direction; rewrite so left matches left_alias.
    if lq == left_alias && rq == right_alias {
        Ok(JoinPredicate {
            left_column: lc,
            right_column: rc,
        })
    } else if lq == right_alias && rq == left_alias {
        Ok(JoinPredicate {
            left_column: rc,
            right_column: lc,
        })
    } else {
        Err(SqlError::InvalidPlan(format!(
            "JOIN ON references unknown aliases: {lq}.{lc} = {rq}.{rc}"
        )))
    }
}

fn qualified_column(expr: &ast::Expr) -> Result<(String, String)> {
    match expr {
        ast::Expr::CompoundIdentifier(ids) if ids.len() == 2 => {
            Ok((ids[0].value.clone(), ids[1].value.clone()))
        }
        other => Err(SqlError::Unsupported(format!(
            "JOIN ON expects qualified column (alias.col): {other}"
        ))),
    }
}

fn split_where_by_alias(
    selection: Option<&ast::Expr>,
    left_alias: &str,
    right_alias: &str,
) -> Result<(Option<ast::Expr>, Option<ast::Expr>)> {
    let Some(expr) = selection else {
        return Err(SqlError::InvalidPlan(
            "JOIN requires WHERE with semantic_match/vector_distance on both sides".into(),
        ));
    };

    let mut leaves: Vec<ast::Expr> = Vec::new();
    flatten_and(expr, &mut leaves)?;

    let mut left_leaves = Vec::new();
    let mut right_leaves = Vec::new();
    for leaf in leaves {
        let side = classify(&leaf, left_alias, right_alias)?;
        // Strip the alias prefix from qualified idents so the side's
        // scan planner (which works against a single bucket, no
        // concept of aliases) sees plain column names. Without this
        // the downstream filter would look up `u.region` against the
        // per-doc metadata and always miss.
        let stripped = strip_alias_qualifiers(leaf, left_alias, right_alias);
        match side {
            Side::Left => left_leaves.push(stripped),
            Side::Right => right_leaves.push(stripped),
        }
    }

    Ok((
        if left_leaves.is_empty() {
            None
        } else {
            Some(and_chain(left_leaves))
        },
        if right_leaves.is_empty() {
            None
        } else {
            Some(and_chain(right_leaves))
        },
    ))
}

/// Replace every `alias.col` with a bare `col` identifier. We only
/// rewrite identifiers that appear inside function args, binary ops,
/// or IN-lists — the places our supported predicates live — so the
/// rewrite is small and targeted.
fn strip_alias_qualifiers(expr: ast::Expr, left_alias: &str, right_alias: &str) -> ast::Expr {
    fn strip_ident(e: ast::Expr, la: &str, ra: &str) -> ast::Expr {
        match e {
            ast::Expr::CompoundIdentifier(ids) if ids.len() == 2 => {
                let q = &ids[0].value;
                if q == la || q == ra {
                    ast::Expr::Identifier(ids.into_iter().nth(1).unwrap())
                } else {
                    ast::Expr::CompoundIdentifier(ids)
                }
            }
            other => other,
        }
    }
    match expr {
        ast::Expr::BinaryOp { op, left, right } => ast::Expr::BinaryOp {
            op,
            left: Box::new(strip_alias_qualifiers(*left, left_alias, right_alias)),
            right: Box::new(strip_alias_qualifiers(*right, left_alias, right_alias)),
        },
        ast::Expr::InList {
            expr,
            list,
            negated,
        } => ast::Expr::InList {
            expr: Box::new(strip_alias_qualifiers(*expr, left_alias, right_alias)),
            list: list
                .into_iter()
                .map(|e| strip_alias_qualifiers(e, left_alias, right_alias))
                .collect(),
            negated,
        },
        ast::Expr::Function(mut f) => {
            if let ast::FunctionArguments::List(ref mut list) = f.args {
                let rewritten: Vec<ast::FunctionArg> = std::mem::take(&mut list.args)
                    .into_iter()
                    .map(|arg| match arg {
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                                strip_alias_qualifiers(e, left_alias, right_alias),
                            ))
                        }
                        other => other,
                    })
                    .collect();
                list.args = rewritten;
            }
            ast::Expr::Function(f)
        }
        other => strip_ident(other, left_alias, right_alias),
    }
}

enum Side {
    Left,
    Right,
}

fn classify(expr: &ast::Expr, left_alias: &str, right_alias: &str) -> Result<Side> {
    // Function call: look at the first arg's alias qualifier. Our
    // semantic functions always take the column as arg 0.
    if let ast::Expr::Function(f) = expr {
        if let ast::FunctionArguments::List(list) = &f.args {
            if let Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                ast::Expr::CompoundIdentifier(ids),
            ))) = list.args.first()
            {
                if ids.len() == 2 {
                    let q = &ids[0].value;
                    if q == left_alias {
                        return Ok(Side::Left);
                    }
                    if q == right_alias {
                        return Ok(Side::Right);
                    }
                }
            }
        }
    }
    // BinaryOp / InList: first operand must be qualified.
    if let ast::Expr::BinaryOp { left, .. } = expr {
        if let ast::Expr::CompoundIdentifier(ids) = left.as_ref() {
            if ids.len() == 2 {
                let q = &ids[0].value;
                if q == left_alias {
                    return Ok(Side::Left);
                }
                if q == right_alias {
                    return Ok(Side::Right);
                }
            }
        }
    }
    if let ast::Expr::InList { expr: op, .. } = expr {
        if let ast::Expr::CompoundIdentifier(ids) = op.as_ref() {
            if ids.len() == 2 {
                let q = &ids[0].value;
                if q == left_alias {
                    return Ok(Side::Left);
                }
                if q == right_alias {
                    return Ok(Side::Right);
                }
            }
        }
    }
    Err(SqlError::Unsupported(format!(
        "WHERE predicate must qualify columns with a join alias: {expr}"
    )))
}

fn flatten_and(expr: &ast::Expr, out: &mut Vec<ast::Expr>) -> Result<()> {
    match expr {
        ast::Expr::BinaryOp {
            op: ast::BinaryOperator::And,
            left,
            right,
        } => {
            flatten_and(left, out)?;
            flatten_and(right, out)?;
        }
        ast::Expr::BinaryOp {
            op: ast::BinaryOperator::Or,
            ..
        } => {
            return Err(SqlError::Unsupported(
                "OR in WHERE is not supported".into(),
            ));
        }
        ast::Expr::Nested(inner) => flatten_and(inner, out)?,
        other => out.push(other.clone()),
    }
    Ok(())
}

fn and_chain(mut leaves: Vec<ast::Expr>) -> ast::Expr {
    let mut acc = leaves.remove(0);
    for leaf in leaves {
        acc = ast::Expr::BinaryOp {
            op: ast::BinaryOperator::And,
            left: Box::new(acc),
            right: Box::new(leaf),
        };
    }
    acc
}
