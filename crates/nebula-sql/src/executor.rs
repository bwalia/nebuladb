//! Execute a [`Plan`] against a [`TextIndex`].
//!
//! Pipeline:
//! 1. Dispatch the retrieval clause — `search_text` for
//!    `semantic_match`, `search_vector` for `vector_distance`.
//! 2. Apply residual filters against each hit's metadata JSON.
//! 3. Sort per the `ORDER BY` clause (default: score ascending).
//! 4. Truncate to `LIMIT`.
//! 5. Project the rows.
//!
//! Every step is small and uninteresting on purpose — complexity
//! lives in the planner, not here.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use nebula_index::{Hit, TextIndex};

use crate::plan::{Filter, OrderBy, OrderDir, OrderKey, Plan, Projection, SemanticClause};
use crate::plan_tree::{AggregateFn, AggregatePlan, JoinPlan, QueryPlan};
use crate::{Result, SqlError};

/// One result row. `score` is the index distance (lower = closer);
/// `fields` is a JSON object projected per the plan's projection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Row {
    pub id: String,
    pub bucket: String,
    pub score: f32,
    pub fields: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryResult {
    pub took_ms: u64,
    pub rows: Vec<Row>,
}

pub struct Executor {
    index: Arc<TextIndex>,
}

impl Executor {
    pub fn new(index: Arc<TextIndex>) -> Self {
        Self { index }
    }

    /// Entry point. Pattern-matches on the plan tree and dispatches
    /// to a specialized routine. Timing is tracked here so every
    /// variant returns the same shape.
    pub async fn run(&self, plan: QueryPlan) -> Result<QueryResult> {
        let started = std::time::Instant::now();
        let rows = match plan {
            QueryPlan::Scan(p) => self.run_scan(p).await?,
            QueryPlan::Aggregate(p) => self.run_aggregate(*p).await?,
            QueryPlan::Join(p) => self.run_join(*p).await?,
        };
        Ok(QueryResult {
            took_ms: started.elapsed().as_millis() as u64,
            rows,
        })
    }

    /// Legacy single-bucket scan. Kept as a private helper because
    /// the aggregate and join paths drive it as their retrieval step.
    async fn run_scan(&self, plan: Plan) -> Result<Vec<Row>> {
        let hits = self.retrieve(&plan).await?;
        let filtered = apply_filters(hits, &plan.filters);
        let mut rows = filtered;
        sort_rows(&mut rows, plan.order_by.as_ref());
        if let Some(n) = plan.limit {
            rows.truncate(n);
        }
        Ok(rows
            .into_iter()
            .map(|h| project(&plan.projection, h))
            .collect())
    }

    /// Retrieval + filter, returning raw hits without projection. The
    /// aggregate and join paths need to see every metadata field, not
    /// just the scan's projection.
    async fn retrieve_filtered(&self, plan: &Plan) -> Result<Vec<Hit>> {
        let hits = self.retrieve(plan).await?;
        Ok(apply_filters(hits, &plan.filters))
    }

    async fn retrieve(&self, plan: &Plan) -> Result<Vec<Hit>> {
        let top_k = plan.top_k();
        match &plan.semantic {
            SemanticClause::Match { query, .. } => Ok(self
                .index
                .search_text(query, Some(&plan.bucket), top_k, None)
                .await?),
            SemanticClause::Distance { vector, .. } => {
                if vector.len() != self.index.dim() {
                    return Err(SqlError::TypeError(format!(
                        "vector length {} does not match index dimension {}",
                        vector.len(),
                        self.index.dim()
                    )));
                }
                Ok(self
                    .index
                    .search_vector(vector, Some(&plan.bucket), top_k, None)?)
            }
        }
    }

    /// GROUP BY. Hash-grouping in a single pass: build a map keyed by
    /// the tuple of group-key values, accumulate each aggregate in
    /// place. The choice of `HashMap` over a sorted vector is the
    /// usual one — smaller corpora this handles, large corpora would
    /// need a proper spill-to-disk strategy, which is out of scope.
    async fn run_aggregate(&self, plan: AggregatePlan) -> Result<Vec<Row>> {
        let hits = self.retrieve_filtered(&plan.input).await?;

        // Group key is a Vec<Value> — we serialize to a stable JSON
        // string for the HashMap key. Using the JSON directly avoids a
        // custom `Eq`/`Hash` impl over `serde_json::Value`.
        let mut groups: HashMap<String, GroupAcc> = HashMap::new();
        for h in hits {
            let key_values: Vec<serde_json::Value> = plan
                .group_keys
                .iter()
                .map(|k| metadata_value(&h, k))
                .collect();
            let key = serde_json::to_string(&key_values).unwrap_or_default();
            let entry = groups.entry(key).or_insert_with(|| GroupAcc {
                key_values: key_values.clone(),
                aggs: vec![AggAcc::default(); plan.aggs.len()],
            });
            for (i, spec) in plan.aggs.iter().enumerate() {
                update_agg(&mut entry.aggs[i], &spec.func, &h);
            }
        }

        // Materialize rows.
        let mut rows: Vec<Row> = groups
            .into_values()
            .map(|g| finalize_group(&g, &plan.group_keys, &plan.aggs))
            .collect();

        // Sort + limit on aggregate output. The plan's order_by
        // already validated that the key is either a group-by column
        // or an aggregate alias; we sort on the corresponding
        // `fields[key]`.
        if let Some(ob) = &plan.order_by {
            rows.sort_by(|a, b| {
                let av = a.fields.get(&ob.key).unwrap_or(&serde_json::Value::Null);
                let bv = b.fields.get(&ob.key).unwrap_or(&serde_json::Value::Null);
                let ord = cmp_json(av, bv);
                if ob.dir == OrderDir::Desc {
                    ord.reverse()
                } else {
                    ord
                }
            });
        }
        if let Some(n) = plan.limit {
            rows.truncate(n);
        }
        Ok(rows)
    }

    /// INNER JOIN with a hash table on the right side. We build an
    /// index from `right_column` value → Vec<Hit>; then stream left
    /// hits and emit one row per match. The right side is hashed
    /// because it's typically the smaller "dimension" input in
    /// analytical workloads; a cost-based optimizer would pick, but
    /// we don't have one yet.
    async fn run_join(&self, plan: JoinPlan) -> Result<Vec<Row>> {
        let left_hits = self.retrieve_filtered(&plan.left).await?;
        let right_hits = self.retrieve_filtered(&plan.right).await?;

        // Build right-side hash table.
        let mut right_by_key: HashMap<String, Vec<Hit>> = HashMap::new();
        for h in right_hits {
            let key = join_key_value(&h, &plan.predicate.right_column);
            right_by_key.entry(key).or_default().push(h);
        }

        // Probe with the left side.
        let mut rows: Vec<Row> = Vec::new();
        for lh in &left_hits {
            let key = join_key_value(lh, &plan.predicate.left_column);
            if let Some(matches) = right_by_key.get(&key) {
                for rh in matches {
                    rows.push(project_join(
                        &plan.projection,
                        &plan.left_alias,
                        lh,
                        &plan.right_alias,
                        rh,
                    ));
                }
            }
        }

        if let Some(ob) = &plan.order_by {
            sort_join_rows(&mut rows, ob);
        }
        if let Some(n) = plan.limit {
            rows.truncate(n);
        }
        Ok(rows)
    }
}

#[derive(Default, Debug, Clone)]
struct AggAcc {
    count: u64,
    sum: f64,
    min: Option<f64>,
    max: Option<f64>,
    // For MIN/MAX on strings we fall back to lexicographic ordering;
    // stored alongside the numeric path so a mixed-type column still
    // produces a deterministic result without panicking.
    min_s: Option<String>,
    max_s: Option<String>,
    saw_numeric: bool,
    saw_string: bool,
}

struct GroupAcc {
    key_values: Vec<serde_json::Value>,
    aggs: Vec<AggAcc>,
}

fn update_agg(acc: &mut AggAcc, func: &AggregateFn, h: &Hit) {
    match func {
        AggregateFn::CountStar => acc.count += 1,
        AggregateFn::Count(col) => {
            if !metadata_value(h, col).is_null() {
                acc.count += 1;
            }
        }
        AggregateFn::Sum(col) | AggregateFn::Avg(col) => {
            if let Some(n) = metadata_number(h, col) {
                acc.count += 1;
                acc.sum += n;
                acc.saw_numeric = true;
            }
        }
        AggregateFn::Min(col) | AggregateFn::Max(col) => {
            let v = metadata_value(h, col);
            if let Some(n) = v.as_f64() {
                acc.saw_numeric = true;
                match func {
                    AggregateFn::Min(_) => {
                        acc.min = Some(acc.min.map_or(n, |x| x.min(n)));
                    }
                    AggregateFn::Max(_) => {
                        acc.max = Some(acc.max.map_or(n, |x| x.max(n)));
                    }
                    _ => unreachable!(),
                }
            } else if let Some(s) = v.as_str() {
                acc.saw_string = true;
                match func {
                    AggregateFn::Min(_) => {
                        if acc.min_s.as_deref().map_or(true, |cur| s < cur) {
                            acc.min_s = Some(s.to_string());
                        }
                    }
                    AggregateFn::Max(_) => {
                        if acc.max_s.as_deref().map_or(true, |cur| s > cur) {
                            acc.max_s = Some(s.to_string());
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
}

fn finalize_group(
    g: &GroupAcc,
    group_keys: &[String],
    aggs: &[crate::plan_tree::AggregateSpec],
) -> Row {
    let mut obj = serde_json::Map::new();
    for (name, value) in group_keys.iter().zip(g.key_values.iter()) {
        obj.insert(name.clone(), value.clone());
    }
    for (spec, acc) in aggs.iter().zip(g.aggs.iter()) {
        let v = match &spec.func {
            AggregateFn::CountStar | AggregateFn::Count(_) => {
                serde_json::Value::Number(acc.count.into())
            }
            AggregateFn::Sum(_) => serde_json::Number::from_f64(acc.sum)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            AggregateFn::Avg(_) => {
                if acc.count == 0 {
                    serde_json::Value::Null
                } else {
                    serde_json::Number::from_f64(acc.sum / acc.count as f64)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null)
                }
            }
            AggregateFn::Min(_) => {
                if acc.saw_numeric {
                    acc.min
                        .and_then(serde_json::Number::from_f64)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null)
                } else if acc.saw_string {
                    acc.min_s
                        .clone()
                        .map(serde_json::Value::String)
                        .unwrap_or(serde_json::Value::Null)
                } else {
                    serde_json::Value::Null
                }
            }
            AggregateFn::Max(_) => {
                if acc.saw_numeric {
                    acc.max
                        .and_then(serde_json::Number::from_f64)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null)
                } else if acc.saw_string {
                    acc.max_s
                        .clone()
                        .map(serde_json::Value::String)
                        .unwrap_or(serde_json::Value::Null)
                } else {
                    serde_json::Value::Null
                }
            }
        };
        obj.insert(spec.alias.clone(), v);
    }
    Row {
        id: String::new(),
        bucket: String::new(),
        score: 0.0,
        fields: serde_json::Value::Object(obj),
    }
}

fn metadata_number(h: &Hit, col: &str) -> Option<f64> {
    metadata_value(h, col).as_f64()
}

fn metadata_value(h: &Hit, col: &str) -> serde_json::Value {
    match col {
        "id" => serde_json::Value::String(h.id.clone()),
        "bucket" => serde_json::Value::String(h.bucket.clone()),
        "text" => serde_json::Value::String(h.text.clone()),
        _ => {
            let path: Vec<String> = col.split('.').map(|s| s.to_string()).collect();
            path_lookup(&h.metadata, &path).clone()
        }
    }
}

fn join_key_value(h: &Hit, col: &str) -> String {
    // Serialize to JSON so non-string keys (numbers, booleans) hash
    // deterministically and without type-punning. `null` (missing
    // column) produces the JSON literal "null"; left-null = right-null
    // is then a real equi-match. We don't want that for joins, so
    // treat any null as a unique never-matching sentinel.
    let v = metadata_value(h, col);
    if v.is_null() {
        // UUID-like sentinel salted with the hit's own id keeps every
        // null-keyed row from collapsing into one join bucket.
        format!("__null__:{}", h.id)
    } else {
        serde_json::to_string(&v).unwrap_or_default()
    }
}

fn project_join(
    p: &Projection,
    la: &str,
    lh: &Hit,
    ra: &str,
    rh: &Hit,
) -> Row {
    // Join output is a map merging both sides; projection selects a
    // subset. Column names are resolved by alias prefix (`users.name`);
    // unqualified names are ambiguous and become null to force the
    // caller to disambiguate.
    let mut full = serde_json::Map::new();
    full.insert(format!("{la}.id"), serde_json::Value::String(lh.id.clone()));
    full.insert(format!("{la}.text"), serde_json::Value::String(lh.text.clone()));
    full.insert(format!("{la}.score"), num(lh.score));
    flatten_metadata(la, &lh.metadata, &mut full);
    full.insert(format!("{ra}.id"), serde_json::Value::String(rh.id.clone()));
    full.insert(format!("{ra}.text"), serde_json::Value::String(rh.text.clone()));
    full.insert(format!("{ra}.score"), num(rh.score));
    flatten_metadata(ra, &rh.metadata, &mut full);

    let fields = match p {
        Projection::All => serde_json::Value::Object(full),
        Projection::Columns(cols) => {
            let mut obj = serde_json::Map::with_capacity(cols.len());
            for col in cols {
                let v = full
                    .get(col)
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                obj.insert(col.clone(), v);
            }
            serde_json::Value::Object(obj)
        }
    };
    Row {
        id: format!("{}:{}", lh.id, rh.id),
        bucket: format!("{la}⋈{ra}"),
        score: (lh.score + rh.score) / 2.0,
        fields,
    }
}

fn flatten_metadata(alias: &str, meta: &serde_json::Value, out: &mut serde_json::Map<String, serde_json::Value>) {
    // Only flatten top-level keys; nested access uses `.`-paths
    // elsewhere and we don't recursively expand to avoid blowing up
    // key space on deeply nested docs.
    if let serde_json::Value::Object(m) = meta {
        for (k, v) in m {
            out.insert(format!("{alias}.{k}"), v.clone());
        }
    }
}

fn num(x: f32) -> serde_json::Value {
    serde_json::Number::from_f64(x as f64)
        .map(serde_json::Value::Number)
        .unwrap_or(serde_json::Value::Null)
}

fn sort_join_rows(rows: &mut [Row], ob: &OrderBy) {
    rows.sort_by(|a, b| {
        let ord = match &ob.key {
            OrderKey::Score => a
                .score
                .partial_cmp(&b.score)
                .unwrap_or(std::cmp::Ordering::Greater),
            OrderKey::Metadata(path) => {
                let key = path.join(".");
                let av = a.fields.get(&key).unwrap_or(&serde_json::Value::Null);
                let bv = b.fields.get(&key).unwrap_or(&serde_json::Value::Null);
                cmp_json(av, bv)
            }
        };
        if ob.dir == OrderDir::Desc {
            ord.reverse()
        } else {
            ord
        }
    });
}

fn apply_filters(hits: Vec<Hit>, filters: &[Filter]) -> Vec<Hit> {
    hits.into_iter()
        .filter(|h| filters.iter().all(|f| eval_filter(f, &h.metadata)))
        .collect()
}

/// Evaluate a filter against a hit's metadata. Paths are `.`-joined;
/// a path resolves to `Value::Null` when any segment is missing.
fn eval_filter(f: &Filter, meta: &serde_json::Value) -> bool {
    match f {
        Filter::Eq { path, value } => path_lookup(meta, path) == value,
        Filter::In { path, values } => {
            let v = path_lookup(meta, path);
            values.iter().any(|cand| cand == v)
        }
    }
}

fn path_lookup<'a>(root: &'a serde_json::Value, path: &[String]) -> &'a serde_json::Value {
    let mut cur = root;
    for seg in path {
        match cur.get(seg) {
            Some(v) => cur = v,
            None => return &serde_json::Value::Null,
        }
    }
    cur
}

fn sort_rows(rows: &mut [Hit], order: Option<&OrderBy>) {
    let ob = match order {
        Some(ob) => ob.clone(),
        None => OrderBy {
            key: OrderKey::Score,
            dir: OrderDir::Asc,
        },
    };
    rows.sort_by(|a, b| {
        let ord = match &ob.key {
            OrderKey::Score => a
                .score
                .partial_cmp(&b.score)
                // NaN pushes to the end regardless of direction; this
                // matches how sort behaves elsewhere in the codebase.
                .unwrap_or(std::cmp::Ordering::Greater),
            OrderKey::Metadata(path) => {
                let av = path_lookup(&a.metadata, path);
                let bv = path_lookup(&b.metadata, path);
                cmp_json(av, bv)
            }
        };
        if ob.dir == OrderDir::Desc { ord.reverse() } else { ord }
    });
}

fn cmp_json(a: &serde_json::Value, b: &serde_json::Value) -> std::cmp::Ordering {
    use serde_json::Value::*;
    match (a, b) {
        (Number(x), Number(y)) => x
            .as_f64()
            .unwrap_or(0.0)
            .partial_cmp(&y.as_f64().unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal),
        (String(x), String(y)) => x.cmp(y),
        (Bool(x), Bool(y)) => x.cmp(y),
        (Null, Null) => std::cmp::Ordering::Equal,
        // Cross-type comparisons are ill-defined; preserve insertion
        // order (Equal) rather than inventing a total order.
        _ => std::cmp::Ordering::Equal,
    }
}

fn project(p: &Projection, h: Hit) -> Row {
    match p {
        Projection::All => Row {
            id: h.id,
            bucket: h.bucket,
            score: h.score,
            fields: serde_json::json!({
                "text": h.text,
                "metadata": h.metadata,
            }),
        },
        Projection::Columns(cols) => {
            let mut obj = serde_json::Map::with_capacity(cols.len());
            for col in cols {
                let v = match col.as_str() {
                    // These four virtual columns expose properties of
                    // the hit that live outside metadata. Everything
                    // else resolves as a metadata path, `.`-split.
                    "id" => serde_json::Value::String(h.id.clone()),
                    "bucket" => serde_json::Value::String(h.bucket.clone()),
                    "text" => serde_json::Value::String(h.text.clone()),
                    "score" => serde_json::Number::from_f64(h.score as f64)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                    _ => {
                        let path: Vec<String> = col.split('.').map(|s| s.to_string()).collect();
                        path_lookup(&h.metadata, &path).clone()
                    }
                };
                obj.insert(col.clone(), v);
            }
            Row {
                id: h.id,
                bucket: h.bucket,
                score: h.score,
                fields: serde_json::Value::Object(obj),
            }
        }
    }
}
