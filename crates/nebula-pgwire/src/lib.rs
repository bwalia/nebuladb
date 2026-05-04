//! PostgreSQL wire-protocol front end for NebulaDB.
//!
//! Uses the [`pgwire`] crate's `SimpleQueryHandler` — good enough to
//! make `psql`, `tokio-postgres`, and most ORMs happy for read-only
//! workloads. Extended query protocol (prepare/bind) is intentionally
//! out of scope: NebulaDB's SQL subset doesn't benefit from plan
//! caching (the semantic-match planner already caches results), and
//! handling the EXTENDED flow correctly is several days of work.
//!
//! # Authentication
//!
//! We use `NoopStartupHandler` — "trust" auth. In practice NebulaDB's
//! pgwire port should sit behind the same network boundary as its
//! gRPC and REST ports, or behind a pg-aware proxy that does real
//! auth. Adding `pgwire`'s md5/scram handlers is mechanical and can
//! happen when a caller needs it.
//!
//! # Type mapping
//!
//! Everything is sent to the client as `text`. That's the simplest
//! correct thing: our rows come back as `serde_json::Value` which we
//! render to strings, and `psql` / `tokio-postgres` can consume text
//! values regardless of the underlying type. A later pass can inspect
//! values and pick proper OIDs for `int4`, `float8`, etc.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, PgWireHandlerFactory, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;

use nebula_core::NodeRole;
use nebula_sql::SqlEngine;

/// One entry point: hand it a `SqlEngine` and a bind address, it runs
/// a pgwire-compatible server until it errors out. No graceful
/// shutdown yet — the caller typically aborts the spawned task.
///
/// Legacy shim: boots in [`NodeRole::Standalone`] for callers that
/// don't care about replication. Use [`serve_with_role`] when serving
/// from a follower to enable the read-only guard.
pub async fn serve(engine: Arc<SqlEngine>, addr: std::net::SocketAddr) -> std::io::Result<()> {
    serve_with_role(engine, addr, NodeRole::default()).await
}

/// Bind a pgwire server with an explicit node role. Writes (any
/// `INSERT`/`UPDATE`/`DELETE`) are rejected with SQLSTATE `25006`
/// (`read_only_sql_transaction`) when the role is
/// [`NodeRole::Follower`]. Mirrors the REST and gRPC guards.
pub async fn serve_with_role(
    engine: Arc<SqlEngine>,
    addr: std::net::SocketAddr,
    role: NodeRole,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("nebula-pgwire listening on {addr} (role={role:?})");
    let factory = Arc::new(NebulaHandlers { engine, role });
    loop {
        let (socket, peer) = listener.accept().await?;
        tracing::debug!(%peer, "pgwire: new connection");
        let factory = Arc::clone(&factory);
        tokio::spawn(async move {
            // `process_socket` owns the socket and drives startup +
            // per-message loop using the handler set we provide. Its
            // error type is our own, so we just log and let the task
            // exit.
            if let Err(e) = process_socket(socket, None, factory).await {
                tracing::warn!(error = %e, "pgwire: connection exited");
            }
        });
    }
}

/// Bundle of handlers the pgwire machinery needs. We return the same
/// `NebulaQueryHandler` for simple queries; extended queries get a
/// placeholder that returns a friendly error.
struct NebulaHandlers {
    engine: Arc<SqlEngine>,
    role: NodeRole,
}

impl PgWireHandlerFactory for NebulaHandlers {
    type StartupHandler = NoopStartupHandler;
    type SimpleQueryHandler = NebulaQueryHandler;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        Arc::new(NebulaQueryHandler {
            engine: Arc::clone(&self.engine),
            role: self.role,
        })
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        Arc::new(NoopStartupHandler)
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }
}

pub struct NebulaQueryHandler {
    engine: Arc<SqlEngine>,
    role: NodeRole,
}

/// Return true for SQL statements that mutate state. A best-effort
/// prefix check — the SqlEngine itself doesn't yet parse DML, so
/// rejecting here gives us a symmetric guarantee with REST/gRPC
/// without depending on an internal AST type that might change.
fn is_mutating_statement(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    // Skip leading comments (`/* ... */` and `-- ...`) before classifying.
    let trimmed = strip_leading_comments(trimmed);
    let head: String = trimmed.chars().take(16).collect::<String>().to_ascii_uppercase();
    matches!(
        head.split_whitespace().next().unwrap_or(""),
        "INSERT" | "UPDATE" | "DELETE" | "UPSERT" | "MERGE"
            | "TRUNCATE" | "COPY" | "DROP" | "CREATE" | "ALTER"
    )
}

fn strip_leading_comments(mut s: &str) -> &str {
    loop {
        s = s.trim_start();
        if let Some(rest) = s.strip_prefix("--") {
            match rest.find('\n') {
                Some(i) => s = &rest[i + 1..],
                None => return "",
            }
        } else if let Some(rest) = s.strip_prefix("/*") {
            match rest.find("*/") {
                Some(i) => s = &rest[i + 2..],
                None => return "",
            }
        } else {
            return s;
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for NebulaQueryHandler {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        // psql / drivers send a handful of startup + probe statements
        // that aren't part of NebulaDB's SQL surface. We reply with
        // the minimum wire-correct shape so the session proceeds.
        //
        // The response differs by shape:
        //   - SET / BEGIN / COMMIT / ... → command complete.
        //   - SELECT 1 / version() / ... → a 1-row, 1-column result
        //     with a harmless integer or string. Drivers only check
        //     that *something* parses back, not the value.
        let trimmed = query.trim().trim_end_matches(';').trim();

        // Follower guard: refuse writes at the wire layer, matching the
        // REST and gRPC behavior. SQLSTATE 25006 is
        // `read_only_sql_transaction`, which psycopg/jdbc/etc. recognize.
        if self.role.is_read_only() && is_mutating_statement(trimmed) {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "25006".to_string(),
                "read_only_follower: this NebulaDB node is a follower and refuses writes".to_string(),
            ))));
        }

        if is_noop_statement(trimmed) {
            let lower = trimmed.to_ascii_lowercase();
            let is_select = lower.starts_with("select");
            if is_select {
                // Echo `1`/`version` as a single VARCHAR column named `result`.
                let value = if lower.contains("version") {
                    "NebulaDB 0.1.0 on pgwire".to_string()
                } else if lower.contains("current_schema") {
                    "public".to_string()
                } else {
                    "1".to_string()
                };
                let fields: Arc<Vec<FieldInfo>> = Arc::new(vec![FieldInfo::new(
                    "result".into(),
                    None,
                    None,
                    Type::VARCHAR,
                    FieldFormat::Text,
                )]);
                let mut enc = DataRowEncoder::new(Arc::clone(&fields));
                enc.encode_field(&Some(value))?;
                let row = enc.finish();
                let stream = stream::iter(vec![row]);
                let response = QueryResponse::new(fields, stream);
                return Ok(vec![Response::Query(response)]);
            }
            return Ok(vec![Response::Execution(Tag::new("SET"))]);
        }

        let result = self
            .engine
            .run(trimmed)
            .await
            .map_err(sql_error_to_pg)?;

        // Columns: all client-visible rows share the same JSON-object
        // shape, so we derive headers from the *first* row. If there
        // are no rows, we still need a schema — use a single `result`
        // column so psql has something to render.
        let fields: Arc<Vec<FieldInfo>> = Arc::new(fields_from_rows(&result.rows));
        let fields_for_rows = Arc::clone(&fields);

        // Encode rows synchronously into a Vec<DataRow>, then wrap in
        // a ready stream. Our row counts are small (ANN k-NN limited)
        // so there's no upside to streaming across an await.
        let mut data_rows: Vec<PgWireResult<DataRow>> = Vec::with_capacity(result.rows.len());
        for row in &result.rows {
            let mut enc = DataRowEncoder::new(Arc::clone(&fields_for_rows));
            for f in fields_for_rows.iter() {
                let val = lookup_row_value(row, f.name());
                enc.encode_field(&val)?;
            }
            data_rows.push(enc.finish());
        }
        let row_stream = stream::iter(data_rows);
        let response = QueryResponse::new(fields, row_stream);
        Ok(vec![Response::Query(response)])
    }
}

/// Statements that pgwire clients send during startup/liveness which
/// we want to acknowledge without actually running. Three buckets:
///
/// 1. Session params — `SET`, `SHOW`.
/// 2. Transaction no-ops — `BEGIN`, `COMMIT`, `ROLLBACK`. NebulaDB
///    is read-only so any transaction is trivially consistent.
/// 3. Client liveness probes — `SELECT 1`, `SELECT version()`,
///    `SELECT current_schema()`, `SELECT pg_catalog.version()`. Most
///    drivers send one on connect to verify the server is real.
fn is_noop_statement(s: &str) -> bool {
    let lower = s.to_ascii_lowercase();
    let lower = lower.trim();
    if lower.is_empty()
        || lower.starts_with("set ")
        || lower.starts_with("show ")
        || lower.starts_with("begin")
        || lower.starts_with("commit")
        || lower.starts_with("rollback")
    {
        return true;
    }
    // Constant-value / info SELECTs issued by drivers as probes.
    matches!(
        lower,
        "select 1"
            | "select 1;"
            | "select version()"
            | "select version();"
            | "select current_schema()"
            | "select current_schema();"
            | "select pg_catalog.version()"
            | "select pg_catalog.version();"
    )
}

fn fields_from_rows(rows: &[nebula_sql::executor::Row]) -> Vec<FieldInfo> {
    // Always surface `id`, `bucket`, `score` so a bare `SELECT *`
    // still has something in the header.
    let mut columns: Vec<String> = vec!["id".into(), "bucket".into(), "score".into()];
    if let Some(first) = rows.first() {
        if let serde_json::Value::Object(map) = &first.fields {
            for k in map.keys() {
                if !columns.contains(k) {
                    columns.push(k.clone());
                }
            }
        }
    }
    columns
        .into_iter()
        .map(|name| FieldInfo::new(name, None, None, Type::VARCHAR, FieldFormat::Text))
        .collect()
}

/// Pull the stringified value for a given column name from a row.
/// Virtual columns `id`/`bucket`/`score` come from the row struct;
/// anything else is looked up in `fields`.
fn lookup_row_value(row: &nebula_sql::executor::Row, name: &str) -> Option<String> {
    let v = match name {
        "id" => return Some(row.id.clone()),
        "bucket" => return Some(row.bucket.clone()),
        "score" => return Some(format!("{}", row.score)),
        other => row.fields.get(other).cloned().unwrap_or(serde_json::Value::Null),
    };
    Some(json_to_text(&v))
}

fn json_to_text(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => String::new(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        // Objects and arrays round-trip through their JSON text form.
        // A bespoke "[1,2,3]"-without-JSON rendering would be
        // slightly nicer for humans, but then clients can't parse it.
        other => other.to_string(),
    }
}

/// Translate our SQL error into a pgwire error with a matching SQLSTATE.
/// These codes are what psql and most drivers display on failure, so
/// a little care here goes a long way for diagnosis.
fn sql_error_to_pg(e: nebula_sql::SqlError) -> PgWireError {
    use nebula_sql::SqlError;
    let (code, severity) = match &e {
        // `42601` = syntax_error. Parsers and our "unsupported
        // feature" rejections both map here — they're both "the query
        // text isn't something we can run".
        SqlError::Parse(_) | SqlError::Unsupported(_) => ("42601", "ERROR"),
        // `42P01`-class: semantic planning issues (bad plan). Closest
        // PG equivalent is `42P17` (invalid_object_definition) but
        // that misleads; use `42601` with a clearer message.
        SqlError::InvalidPlan(_) => ("42601", "ERROR"),
        // `42804` = datatype_mismatch — what our TypeError actually is.
        SqlError::TypeError(_) => ("42804", "ERROR"),
        // Internal: XX000.
        SqlError::Index(_) => ("XX000", "ERROR"),
    };
    PgWireError::UserError(Box::new(ErrorInfo::new(
        severity.to_string(),
        code.to_string(),
        e.to_string(),
    )))
}
