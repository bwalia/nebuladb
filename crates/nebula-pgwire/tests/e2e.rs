//! End-to-end pgwire test.
//!
//! Spins up the server on an ephemeral port and drives it with
//! `tokio-postgres` — i.e. a real PG client — to confirm that the
//! startup/query flow actually works and that text-format rows round
//! trip as expected.

use std::sync::Arc;

use nebula_core::NodeRole;
use nebula_embed::{Embedder, MockEmbedder};
use nebula_index::TextIndex;
use nebula_sql::SqlEngine;
use nebula_vector::{HnswConfig, Metric};
use tokio::time::{sleep, Duration};
use tokio_postgres::NoTls;

async fn boot_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(32));
    let index = Arc::new(
        TextIndex::new(emb, Metric::Cosine, HnswConfig::default()).unwrap(),
    );
    // Seed a couple of docs so `SELECT` returns rows we can inspect.
    index
        .upsert_text("docs", "1", "zero trust", serde_json::json!({"region": "eu"}))
        .await
        .unwrap();
    index
        .upsert_text("docs", "2", "zero trust", serde_json::json!({"region": "us"}))
        .await
        .unwrap();

    let engine = Arc::new(SqlEngine::new(index));

    // Pick an ephemeral port by binding a temporary listener.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let handle = tokio::spawn(async move {
        if let Err(e) = nebula_pgwire::serve(engine, addr).await {
            eprintln!("pgwire server exited: {e}");
        }
    });
    // Let the listener bind before the client connects.
    sleep(Duration::from_millis(100)).await;
    (addr, handle)
}

async fn connect(addr: std::net::SocketAddr) -> tokio_postgres::Client {
    let conn_str = format!("host={} port={} user=any dbname=any", addr.ip(), addr.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

#[tokio::test]
async fn simple_select_returns_rows() {
    // `tokio-postgres::query` uses the *extended* protocol (Parse /
    // Bind / Execute). Our server speaks simple-query only — which is
    // what `psql`'s default command interface speaks too — so we use
    // `simple_query` here. Extended-protocol support is a follow-up.
    let (addr, _handle) = boot_server().await;
    let client = connect(addr).await;

    let messages = client
        .simple_query(
            "SELECT id, region FROM docs WHERE semantic_match(content, 'zero trust') LIMIT 10",
        )
        .await
        .unwrap();
    let rows: Vec<_> = messages
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();
    assert!(!rows.is_empty(), "expected at least one row");
    for r in &rows {
        let id = r.get("id").unwrap();
        let region = r.get("region").unwrap();
        assert!(!id.is_empty());
        assert!(region == "eu" || region == "us", "got region={region}");
    }
}

#[tokio::test]
async fn psql_style_set_statements_are_noop() {
    // `tokio-postgres` sends `SET client_encoding TO 'UTF8'` during
    // startup param negotiation in some configurations. Our server
    // must acknowledge bare SET / SHOW without parsing them.
    let (addr, _handle) = boot_server().await;
    let client = connect(addr).await;
    client
        .simple_query("SET TIMEZONE TO 'UTC'")
        .await
        .expect("SET should succeed");
    client
        .simple_query("SHOW client_encoding")
        .await
        .expect("SHOW should succeed");
}

#[tokio::test]
async fn parse_error_surfaces_as_sqlstate() {
    let (addr, _handle) = boot_server().await;
    let client = connect(addr).await;
    let err = client
        .simple_query("THIS IS NOT SQL")
        .await
        .expect_err("expected parse error");
    let db_err = err.as_db_error().expect("should be a DB-side error");
    // 42601 = syntax_error. We map both parse and "unsupported
    // feature" to this code because clients treat them the same.
    assert_eq!(db_err.code().code(), "42601", "got {}", db_err.code().code());
}

#[tokio::test]
async fn aggregate_query_runs_over_pgwire() {
    let (addr, _handle) = boot_server().await;
    let client = connect(addr).await;
    let messages = client
        .simple_query(
            "SELECT region, COUNT(*) AS n FROM docs \
             WHERE semantic_match(content, 'zero trust') GROUP BY region",
        )
        .await
        .unwrap();
    let rows: Vec<_> = messages
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();
    assert!(!rows.is_empty());
    for r in &rows {
        let n = r.get("n").unwrap();
        assert!(n.parse::<i64>().unwrap() >= 1);
    }
}

/// Follower-role pgwire servers must reject DML at the wire layer with
/// SQLSTATE 25006 (`read_only_sql_transaction`). Mirrors the REST and
/// gRPC guards — a follower pod reachable over any protocol cannot
/// cause local divergence.
async fn boot_follower() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(32));
    let index = Arc::new(TextIndex::new(emb, Metric::Cosine, HnswConfig::default()).unwrap());
    let engine = Arc::new(SqlEngine::new(index));

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let handle = tokio::spawn(async move {
        if let Err(e) = nebula_pgwire::serve_with_role(engine, addr, NodeRole::Follower).await {
            eprintln!("pgwire follower exited: {e}");
        }
    });
    sleep(Duration::from_millis(100)).await;
    (addr, handle)
}

#[tokio::test]
async fn follower_rejects_insert() {
    let (addr, _handle) = boot_follower().await;
    let client = connect(addr).await;
    let err = client
        .simple_query("INSERT INTO docs (id, text) VALUES ('x', 'y')")
        .await
        .unwrap_err();
    // `Display` for tokio-postgres errors just says "db error"; the
    // interesting bit is the wrapped `DbError` we peek at via `as_db_error`.
    let db = err.as_db_error().expect("expected DbError");
    assert_eq!(db.code().code(), "25006", "wrong SQLSTATE: {db:?}");
    assert!(
        db.message().contains("read_only_follower"),
        "unexpected message: {}",
        db.message()
    );
}

/// A region-configured pgwire server refuses DML. Reads still work.
#[tokio::test]
async fn region_configured_pgwire_refuses_writes() {
    let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(32));
    let index = Arc::new(TextIndex::new(emb, Metric::Cosine, HnswConfig::default()).unwrap());
    let engine = Arc::new(SqlEngine::new(index));

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let handle = tokio::spawn(async move {
        nebula_pgwire::serve_with_role_and_region(
            engine,
            addr,
            NodeRole::Leader,
            Some("us-east-1".into()),
        )
        .await
        .ok();
    });
    sleep(Duration::from_millis(100)).await;
    let client = connect(addr).await;

    let err = client
        .simple_query("INSERT INTO docs (id) VALUES ('x')")
        .await
        .unwrap_err();
    let db = err.as_db_error().expect("DbError");
    assert_eq!(db.code().code(), "25006");
    assert!(
        db.message().contains("wrong_home_region"),
        "got: {}",
        db.message()
    );

    // Reads must still work.
    let msgs = client.simple_query("SELECT 1").await.unwrap();
    let rows: Vec<_> = msgs
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(rows.len(), 1);
    handle.abort();
}

#[tokio::test]
async fn follower_allows_select() {
    let (addr, _handle) = boot_follower().await;
    let client = connect(addr).await;
    // Reads must stay open. SELECT 1 is a no-op that returns one row.
    let msgs = client.simple_query("SELECT 1").await.unwrap();
    let rows: Vec<_> = msgs
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(rows.len(), 1);
}
