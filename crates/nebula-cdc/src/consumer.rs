//! `CdcConsumer` trait and two concrete impls (file log, webhook).
//!
//! A consumer's job is "side-effect on every event the engine hands
//! me." Failures are surfaced as [`ConsumerError`] so the engine can
//! decide whether to retry, drop, or propagate. Today's engine logs
//! the error and drops the event — no retry policy, no DLQ. RFC 0005
//! §5.5 covers retry semantics as a follow-up.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::io::AsyncWriteExt;

use crate::event::CdcEvent;

/// Failures a consumer can report.
#[derive(Debug, thiserror::Error)]
pub enum ConsumerError {
    #[error("consumer io: {0}")]
    Io(#[from] std::io::Error),
    #[error("consumer http: {0}")]
    Http(String),
    #[error("consumer codec: {0}")]
    Codec(String),
    #[error("consumer rejected event: {0}")]
    Rejected(String),
}

/// Side-effect interface. The engine spawns one task per consumer
/// and calls `apply` on every event that passes the filter. `Send`
/// + `Sync` so the engine's spawn loop can drive it from any worker.
#[async_trait]
pub trait CdcConsumer: Send + Sync + 'static {
    /// Human-readable consumer id; surfaced in error logs and used
    /// as the unique key for the in-memory cursor map (one cursor
    /// per consumer).
    fn id(&self) -> &str;

    /// Apply one event. Returning `Err` does NOT halt the engine —
    /// it's logged and the event is dropped. The cursor advances
    /// regardless so a single bad event can't park a consumer
    /// forever; callers who need at-least-once semantics should
    /// build retry inside `apply`.
    async fn apply(&self, event: &CdcEvent) -> Result<(), ConsumerError>;
}

// ---------------------------------------------------------------
// FileLogConsumer — JSONL appender
// ---------------------------------------------------------------

/// Write each event as one JSON line to a file. Useful for tests,
/// debugging, and as the cheapest possible "is the engine working"
/// integration. Not durable in any meaningful sense — no fsync, no
/// rotation. Reach for the webhook consumer (or a real Kafka sink in
/// a follow-up) for production.
pub struct FileLogConsumer {
    id: String,
    path: PathBuf,
    /// Tokio's `Mutex` would block the runtime; we use parking_lot +
    ///   `tokio::task::block_in_place`-style writes inside `apply`. In
    ///   practice the write hits the page cache and returns in
    ///   microseconds, so the mutex contention here is fine.
    handle: Arc<Mutex<Option<tokio::fs::File>>>,
}

impl FileLogConsumer {
    pub fn new(id: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        Self {
            id: id.into(),
            path: path.into(),
            handle: Arc::new(Mutex::new(None)),
        }
    }

    async fn ensure_open(&self) -> Result<(), ConsumerError> {
        if self.handle.lock().is_some() {
            return Ok(());
        }
        let f = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;
        *self.handle.lock() = Some(f);
        Ok(())
    }
}

#[async_trait]
impl CdcConsumer for FileLogConsumer {
    fn id(&self) -> &str {
        &self.id
    }

    async fn apply(&self, event: &CdcEvent) -> Result<(), ConsumerError> {
        self.ensure_open().await?;
        let mut line =
            serde_json::to_vec(event).map_err(|e| ConsumerError::Codec(e.to_string()))?;
        line.push(b'\n');
        // Lock briefly to take the file handle out, write, replace.
        // Holding the parking_lot lock across an await would be a
        // bug; the take/replace pattern is the standard workaround.
        let mut h = self.handle.lock().take();
        let res = match h.as_mut() {
            Some(f) => f.write_all(&line).await.map_err(ConsumerError::from),
            None => Err(ConsumerError::Io(std::io::Error::other(
                "file handle vanished",
            ))),
        };
        *self.handle.lock() = h;
        res
    }
}

// ---------------------------------------------------------------
// WebhookConsumer — POST JSON to a URL
// ---------------------------------------------------------------

/// POST each event as a JSON body to the configured URL. The body
/// shape is the same JSON [`CdcEvent`] serializes to (top-level
/// fields `cursor`, `next_cursor`, `op`, `key`, `body`).
///
/// Single-shot per event: no batching, no retry. A failed POST
/// surfaces as `ConsumerError::Http`; the engine logs and moves on.
/// Retry / batching belong in a follow-up — RFC 0005 §5.5 calls
/// retry semantics out as deferred.
pub struct WebhookConsumer {
    id: String,
    url: String,
    client: reqwest::Client,
}

impl WebhookConsumer {
    /// Build a consumer with default 5s timeout.
    pub fn new(id: impl Into<String>, url: impl Into<String>) -> Self {
        Self::with_timeout(id, url, Duration::from_secs(5))
    }

    pub fn with_timeout(id: impl Into<String>, url: impl Into<String>, timeout: Duration) -> Self {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .expect("reqwest client builder is infallible with default config");
        Self {
            id: id.into(),
            url: url.into(),
            client,
        }
    }
}

#[async_trait]
impl CdcConsumer for WebhookConsumer {
    fn id(&self) -> &str {
        &self.id
    }

    async fn apply(&self, event: &CdcEvent) -> Result<(), ConsumerError> {
        let resp = self
            .client
            .post(&self.url)
            .json(event)
            .send()
            .await
            .map_err(|e| ConsumerError::Http(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(ConsumerError::Http(format!(
                "webhook returned {}: {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::CdcEvent;
    use nebula_wal::{WalCursor, WalEntry, WalRecord};

    fn sample_event() -> CdcEvent {
        CdcEvent::from_wal_entry(WalEntry {
            cursor: WalCursor::BEGIN,
            next_cursor: WalCursor::BEGIN,
            record: WalRecord::UpsertText {
                bucket: "b".into(),
                external_id: "x".into(),
                text: "hi".into(),
                vector: vec![1.0],
                metadata_json: "null".into(),
            },
        })
    }

    #[tokio::test]
    async fn file_consumer_appends_jsonl() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let c = FileLogConsumer::new("test", &path);
        c.apply(&sample_event()).await.unwrap();
        c.apply(&sample_event()).await.unwrap();
        let body = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 2, "got: {body:?}");
        // Each line is valid JSON.
        for line in &lines {
            let _: serde_json::Value = serde_json::from_str(line).unwrap();
        }
    }

    #[tokio::test]
    async fn file_consumer_id_is_returned() {
        let dir = tempfile::tempdir().unwrap();
        let c = FileLogConsumer::new("logger-1", dir.path().join("e.jsonl"));
        assert_eq!(c.id(), "logger-1");
    }

    #[tokio::test]
    async fn webhook_consumer_posts_event_body() {
        // Spin up a tiny axum server that captures the POST body, run
        // the consumer against it, assert the body shape.
        use axum::{routing::post, Json, Router};
        use std::sync::Arc;
        use tokio::sync::Mutex as TokioMutex;

        let captured: Arc<TokioMutex<Vec<serde_json::Value>>> =
            Arc::new(TokioMutex::new(Vec::new()));
        let captured_for_handler = captured.clone();
        let app = Router::new().route(
            "/hook",
            post(move |Json(body): Json<serde_json::Value>| {
                let captured = captured_for_handler.clone();
                async move {
                    captured.lock().await.push(body);
                    "ok"
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("http://{addr}/hook");
        let c = WebhookConsumer::new("hook-1", url);
        c.apply(&sample_event()).await.unwrap();

        let bodies = captured.lock().await;
        assert_eq!(bodies.len(), 1);
        let b = &bodies[0];
        assert_eq!(b["op"], "insert");
        assert_eq!(b["key"]["bucket"], "b");
        assert_eq!(b["key"]["id"], "x");
        // Vector arrived as a JSON array.
        assert!(b["body"]["vector"].is_array());
    }

    #[tokio::test]
    async fn webhook_consumer_surfaces_non_2xx_as_error() {
        // Server that always returns 500.
        use axum::{http::StatusCode, routing::post, Router};
        let app = Router::new().route(
            "/hook",
            post(|| async { (StatusCode::INTERNAL_SERVER_ERROR, "boom") }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("http://{addr}/hook");
        let c = WebhookConsumer::with_timeout("hook-fail", url, Duration::from_secs(2));
        let err = c.apply(&sample_event()).await.unwrap_err();
        match err {
            ConsumerError::Http(msg) => assert!(msg.contains("500"), "got: {msg}"),
            other => panic!("expected Http error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn webhook_consumer_unreachable_returns_http_error() {
        // Aim at a port nothing's listening on.
        let c = WebhookConsumer::with_timeout(
            "hook-dead",
            "http://127.0.0.1:1/hook",
            Duration::from_secs(1),
        );
        let err = c.apply(&sample_event()).await.unwrap_err();
        assert!(matches!(err, ConsumerError::Http(_)));
    }
}
