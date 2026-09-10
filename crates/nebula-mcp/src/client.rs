//! Thin HTTP client to a running `nebula-server`.
//!
//! `nebula-mcp` is deliberately a *client* of NebulaDB's existing REST
//! surface, not an embedder of the index (design 0012 §3). This keeps
//! the MCP server independently deployable and lets it reuse
//! nebula-server's auth, rate limiting, admission control, and
//! durability guarantees unchanged — the MCP layer adds an
//! agent-facing protocol, not a second copy of the database.
//!
//! Every call targets `{base}/api/v1/...` and returns the parsed JSON
//! body as a `serde_json::Value`, which the tool layer renders back to
//! the agent. Non-2xx responses are surfaced as [`ClientError`] with
//! the server's own error body attached, so an agent sees NebulaDB's
//! real diagnostics (e.g. `sql_unsupported`, `not_leader`) rather than
//! a generic failure.

use std::time::Duration;

use serde_json::Value;

/// Errors talking to nebula-server. Kept small; the tool layer converts
/// these into MCP tool errors carrying the message.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("transport: {0}")]
    Transport(String),
    /// The server answered with a non-2xx status. `body` is the raw
    /// response text (usually NebulaDB's `{"error":{"code",...}}`).
    #[error("nebula-server returned {status}: {body}")]
    Status { status: u16, body: String },
    #[error("decode: {0}")]
    Decode(String),
}

/// Client to a single nebula-server base URL.
#[derive(Clone)]
pub struct NebulaClient {
    http: reqwest::Client,
    base: String,
    /// Default bearer token used when a per-request token is not
    /// supplied. Set from `NEBULA_MCP_UPSTREAM_TOKEN`. A per-request
    /// token (forwarded from the MCP caller's own `Authorization`
    /// header) takes precedence — see [`Self::with_token`].
    default_token: Option<String>,
}

impl NebulaClient {
    /// Build a client. `base` is the nebula-server root, e.g.
    /// `http://localhost:8080` (no trailing `/api/v1`).
    pub fn new(base: impl Into<String>, default_token: Option<String>, timeout: Duration) -> Self {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .expect("reqwest client builds with static config");
        Self {
            http,
            base: base.into().trim_end_matches('/').to_string(),
            default_token,
        }
    }

    /// Return a copy of this client that authenticates with `token`
    /// (the MCP caller's forwarded bearer). Falls back to the default
    /// token when `token` is `None`. Cheap — the underlying reqwest
    /// client is `Arc`-shared.
    pub fn with_token(&self, token: Option<String>) -> Self {
        Self {
            http: self.http.clone(),
            base: self.base.clone(),
            default_token: token.or_else(|| self.default_token.clone()),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}/api/v1{}", self.base, path)
    }

    fn auth(&self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.default_token {
            Some(t) => rb.bearer_auth(t),
            None => rb,
        }
    }

    /// GET `{base}/api/v1{path}`.
    pub async fn get(&self, path: &str) -> Result<Value, ClientError> {
        let req = self.auth(self.http.get(self.url(path)));
        Self::send(req).await
    }

    /// POST `{base}/api/v1{path}` with a JSON body.
    pub async fn post(&self, path: &str, body: &Value) -> Result<Value, ClientError> {
        let req = self.auth(self.http.post(self.url(path)).json(body));
        Self::send(req).await
    }

    /// DELETE `{base}/api/v1{path}`.
    pub async fn delete(&self, path: &str) -> Result<Value, ClientError> {
        let req = self.auth(self.http.delete(self.url(path)));
        Self::send(req).await
    }

    /// GET a non-`/api/v1` path (e.g. `/healthz`, `/metrics`) verbatim
    /// under the base. Returns the raw text body.
    pub async fn get_raw(&self, path: &str) -> Result<String, ClientError> {
        let req = self.auth(self.http.get(format!("{}{}", self.base, path)));
        let resp = req
            .send()
            .await
            .map_err(|e| ClientError::Transport(e.to_string()))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| ClientError::Transport(e.to_string()))?;
        if status.is_success() {
            Ok(text)
        } else {
            Err(ClientError::Status {
                status: status.as_u16(),
                body: text,
            })
        }
    }

    async fn send(req: reqwest::RequestBuilder) -> Result<Value, ClientError> {
        let resp = req
            .send()
            .await
            .map_err(|e| ClientError::Transport(e.to_string()))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| ClientError::Transport(e.to_string()))?;
        if !status.is_success() {
            return Err(ClientError::Status {
                status: status.as_u16(),
                body: text,
            });
        }
        // Some endpoints (e.g. an empty 200) may return no body; treat
        // that as JSON null rather than a decode error.
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&text).map_err(|e| ClientError::Decode(e.to_string()))
    }
}
