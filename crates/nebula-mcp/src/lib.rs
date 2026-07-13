//! NebulaDB MCP server — exposes NebulaDB to AI agents via the Model
//! Context Protocol (design 0012).
//!
//! `nebula-mcp` is a standalone service that speaks MCP over Streamable
//! HTTP to any MCP-compatible client (Claude Desktop, IDE assistants,
//! agent frameworks) and translates tool/resource/prompt requests into
//! calls against a running `nebula-server`'s REST API. It embeds no
//! index and holds no data — it is a protocol adapter that inherits
//! NebulaDB's auth and durability by forwarding the caller's bearer
//! token upstream.
//!
//! See [`tools`] for the tool surface, [`server`] for the
//! `ServerHandler` (resources + prompts), and [`client`] for the thin
//! upstream HTTP client.

pub mod client;
pub mod server;
pub mod tools;

pub use client::{ClientError, NebulaClient};
pub use tools::NebulaMcp;

use std::time::Duration;

/// Runtime configuration, assembled from the environment by
/// [`Config::from_env`].
#[derive(Debug, Clone)]
pub struct Config {
    /// Address the MCP HTTP server binds, e.g. `0.0.0.0:8090`.
    pub bind: String,
    /// Base URL of the upstream nebula-server, e.g. `http://localhost:8080`.
    pub upstream_url: String,
    /// Optional default bearer used for upstream calls when the MCP
    /// caller does not forward their own `Authorization` header.
    pub upstream_token: Option<String>,
    /// Per-request timeout for upstream calls.
    pub upstream_timeout: Duration,
    /// When true, allow any `Host` header (needed when the server is
    /// reached via a non-localhost hostname / behind a proxy). Defaults
    /// to false — rmcp restricts to loopback hosts, which is the safe
    /// default for local use. Set `NEBULA_MCP_ALLOW_ANY_HOST=true` for
    /// containerized / remote deployments.
    pub allow_any_host: bool,
}

impl Config {
    /// Assemble from env vars. Only `NEBULA_MCP_UPSTREAM_URL` has no safe
    /// universal default, so it falls back to the conventional local
    /// nebula-server address.
    pub fn from_env() -> Self {
        let bind = std::env::var("NEBULA_MCP_BIND").unwrap_or_else(|_| "0.0.0.0:8090".to_string());
        let upstream_url = std::env::var("NEBULA_MCP_UPSTREAM_URL")
            .unwrap_or_else(|_| "http://localhost:8080".to_string());
        let upstream_token = std::env::var("NEBULA_MCP_UPSTREAM_TOKEN")
            .ok()
            .filter(|s| !s.is_empty());
        let upstream_timeout = std::env::var("NEBULA_MCP_UPSTREAM_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(30));
        let allow_any_host = std::env::var("NEBULA_MCP_ALLOW_ANY_HOST")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);
        Self {
            bind,
            upstream_url,
            upstream_token,
            upstream_timeout,
            allow_any_host,
        }
    }

    /// Construct the upstream client from this config.
    pub fn client(&self) -> NebulaClient {
        NebulaClient::new(
            self.upstream_url.clone(),
            self.upstream_token.clone(),
            self.upstream_timeout,
        )
    }
}
