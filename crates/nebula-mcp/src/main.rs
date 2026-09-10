//! `nebula-mcp` binary — serves the NebulaDB MCP server over Streamable
//! HTTP and mounts it on an axum router at `/mcp`.
//!
//! # Env
//!
//! | Variable | Default | Purpose |
//! |----------|---------|---------|
//! | `NEBULA_MCP_BIND` | `0.0.0.0:8090` | Address the MCP HTTP server binds |
//! | `NEBULA_MCP_UPSTREAM_URL` | `http://localhost:8080` | nebula-server REST base |
//! | `NEBULA_MCP_UPSTREAM_TOKEN` | *(unset)* | Default bearer for upstream calls |
//! | `NEBULA_MCP_UPSTREAM_TIMEOUT_SECS` | `30` | Upstream request timeout |
//! | `NEBULA_MCP_ALLOW_ANY_HOST` | `false` | Allow non-loopback Host headers |
//!
//! Point any MCP client at `http://<bind>/mcp`. The caller's
//! `Authorization: Bearer <token>` is forwarded to nebula-server, so
//! MCP access is governed by NebulaDB's existing auth.

use std::sync::Arc;

use nebula_mcp::{Config, NebulaMcp};
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,nebula_mcp=debug")),
        )
        .init();

    let config = Config::from_env();
    tracing::info!(
        bind = %config.bind,
        upstream = %config.upstream_url,
        allow_any_host = config.allow_any_host,
        "starting nebula-mcp",
    );

    let client = config.client();
    let ct = CancellationToken::new();

    // The factory is called once per MCP session. NebulaMcp is cheap to
    // clone (an Arc-backed reqwest client + the tool router), so each
    // session gets its own handler sharing the same upstream connection
    // pool.
    let mut http_config = StreamableHttpServerConfig::default()
        .with_cancellation_token(ct.child_token());
    if config.allow_any_host {
        // Behind a proxy / reached by hostname: rmcp otherwise restricts
        // to loopback Host headers as an anti-DNS-rebinding measure.
        http_config = http_config.disable_allowed_hosts();
    }

    let service = StreamableHttpService::<NebulaMcp, LocalSessionManager>::new(
        move || Ok(NebulaMcp::new(client.clone())),
        Arc::new(LocalSessionManager::default()),
        http_config,
    );

    let app = axum::Router::new().nest_service("/mcp", service);
    let listener = tokio::net::TcpListener::bind(&config.bind).await?;
    tracing::info!("nebula-mcp listening on http://{}/mcp", config.bind);

    let shutdown = {
        let ct = ct.clone();
        async move {
            tokio::signal::ctrl_c().await.ok();
            tracing::info!("shutdown signal received");
            ct.cancel();
        }
    };

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await?;

    Ok(())
}
