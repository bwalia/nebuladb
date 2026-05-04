//! Axum-based REST server for NebulaDB.
//!
//! Exposes:
//! - `GET  /healthz`           liveness + metadata
//! - `GET  /metrics`           Prometheus text format
//! - `POST /api/v1/bucket/:b/doc`         insert / upsert
//! - `GET  /api/v1/bucket/:b/doc/:id`     fetch
//! - `DELETE /api/v1/bucket/:b/doc/:id`   delete
//! - `POST /api/v1/vector/search`         search by vector
//! - `POST /api/v1/ai/search`             semantic search by text
//! - `POST /api/v1/ai/rag`                RAG (JSON or SSE stream)
//!
//! Middleware: request tracing, bearer-token auth, per-route metrics.
//! All handlers return structured JSON errors with stable `code` strings.

pub mod audit;
pub mod build_info;
pub mod cluster;
pub mod error;
pub mod jwt;
pub mod log_stream;
pub mod metrics;
pub mod middleware;
pub mod ratelimit;
pub mod router;
pub mod slow_log;
pub mod state;

pub use audit::AuditLog;
pub use cluster::{ClusterConfig, NodeRole, PeerInfo};
pub use jwt::JwtConfig;
pub use log_stream::{CapturingSubscriber, LogBus, LogEvent, LogLevel};
pub use ratelimit::{RateLimitConfig, RateLimiter};
pub use router::build_router;
pub use slow_log::SlowQueryLog;
pub use state::{AppConfig, AppState};
