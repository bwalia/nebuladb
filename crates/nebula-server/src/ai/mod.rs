//! NebulaDB AI Gateway — provider registry, tool loop, traces, routes.
//!
//! Frontier models provide reasoning; this module is the persistent
//! intelligence layer (retrieval, memory, SQL, tools, observability).

pub mod registry;
pub mod routes;
pub mod sql_guard;
pub mod tools;
pub mod traces;

pub use registry::{AiGateway, ProviderEntry, TaskKind};
pub use traces::{AiTrace, TraceStore};
