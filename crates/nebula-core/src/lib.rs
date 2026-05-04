//! Shared primitives for NebulaDB: IDs, errors, traits.
//!
//! This crate is intentionally small and dependency-light so every other
//! crate can depend on it without pulling in heavy transitive deps.

use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Monotonically-assigned identifier for vectors, documents, and chunks.
///
/// Wrapping a `u64` in a newtype lets us later swap the representation
/// (ULID, UUIDv7) without a source-level churn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Id(pub u64);

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for Id {
    fn from(value: u64) -> Self {
        Id(value)
    }
}

#[derive(Debug, Error)]
pub enum NebulaError {
    #[error("dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },

    #[error("index is empty")]
    EmptyIndex,

    #[error("id not found: {0}")]
    NotFound(Id),

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("embedder error: {0}")]
    Embedder(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, NebulaError>;

/// Node role in a replication topology. Moved here (rather than kept in
/// `nebula-server`) so `nebula-grpc` and `nebula-pgwire` can refuse
/// writes on a follower without taking a dependency on the server crate.
///
/// The serialized form is `"standalone" | "leader" | "follower"` to
/// match `NEBULA_NODE_ROLE` and `/admin/cluster/nodes` JSON output.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    /// Single-process deployment with no replication. Default when
    /// `NEBULA_NODE_ROLE` is unset.
    #[default]
    Standalone,
    /// Accepts writes and serves the WAL to followers.
    Leader,
    /// Read-only mirror of a leader. Writes are refused at every protocol.
    Follower,
}

impl NodeRole {
    /// `true` when this role should refuse writes. Shared by the REST
    /// middleware, gRPC services, and pgwire query handler.
    pub fn is_read_only(&self) -> bool {
        matches!(self, NodeRole::Follower)
    }
}

impl std::str::FromStr for NodeRole {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "standalone" => Ok(Self::Standalone),
            "leader" => Ok(Self::Leader),
            "follower" => Ok(Self::Follower),
            other => Err(format!("unknown node role: {other}")),
        }
    }
}
