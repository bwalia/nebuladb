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

impl NodeRole {
    fn as_u8(self) -> u8 {
        match self {
            NodeRole::Standalone => 0,
            NodeRole::Leader => 1,
            NodeRole::Follower => 2,
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            1 => NodeRole::Leader,
            2 => NodeRole::Follower,
            _ => NodeRole::Standalone,
        }
    }
}

/// A [`NodeRole`] that can be flipped at runtime and read concurrently.
///
/// Role is read on the write path of three independent protocol servers
/// (REST middleware, gRPC services, pgwire handler), each constructed
/// with its own copy of the role at boot. Runtime promotion (design 0009
/// §5) requires all three to observe the change at once, so they share a
/// single `Arc<AtomicNodeRole>` instead of holding `NodeRole` copies.
/// Backed by an `AtomicU8`; `Relaxed` ordering is sufficient — promotion
/// is an operator-triggered, rare event and the write path only needs to
/// *eventually* observe the flip, not synchronize other memory.
#[derive(Debug)]
pub struct AtomicNodeRole(std::sync::atomic::AtomicU8);

impl AtomicNodeRole {
    pub fn new(role: NodeRole) -> Self {
        Self(std::sync::atomic::AtomicU8::new(role.as_u8()))
    }

    pub fn load(&self) -> NodeRole {
        NodeRole::from_u8(self.0.load(std::sync::atomic::Ordering::Relaxed))
    }

    pub fn store(&self, role: NodeRole) {
        self.0.store(role.as_u8(), std::sync::atomic::Ordering::Relaxed);
    }

    /// Convenience mirror of [`NodeRole::is_read_only`] reading the
    /// current value — the write-guard call site on every protocol.
    pub fn is_read_only(&self) -> bool {
        self.load().is_read_only()
    }
}

impl Default for AtomicNodeRole {
    fn default() -> Self {
        Self::new(NodeRole::default())
    }
}
