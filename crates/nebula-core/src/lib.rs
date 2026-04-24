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
