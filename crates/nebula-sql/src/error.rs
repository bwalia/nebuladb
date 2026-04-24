//! Error type for the SQL subsystem.
//!
//! The variants are deliberately fine-grained so the REST layer can
//! map them to the right HTTP status (400 vs 500) without string
//! matching.

use thiserror::Error;

use nebula_index::IndexError;

#[derive(Debug, Error)]
pub enum SqlError {
    #[error("parse error: {0}")]
    Parse(String),

    /// Syntactically valid SQL, but using a feature we don't support.
    /// Kept separate from `Parse` so the API can return a 400 with a
    /// helpful "NebulaDB doesn't support X" message instead of a raw
    /// parser diagnostic.
    #[error("unsupported: {0}")]
    Unsupported(String),

    /// Something the planner can't execute, e.g. a WHERE clause with
    /// neither a semantic predicate nor a direct id lookup.
    #[error("invalid plan: {0}")]
    InvalidPlan(String),

    /// Literal shape mismatch, e.g. `vector_distance(col, 'abc')`.
    #[error("type error: {0}")]
    TypeError(String),

    #[error(transparent)]
    Index(#[from] IndexError),
}
