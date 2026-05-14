//! Raft log + snapshot storage for NebulaDB.
//!
//! This crate is the storage substrate for the within-region HA story
//! described in `docs/design/0004-raft-implementation.md`. It owns:
//!
//! - The on-disk `.nrlog` segment format (parallel to `nebula-wal`'s
//!   `.nwal` but extended with Raft term/index slots).
//! - Low-level append, read-by-index, and truncate primitives that
//!   the `RaftLogStorage` trait impl will sit on top of.
//!
//! What this crate does **not** yet contain:
//!
//! - The `openraft::RaftLogStorage` impl itself. That lands once the
//!   openraft 0.9.x version pin is locked (ADR §12.1) so the
//!   workspace doesn't carry an unconfirmed major dependency.
//! - The state machine wrapping `TextIndex`. That is Phase 2.2.
//! - Snapshot integration. That is Phase 2.3 — it reuses the existing
//!   `nsnap` format from `nebula-index/src/durability.rs` unchanged.
//!
//! Splitting the openraft trait impl into 2.1b keeps the diffs
//! reviewable: this commit is pure-Rust I/O code with no new
//! transitive deps; 2.1b adds openraft and the trait wiring.

pub mod log;

pub use log::{LogEntry, LogPayload, LogSegment, LogStore, LogStoreError};

/// Crate-wide result alias.
pub type Result<T> = std::result::Result<T, LogStoreError>;
