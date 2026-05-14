//! Raft log + snapshot storage for NebulaDB.
//!
//! This crate is the storage substrate for the within-region HA story
//! described in `docs/design/0004-raft-implementation.md`. It owns:
//!
//! - The on-disk `.nrlog` segment format (parallel to `nebula-wal`'s
//!   `.nwal` but extended with Raft term/index slots).
//! - The `openraft::RaftLogStorage` trait impl that sits on top of the
//!   raw segment store, plus a `RaftLogReader` for openraft's read
//!   path.
//! - The `NebulaTypeConfig` openraft type binding shared by every
//!   storage and network trait we implement.
//!
//! What this crate does **not** yet contain:
//!
//! - The state machine wrapping `TextIndex`. That is Phase 2.2.
//! - Snapshot integration. That is Phase 2.3 — it reuses the existing
//!   `nsnap` format from `nebula-index/src/durability.rs` unchanged.
//! - Network transport (gRPC `RaftService`). That is Phase 2.4.
//!
//! The 2.1a / 2.1b split keeps PR diffs reviewable: 2.1a was pure-Rust
//! I/O code with no new transitive deps; 2.1b (this) adds openraft and
//! the trait wiring on top.

pub mod log;
pub mod state_machine;
pub mod storage;
pub mod types;

pub use log::{LogConfig, LogEntry, LogPayload, LogSegment, LogStore, LogStoreError};
pub use state_machine::{NebulaStateMachine, StateMachineError};
pub use storage::NebulaLogStorage;
pub use types::{ClientWriteResponse, NebulaNode, NebulaTypeConfig, NodeId};

/// Crate-wide result alias for our own (non-openraft) operations.
pub type Result<T> = std::result::Result<T, LogStoreError>;
