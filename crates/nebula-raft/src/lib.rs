//! Raft log + state machine + snapshot storage for NebulaDB.
//!
//! This crate is the storage substrate for the within-region HA story
//! described in `docs/design/0004-raft-implementation.md`. It owns:
//!
//! - The on-disk `.nrlog` segment format (parallel to `nebula-wal`'s
//!   `.nwal` but extended with Raft term/index slots).
//! - The `NebulaTypeConfig` openraft type binding shared by every
//!   storage and network trait we implement.
//! - The state machine wrapping `TextIndex` and applying log entries
//!   via the standalone-mode `apply_wal_record` entrypoint —
//!   guaranteeing byte-identical effect on the index regardless of
//!   whether the driver is the WAL or Raft commit.
//! - Snapshot building / install / read integrating with the existing
//!   `nsnap` format from `nebula-index/src/durability.rs` unchanged.
//! - The unified `RaftStorage` trait impl combining all three.
//!
//! What this crate does **not** yet contain:
//!
//! - Network transport (gRPC `RaftService`). That is Phase 2.4.
//! - `nebula-server` boot integration (the `NEBULA_RAFT_*` env vars
//!   that select between standalone and raft modes). That is
//!   Phase 2.5.
//!
//! The phased split (2.1a → 2.1b → 2.2 → 2.3) kept PR diffs
//! reviewable; this `lib.rs` re-exports the surface a 2.4/2.5 caller
//! needs as one flat API.

pub mod boot;
pub mod log;
pub mod network;
pub mod raft_storage;
pub mod snapshot;
pub mod state_machine;
pub mod storage;
pub mod types;

pub use boot::{bootstrap, BootError, RaftConfig, RaftHandle, SubmitError};
pub use log::{LogConfig, LogEntry, LogPayload, LogSegment, LogStore, LogStoreError};
pub use network::{GrpcRaftNetwork, GrpcRaftNetworkFactory, NebulaRaftServer, RaftRpcServer};
pub use raft_storage::NebulaRaftStorage;
pub use snapshot::{NebulaSnapshotStore, SnapshotError};
pub use state_machine::{NebulaStateMachine, StateMachineError};
pub use storage::NebulaLogStorage;
pub use types::{ClientWriteResponse, NebulaNode, NebulaTypeConfig, NodeId};

/// Crate-wide result alias for our own (non-openraft) operations.
pub type Result<T> = std::result::Result<T, LogStoreError>;
