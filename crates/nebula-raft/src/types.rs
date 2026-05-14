//! openraft type configuration for NebulaDB.
//!
//! `openraft` is generic over a `RaftTypeConfig`: a single trait that
//! pins down node id, request, response, and entry types. Once
//! declared, every other openraft trait (storage, network, the Raft
//! handle itself) takes our config as a type parameter.
//!
//! This file is the smallest stable surface for that config; the
//! storage impls and 2.2's state machine both type-parameterize on
//! `NebulaTypeConfig`.

use serde::{Deserialize, Serialize};

use crate::log::LogPayload;

/// Node id type. We use `u64` matching openraft's defaults.
///
/// Each NebulaDB node gets a unique id at boot via `NEBULA_RAFT_NODE_ID`.
/// The operator hands these out monotonically per Raft cluster — a
/// new pod replacing a dead one keeps the same id, so the cluster
/// sees a restart, not a membership change.
pub type NodeId = u64;

/// Per-node metadata openraft will gossip with its peers.
///
/// The `addr` is the gRPC address peers should reach this node at for
/// Raft transport (the `:50052` from ADR §12.2). It is **not** the
/// REST or client gRPC address — those are independent.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NebulaNode {
    pub addr: String,
}

impl NebulaNode {
    pub fn new(addr: impl Into<String>) -> Self {
        Self { addr: addr.into() }
    }
}

/// What the leader returns to a client after committing a log entry.
///
/// Empty for now: the state machine in 2.2 will populate this with
/// the apply result (e.g. the dim/bucket from an upsert). For 2.1b
/// we just need a type that round-trips through serde.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct ClientWriteResponse {}

openraft::declare_raft_types! {
    /// Master type binding. Every openraft trait we implement
    /// references `NebulaTypeConfig` so any change here cascades
    /// in one place.
    pub NebulaTypeConfig:
        D = LogPayload,
        R = ClientWriteResponse,
        NodeId = NodeId,
        Node = NebulaNode,
        Entry = openraft::Entry<NebulaTypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
}
