//! Server-side boot helpers: read env config, wire storage + network,
//! spawn an `openraft::Raft<NebulaTypeConfig>` instance.
//!
//! This module is the *consumer* of everything Phases 2.1–2.4 built.
//! `nebula-server`'s main.rs calls into here when raft mode is
//! configured; otherwise the standalone WAL path runs unchanged.
//!
//! # Env-var surface
//!
//! Raft mode is opt-in. Setting `NEBULA_RAFT_PEERS` (alongside
//! `NEBULA_RAFT_NODE_ID` and `NEBULA_RAFT_DATA_DIR`) switches the
//! server into raft mode at boot. Without those, the existing
//! standalone path runs unchanged.
//!
//! | Variable | Required | Purpose |
//! |----------|----------|---------|
//! | `NEBULA_RAFT_PEERS` | yes | Cluster peer list: `1=host:50052,2=host2:50052,3=host3:50052` |
//! | `NEBULA_RAFT_NODE_ID` | yes | This node's id (must appear in PEERS) |
//! | `NEBULA_RAFT_DATA_DIR` | yes | Where the Raft log + state-machine sidecars + snapshots live |
//! | `NEBULA_RAFT_BIND` | no | Raft gRPC bind, default `0.0.0.0:50052` (ADR §12.2) |
//! | `NEBULA_RAFT_CLUSTER_NAME` | no | Default `nebula`. Cluster identity for openraft logs. |
//!
//! # What this module does NOT do (yet)
//!
//! - Modify `nebula-server`'s `main.rs` to actually call into here.
//!   Wiring into the boot path lives in a follow-up (call it 2.5b);
//!   this PR is the additive helper crate so the boot integration
//!   is reviewable in isolation.
//! - Switch the write-path middleware to ask Raft "am I leader?"
//!   instead of the static `NEBULA_NODE_ROLE`. Same follow-up.
//! - pgwire / gRPC parity for the new "wrong node" guard. Same.
//!
//! Splitting 2.5 like this keeps each PR small enough to review and
//! limits the blast radius if the boot integration needs revision.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use openraft::storage::Adaptor;
use openraft::{Config, Raft};

use crate::log::LogConfig;
use crate::network::GrpcRaftNetworkFactory;
use crate::raft_storage::NebulaRaftStorage;
use crate::snapshot::NebulaSnapshotStore;
use crate::state_machine::NebulaStateMachine;
use crate::storage::NebulaLogStorage;
use crate::types::{NebulaNode, NebulaTypeConfig, NodeId};

const DEFAULT_RAFT_BIND: &str = "0.0.0.0:50052";
const DEFAULT_CLUSTER_NAME: &str = "nebula";

/// Errors raised during boot config parsing or Raft spawn.
#[derive(Debug, thiserror::Error)]
pub enum BootError {
    #[error("raft config: {0}")]
    Config(String),
    #[error("raft storage: {0}")]
    Storage(String),
    #[error("raft fatal: {0}")]
    Fatal(String),
}

/// Parsed Raft configuration. `from_env` returns `Ok(None)` when raft
/// mode is not configured — that's the cue for the caller to fall
/// back to the standalone WAL path.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub node_id: NodeId,
    /// Map of peer-id → "host:port" for the gRPC raft transport.
    /// **Includes** this node's own entry (openraft requires the
    /// initial-membership map to list every voter, including self).
    pub peers: BTreeMap<NodeId, NebulaNode>,
    /// Directory under which raft log segments, state-machine
    /// sidecars, and snapshots live. Created if absent.
    pub data_dir: PathBuf,
    /// `host:port` to bind the gRPC `NebulaRaft` service on.
    pub bind: String,
    pub cluster_name: String,
}

impl RaftConfig {
    /// Read env vars; return `Ok(None)` if raft mode is not opted in.
    /// Returns `Err` only on parse errors — a half-configured set
    /// (e.g. PEERS without NODE_ID) trips this so boot fails loudly
    /// rather than silently falling back to standalone.
    pub fn from_env() -> Result<Option<Self>, BootError> {
        let peers_raw = match std::env::var("NEBULA_RAFT_PEERS") {
            Ok(s) if !s.is_empty() => s,
            _ => return Ok(None),
        };
        let node_id_raw = std::env::var("NEBULA_RAFT_NODE_ID")
            .ok()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                BootError::Config(
                    "NEBULA_RAFT_PEERS is set but NEBULA_RAFT_NODE_ID is missing".into(),
                )
            })?;
        let node_id: NodeId = node_id_raw
            .parse()
            .map_err(|e| BootError::Config(format!("NEBULA_RAFT_NODE_ID must be u64: {e}")))?;
        let data_dir = std::env::var("NEBULA_RAFT_DATA_DIR")
            .ok()
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| {
                BootError::Config(
                    "NEBULA_RAFT_PEERS is set but NEBULA_RAFT_DATA_DIR is missing".into(),
                )
            })?;
        let bind =
            std::env::var("NEBULA_RAFT_BIND").unwrap_or_else(|_| DEFAULT_RAFT_BIND.to_string());
        let cluster_name = std::env::var("NEBULA_RAFT_CLUSTER_NAME")
            .unwrap_or_else(|_| DEFAULT_CLUSTER_NAME.to_string());

        let peers = parse_peers(&peers_raw)?;
        if !peers.contains_key(&node_id) {
            return Err(BootError::Config(format!(
                "NEBULA_RAFT_NODE_ID={node_id} is not in NEBULA_RAFT_PEERS"
            )));
        }
        // ADR §7: at least 3 voting peers, odd count.
        if peers.len() < 3 {
            return Err(BootError::Config(format!(
                "Raft cluster must have at least 3 peers (got {})",
                peers.len()
            )));
        }
        if peers.len() % 2 == 0 {
            return Err(BootError::Config(format!(
                "Raft cluster peer count must be odd (got {})",
                peers.len()
            )));
        }

        Ok(Some(Self {
            node_id,
            peers,
            data_dir,
            bind,
            cluster_name,
        }))
    }
}

/// Parse `id=host:port,id=host:port,...` into a peer map.
///
/// Empty entries are silently skipped; malformed entries fail. We
/// validate at boot rather than letting openraft swallow a typo'd
/// peer at first heartbeat.
fn parse_peers(s: &str) -> Result<BTreeMap<NodeId, NebulaNode>, BootError> {
    let mut out = BTreeMap::new();
    for raw in s.split(',') {
        let raw = raw.trim();
        if raw.is_empty() {
            continue;
        }
        let (id_str, addr) = raw
            .split_once('=')
            .ok_or_else(|| BootError::Config(format!("peer entry missing '=' in: {raw}")))?;
        let id: NodeId = id_str
            .trim()
            .parse()
            .map_err(|e| BootError::Config(format!("peer id must be u64 in {raw}: {e}")))?;
        let addr = addr.trim();
        if addr.is_empty() {
            return Err(BootError::Config(format!(
                "peer entry has empty address in: {raw}"
            )));
        }
        if out.insert(id, NebulaNode::new(addr.to_string())).is_some() {
            return Err(BootError::Config(format!("duplicate peer id {id}")));
        }
    }
    Ok(out)
}

/// Wired-together raft instance. The server holds this for the
/// process lifetime; `Drop` shuts the Raft instance cleanly.
///
/// Phase 2.5b will plumb writes through `RaftHandle::client_write`
/// and reads through `storage().state_machine().index()`.
pub struct RaftHandle {
    pub raft: Arc<Raft<NebulaTypeConfig>>,
    pub storage: NebulaRaftStorage,
    pub network: GrpcRaftNetworkFactory,
    pub bind: String,
    pub config: RaftConfig,
}

impl RaftHandle {
    pub fn raft(&self) -> &Arc<Raft<NebulaTypeConfig>> {
        &self.raft
    }

    pub fn storage(&self) -> &NebulaRaftStorage {
        &self.storage
    }

    /// Submit a mutation through Raft consensus.
    ///
    /// On the leader: appends to the log, replicates to a quorum,
    /// applies via the state machine, returns once committed.
    /// On a follower: returns [`SubmitError::NotLeader`] carrying
    /// the current leader's id (when known) so the caller can
    /// reject with the right HTTP status (409 / 421) and the
    /// leader address so a smart client can retry.
    ///
    /// `app_data` is the same `LogPayload` shape the state machine
    /// applies — typically `LogPayload::Mutation(WalRecord::...)`.
    pub async fn submit_mutation(
        &self,
        app_data: crate::log::LogPayload,
    ) -> Result<(), SubmitError> {
        match self.raft.client_write(app_data).await {
            Ok(_) => Ok(()),
            Err(e) => {
                use openraft::error::{ClientWriteError, RaftError};
                match e {
                    RaftError::APIError(ClientWriteError::ForwardToLeader(f)) => {
                        Err(SubmitError::NotLeader {
                            leader_id: f.leader_id,
                            leader_addr: f.leader_node.map(|n| n.addr),
                        })
                    }
                    RaftError::APIError(ClientWriteError::ChangeMembershipError(_)) => {
                        // We never submit membership changes through
                        // submit_mutation — those go through
                        // Raft::change_membership directly.
                        Err(SubmitError::Other(
                            "unexpected ChangeMembership variant".into(),
                        ))
                    }
                    RaftError::Fatal(f) => Err(SubmitError::Other(f.to_string())),
                }
            }
        }
    }
}

/// Outcome of a failed [`RaftHandle::submit_mutation`].
#[derive(Debug, thiserror::Error)]
pub enum SubmitError {
    /// This node isn't the leader. The optional leader id + addr come
    /// from openraft's `ForwardToLeader` so a smart REST client (or
    /// our own retry middleware) can re-target without polling
    /// `/admin/cluster/nodes`.
    #[error("not leader (current leader: {leader_id:?}, addr: {leader_addr:?})")]
    NotLeader {
        leader_id: Option<NodeId>,
        leader_addr: Option<String>,
    },
    /// Anything else openraft reports — typically a fatal Raft state
    /// (storage error mid-apply, runaway election). Client retry is
    /// safe; the operator should investigate the node.
    #[error("raft submit: {0}")]
    Other(String),
}

/// Bootstrap a Raft instance from parsed config + a fresh `TextIndex`.
///
/// Steps:
/// 1. Open the on-disk log + vote storage at `<data_dir>/log`.
/// 2. Open the state-machine sidecars at `<data_dir>/sm`. Note the
///    caller hands us the `Arc<TextIndex>` it wants the SM to wrap;
///    standalone-vs-raft mode shares the same index type, so the
///    caller can construct it once and pass it in here.
/// 3. Open the snapshot store at `<data_dir>` (snapshots/ + raft-recv/).
/// 4. Compose into `NebulaRaftStorage`, then `Adaptor` it into the
///    v2 `RaftLogStorage` + `RaftStateMachine` shape `Raft::new`
///    expects in 0.9.
/// 5. Spawn an `openraft::Raft` with the network factory.
///
/// The caller is responsible for separately starting the gRPC
/// transport server (binding `config.bind`, mounting `RaftRpcServer`
/// over the Raft handle) — that's a tonic plumbing detail that
/// belongs in `nebula-server`'s main.rs, not here.
pub async fn bootstrap(
    config: RaftConfig,
    index: Arc<nebula_index::TextIndex>,
) -> Result<RaftHandle, BootError> {
    let log_dir = config.data_dir.join("log");
    let sm_dir = config.data_dir.join("sm");

    let log = NebulaLogStorage::open(&log_dir, LogConfig::default())
        .map_err(|e| BootError::Storage(format!("open log at {log_dir:?}: {e}")))?;
    let state_machine = NebulaStateMachine::open(index, &sm_dir)
        .map_err(|e| BootError::Storage(format!("open state machine at {sm_dir:?}: {e}")))?;
    let snapshots = NebulaSnapshotStore::new(state_machine.clone(), &config.data_dir)
        .map_err(|e| BootError::Storage(format!("open snapshot store: {e}")))?;

    let storage = NebulaRaftStorage::new(log, state_machine, snapshots);
    let (log_store, sm_adaptor) = Adaptor::new(storage.clone());

    let network = GrpcRaftNetworkFactory::new();

    let raft_config = Config {
        cluster_name: config.cluster_name.clone(),
        // openraft's election timeout defaults are 150ms-300ms which
        // is appropriate for a same-DC cluster. ADR §10 calls out
        // soak as the time to tune these — leaving defaults until
        // 2.6 says otherwise.
        ..Default::default()
    };

    let raft = Raft::<NebulaTypeConfig>::new(
        config.node_id,
        Arc::new(raft_config),
        network.clone(),
        log_store,
        sm_adaptor,
    )
    .await
    .map_err(|e| BootError::Fatal(format!("Raft::new: {e}")))?;

    Ok(RaftHandle {
        raft: Arc::new(raft),
        storage,
        network,
        bind: config.bind.clone(),
        config,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Process-wide lock for the env-mutating tests. cargo test runs
    /// tests in parallel by default; without a mutex two `with_env`
    /// callers race on `NEBULA_RAFT_*` and one sees a half-set
    /// fixture. Serializes only the env tests, leaving the rest of
    /// the suite to run in parallel.
    static ENV_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

    fn with_env<F: FnOnce()>(vars: &[(&str, Option<&str>)], f: F) {
        let _guard = ENV_LOCK.lock();
        // Snapshot then restore. Avoids leaking state to peer tests
        // even though the lock keeps them out of the critical section
        // — defence in depth.
        let saved: Vec<_> = vars
            .iter()
            .map(|(k, _)| (*k, std::env::var(k).ok()))
            .collect();
        for (k, v) in vars {
            match v {
                Some(val) => std::env::set_var(k, val),
                None => std::env::remove_var(k),
            }
        }
        f();
        for (k, v) in saved {
            match v {
                Some(val) => std::env::set_var(k, val),
                None => std::env::remove_var(k),
            }
        }
    }

    #[test]
    fn from_env_returns_none_without_peers() {
        with_env(&[("NEBULA_RAFT_PEERS", None)], || {
            assert!(RaftConfig::from_env().unwrap().is_none());
        });
    }

    #[test]
    fn from_env_parses_minimal_valid_config() {
        with_env(
            &[
                (
                    "NEBULA_RAFT_PEERS",
                    Some("1=host1:50052,2=host2:50052,3=host3:50052"),
                ),
                ("NEBULA_RAFT_NODE_ID", Some("2")),
                ("NEBULA_RAFT_DATA_DIR", Some("/tmp/test")),
                ("NEBULA_RAFT_BIND", None),
                ("NEBULA_RAFT_CLUSTER_NAME", None),
            ],
            || {
                let cfg = RaftConfig::from_env().unwrap().unwrap();
                assert_eq!(cfg.node_id, 2);
                assert_eq!(cfg.peers.len(), 3);
                assert_eq!(cfg.peers[&1].addr, "host1:50052");
                assert_eq!(cfg.bind, DEFAULT_RAFT_BIND);
                assert_eq!(cfg.cluster_name, DEFAULT_CLUSTER_NAME);
            },
        );
    }

    #[test]
    fn from_env_rejects_node_id_not_in_peers() {
        with_env(
            &[
                ("NEBULA_RAFT_PEERS", Some("1=h:1,2=h:2,3=h:3")),
                ("NEBULA_RAFT_NODE_ID", Some("99")),
                ("NEBULA_RAFT_DATA_DIR", Some("/tmp")),
            ],
            || {
                let err = RaftConfig::from_env().unwrap_err();
                assert!(matches!(err, BootError::Config(_)));
            },
        );
    }

    #[test]
    fn from_env_rejects_even_peer_count() {
        with_env(
            &[
                ("NEBULA_RAFT_PEERS", Some("1=h:1,2=h:2,3=h:3,4=h:4")),
                ("NEBULA_RAFT_NODE_ID", Some("1")),
                ("NEBULA_RAFT_DATA_DIR", Some("/tmp")),
            ],
            || {
                let err = RaftConfig::from_env().unwrap_err();
                let msg = err.to_string();
                assert!(msg.contains("odd"), "expected 'odd' in: {msg}");
            },
        );
    }

    #[test]
    fn from_env_rejects_too_few_peers() {
        with_env(
            &[
                ("NEBULA_RAFT_PEERS", Some("1=h:1")),
                ("NEBULA_RAFT_NODE_ID", Some("1")),
                ("NEBULA_RAFT_DATA_DIR", Some("/tmp")),
            ],
            || {
                let err = RaftConfig::from_env().unwrap_err();
                let msg = err.to_string();
                assert!(msg.contains("at least 3"), "expected '3' in: {msg}");
            },
        );
    }

    #[test]
    fn from_env_rejects_missing_node_id_when_peers_set() {
        with_env(
            &[
                ("NEBULA_RAFT_PEERS", Some("1=h:1,2=h:2,3=h:3")),
                ("NEBULA_RAFT_NODE_ID", None),
                ("NEBULA_RAFT_DATA_DIR", Some("/tmp")),
            ],
            || {
                let err = RaftConfig::from_env().unwrap_err();
                let msg = err.to_string();
                assert!(msg.contains("NEBULA_RAFT_NODE_ID"));
            },
        );
    }

    #[test]
    fn parse_peers_rejects_duplicate_id() {
        let err = parse_peers("1=h:1,1=h:2").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("duplicate"));
    }

    #[test]
    fn parse_peers_rejects_non_numeric_id() {
        let err = parse_peers("foo=h:1").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("u64"));
    }

    #[tokio::test]
    async fn bootstrap_brings_up_a_raft_instance() {
        use nebula_embed::MockEmbedder;
        use nebula_index::TextIndex;
        use nebula_vector::{HnswConfig, Metric};
        use std::sync::Arc;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let cfg = RaftConfig {
            node_id: 1,
            peers: {
                let mut m = BTreeMap::new();
                m.insert(1u64, NebulaNode::new("127.0.0.1:50052"));
                m.insert(2u64, NebulaNode::new("127.0.0.1:50053"));
                m.insert(3u64, NebulaNode::new("127.0.0.1:50054"));
                m
            },
            data_dir: dir.path().to_path_buf(),
            bind: "127.0.0.1:0".into(),
            cluster_name: "test".into(),
        };
        let index = Arc::new(
            TextIndex::new(
                Arc::new(MockEmbedder::default()),
                Metric::Cosine,
                HnswConfig::default(),
            )
            .unwrap(),
        );
        let handle = bootstrap(cfg, index)
            .await
            .expect("bootstrap should succeed");
        // The raft instance is running and reports a current node id.
        let metrics = handle.raft().metrics().borrow().clone();
        assert_eq!(metrics.id, 1);
        // Storage and network handles are accessible via the public surface.
        assert!(handle.storage().state_machine().last_applied().is_none());
    }
}
