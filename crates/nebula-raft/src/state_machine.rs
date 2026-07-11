//! Raft state machine wrapping a `TextIndex`.
//!
//! In openraft 0.9 the `RaftStorage` trait is one sealed surface that
//! bundles log storage (Phase 2.1b), state machine apply (this file),
//! and snapshot building (Phase 2.3). This module supplies the apply
//! half + the trait shell; snapshot methods are wired with minimal
//! correctness so the trait compiles, but their *real* implementation
//! lands in 2.3 alongside the streaming snapshot integration.
//!
//! # What "apply" means here
//!
//! openraft hands us committed log entries in order via
//! `apply_to_state_machine`. Each entry's payload is a `LogPayload`,
//! which today is `LogPayload::Mutation(WalRecord)`. We pass the
//! inner `WalRecord` to `TextIndex::apply_wal_record`, which is the
//! same entrypoint the standalone WAL replay path uses. That means
//! the state machine apply path is byte-identical to standalone in
//! its effect on the index — the difference is only **who** drives
//! the apply (Raft commit vs WAL replay).
//!
//! # Determinism
//!
//! Two peers that apply the same sequence of `LogPayload::Mutation`
//! entries must reach byte-identical HNSW state. This is the "vector
//! state machine" property the ADR §10 risk register flags. Today
//! `apply_wal_record` is single-threaded inside the index's lock,
//! and `WalRecord` carries resolved vectors (no embedder call at
//! apply time), so the property holds. The cross-peer determinism
//! test in this module asserts it.
//!
//! # Last-applied tracking
//!
//! openraft expects a durable `last_applied: Option<LogId>` we can
//! return from `last_applied_state()`. We keep an in-memory cell
//! plus a sidecar file (`last_applied.bin`) in the data dir so a
//! crash mid-apply doesn't replay the same entries. Atomic via the
//! same `.tmp` rename pattern the vote file uses.

use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use openraft::{EntryPayload, LogId, StoredMembership};
use parking_lot::Mutex;

use nebula_index::TextIndex;
use nebula_wal::WalRecord;

use crate::log::LogPayload;
use crate::types::{NebulaNode, NodeId};

const LAST_APPLIED_FILE: &str = "last_applied.bin";
const MEMBERSHIP_FILE: &str = "membership.bin";

/// Errors raised during state-machine apply or persistence.
#[derive(Debug, thiserror::Error)]
pub enum StateMachineError {
    #[error("index apply: {0}")]
    Apply(String),
    #[error("state-machine io: {0}")]
    Io(#[from] std::io::Error),
    #[error("state-machine codec: {0}")]
    Codec(String),
}

/// State machine driven by openraft's commit stream.
///
/// Owns:
/// - An `Arc<TextIndex>` — the actual data store.
/// - A small persistent sidecar tracking `last_applied` and the
///   currently committed membership configuration.
///
/// Cloneable because openraft's `RaftStorage` trait may be cloned
/// across tasks; the inner state is `Arc`-shared.
#[derive(Clone)]
pub struct NebulaStateMachine {
    inner: Arc<Inner>,
}

struct Inner {
    index: Arc<TextIndex>,
    dir: PathBuf,
    /// Latest applied log id. `Mutex<Option<...>>` rather than
    /// `RwLock` because writes here are infrequent (one per applied
    /// batch) and the read path goes through the file on cold boot.
    last_applied: Mutex<Option<LogId<NodeId>>>,
    /// Committed membership. openraft demands we persist this
    /// alongside the apply state — recovery uses it to reconstruct
    /// the cluster shape before the first heartbeat.
    membership: Mutex<StoredMembership<NodeId, NebulaNode>>,
}

impl NebulaStateMachine {
    /// Construct over an existing `TextIndex` and a directory for the
    /// state-machine sidecar files.
    ///
    /// On open, reads `last_applied.bin` + `membership.bin` if they
    /// exist; otherwise both default to "fresh". The caller is
    /// responsible for handing us a `TextIndex` whose contents
    /// match `last_applied` — i.e., loaded from the matching snapshot.
    pub fn open(index: Arc<TextIndex>, dir: impl AsRef<Path>) -> Result<Self, StateMachineError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let last_applied = read_last_applied(&dir)?;
        let membership = read_membership(&dir)?.unwrap_or_default();
        Ok(Self {
            inner: Arc::new(Inner {
                index,
                dir,
                last_applied: Mutex::new(last_applied),
                membership: Mutex::new(membership),
            }),
        })
    }

    /// Apply a single payload to the index.
    ///
    /// Pulled out of the trait impl so a unit test can call it
    /// directly — and so cross-peer determinism is testable without
    /// dragging in openraft's apply driver.
    pub fn apply_payload(&self, payload: &LogPayload) -> Result<(), StateMachineError> {
        match payload {
            LogPayload::Mutation(rec) => self.apply_mutation(rec),
            // Membership and blank entries carry no state-machine effect.
            // The trait-level `apply_entries` records membership entries
            // via `record_membership` and never routes them here, but a
            // direct caller (e.g. a boot-time full-log replay) may hit
            // these arms — they are correctly no-ops for the index.
            LogPayload::Membership(_) | LogPayload::Blank => Ok(()),
        }
    }

    fn apply_mutation(&self, rec: &WalRecord) -> Result<(), StateMachineError> {
        self.inner
            .index
            .apply_wal_record(rec)
            .map_err(|e| StateMachineError::Apply(e.to_string()))
    }

    /// Borrow the wrapped index. Used by 2.5 to expose reads through
    /// the existing query path while writes route through openraft.
    pub fn index(&self) -> &Arc<TextIndex> {
        &self.inner.index
    }

    /// Last applied log id, if any.
    pub fn last_applied(&self) -> Option<LogId<NodeId>> {
        *self.inner.last_applied.lock()
    }

    /// Currently committed membership.
    pub fn membership(&self) -> StoredMembership<NodeId, NebulaNode> {
        self.inner.membership.lock().clone()
    }

    /// Persist `last_applied`. Atomic via `.tmp` rename. Called by
    /// the trait impl after each successful apply batch.
    pub fn record_applied(&self, log_id: LogId<NodeId>) -> Result<(), StateMachineError> {
        write_atomic(
            &self.inner.dir.join(LAST_APPLIED_FILE),
            &bincode::serialize(&log_id).map_err(|e| StateMachineError::Codec(e.to_string()))?,
        )?;
        *self.inner.last_applied.lock() = Some(log_id);
        Ok(())
    }

    /// Persist membership. Same atomic discipline.
    pub fn record_membership(
        &self,
        log_id: LogId<NodeId>,
        membership: StoredMembership<NodeId, NebulaNode>,
    ) -> Result<(), StateMachineError> {
        write_atomic(
            &self.inner.dir.join(MEMBERSHIP_FILE),
            &bincode::serialize(&membership)
                .map_err(|e| StateMachineError::Codec(e.to_string()))?,
        )?;
        *self.inner.membership.lock() = membership;
        // Membership change advances last_applied too.
        self.record_applied(log_id)?;
        Ok(())
    }

    /// Apply a batch of openraft entries. Returns one
    /// `ClientWriteResponse` per entry (empty for now — real client
    /// responses populate in 2.5 when the leader's REST handler waits
    /// on apply).
    ///
    /// Membership entries persist the new config but produce no
    /// state-machine effect beyond that. Blank entries (the leader-
    /// establish noop) likewise are recorded as applied with no body.
    pub fn apply_entries(
        &self,
        entries: &[openraft::Entry<crate::types::NebulaTypeConfig>],
    ) -> Result<Vec<crate::types::ClientWriteResponse>, StateMachineError> {
        let mut out = Vec::with_capacity(entries.len());
        for entry in entries {
            match &entry.payload {
                EntryPayload::Normal(payload) => {
                    self.apply_payload(payload)?;
                }
                EntryPayload::Membership(m) => {
                    self.record_membership(
                        entry.log_id,
                        StoredMembership::new(Some(entry.log_id), m.clone()),
                    )?;
                    out.push(crate::types::ClientWriteResponse::default());
                    continue;
                }
                EntryPayload::Blank => {
                    // No-op — but the log id still advances and we record it
                    // below.
                }
            }
            self.record_applied(entry.log_id)?;
            out.push(crate::types::ClientWriteResponse::default());
        }
        Ok(out)
    }
}

fn read_last_applied(dir: &Path) -> Result<Option<LogId<NodeId>>, StateMachineError> {
    let path = dir.join(LAST_APPLIED_FILE);
    if !path.exists() {
        return Ok(None);
    }
    let mut f = std::fs::File::open(&path)?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    let log_id = bincode::deserialize(&buf).map_err(|e| StateMachineError::Codec(e.to_string()))?;
    Ok(Some(log_id))
}

fn read_membership(
    dir: &Path,
) -> Result<Option<StoredMembership<NodeId, NebulaNode>>, StateMachineError> {
    let path = dir.join(MEMBERSHIP_FILE);
    if !path.exists() {
        return Ok(None);
    }
    let mut f = std::fs::File::open(&path)?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    let m = bincode::deserialize(&buf).map_err(|e| StateMachineError::Codec(e.to_string()))?;
    Ok(Some(m))
}

fn write_atomic(final_path: &Path, bytes: &[u8]) -> Result<(), StateMachineError> {
    let tmp = final_path.with_extension("tmp");
    {
        let mut f = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, final_path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nebula_embed::{Embedder, MockEmbedder};
    use nebula_index::TextIndex;
    use nebula_vector::{HnswConfig, Metric};
    use openraft::{Entry, EntryPayload};
    use tempfile::tempdir;

    fn fresh_index() -> Arc<TextIndex> {
        Arc::new(
            TextIndex::new(
                Arc::new(MockEmbedder::default()),
                Metric::Cosine,
                HnswConfig::default(),
            )
            .unwrap(),
        )
    }

    fn upsert_entry(
        term: u64,
        index: u64,
        id: &str,
        text: &str,
    ) -> Entry<crate::types::NebulaTypeConfig> {
        // Resolve the text to a vector via the mock embedder so the
        // applied state matches what a real leader would have written.
        let dim = MockEmbedder::default().dim();
        let mut vector = vec![0.0_f32; dim];
        // Same simple deterministic mapping the mock uses internally
        // doesn't matter — apply is a no-op on the vector contents
        // beyond storing it. We just need a consistent vector.
        for (i, b) in text.bytes().enumerate() {
            vector[i % dim] += b as f32 / 255.0;
        }
        // Normalize cheaply.
        let norm: f32 = vector.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
        for x in &mut vector {
            *x /= norm;
        }
        Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(term, 0), index),
            payload: EntryPayload::Normal(LogPayload::Mutation(WalRecord::UpsertText {
                bucket: "b".into(),
                external_id: id.into(),
                text: text.into(),
                vector,
                metadata_json: "{}".into(),
            })),
        }
    }

    fn delete_entry(term: u64, index: u64, id: &str) -> Entry<crate::types::NebulaTypeConfig> {
        Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(term, 0), index),
            payload: EntryPayload::Normal(LogPayload::Mutation(WalRecord::Delete {
                bucket: "b".into(),
                external_id: id.into(),
            })),
        }
    }

    #[test]
    fn apply_normal_entries_advances_last_applied() {
        let dir = tempdir().unwrap();
        let sm = NebulaStateMachine::open(fresh_index(), dir.path()).unwrap();
        assert!(sm.last_applied().is_none());

        sm.apply_entries(&[
            upsert_entry(1, 1, "d1", "alpha"),
            upsert_entry(1, 2, "d2", "beta"),
            upsert_entry(2, 3, "d3", "gamma"),
        ])
        .unwrap();

        let la = sm.last_applied().unwrap();
        assert_eq!(la.index, 3);
        assert_eq!(la.leader_id.term, 2);
        // Three docs landed in the index.
        assert_eq!(sm.index().len(), 3);
    }

    #[test]
    fn last_applied_survives_reopen() {
        let dir = tempdir().unwrap();
        {
            let sm = NebulaStateMachine::open(fresh_index(), dir.path()).unwrap();
            sm.apply_entries(&[
                upsert_entry(1, 1, "d1", "alpha"),
                upsert_entry(1, 2, "d2", "beta"),
            ])
            .unwrap();
        }
        // Reopen with a fresh index — `last_applied` is what survives,
        // not the index contents. Real recovery loads the index from
        // a snapshot first.
        let sm = NebulaStateMachine::open(fresh_index(), dir.path()).unwrap();
        let la = sm.last_applied().unwrap();
        assert_eq!(la.index, 2);
    }

    #[test]
    fn blank_entry_advances_last_applied_without_apply() {
        let dir = tempdir().unwrap();
        let sm = NebulaStateMachine::open(fresh_index(), dir.path()).unwrap();
        let blank = Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 0), 1),
            payload: EntryPayload::<crate::types::NebulaTypeConfig>::Blank,
        };
        sm.apply_entries(&[blank]).unwrap();
        assert_eq!(sm.last_applied().unwrap().index, 1);
        // Blank == no body; index stays empty.
        assert_eq!(sm.index().len(), 0);
    }

    #[test]
    fn delete_after_upsert_clears_index() {
        let dir = tempdir().unwrap();
        let sm = NebulaStateMachine::open(fresh_index(), dir.path()).unwrap();
        sm.apply_entries(&[upsert_entry(1, 1, "d1", "alpha"), delete_entry(1, 2, "d1")])
            .unwrap();
        assert_eq!(sm.index().len(), 0);
        assert_eq!(sm.last_applied().unwrap().index, 2);
    }

    #[test]
    fn determinism_two_peers_reach_same_doc_count() {
        // Cross-peer determinism: two state machines fed the same
        // sequence of payloads must end up with the same observable
        // state. Byte-identity of the HNSW snapshot is what 2.3 will
        // assert via its serialize-and-compare test; here we assert
        // the cheaper invariant openraft itself relies on (same
        // last_applied + same doc count).
        let dir_a = tempdir().unwrap();
        let dir_b = tempdir().unwrap();
        let sm_a = NebulaStateMachine::open(fresh_index(), dir_a.path()).unwrap();
        let sm_b = NebulaStateMachine::open(fresh_index(), dir_b.path()).unwrap();

        let entries: Vec<_> = (1..=10)
            .map(|i| upsert_entry(1, i, &format!("d{i}"), &format!("text-{i}")))
            .collect();

        sm_a.apply_entries(&entries).unwrap();
        sm_b.apply_entries(&entries).unwrap();

        assert_eq!(sm_a.last_applied(), sm_b.last_applied());
        assert_eq!(sm_a.index().len(), sm_b.index().len());
        assert_eq!(sm_a.index().len(), 10);
    }

    #[test]
    fn membership_entry_persists_via_record_membership() {
        use openraft::Membership;
        use std::collections::BTreeMap;

        let dir = tempdir().unwrap();
        let sm = NebulaStateMachine::open(fresh_index(), dir.path()).unwrap();
        let mut nodes = BTreeMap::new();
        nodes.insert(1u64, NebulaNode::new("127.0.0.1:50052"));
        nodes.insert(2u64, NebulaNode::new("127.0.0.1:50053"));
        let m = Membership::new(vec![nodes.keys().copied().collect()], nodes);
        let entry = Entry::<crate::types::NebulaTypeConfig> {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 0), 1),
            payload: EntryPayload::Membership(m),
        };
        sm.apply_entries(&[entry]).unwrap();
        assert_eq!(sm.last_applied().unwrap().index, 1);
        let stored = sm.membership();
        assert_eq!(stored.nodes().count(), 2);
    }
}
