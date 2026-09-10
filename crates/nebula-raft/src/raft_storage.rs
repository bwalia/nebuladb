//! `openraft::RaftStorage` trait impl combining log (Phase 2.1b),
//! state machine (Phase 2.2), and snapshot (Phase 2.3).
//!
//! In openraft 0.9 the `RaftStorage` trait is a single sealed surface
//! that satisfies log + apply + snapshot. Only now — with all three
//! halves built — can we wire it. The impl is mostly delegation:
//!
//! - Log reads / appends → [`NebulaLogStorage`] + [`crate::log::LogStore`]
//! - Vote save/read → [`NebulaLogStorage::save_vote`] / `read_vote`
//! - Apply → [`NebulaStateMachine::apply_entries`]
//! - Snapshot build / install / read → [`NebulaSnapshotStore`]
//!
//! Errors from each layer get wrapped in the right `StorageIOError`
//! variant so openraft can categorize failures correctly. When in
//! doubt we map to `read_logs` since most surfaces are log-shaped.
//!
//! # Why one big trait impl
//!
//! openraft 0.9's `RaftStorage` is `Sealed`. That means we can't
//! satisfy a partial impl in one crate and a partial in another —
//! the compiler rejects it. Bundling log/apply/snapshot into this
//! one file is what 0.9's API requires; in 0.10 the v2 API splits
//! into `RaftLogStorage` + `RaftStateMachine` and this file would
//! split too. The migration is a known follow-up, ADR §12.1.

use std::ops::RangeBounds;

use openraft::storage::{LogState, RaftLogReader, RaftSnapshotBuilder, RaftStorage};
use openraft::{
    Entry, LogId, OptionalSend, Snapshot, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership, Vote,
};

use crate::snapshot::NebulaSnapshotStore;
use crate::state_machine::NebulaStateMachine;
use crate::storage::{from_openraft_entry, into_openraft_entry, NebulaLogStorage};
use crate::types::{NebulaNode, NebulaTypeConfig, NodeId};

/// Single object satisfying the full `RaftStorage` trait. Holds clones
/// of the three halves; openraft drives method dispatch on `&mut self`
/// so the inner Arc-shared state coordinates concurrency.
#[derive(Clone)]
pub struct NebulaRaftStorage {
    log: NebulaLogStorage,
    state_machine: NebulaStateMachine,
    snapshots: NebulaSnapshotStore,
}

impl NebulaRaftStorage {
    /// Compose the three halves into the trait surface.
    pub fn new(
        log: NebulaLogStorage,
        state_machine: NebulaStateMachine,
        snapshots: NebulaSnapshotStore,
    ) -> Self {
        Self {
            log,
            state_machine,
            snapshots,
        }
    }

    /// Borrow the state machine, e.g. for read-side queries on the
    /// leader. Phase 2.5 plugs this into the REST handler so reads
    /// land on the same `TextIndex` Raft is committing into.
    pub fn state_machine(&self) -> &NebulaStateMachine {
        &self.state_machine
    }
}

fn map_log_io(e: std::io::Error) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::read_logs(&e),
    }
}

fn map_log_err(e: crate::log::LogStoreError) -> StorageError<NodeId> {
    use crate::log::LogStoreError;
    match e {
        LogStoreError::Io(e) => map_log_io(e),
        other => map_log_io(std::io::Error::other(other.to_string())),
    }
}

fn map_vote_err(e: crate::storage::VoteIoError) -> StorageError<NodeId> {
    let inner = match e {
        crate::storage::VoteIoError::Io(e) => e,
        crate::storage::VoteIoError::Codec(s) => std::io::Error::other(s),
    };
    StorageError::IO {
        source: StorageIOError::read_vote(&inner),
    }
}

fn map_sm_err(e: crate::state_machine::StateMachineError) -> StorageError<NodeId> {
    let inner = std::io::Error::other(e.to_string());
    StorageError::IO {
        source: StorageIOError::read_state_machine(&inner),
    }
}

fn map_snap_err(e: crate::snapshot::SnapshotError) -> StorageError<NodeId> {
    let inner = std::io::Error::other(e.to_string());
    StorageError::IO {
        source: StorageIOError::read_snapshot(None, &inner),
    }
}

impl RaftLogReader<NebulaTypeConfig> for NebulaRaftStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<NebulaTypeConfig>>, StorageError<NodeId>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(&i) => i,
            std::ops::Bound::Excluded(&i) => i + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(&i) => i.saturating_add(1),
            std::ops::Bound::Excluded(&i) => i,
            std::ops::Bound::Unbounded => u64::MAX,
        };
        let entries = self.log.log().read_range(start, end).map_err(map_log_err)?;
        Ok(entries.into_iter().map(into_openraft_entry).collect())
    }
}

/// `RaftSnapshotBuilder` impl. openraft asks us for a builder via
/// `get_snapshot_builder`; we hand back a clone of `NebulaRaftStorage`.
impl RaftSnapshotBuilder<NebulaTypeConfig> for NebulaRaftStorage {
    async fn build_snapshot(&mut self) -> Result<Snapshot<NebulaTypeConfig>, StorageError<NodeId>> {
        self.snapshots.build().map_err(map_snap_err)
    }
}

impl RaftStorage<NebulaTypeConfig> for NebulaRaftStorage {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    // --- Vote ---

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.log.save_vote(vote).map_err(map_vote_err)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        self.log.read_vote().map_err(map_vote_err)
    }

    // --- Log ---

    async fn get_log_state(&mut self) -> Result<LogState<NebulaTypeConfig>, StorageError<NodeId>> {
        // We don't yet persist `last_purged_log_id` — would require
        // a fourth sidecar file. openraft tolerates `None` here for
        // now because `purge_logs_upto` is the authority on the
        // truncation horizon and openraft tracks that itself in
        // memory across a single process lifetime. A persistent
        // last-purged is a cleanup item for Phase 2.5.
        // Report the *real* term of the last entry. openraft's
        // replication-matching compares full LogIds (term + index); a
        // fabricated term-0 makes every follower's log look like it
        // conflicts, so quorum-commit never advances and client writes
        // block forever. This is the bug the multi-node harness caught
        // (design 0011 §6) — invisible single-node because a 1-node
        // cluster is its own quorum and skips follower matching.
        //
        // `CommittedLeaderId::new(term, node_id)`: openraft's LeaderId
        // for this type config carries a term and a node id, but the
        // *index-comparison* path only needs term+index to match. We
        // don't persist the originating node id per entry (the frame
        // stores term+index only), and openraft only compares the
        // (term, index) pair for log matching — node id 0 is fine here.
        let last_log_id = self
            .log
            .log()
            .last_log_id_parts()
            .map(|(idx, term)| LogId::new(openraft::CommittedLeaderId::new(term, 0), idx));
        Ok(LogState {
            last_purged_log_id: None,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<NebulaTypeConfig>> + OptionalSend,
    {
        for entry in entries {
            let stored = from_openraft_entry(&entry);
            self.log.log().append(&stored).map_err(map_log_err)?;
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        self.log
            .log()
            .truncate_from(log_id.index)
            .map_err(map_log_err)
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.log
            .log()
            .purge_through(log_id.index)
            .map_err(map_log_err)
    }

    // --- State machine ---

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, NebulaNode>), StorageError<NodeId>>
    {
        Ok((
            self.state_machine.last_applied(),
            self.state_machine.membership(),
        ))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<NebulaTypeConfig>],
    ) -> Result<Vec<crate::types::ClientWriteResponse>, StorageError<NodeId>> {
        self.state_machine
            .apply_entries(entries)
            .map_err(map_sm_err)
    }

    // --- Snapshot ---

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<std::io::Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(self.snapshots.begin_receiving()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, NebulaNode>,
        snapshot: Box<std::io::Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.snapshots
            .install(meta, *snapshot)
            .map_err(map_snap_err)
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<NebulaTypeConfig>>, StorageError<NodeId>> {
        self.snapshots.current().map_err(map_snap_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::{LogConfig, LogPayload};
    use crate::snapshot::NebulaSnapshotStore;
    use crate::state_machine::NebulaStateMachine;
    use crate::storage::NebulaLogStorage;
    use nebula_embed::{Embedder, MockEmbedder};
    use nebula_index::TextIndex;
    use nebula_vector::{HnswConfig, Metric};
    use openraft::{CommittedLeaderId, EntryPayload};
    use std::sync::Arc;
    use tempfile::tempdir;

    fn fresh_storage() -> (tempfile::TempDir, NebulaRaftStorage) {
        let dir = tempdir().unwrap();
        let log = NebulaLogStorage::open(dir.path().join("log"), LogConfig::default()).unwrap();
        let index = Arc::new(
            TextIndex::new(
                Arc::new(MockEmbedder::default()),
                Metric::Cosine,
                HnswConfig::default(),
            )
            .unwrap(),
        );
        let sm = NebulaStateMachine::open(index, dir.path().join("sm")).unwrap();
        let snaps = NebulaSnapshotStore::new(sm.clone(), dir.path().join("snap")).unwrap();
        let storage = NebulaRaftStorage::new(log, sm, snaps);
        (dir, storage)
    }

    fn upsert_entry(term: u64, index: u64, id: &str, text: &str) -> Entry<NebulaTypeConfig> {
        let dim = MockEmbedder::default().dim();
        let mut v = vec![0.0_f32; dim];
        for (i, b) in text.bytes().enumerate() {
            v[i % dim] += b as f32 / 255.0;
        }
        let n: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
        for x in &mut v {
            *x /= n;
        }
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(term, 0), index),
            payload: EntryPayload::Normal(LogPayload::Mutation(
                nebula_wal::WalRecord::UpsertText {
                    bucket: "b".into(),
                    external_id: id.into(),
                    text: text.into(),
                    vector: v,
                    metadata_json: "{}".into(),
                },
            )),
        }
    }

    #[tokio::test]
    async fn vote_round_trips_via_trait() {
        let (_dir, mut storage) = fresh_storage();
        assert!(storage.read_vote().await.unwrap().is_none());
        let v = Vote::new(3, 7);
        storage.save_vote(&v).await.unwrap();
        assert_eq!(storage.read_vote().await.unwrap(), Some(v));
    }

    #[tokio::test]
    async fn append_then_get_log_state_advances() {
        let (_dir, mut storage) = fresh_storage();
        let s0 = storage.get_log_state().await.unwrap();
        assert!(s0.last_log_id.is_none());

        storage
            .append_to_log(vec![
                upsert_entry(1, 1, "a", "alpha"),
                upsert_entry(1, 2, "b", "beta"),
            ])
            .await
            .unwrap();

        let s1 = storage.get_log_state().await.unwrap();
        assert_eq!(s1.last_log_id.unwrap().index, 2);
    }

    #[tokio::test]
    async fn apply_then_last_applied_state_reflects_changes() {
        let (_dir, mut storage) = fresh_storage();
        let entries = vec![
            upsert_entry(1, 1, "a", "alpha"),
            upsert_entry(1, 2, "b", "beta"),
        ];
        storage.append_to_log(entries.clone()).await.unwrap();
        storage.apply_to_state_machine(&entries).await.unwrap();

        let (la, _membership) = storage.last_applied_state().await.unwrap();
        assert_eq!(la.unwrap().index, 2);
    }

    #[tokio::test]
    async fn build_then_get_current_snapshot_round_trips() {
        let (_dir, mut storage) = fresh_storage();
        let entries = vec![
            upsert_entry(1, 1, "a", "alpha"),
            upsert_entry(1, 2, "b", "beta"),
        ];
        storage.append_to_log(entries.clone()).await.unwrap();
        storage.apply_to_state_machine(&entries).await.unwrap();

        let mut builder = storage.get_snapshot_builder().await;
        let built = builder.build_snapshot().await.unwrap();
        assert_eq!(built.meta.last_log_id.unwrap().index, 2);

        let current = storage.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(current.meta.last_log_id.unwrap().index, 2);
    }

    #[tokio::test]
    async fn truncate_then_purge_compose() {
        let (_dir, mut storage) = fresh_storage();
        let entries = vec![
            upsert_entry(1, 1, "a", "alpha"),
            upsert_entry(1, 2, "b", "beta"),
            upsert_entry(1, 3, "c", "gamma"),
            upsert_entry(1, 4, "d", "delta"),
            upsert_entry(1, 5, "e", "epsilon"),
        ];
        storage.append_to_log(entries).await.unwrap();

        // Truncate from index 4 onward — leaves indices 1..=3.
        storage
            .delete_conflict_logs_since(LogId::new(CommittedLeaderId::new(1, 0), 4))
            .await
            .unwrap();
        let remaining = storage.try_get_log_entries(0..u64::MAX).await.unwrap();
        assert_eq!(
            remaining.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            [1, 2, 3]
        );

        // Purge through index 1 — leaves 2, 3.
        storage
            .purge_logs_upto(LogId::new(CommittedLeaderId::new(1, 0), 1))
            .await
            .unwrap();
        let remaining = storage.try_get_log_entries(0..u64::MAX).await.unwrap();
        assert_eq!(
            remaining.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            [2, 3]
        );
    }
}
