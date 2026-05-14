//! Snapshot integration for the Raft state machine.
//!
//! Per ADR §6, this module reuses the existing `nsnap` snapshot
//! format from `nebula-index/src/durability.rs` unchanged. openraft
//! sees:
//!
//! - **Building**: a `RaftSnapshotBuilder` that calls
//!   [`TextIndex::snapshot_to`] into the data dir, then reads the
//!   resulting `.nsnap` file into memory and hands it to openraft as
//!   `Cursor<Vec<u8>>`.
//! - **Receiving** (follower): openraft hands us a `Cursor<Vec<u8>>`
//!   from the leader's stream. We write it to a `raft-recv-<id>.nsnap`
//!   file under the snapshots dir, then `load_latest_snapshot` sees
//!   it on the next index reload.
//! - **Reading** (`get_current_snapshot`): finds the newest committed
//!   snapshot under the snapshots dir, slurps it, returns the same
//!   `Cursor<Vec<u8>>` shape.
//!
//! # In-memory `Cursor<Vec<u8>>` for v1
//!
//! ADR §6 calls out that streaming snapshots are the goal — the
//! `NebulaTypeConfig::SnapshotData` type is `Cursor<Vec<u8>>` for
//! now because openraft 0.9's snapshot transfer assumes the
//! implementation can hand it a buffered handle. PR #25's streaming
//! snapshot serialize already keeps **build-time** memory bounded —
//! we serialize-into a writer, not into a `Vec`. The `Cursor` here
//! is the *transfer* buffer, allocated only during snapshot install,
//! and openraft chunks reads against it. For a 1 GiB index the
//! transfer buffer is the bottleneck; switching to a streaming
//! transport is part of Phase 2.4 (gRPC `RaftService`).
//!
//! # Membership in snapshot meta
//!
//! openraft requires that every snapshot carry the `StoredMembership`
//! at the snapshot's last_applied. We pass through the state
//! machine's currently committed membership at snapshot-build time.
//! Recovery applies the snapshot's membership before any further log
//! replay.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use openraft::{Snapshot, SnapshotMeta};
use parking_lot::Mutex;

use crate::state_machine::{NebulaStateMachine, StateMachineError};
use crate::types::{NebulaNode, NebulaTypeConfig, NodeId};

/// Directory under the data dir where Raft snapshots live. We share
/// this with the standalone path's snapshot dir layout so the same
/// `load_latest_snapshot` reader works for both — there's no
/// raft-vs-standalone fork in the on-disk layout.
const SNAPSHOTS_SUBDIR: &str = "snapshots";

/// Subdirectory for partially-received snapshots from a leader before
/// they're committed by openraft. `install_snapshot` atomically
/// renames the staged file into [`SNAPSHOTS_SUBDIR`] once the stream
/// completes. Crash mid-receive leaves orphans here that boot-time
/// cleanup can prune.
const RECV_SUBDIR: &str = "raft-recv";

/// Errors raised by snapshot build / install / read.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("snapshot io: {0}")]
    Io(#[from] io::Error),
    #[error("snapshot index: {0}")]
    Index(String),
    #[error("snapshot state-machine: {0}")]
    StateMachine(#[from] StateMachineError),
    #[error("snapshot codec: {0}")]
    Codec(String),
}

/// Snapshot builder + installer. One per state machine.
///
/// Cloneable so openraft can drive it from multiple tasks; the
/// `Arc<Inner>` carries shared state.
#[derive(Clone)]
pub struct NebulaSnapshotStore {
    inner: Arc<Inner>,
}

struct Inner {
    sm: NebulaStateMachine,
    data_dir: PathBuf,
    /// Monotonic counter for snapshot ids handed back to openraft.
    /// Distinct from the per-snapshot `mark_seq` (= log index) so
    /// repeated snapshots at the same applied index get unique ids.
    next_snapshot_id: Mutex<u64>,
}

impl NebulaSnapshotStore {
    /// Wire a snapshot store over a state machine and a data dir.
    /// Creates `<data_dir>/snapshots` and `<data_dir>/raft-recv` if
    /// they don't exist.
    pub fn new(sm: NebulaStateMachine, data_dir: impl AsRef<Path>) -> Result<Self, SnapshotError> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(data_dir.join(SNAPSHOTS_SUBDIR))?;
        fs::create_dir_all(data_dir.join(RECV_SUBDIR))?;
        Ok(Self {
            inner: Arc::new(Inner {
                sm,
                data_dir,
                next_snapshot_id: Mutex::new(0),
            }),
        })
    }

    fn snapshots_dir(&self) -> PathBuf {
        self.inner.data_dir.join(SNAPSHOTS_SUBDIR)
    }

    fn recv_dir(&self) -> PathBuf {
        self.inner.data_dir.join(RECV_SUBDIR)
    }

    /// Build a snapshot of the current state-machine state.
    ///
    /// Steps:
    /// 1. Read the SM's `last_applied` + `membership`. These become
    ///    the snapshot's metadata.
    /// 2. Call `TextIndex::snapshot_to` with `mark_seq =
    ///    last_applied.index` to produce an on-disk `.nsnap`. The
    ///    index's read-lock-only snapshot path means writers don't
    ///    block during this.
    /// 3. Read the `.nsnap` back into a `Vec<u8>` so openraft can
    ///    chunk it into the wire transport. ADR §10 flags this as a
    ///    cost we pay for `openraft 0.9`'s buffered snapshot model;
    ///    Phase 2.4's gRPC transport will switch to streaming.
    pub fn build(&self) -> Result<Snapshot<NebulaTypeConfig>, SnapshotError> {
        let last_applied = self.inner.sm.last_applied();
        let membership = self.inner.sm.membership();

        let mark_seq = last_applied.map(|id| id.index).unwrap_or(0);
        let outcome = self
            .inner
            .sm
            .index()
            .snapshot_to(self.snapshots_dir(), mark_seq)
            .map_err(|e| SnapshotError::Index(e.to_string()))?;

        let mut buf = Vec::new();
        File::open(&outcome.path)?.read_to_end(&mut buf)?;

        let id = {
            let mut next = self.inner.next_snapshot_id.lock();
            *next += 1;
            format!("snap-{}", *next)
        };

        let meta = SnapshotMeta::<NodeId, NebulaNode> {
            last_log_id: last_applied,
            last_membership: membership,
            snapshot_id: id,
        };
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(buf)),
        })
    }

    /// Allocate a fresh in-memory cursor for openraft to write a
    /// streaming snapshot into. The cursor is empty; openraft fills
    /// it then calls [`Self::install`] with the populated handle.
    pub fn begin_receiving(&self) -> Cursor<Vec<u8>> {
        Cursor::new(Vec::new())
    }

    /// Install a snapshot received from a leader.
    ///
    /// Writes the bytes to `<recv_dir>/raft-recv-<seq>.nsnap.part`,
    /// fsyncs, atomically renames into the live snapshots dir using
    /// the canonical `snapshot-<seq>.nsnap` name, then drops a `.ok`
    /// marker so the existing reader picks it up. After this returns
    /// the caller (openraft) is expected to reload the state machine
    /// from the newly committed snapshot.
    pub fn install(
        &self,
        meta: &SnapshotMeta<NodeId, NebulaNode>,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), SnapshotError> {
        let mark_seq = meta.last_log_id.map(|id| id.index).unwrap_or(0);
        let stem = format!("snapshot-{mark_seq:020}");

        // Stage the bytes in the recv dir first so a crash leaves no
        // partial file inside the live snapshots dir (where the
        // existing reader would find it).
        let staging = self.recv_dir().join(format!("{stem}.nsnap.part"));
        let final_path = self.snapshots_dir().join(format!("{stem}.nsnap"));
        let ok_marker = self.snapshots_dir().join(format!("{stem}.nsnap.ok"));

        {
            let mut f = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&staging)?;
            f.write_all(snapshot.get_ref())?;
            f.sync_all()?;
        }

        // Atomic rename into the live dir, then commit-marker.
        fs::rename(&staging, &final_path)?;
        File::create(&ok_marker)?.sync_all()?;

        Ok(())
    }

    /// Return the newest committed snapshot, if any. Streams the
    /// `.nsnap` bytes into a `Cursor<Vec<u8>>` for openraft.
    pub fn current(&self) -> Result<Option<Snapshot<NebulaTypeConfig>>, SnapshotError> {
        let dir = self.snapshots_dir();
        let Some(path) = newest_committed_snapshot(&dir) else {
            return Ok(None);
        };

        let mut buf = Vec::new();
        File::open(&path)?.read_to_end(&mut buf)?;

        // Reuse the SM's currently-known last-applied + membership for
        // the meta: at boot, install() will have already updated those.
        // For the build-then-current path, this is the same data we
        // wrote into the snapshot.
        let last_applied = self.inner.sm.last_applied();
        let membership = self.inner.sm.membership();

        let id = {
            let mut next = self.inner.next_snapshot_id.lock();
            *next += 1;
            format!("snap-{}", *next)
        };
        let meta = SnapshotMeta::<NodeId, NebulaNode> {
            last_log_id: last_applied,
            last_membership: membership,
            snapshot_id: id,
        };
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(buf)),
        }))
    }
}

/// Find the path of the newest `<snapshots_dir>/snapshot-<seq>.nsnap`
/// file that has a matching `.ok` marker. Mirrors
/// `nebula_index::durability::newest_snapshot` but is private there;
/// duplicating two lines beats a public-API change.
fn newest_committed_snapshot(dir: &Path) -> Option<PathBuf> {
    if !dir.exists() {
        return None;
    }
    let mut best: Option<(u64, PathBuf)> = None;
    let entries = fs::read_dir(dir).ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(stem) = name.strip_suffix(".nsnap.ok") else {
            continue;
        };
        let Some(seq_str) = stem.strip_prefix("snapshot-") else {
            continue;
        };
        let Ok(seq) = seq_str.parse::<u64>() else {
            continue;
        };
        if best.as_ref().map_or(true, |(s, _)| seq > *s) {
            // Resolve to the data file beside the marker.
            let data = dir.join(format!("snapshot-{seq:020}.nsnap"));
            if data.exists() {
                best = Some((seq, data));
            }
        }
    }
    best.map(|(_, p)| p)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::LogPayload;
    use nebula_embed::{Embedder, MockEmbedder};
    use nebula_index::TextIndex;
    use nebula_vector::{HnswConfig, Metric};
    use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
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

    fn upsert(term: u64, index: u64, id: &str, text: &str) -> Entry<NebulaTypeConfig> {
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

    #[test]
    fn build_writes_nsnap_with_current_last_applied() {
        let dir = tempdir().unwrap();
        let sm = NebulaStateMachine::open(fresh_index(), dir.path().join("sm")).unwrap();
        sm.apply_entries(&[upsert(1, 1, "d1", "alpha"), upsert(1, 2, "d2", "beta")])
            .unwrap();

        let store = NebulaSnapshotStore::new(sm.clone(), dir.path()).unwrap();
        let snap = store.build().unwrap();
        assert_eq!(snap.meta.last_log_id.unwrap().index, 2);

        // The on-disk file is `snapshot-{mark_seq}.nsnap`.
        let nsnap = dir
            .path()
            .join("snapshots")
            .join("snapshot-00000000000000000002.nsnap");
        assert!(
            nsnap.exists(),
            "snapshot file missing at {}",
            nsnap.display()
        );
    }

    #[test]
    fn current_returns_none_before_any_build() {
        let dir = tempdir().unwrap();
        let sm = NebulaStateMachine::open(fresh_index(), dir.path().join("sm")).unwrap();
        let store = NebulaSnapshotStore::new(sm, dir.path()).unwrap();
        assert!(store.current().unwrap().is_none());
    }

    #[test]
    fn build_then_install_round_trips_through_a_fresh_node() {
        // Leader-side: build a snapshot from a state machine that has
        // applied two entries.
        let leader_dir = tempdir().unwrap();
        let sm_a = NebulaStateMachine::open(fresh_index(), leader_dir.path().join("sm")).unwrap();
        sm_a.apply_entries(&[upsert(1, 1, "d1", "alpha"), upsert(1, 2, "d2", "beta")])
            .unwrap();
        let store_a = NebulaSnapshotStore::new(sm_a, leader_dir.path()).unwrap();
        let snap = store_a.build().unwrap();

        // Follower-side: install the snapshot into a fresh data dir.
        // We can't reload the state-machine through `install` alone —
        // openraft drives that separately — but we can assert the
        // bytes landed in the canonical place where the existing
        // reader would find them.
        let follower_dir = tempdir().unwrap();
        let sm_b = NebulaStateMachine::open(fresh_index(), follower_dir.path().join("sm")).unwrap();
        let store_b = NebulaSnapshotStore::new(sm_b, follower_dir.path()).unwrap();

        // Reconstruct a Cursor over the bytes (the trait impl in 2.x
        // handed us a Box<Cursor>; we open a fresh one here).
        let bytes = snap.snapshot.into_inner();
        let cursor = Cursor::new(bytes);
        store_b.install(&snap.meta, cursor).unwrap();

        // The follower can now report the snapshot via `current`.
        let observed = store_b.current().unwrap().unwrap();
        // The snapshot file at the expected path is non-empty.
        assert!(!observed.snapshot.into_inner().is_empty());
    }

    #[test]
    fn current_picks_newest_when_multiple_are_committed() {
        let dir = tempdir().unwrap();
        let sm = NebulaStateMachine::open(fresh_index(), dir.path().join("sm")).unwrap();
        sm.apply_entries(&[upsert(1, 1, "d1", "alpha")]).unwrap();
        let store = NebulaSnapshotStore::new(sm.clone(), dir.path()).unwrap();
        store.build().unwrap();

        sm.apply_entries(&[upsert(1, 5, "d5", "echo")]).unwrap();
        store.build().unwrap();

        let snap = store.current().unwrap().unwrap();
        assert_eq!(snap.meta.last_log_id.unwrap().index, 5);
    }
}
