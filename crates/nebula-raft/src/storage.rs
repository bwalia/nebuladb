//! Vote-persistence + log handle for openraft.
//!
//! In openraft 0.9, the `RaftStorage` trait is a single sealed surface
//! that bundles log storage, vote storage, the state-machine apply
//! path, and snapshot building. We can't satisfy that trait without
//! wiring the state machine — which is Phase 2.2's job, not 2.1b's.
//!
//! This module therefore provides only the **vote storage half** plus
//! a typed handle around [`LogStore`], so the controller code in
//! Phase 2.5 has a single object it composes with the state-machine
//! type from 2.2 to satisfy `RaftStorage` end-to-end.
//!
//! What this module **does not** own:
//!
//! - `RaftStorage` impl — combined trait, lands when both halves
//!   exist (after Phase 2.2 + 2.3).
//! - State-machine apply — Phase 2.2.
//! - Snapshot builder — Phase 2.3.
//! - Network transport — Phase 2.4.
//!
//! Why ship this in 2.1b at all? Because the on-disk vote file format
//! and the `LogStore` ↔ openraft type-conversion glue (terms,
//! `LogId`s, `Entry` round-trip) are surfaces the state machine
//! depends on. Locking them now means 2.2 doesn't have to backtrack
//! through schema decisions.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use openraft::{Entry, EntryPayload, LogId, Vote};

use crate::log::{LogConfig, LogEntry, LogPayload, LogStore, LogStoreError};
use crate::types::{NebulaNode, NebulaTypeConfig, NodeId};

/// File name for the persisted vote. Sits next to the log segments.
const VOTE_FILE: &str = "vote.bin";

/// Wrapper around an [`Arc<LogStore>`] plus a vote-persistence helper.
///
/// In Phase 2.2 the state-machine type will compose this with its
/// apply path to satisfy `openraft::RaftStorage` end-to-end. Until
/// then, it exposes the conversion glue and the durable vote slot
/// any honest `RaftStorage` impl will need.
#[derive(Clone)]
pub struct NebulaLogStorage {
    inner: Arc<LogStoreInner>,
}

struct LogStoreInner {
    log: LogStore,
    dir: PathBuf,
}

impl NebulaLogStorage {
    /// Open or create the on-disk log + vote file in `dir`.
    pub fn open(dir: impl AsRef<Path>, config: LogConfig) -> Result<Self, LogStoreError> {
        let dir = dir.as_ref().to_path_buf();
        let log = LogStore::open(&dir, config)?;
        Ok(Self {
            inner: Arc::new(LogStoreInner { log, dir }),
        })
    }

    /// Borrow the underlying log handle. Used by the trait impls in
    /// 2.2/2.5 that need to call append/truncate/purge directly.
    pub fn log(&self) -> &LogStore {
        &self.inner.log
    }

    /// Persist a Raft vote durably.
    ///
    /// Atomic via `<vote.bin>.tmp` rename so a crash mid-write leaves
    /// the previous vote intact. openraft requires the vote be fsynced
    /// before this returns — losing a vote across a crash can lead to
    /// double-voting, which violates Raft safety.
    pub fn save_vote(&self, vote: &Vote<NodeId>) -> Result<(), VoteIoError> {
        let bytes = bincode::serialize(vote).map_err(|e| VoteIoError::Codec(e.to_string()))?;
        let final_path = self.vote_path();
        let tmp = final_path.with_extension("tmp");
        {
            let mut f = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&tmp)?;
            f.write_all(&bytes)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &final_path)?;
        Ok(())
    }

    /// Read the persisted Raft vote, if any.
    pub fn read_vote(&self) -> Result<Option<Vote<NodeId>>, VoteIoError> {
        let path = self.vote_path();
        if !path.exists() {
            return Ok(None);
        }
        let mut f = File::open(&path)?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        let v = bincode::deserialize(&buf).map_err(|e| VoteIoError::Codec(e.to_string()))?;
        Ok(Some(v))
    }

    fn vote_path(&self) -> PathBuf {
        self.inner.dir.join(VOTE_FILE)
    }
}

/// Errors raised by the vote-persistence half. Distinct from
/// `LogStoreError` so the trait impl in 2.2 can map vote vs log
/// errors to the right `openraft::StorageIOError` variant.
#[derive(Debug, thiserror::Error)]
pub enum VoteIoError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("vote codec: {0}")]
    Codec(String),
}

/// Convert a stored `LogEntry` into openraft's `Entry`.
///
/// All three openraft entry kinds round-trip: a `Mutation` becomes a
/// `Normal`, a `Membership` is bincode-decoded back into openraft's
/// `Membership` struct, and `Blank` maps straight through. Faithful
/// round-tripping of membership entries is required — openraft reads
/// them back out of the log to answer `change_membership` (design
/// 0011); a lossy conversion makes that panic on a `None` membership.
pub fn into_openraft_entry(e: LogEntry) -> Entry<NebulaTypeConfig> {
    let payload = match e.payload {
        LogPayload::Mutation(_) => EntryPayload::Normal(e.payload),
        LogPayload::Membership(bytes) => {
            // A corrupt/undecodable membership record is unrecoverable —
            // openraft cannot proceed without the config. Fail loudly
            // rather than silently dropping it (which reintroduces the
            // very bug this format change fixes).
            let m: openraft::Membership<NodeId, NebulaNode> = bincode::deserialize(&bytes)
                .expect("membership log entry must decode; log corruption");
            EntryPayload::Membership(m)
        }
        LogPayload::Blank => EntryPayload::Blank,
    };
    Entry {
        log_id: LogId::new(openraft::CommittedLeaderId::new(e.term, 0), e.index),
        payload,
    }
}

/// Convert an openraft `Entry` into our stored `LogEntry`.
///
/// The inverse of [`into_openraft_entry`]: `Normal` mutations store as
/// `Mutation`, membership entries are bincode-encoded into
/// `LogPayload::Membership`, and blank entries store as `Blank`. Every
/// kind persists losslessly so the log can be read back for both apply
/// and openraft's own membership queries.
pub fn from_openraft_entry(e: &Entry<NebulaTypeConfig>) -> LogEntry {
    let payload = match &e.payload {
        EntryPayload::Normal(p) => p.clone(),
        EntryPayload::Membership(m) => LogPayload::Membership(
            bincode::serialize(m).expect("membership must serialize"),
        ),
        EntryPayload::Blank => LogPayload::Blank,
    };
    LogEntry {
        term: e.log_id.leader_id.term,
        index: e.log_id.index,
        payload,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn vote_round_trips_through_disk() {
        let dir = tempdir().unwrap();
        let storage = NebulaLogStorage::open(dir.path(), LogConfig::default()).unwrap();
        assert!(storage.read_vote().unwrap().is_none());

        let vote = Vote::new(7, 42);
        storage.save_vote(&vote).unwrap();
        let got = storage.read_vote().unwrap();
        assert_eq!(got, Some(vote));
    }

    #[test]
    fn vote_overwrite_is_atomic() {
        let dir = tempdir().unwrap();
        let storage = NebulaLogStorage::open(dir.path(), LogConfig::default()).unwrap();
        for term in 1..=5 {
            storage.save_vote(&Vote::new(term, 0)).unwrap();
        }
        assert_eq!(storage.read_vote().unwrap().unwrap().leader_id.term, 5,);
    }

    #[test]
    fn entry_conversion_round_trips_normal_payload() {
        let stored = LogEntry {
            term: 3,
            index: 42,
            payload: LogPayload::Mutation(nebula_wal::WalRecord::Delete {
                bucket: "b".into(),
                external_id: "x".into(),
            }),
        };
        let entry = into_openraft_entry(stored.clone());
        assert_eq!(entry.log_id.index, 42);
        assert_eq!(entry.log_id.leader_id.term, 3);
        let back = from_openraft_entry(&entry);
        assert_eq!(back, stored);
    }

    #[test]
    fn log_handle_is_borrowable_and_appendable() {
        // The state-machine impl in 2.2 will reach for .log() to drive
        // append/read/truncate. Just confirm the borrow shape works.
        let dir = tempdir().unwrap();
        let storage = NebulaLogStorage::open(dir.path(), LogConfig::default()).unwrap();
        storage
            .log()
            .append(&LogEntry {
                term: 1,
                index: 1,
                payload: LogPayload::Mutation(nebula_wal::WalRecord::Delete {
                    bucket: "b".into(),
                    external_id: "x".into(),
                }),
            })
            .unwrap();
        assert_eq!(storage.log().last_index(), Some(1));
    }
}
