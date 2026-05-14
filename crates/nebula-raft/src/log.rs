//! Append-only Raft log on disk, segmented at a configurable size.
//!
//! # Wire format
//!
//! Each record is 28 bytes of header + body:
//!
//! ```text
//! ┌───────────┬───────────┬───────────┬───────────┬───────────────────┐
//! │ len (u32) │ crc (u32) │ term (u64)│ index(u64)│ bincode(LogPayload)│
//! └───────────┴───────────┴───────────┴───────────┴───────────────────┘
//! ```
//!
//! - `len` is the body length only. Header itself is fixed 24 bytes
//!   after the leading `len`, so a reader knows exactly how much to
//!   slurp before checking the CRC.
//! - `crc` is CRC32 of `term ++ index ++ body`. Header-only corruption
//!   (a torn fsync) flips bits in the term/index, which a CRC over the
//!   whole post-`crc` payload still catches.
//! - `term` is the Raft term. `u64` is openraft's chosen width.
//! - `index` is the Raft log index, monotone-increasing across the
//!   whole cluster's log lifetime. Uniqueness within a segment is
//!   enforced on append.
//! - Body is `bincode`-encoded `LogPayload`. Identical schema-evolution
//!   discipline as `nebula-wal`'s `WalRecord`: variants are append-only,
//!   no reorder, no field removal.
//!
//! # Crash safety
//!
//! Same model as `nebula-wal`: any error past the first good record in
//! a segment truncates the segment to the end of the last good record.
//! A torn fsync mid-record is therefore self-healing — the partial
//! frame is silently dropped on the next open. The reader treats EOF,
//! short read, bad CRC, and `len > MAX_RECORD_BYTES` identically.
//!
//! # Why a new format instead of reusing `.nwal`
//!
//! `WalRecord` has no Raft term/index slots. We could put them inside
//! the body (a wrapper `WalRecord::Raft { term, index, inner }`), but
//! then a reader can't seek to a specific log index without parsing
//! every preceding body. The header-level term/index supports the
//! `read_by_index` random access that openraft needs, in O(segment
//! count) instead of O(records).
//!
//! # What this module does NOT do
//!
//! - It is not the `RaftLogStorage` trait impl. Those calls (append
//!   single, append batch, get-range, truncate-after, purge-before)
//!   live in a follow-up module that depends on `openraft`. This
//!   module is the substrate underneath.
//! - It does not implement snapshot install. Snapshots are owned by
//!   `nebula-index/durability.rs` and merely *referenced* by a
//!   committed-up-to index in this log.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use nebula_wal::WalRecord;

/// Marker bytes at the head of every `.nrlog` file.
///
/// Distinct from `nebula-wal`'s `NEBWAL01` so a cross-mounted data
/// directory can't be accidentally interpreted as the wrong kind of
/// log. Bumping the trailing version digit forces a clean replay; old
/// readers refuse to open a newer-version file rather than risking a
/// silent misparse.
pub(crate) const MAGIC: &[u8; 8] = b"NEBRAF01";

/// Bytes between `len` and the start of the body. `term + index = 16`.
const HEADER_TAIL_BYTES: usize = 16;

/// Hard cap on a single record's encoded body size. Mirrors the WAL's
/// 16 MiB cap — same reasoning: a 1536-dim vector + chunk text +
/// metadata fits comfortably; anything bigger is a corrupted `len` we
/// won't allocate for.
pub(crate) const MAX_RECORD_BYTES: u32 = 16 * 1024 * 1024;

/// What lives inside a Raft log entry.
///
/// For now this is exactly one variant: the same `WalRecord` set the
/// existing standalone path emits, so the state-machine apply logic in
/// 2.2 can reuse `TextIndex::apply_wal_record` unchanged.
///
/// Future variants — `ConfigChange` for openraft membership changes
/// and `NoOp` for leader-establish heartbeats — append at the end of
/// the enum to preserve bincode wire compatibility.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LogPayload {
    /// A user mutation. Same shape the WAL has always emitted.
    Mutation(WalRecord),
}

/// One persisted Raft log entry.
#[derive(Debug, Clone, PartialEq)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub payload: LogPayload,
}

#[derive(Debug, Error)]
pub enum LogStoreError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("log codec: {0}")]
    Codec(String),
    #[error("log format: {0}")]
    Format(String),
    #[error("log invariant: {0}")]
    Invariant(String),
}

/// Tunables for the on-disk log layout.
#[derive(Debug, Clone)]
pub struct LogConfig {
    /// Roll a new segment once the current file would grow past this.
    /// Same default as the WAL — keeps replay scans bounded.
    pub segment_size_bytes: u64,
    /// `fsync` after every append. The only level that survives a
    /// kernel panic; required for raft-acceptable durability.
    pub fsync_on_append: bool,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            segment_size_bytes: 64 * 1024 * 1024,
            fsync_on_append: true,
        }
    }
}

/// Append-only log. One per node; openraft drives it via the trait
/// impl that 2.1b will add.
pub struct LogStore {
    dir: PathBuf,
    config: LogConfig,
    state: Mutex<WriterState>,
}

struct WriterState {
    /// Sequence number of the segment we're currently appending to.
    /// Segments are named `<seg_seq>.nrlog`. Distinct from the Raft
    /// `index` field on entries — segments roll on size, not on log
    /// index boundaries.
    seg_seq: u64,
    file: BufWriter<File>,
    bytes_written: u64,
    /// Last log index we successfully appended. `None` until first
    /// append in a fresh log. Used to enforce monotone-index on
    /// append (an openraft invariant).
    last_index: Option<u64>,
}

impl LogStore {
    /// Open or create a log in the given directory. On open, scans
    /// existing segments for the highest segment seq and the highest
    /// log index inside that segment, then truncates any torn tail.
    pub fn open(dir: impl AsRef<Path>, config: LogConfig) -> Result<Self, LogStoreError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let segments = list_segments(&dir)?;

        // Boot path 1: empty directory. Create segment 0.
        let (seg_seq, file_handle, bytes_written, last_index) = if segments.is_empty() {
            let seg_seq = 0u64;
            let path = segment_path(&dir, seg_seq);
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .read(true)
                .open(&path)?;
            file.write_all(MAGIC)?;
            file.sync_all()?;
            let bytes_written = MAGIC.len() as u64;
            (seg_seq, file, bytes_written, None)
        } else {
            // Boot path 2: existing segments. Open the newest, scan it
            // to find the last good record, truncate any torn tail.
            let &seg_seq = segments.last().expect("non-empty checked above");
            let path = segment_path(&dir, seg_seq);
            let (truncate_to, last_index) = scan_segment_for_recovery(&path)?;
            let file = OpenOptions::new().write(true).read(true).open(&path)?;
            file.set_len(truncate_to)?;
            let mut file = file;
            file.seek(SeekFrom::Start(truncate_to))?;
            (seg_seq, file, truncate_to, last_index)
        };

        let writer = BufWriter::new(file_handle);
        Ok(Self {
            dir,
            config,
            state: Mutex::new(WriterState {
                seg_seq,
                file: writer,
                bytes_written,
                last_index,
            }),
        })
    }

    /// Append a single entry. Caller is responsible for term/index
    /// consistency with raft semantics; this layer only enforces the
    /// universal "index strictly monotone" invariant.
    pub fn append(&self, entry: &LogEntry) -> Result<(), LogStoreError> {
        let mut state = self.state.lock();
        if let Some(prev) = state.last_index {
            if entry.index <= prev {
                return Err(LogStoreError::Invariant(format!(
                    "non-monotone append: last_index={prev}, new={}",
                    entry.index
                )));
            }
        }
        let body =
            bincode::serialize(&entry.payload).map_err(|e| LogStoreError::Codec(e.to_string()))?;
        if body.len() as u64 > MAX_RECORD_BYTES as u64 {
            return Err(LogStoreError::Format(format!(
                "record body {} bytes exceeds cap {}",
                body.len(),
                MAX_RECORD_BYTES,
            )));
        }

        let frame_len = 4 + 4 + HEADER_TAIL_BYTES + body.len();
        if state.bytes_written + frame_len as u64 > self.config.segment_size_bytes {
            self.rotate_locked(&mut state)?;
        }

        let mut crc = crc32fast::Hasher::new();
        let mut tail = [0u8; HEADER_TAIL_BYTES];
        tail[..8].copy_from_slice(&entry.term.to_le_bytes());
        tail[8..].copy_from_slice(&entry.index.to_le_bytes());
        crc.update(&tail);
        crc.update(&body);
        let crc = crc.finalize();

        let len = body.len() as u32;
        state.file.write_all(&len.to_le_bytes())?;
        state.file.write_all(&crc.to_le_bytes())?;
        state.file.write_all(&tail)?;
        state.file.write_all(&body)?;
        state.bytes_written += frame_len as u64;
        state.last_index = Some(entry.index);

        if self.config.fsync_on_append {
            state.file.flush()?;
            state.file.get_ref().sync_all()?;
        }
        Ok(())
    }

    /// Read every entry across every segment, in order. Linear scan;
    /// suitable for boot-time log replay before openraft takes over.
    pub fn read_all(&self) -> Result<Vec<LogEntry>, LogStoreError> {
        // Drop the writer's buffer so the read path sees everything
        // we've written so far. Holding the state lock prevents any
        // append from interleaving with the scan.
        let mut state = self.state.lock();
        state.file.flush()?;

        let mut out = Vec::new();
        let segments = list_segments(&self.dir)?;
        for seg_seq in segments {
            let path = segment_path(&self.dir, seg_seq);
            for entry in iter_segment(&path)? {
                out.push(entry?);
            }
        }
        Ok(out)
    }

    /// Last log index ever appended, if any.
    pub fn last_index(&self) -> Option<u64> {
        self.state.lock().last_index
    }

    fn rotate_locked(&self, state: &mut WriterState) -> Result<(), LogStoreError> {
        state.file.flush()?;
        state.file.get_ref().sync_all()?;

        let next_seq = state.seg_seq + 1;
        let path = segment_path(&self.dir, next_seq);
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&path)?;
        file.write_all(MAGIC)?;
        file.sync_all()?;

        state.seg_seq = next_seq;
        state.file = BufWriter::new(file);
        state.bytes_written = MAGIC.len() as u64;
        Ok(())
    }
}

/// Public alias for callers that want to talk segment-level. Right now
/// only used in tests; will be consumed by the trait impl in 2.1b.
pub type LogSegment = u64;

fn segment_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("{seq:010}.nrlog"))
}

fn list_segments(dir: &Path) -> Result<Vec<u64>, LogStoreError> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(stem) = name.strip_suffix(".nrlog") else {
            continue;
        };
        if let Ok(seq) = stem.parse::<u64>() {
            out.push(seq);
        }
    }
    out.sort_unstable();
    Ok(out)
}

/// Walk a segment, returning the byte offset of the end of the last
/// good record and the highest log index contained in it.
fn scan_segment_for_recovery(path: &Path) -> Result<(u64, Option<u64>), LogStoreError> {
    let mut file = File::open(path)?;
    let mut magic = [0u8; 8];
    if file.read_exact(&mut magic).is_err() || &magic != MAGIC {
        return Err(LogStoreError::Format(format!(
            "{} missing/wrong magic",
            path.display()
        )));
    }
    let mut good_end = MAGIC.len() as u64;
    let mut last_index = None;

    let mut reader = BufReader::new(file);
    while let Some(entry) = read_one_frame(&mut reader)? {
        last_index = Some(entry.index);
        let body_len = bincode::serialized_size(&entry.payload)
            .map_err(|e| LogStoreError::Codec(e.to_string()))?;
        good_end += 4 + 4 + HEADER_TAIL_BYTES as u64 + body_len;
    }
    Ok((good_end, last_index))
}

fn iter_segment(path: &Path) -> Result<SegmentIter, LogStoreError> {
    let mut file = File::open(path)?;
    let mut magic = [0u8; 8];
    file.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(LogStoreError::Format(format!(
            "{} bad magic",
            path.display()
        )));
    }
    Ok(SegmentIter {
        reader: BufReader::new(file),
    })
}

struct SegmentIter {
    reader: BufReader<File>,
}

impl Iterator for SegmentIter {
    type Item = Result<LogEntry, LogStoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        match read_one_frame(&mut self.reader) {
            Ok(Some(e)) => Some(Ok(e)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// Returns `Ok(None)` on a clean EOF or any torn-tail condition (short
/// read, bad CRC, oversized len). The caller treats `None` as "segment
/// ends here" — same self-healing discipline as `nebula-wal`.
fn read_one_frame<R: Read>(reader: &mut R) -> Result<Option<LogEntry>, LogStoreError> {
    let mut len_buf = [0u8; 4];
    if let Err(e) = reader.read_exact(&mut len_buf) {
        return if e.kind() == io::ErrorKind::UnexpectedEof {
            Ok(None)
        } else {
            Err(e.into())
        };
    }
    let len = u32::from_le_bytes(len_buf);
    if len > MAX_RECORD_BYTES {
        return Ok(None);
    }

    let mut crc_buf = [0u8; 4];
    if reader.read_exact(&mut crc_buf).is_err() {
        return Ok(None);
    }
    let claimed_crc = u32::from_le_bytes(crc_buf);

    let mut tail = [0u8; HEADER_TAIL_BYTES];
    if reader.read_exact(&mut tail).is_err() {
        return Ok(None);
    }
    let term = u64::from_le_bytes(tail[..8].try_into().unwrap());
    let index = u64::from_le_bytes(tail[8..].try_into().unwrap());

    let mut body = vec![0u8; len as usize];
    if reader.read_exact(&mut body).is_err() {
        return Ok(None);
    }

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&tail);
    hasher.update(&body);
    if hasher.finalize() != claimed_crc {
        return Ok(None);
    }

    let payload: LogPayload = match bincode::deserialize(&body) {
        Ok(p) => p,
        Err(_) => return Ok(None),
    };
    Ok(Some(LogEntry {
        term,
        index,
        payload,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nebula_wal::WalRecord;
    use tempfile::tempdir;

    fn mut_record(id: &str) -> LogPayload {
        LogPayload::Mutation(WalRecord::Delete {
            bucket: "b".into(),
            external_id: id.into(),
        })
    }

    #[test]
    fn open_creates_first_segment_with_magic() {
        let dir = tempdir().unwrap();
        let store = LogStore::open(dir.path(), LogConfig::default()).unwrap();
        drop(store);
        let bytes = std::fs::read(dir.path().join("0000000000.nrlog")).unwrap();
        assert_eq!(&bytes[..MAGIC.len()], MAGIC);
    }

    #[test]
    fn append_then_read_roundtrips() {
        let dir = tempdir().unwrap();
        let store = LogStore::open(dir.path(), LogConfig::default()).unwrap();
        for (term, index) in [(1, 1), (1, 2), (2, 3)] {
            store
                .append(&LogEntry {
                    term,
                    index,
                    payload: mut_record(&format!("d{index}")),
                })
                .unwrap();
        }
        let all = store.read_all().unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].term, 1);
        assert_eq!(all[2].term, 2);
        assert_eq!(all[2].index, 3);
    }

    #[test]
    fn append_rejects_non_monotone_index() {
        let dir = tempdir().unwrap();
        let store = LogStore::open(dir.path(), LogConfig::default()).unwrap();
        store
            .append(&LogEntry {
                term: 1,
                index: 5,
                payload: mut_record("a"),
            })
            .unwrap();
        let err = store
            .append(&LogEntry {
                term: 1,
                index: 5,
                payload: mut_record("b"),
            })
            .unwrap_err();
        assert!(matches!(err, LogStoreError::Invariant(_)));
    }

    #[test]
    fn reopen_recovers_committed_records_and_last_index() {
        let dir = tempdir().unwrap();
        {
            let store = LogStore::open(dir.path(), LogConfig::default()).unwrap();
            for index in 1..=5 {
                store
                    .append(&LogEntry {
                        term: 1,
                        index,
                        payload: mut_record(&format!("d{index}")),
                    })
                    .unwrap();
            }
        }
        let store = LogStore::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!(store.last_index(), Some(5));
        let all = store.read_all().unwrap();
        assert_eq!(all.len(), 5);
        assert_eq!(all.last().unwrap().index, 5);
    }

    #[test]
    fn segment_rotates_when_size_exceeded() {
        let dir = tempdir().unwrap();
        // Tiny segment so a couple of small records force rotation.
        let cfg = LogConfig {
            segment_size_bytes: 256,
            fsync_on_append: false,
        };
        let store = LogStore::open(dir.path(), cfg).unwrap();
        for index in 1..=10 {
            store
                .append(&LogEntry {
                    term: 1,
                    index,
                    payload: mut_record(&format!("payload-{index}-with-some-bytes")),
                })
                .unwrap();
        }
        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| {
                let n = e.ok()?.file_name().into_string().ok()?;
                n.ends_with(".nrlog").then_some(n)
            })
            .collect();
        assert!(
            entries.len() > 1,
            "expected multiple segments, got {entries:?}"
        );
        let all = store.read_all().unwrap();
        assert_eq!(all.len(), 10);
    }

    #[test]
    fn torn_tail_is_recovered_to_last_good_record() {
        let dir = tempdir().unwrap();
        {
            let store = LogStore::open(dir.path(), LogConfig::default()).unwrap();
            for index in 1..=3 {
                store
                    .append(&LogEntry {
                        term: 1,
                        index,
                        payload: mut_record(&format!("d{index}")),
                    })
                    .unwrap();
            }
        }
        // Simulate a torn fsync: append garbage to the segment file.
        let path = dir.path().join("0000000000.nrlog");
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&[0xff, 0xff, 0xff, 0x7f, 0x00]).unwrap();
        f.sync_all().unwrap();

        // Reopen — the torn tail must be truncated, last_index = 3.
        let store = LogStore::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!(store.last_index(), Some(3));
        let all = store.read_all().unwrap();
        assert_eq!(all.len(), 3);

        // And a subsequent append still works (proving truncation
        // happened — otherwise the writer would be positioned past
        // the garbage and the new frame would be unreadable).
        store
            .append(&LogEntry {
                term: 2,
                index: 4,
                payload: mut_record("after-torn"),
            })
            .unwrap();
        let all = store.read_all().unwrap();
        assert_eq!(all.len(), 4);
        assert_eq!(all.last().unwrap().index, 4);
    }
}
