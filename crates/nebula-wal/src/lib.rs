//! Write-ahead log for NebulaDB.
//!
//! # Wire format
//!
//! Every record frame is 8 bytes of header + body:
//!
//! ```text
//! ┌─────────────┬─────────────┬───────────────────────┐
//! │ len (u32 LE)│ crc32 (u32) │ bincode(WalRecord)    │
//! └─────────────┴─────────────┴───────────────────────┘
//! ```
//!
//! - `len` is the body length in bytes.
//! - `crc32` is CRC32 of the body only. A header-corrupting crash
//!   produces a garbage `len` that the reader catches via a sanity
//!   cap (16 MiB per record).
//! - Body is `bincode`-encoded `WalRecord`. Versioned via the
//!   record enum itself — adding a new variant is forward-safe for
//!   old readers (they treat the leading discriminant as unknown
//!   and stop). We check this with a "magic file header".
//!
//! # Crash safety
//!
//! The reader treats *any* error at the end of a segment (EOF in
//! the middle of a frame, bad CRC, len > cap) as "segment ends
//! here". This is the crash-mid-write case: `fsync` lands the
//! successful records, and everything after the last good record
//! is implicitly discarded. The writer truncates the file to the
//! end of the last good record on open, so a subsequent append
//! doesn't create a hole the reader would have to skip.
//!
//! # Segment rotation
//!
//! Segments are named `<seq>.nwal` zero-padded to 10 digits. New
//! segments are opened when the current file would exceed
//! `WalConfig::segment_size_bytes`. Nothing ever overwrites a
//! closed segment — recovery iterates segments in lexical order,
//! which equals seq order.
//!
//! # What records carry
//!
//! Records store the **resolved vector**, not the source text +
//! metadata only. This decouples recovery from the embedder —
//! bringing up a server with a dead OpenAI key still recovers
//! correctly. The tradeoff is disk (6 KB per 1536-dim vector)
//! but it's the only honest way to guarantee durable writes
//! without requiring the embedder to be live during replay.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WalError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("wal codec: {0}")]
    Codec(String),
    #[error("wal format: {0}")]
    Format(String),
}

pub type Result<T> = std::result::Result<T, WalError>;

/// Marker bytes at the top of every `.nwal` file so we can tell
/// a stray file from ours. Bumping the version byte forces a
/// clean-slate replay; old writers would refuse to append to a
/// new-version file.
const MAGIC: &[u8; 8] = b"NEBWAL01";

/// Hard cap on a single record's encoded size. 16 MiB comfortably
/// fits a 1536-dim vector + long chunk text + metadata; anything
/// larger is almost certainly a corrupted `len` we shouldn't try
/// to allocate for.
const MAX_RECORD_BYTES: u32 = 16 * 1024 * 1024;

/// Written body of a WAL frame. Every mutation the index performs
/// is expressible as one variant. Variants are explicitly listed
/// rather than using a generic "kv" pair so future readers can
/// continue to parse old logs without schema guesswork.
///
/// Bincode's default is compact and fast; it does *not* tolerate
/// enum-variant reordering, so new variants MUST be appended.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WalRecord {
    /// `TextIndex::upsert_text` — carries the resolved vector so
    /// replay doesn't need the embedder.
    ///
    /// `metadata_json` stores the metadata as a serialized JSON
    /// string. We can't round-trip `serde_json::Value` through
    /// bincode directly — bincode's wire format is schema-bound
    /// and can't handle the serde "any" pattern Value uses. The
    /// string boundary is clean, tiny overhead, and keeps the WAL
    /// format simple to evolve.
    UpsertText {
        bucket: String,
        external_id: String,
        text: String,
        vector: Vec<f32>,
        metadata_json: String,
    },
    /// `TextIndex::upsert_document` — the chunked flavor. One
    /// record per document (not per chunk) so a partial apply
    /// can't leave a document half-indexed.
    UpsertDocument {
        bucket: String,
        doc_id: String,
        /// Chunked pieces, each with its own vector. Same
        /// "resolved" principle — no embedder call at replay.
        chunks: Vec<WalChunk>,
        metadata_json: String,
    },
    Delete {
        bucket: String,
        external_id: String,
    },
    DeleteDocument {
        bucket: String,
        doc_id: String,
    },
    EmptyBucket {
        bucket: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WalChunk {
    pub index: usize,
    pub char_start: usize,
    pub text: String,
    pub vector: Vec<f32>,
}

#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Roll to a new segment once the current file would grow past
    /// this. 64 MiB is small enough that replay scans are quick,
    /// large enough that we don't churn file handles.
    pub segment_size_bytes: u64,
    /// `fsync` after every appended record. Slow but the only
    /// level that guarantees durability across a kernel panic.
    /// Set false to batch (survives process crash, not kernel).
    pub fsync_on_append: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            segment_size_bytes: 64 * 1024 * 1024,
            fsync_on_append: true,
        }
    }
}

pub struct Wal {
    dir: PathBuf,
    config: WalConfig,
    // All mutation is serialized through this mutex: the WAL is
    // the commit point and must never interleave with itself.
    state: Mutex<WriterState>,
}

struct WriterState {
    current_seq: u64,
    current_file: BufWriter<File>,
    current_bytes: u64,
}

impl Wal {
    /// Open (or create) a WAL in the given directory. On open we
    /// scan all existing segments to find the last good record
    /// position and truncate anything after it — recovery from
    /// a crash mid-write happens transparently here.
    pub fn open(dir: impl AsRef<Path>, config: WalConfig) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        // Find the most recent segment; if none, start at seq 0.
        let segments = list_segments(&dir)?;
        let mut current_seq = segments.last().map(|s| s.seq).unwrap_or(0);

        // If there's a prior segment, validate + truncate it.
        // A fresh segment (no segments at all) gets the magic
        // header written on first open.
        let path = segment_path(&dir, current_seq);
        let (file, bytes) = if path.exists() {
            let truncated_to = validate_and_truncate(&path)?;
            let mut f = OpenOptions::new().append(true).open(&path)?;
            // Ensure we actually append at the truncated end — the
            // OS should already have us there but an append mode
            // fd position is mostly advisory on some platforms.
            f.seek(SeekFrom::End(0))?;
            (f, truncated_to)
        } else {
            current_seq = segments.last().map(|s| s.seq + 1).unwrap_or(0);
            let mut f = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(segment_path(&dir, current_seq))?;
            f.write_all(MAGIC)?;
            f.sync_data()?;
            (OpenOptions::new().append(true).open(segment_path(&dir, current_seq))?, MAGIC.len() as u64)
        };

        Ok(Self {
            dir,
            config,
            state: Mutex::new(WriterState {
                current_seq,
                current_file: BufWriter::new(file),
                current_bytes: bytes,
            }),
        })
    }

    /// Append one record. Returns once the record is in the
    /// kernel buffer (or fsynced, per config).
    pub fn append(&self, rec: &WalRecord) -> Result<()> {
        let body = bincode::serialize(rec).map_err(|e| WalError::Codec(e.to_string()))?;
        let len: u32 = body
            .len()
            .try_into()
            .map_err(|_| WalError::Format("record exceeds u32 length".into()))?;
        if len > MAX_RECORD_BYTES {
            return Err(WalError::Format(format!(
                "record of {len} bytes exceeds cap {MAX_RECORD_BYTES}"
            )));
        }
        let crc = crc32fast::hash(&body);

        let mut s = self.state.lock();

        // Rotate if this append would push us past the size cap.
        // The current segment ends at its last complete record;
        // the new one gets a fresh magic header.
        let frame_len = 8 + body.len() as u64;
        if s.current_bytes + frame_len > self.config.segment_size_bytes
            && s.current_bytes > MAGIC.len() as u64
        {
            // Flush + fsync the old segment so its tail is durable
            // before we move on. Not doing this would let a crash
            // during rotation lose the last pre-rotation records.
            s.current_file.flush()?;
            s.current_file.get_ref().sync_data()?;
            let next_seq = s.current_seq + 1;
            let mut f = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(segment_path(&self.dir, next_seq))?;
            f.write_all(MAGIC)?;
            f.sync_data()?;
            s.current_seq = next_seq;
            s.current_file =
                BufWriter::new(OpenOptions::new().append(true).open(segment_path(&self.dir, next_seq))?);
            s.current_bytes = MAGIC.len() as u64;
        }

        s.current_file.write_all(&len.to_le_bytes())?;
        s.current_file.write_all(&crc.to_le_bytes())?;
        s.current_file.write_all(&body)?;
        s.current_bytes += frame_len;

        if self.config.fsync_on_append {
            s.current_file.flush()?;
            s.current_file.get_ref().sync_data()?;
        }
        Ok(())
    }

    /// Scan every segment in order, yielding records. Short reads
    /// at the end of any segment stop that segment's iteration
    /// cleanly — this is where crash recovery happens.
    pub fn replay(&self) -> Result<Vec<WalRecord>> {
        replay_dir(&self.dir)
    }

    /// Flush + fsync. Call before a snapshot that expects every
    /// buffered record on disk.
    pub fn flush(&self) -> Result<()> {
        let mut s = self.state.lock();
        s.current_file.flush()?;
        s.current_file.get_ref().sync_data()?;
        Ok(())
    }

    /// Delete every segment strictly older than `oldest_to_keep`.
    /// Used after a successful snapshot: the snapshot supersedes
    /// every record up to a certain point, so earlier segments
    /// are pure overhead. Never deletes the current (in-use)
    /// segment even if the caller passes a higher seq.
    pub fn compact(&self, oldest_to_keep: u64) -> Result<usize> {
        let s = self.state.lock();
        let segments = list_segments(&self.dir)?;
        let mut removed = 0;
        for seg in segments {
            if seg.seq < oldest_to_keep && seg.seq < s.current_seq {
                fs::remove_file(&seg.path)?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Current segment sequence + total on-disk size. Surfaced to
    /// the admin endpoint so operators can tell when a compaction
    /// would actually free space.
    pub fn stats(&self) -> Result<WalStats> {
        let segments = list_segments(&self.dir)?;
        let segment_count = segments.len();
        let mut total_bytes = 0u64;
        let mut oldest_seq = u64::MAX;
        let mut newest_seq = 0u64;
        for seg in &segments {
            total_bytes += fs::metadata(&seg.path)?.len();
            oldest_seq = oldest_seq.min(seg.seq);
            newest_seq = newest_seq.max(seg.seq);
        }
        if segment_count == 0 {
            oldest_seq = 0;
        }
        Ok(WalStats {
            segment_count,
            total_bytes,
            oldest_seq,
            newest_seq,
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct WalStats {
    pub segment_count: usize,
    pub total_bytes: u64,
    pub oldest_seq: u64,
    pub newest_seq: u64,
}

// ---------------------------------------------------------------------------
// segment file helpers
// ---------------------------------------------------------------------------

struct Segment {
    seq: u64,
    path: PathBuf,
}

fn segment_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("{seq:010}.nwal"))
}

fn list_segments(dir: &Path) -> Result<Vec<Segment>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(stem) = name.strip_suffix(".nwal") {
            if let Ok(seq) = stem.parse::<u64>() {
                out.push(Segment {
                    seq,
                    path: entry.path(),
                });
            }
        }
    }
    out.sort_by_key(|s| s.seq);
    Ok(out)
}

/// Walk through the file verifying each record's framing and CRC.
/// Stop at the first short / bad record; return the byte offset
/// at which to truncate so future appends start clean.
///
/// Also validates the magic prefix; a missing or wrong magic is a
/// hard error — we'd rather refuse to open than silently overwrite
/// an unrelated file.
fn validate_and_truncate(path: &Path) -> Result<u64> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let mut reader = BufReader::new(&mut file);

    let mut magic = [0u8; 8];
    if reader.read_exact(&mut magic).is_err() {
        // Zero-byte file — treat as empty, write magic and return.
        drop(reader);
        file.set_len(0)?;
        file.write_all(MAGIC)?;
        file.sync_data()?;
        return Ok(MAGIC.len() as u64);
    }
    if &magic != MAGIC {
        return Err(WalError::Format(format!(
            "unknown magic in {}",
            path.display()
        )));
    }

    let mut valid_end = MAGIC.len() as u64;
    loop {
        let mut header = [0u8; 8];
        match reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        if len == 0 || len > MAX_RECORD_BYTES {
            break;
        }
        let mut body = vec![0u8; len as usize];
        match reader.read_exact(&mut body) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        if crc32fast::hash(&body) != crc {
            // Corrupt record — stop here. Earlier records are
            // trusted because they had valid CRCs.
            break;
        }
        valid_end += 8 + len as u64;
    }

    drop(reader);
    let file_len = file.metadata()?.len();
    if valid_end < file_len {
        // Crash mid-write: truncate so the next append starts
        // at a clean boundary.
        file.set_len(valid_end)?;
        file.sync_data()?;
    }
    Ok(valid_end)
}

/// Scan every segment in the directory in seq order, returning
/// the parsed records. Used both by `Wal::replay` and by tests
/// that want to read a WAL without reopening it for writes.
pub fn replay_dir(dir: &Path) -> Result<Vec<WalRecord>> {
    let mut out = Vec::new();
    for seg in list_segments(dir)? {
        let file = File::open(&seg.path)?;
        let mut reader = BufReader::new(file);
        let mut magic = [0u8; 8];
        if reader.read_exact(&mut magic).is_err() {
            continue;
        }
        if &magic != MAGIC {
            return Err(WalError::Format(format!(
                "unknown magic in {}",
                seg.path.display()
            )));
        }
        loop {
            let mut header = [0u8; 8];
            match reader.read_exact(&mut header) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
            let crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
            if len == 0 || len > MAX_RECORD_BYTES {
                break;
            }
            let mut body = vec![0u8; len as usize];
            match reader.read_exact(&mut body) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            if crc32fast::hash(&body) != crc {
                break;
            }
            let rec: WalRecord = bincode::deserialize(&body)
                .map_err(|e| WalError::Codec(e.to_string()))?;
            out.push(rec);
        }
    }
    Ok(out)
}

/// Current segment sequence number for code that snapshots and
/// wants to record "any record up to seq N is superseded by this
/// snapshot, safe to compact". Returns `None` when the WAL is
/// empty (no segments yet).
pub fn current_seq(dir: &Path) -> Result<Option<u64>> {
    let segments = list_segments(dir)?;
    Ok(segments.last().map(|s| s.seq))
}

/// Wall-clock timestamp at which a given segment was last
/// modified. Surfaced by the admin endpoint so the UI can show
/// "WAL last written X seconds ago".
pub fn newest_segment_modified_ms(dir: &Path) -> Result<Option<u64>> {
    let segments = list_segments(dir)?;
    let Some(newest) = segments.last() else {
        return Ok(None);
    };
    let meta = fs::metadata(&newest.path)?;
    let modified = meta
        .modified()
        .unwrap_or(SystemTime::UNIX_EPOCH)
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    Ok(Some(modified))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn mk_rec(i: u32) -> WalRecord {
        WalRecord::UpsertText {
            bucket: "b".into(),
            external_id: format!("id-{i}"),
            text: format!("text {i}"),
            vector: vec![0.1, 0.2, 0.3],
            metadata_json: format!("{{\"i\":{i}}}"),
        }
    }

    #[test]
    fn round_trip_simple() {
        let dir = tempdir().unwrap();
        let wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
        for i in 0..10 {
            wal.append(&mk_rec(i)).unwrap();
        }
        let back = wal.replay().unwrap();
        assert_eq!(back.len(), 10);
        for (i, r) in back.iter().enumerate() {
            match r {
                WalRecord::UpsertText { external_id, .. } => {
                    assert_eq!(external_id, &format!("id-{i}"))
                }
                _ => panic!(),
            }
        }
    }

    #[test]
    fn reopen_truncates_partial_tail() {
        // Simulate a crash mid-write: append 5 good records, then
        // *corrupt* the file by appending a partial header. A
        // subsequent open should truncate and continue.
        let dir = tempdir().unwrap();
        {
            let wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
            for i in 0..5 {
                wal.append(&mk_rec(i)).unwrap();
            }
            wal.flush().unwrap();
        }

        // Corrupt the file directly by appending garbage.
        let path = segment_path(dir.path(), 0);
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        // Write a len of 2 KB but no body — classic mid-write
        // pattern.
        f.write_all(&2048u32.to_le_bytes()).unwrap();
        f.write_all(&0u32.to_le_bytes()).unwrap();
        f.write_all(&[0u8; 10]).unwrap();
        drop(f);

        // Reopen: the truncation path should discard the garbage
        // and we should still see exactly 5 good records.
        let wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
        let back = wal.replay().unwrap();
        assert_eq!(back.len(), 5, "expected 5 good records after recovery");
        // And appending now works without leaving a hole.
        wal.append(&mk_rec(99)).unwrap();
        wal.flush().unwrap();
        let back = wal.replay().unwrap();
        assert_eq!(back.len(), 6);
    }

    #[test]
    fn detects_crc_corruption() {
        let dir = tempdir().unwrap();
        {
            let wal = Wal::open(dir.path(), WalConfig::default()).unwrap();
            for i in 0..3 {
                wal.append(&mk_rec(i)).unwrap();
            }
            wal.flush().unwrap();
        }
        // Flip a bit inside a record body to break the CRC.
        let path = segment_path(dir.path(), 0);
        let mut bytes = std::fs::read(&path).unwrap();
        let idx = bytes.len() - 4;
        bytes[idx] ^= 0x40;
        std::fs::write(&path, bytes).unwrap();

        // replay_dir should stop at the broken record — the two
        // earlier good ones are still retrievable.
        let back = replay_dir(dir.path()).unwrap();
        assert_eq!(back.len(), 2);
    }

    #[test]
    fn rotates_at_segment_boundary() {
        // Tiny segment cap to force rotation after a couple of records.
        let dir = tempdir().unwrap();
        let wal = Wal::open(
            dir.path(),
            WalConfig {
                segment_size_bytes: 256,
                fsync_on_append: false,
            },
        )
        .unwrap();
        for i in 0..10 {
            wal.append(&mk_rec(i)).unwrap();
        }
        wal.flush().unwrap();
        let segs = list_segments(dir.path()).unwrap();
        assert!(segs.len() > 1, "expected rotation, got {} segments", segs.len());
        let back = wal.replay().unwrap();
        assert_eq!(back.len(), 10, "rotation lost records");
    }

    #[test]
    fn compact_drops_old_segments() {
        let dir = tempdir().unwrap();
        let wal = Wal::open(
            dir.path(),
            WalConfig {
                segment_size_bytes: 256,
                fsync_on_append: false,
            },
        )
        .unwrap();
        for i in 0..20 {
            wal.append(&mk_rec(i)).unwrap();
        }
        wal.flush().unwrap();
        let before = list_segments(dir.path()).unwrap().len();
        assert!(before >= 2);
        let removed = wal.compact(u64::MAX).unwrap(); // drop everything except current
        let after = list_segments(dir.path()).unwrap().len();
        assert_eq!(before - after, removed);
        assert_eq!(after, 1, "current segment must survive compact");
    }
}
