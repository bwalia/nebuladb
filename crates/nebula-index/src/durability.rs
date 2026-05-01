//! Snapshot + WAL recovery for `TextIndex`.
//!
//! The durability design in plain English:
//!
//! 1. Every mutation goes through the WAL before it lands in RAM.
//!    If the WAL write fails, the in-memory apply doesn't happen —
//!    the caller sees an error and retries. If the WAL write
//!    succeeds but the process crashes before the apply, recovery
//!    replays that record on the next boot.
//!
//! 2. Periodically (or on demand) the server takes a *snapshot* —
//!    a full, compressed, atomic dump of the in-memory state plus
//!    the HNSW graph. The snapshot records the latest WAL sequence
//!    number it captures.
//!
//! 3. On boot: load the newest snapshot (if any), then replay
//!    every WAL record with `seq > snapshot.wal_seq`. The result
//!    is byte-identical to the pre-crash state (modulo RNG which
//!    only affects future inserts).
//!
//! 4. After a successful snapshot, WAL segments strictly older
//!    than the snapshot's `wal_seq` can be compacted away.
//!
//! This module owns serialization of the index state. It does NOT
//! own the decision of when to snapshot — that's the admin API's
//! job. Snapshots are always explicit.

use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use ahash::{AHashMap, AHashSet};
use serde::{Deserialize, Serialize};

use nebula_core::Id;
use nebula_vector::HnswSnapshot;
use nebula_wal::WalError;

use crate::{Document, IndexError, Result};

/// File naming + atomic-rename dance:
///
/// * We write to `snapshot-<seq>.nsnap.part`.
/// * Fsync the body + directory.
/// * Rename to `snapshot-<seq>.nsnap`.
/// * Fsync the directory again.
/// * Touch `snapshot-<seq>.nsnap.ok` (zero-byte marker).
///
/// The `.ok` marker is the commit point: recovery only considers
/// snapshots with a matching `.ok`. A crash mid-snapshot leaves a
/// `.part` (ignored) or an unblessed `.nsnap` (also ignored).
const SNAPSHOT_EXT: &str = "nsnap";
const SNAPSHOT_PART_EXT: &str = "nsnap.part";
const SNAPSHOT_OK_EXT: &str = "nsnap.ok";

/// Snapshot header. Bincode-encoded + zstd-compressed, then
/// concatenated with the HNSW body. A leading u64 gives the header
/// length so we can cleanly separate the two sections.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotHeader {
    /// Schema version for the on-disk format. Bumping requires a
    /// matching `restore_from_snapshot` branch.
    pub version: u32,
    /// WAL seq this snapshot supersedes. On boot we replay
    /// records with seq > this.
    pub wal_seq_at_snapshot: u64,
    /// Unix millis for human-readable "snapshot X minutes old".
    pub taken_at_ms: u64,
    /// Serialized DocState — the in-memory `Inner` fields minus
    /// the HNSW. Kept inside the header so we can deserialize
    /// everything in two passes (header, then HNSW body).
    pub docs: SerializedDocState,
}

/// Plain-data view of the index's in-memory maps. Vectors aren't
/// here — they live in the HNSW snapshot, which already has them
/// in a flat arena. Storing them twice would double the snapshot
/// size for no gain.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SerializedDocState {
    pub next_id: u64,
    pub docs: Vec<SerializedDoc>,
    /// `(bucket, parent_doc_id)` → set of chunk external ids.
    /// Serialized as `Vec<(bucket, parent, chunks)>` because the
    /// `AHashMap` key type (a tuple) isn't serde-friendly without
    /// effort.
    pub parents: Vec<ParentEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedDoc {
    pub internal_id: u64,
    pub bucket: String,
    pub external_id: String,
    pub text: String,
    /// JSON text; same boundary as the WAL — see that module.
    pub metadata_json: String,
    pub parent_doc_id: Option<String>,
    pub chunk_index: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParentEntry {
    pub bucket: String,
    pub parent_doc_id: String,
    pub external_ids: Vec<String>,
}

/// Serialize to `<dir>/snapshot-<seq>.nsnap`, durably. `wal_seq`
/// is the HIGHEST WAL seq number that this snapshot captures —
/// recovery discards everything up to and including it.
pub fn write_snapshot(
    dir: &Path,
    wal_seq: u64,
    docs: SerializedDocState,
    hnsw: HnswSnapshot,
) -> Result<PathBuf> {
    fs::create_dir_all(dir).map_err(io_to_index)?;

    let header = SnapshotHeader {
        version: 1,
        wal_seq_at_snapshot: wal_seq,
        taken_at_ms: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
        docs,
    };
    let header_bytes =
        bincode::serialize(&header).map_err(|e| IndexError::Invalid(format!("snapshot header: {e}")))?;
    let hnsw_bytes =
        bincode::serialize(&hnsw).map_err(|e| IndexError::Invalid(format!("snapshot hnsw: {e}")))?;

    let stem = format!("snapshot-{wal_seq:020}");
    let part_path = dir.join(format!("{stem}.{SNAPSHOT_PART_EXT}"));
    let final_path = dir.join(format!("{stem}.{SNAPSHOT_EXT}"));
    let ok_path = dir.join(format!("{stem}.{SNAPSHOT_OK_EXT}"));

    // Stream directly into zstd so we never hold the whole
    // compressed blob in memory. Level 3 is a reasonable
    // throughput/size tradeoff; snapshots run out-of-band.
    {
        let file = File::create(&part_path).map_err(io_to_index)?;
        let mut writer = BufWriter::new(file);
        let mut enc = zstd::Encoder::new(&mut writer, 3)
            .map_err(|e| IndexError::Invalid(format!("zstd enc: {e}")))?;
        // Layout: [u64 header_len] [header_bytes] [hnsw_bytes]
        enc.write_all(&(header_bytes.len() as u64).to_le_bytes())
            .map_err(io_to_index)?;
        enc.write_all(&header_bytes).map_err(io_to_index)?;
        enc.write_all(&hnsw_bytes).map_err(io_to_index)?;
        enc.finish().map_err(io_to_index)?.flush().map_err(io_to_index)?;
    }
    // fsync body before rename.
    File::open(&part_path)
        .and_then(|f| f.sync_data())
        .map_err(io_to_index)?;
    fs::rename(&part_path, &final_path).map_err(io_to_index)?;
    sync_dir(dir)?;
    // Commit marker.
    File::create(&ok_path).map_err(io_to_index)?;
    sync_dir(dir)?;

    Ok(final_path)
}

/// Load the newest `.ok`-blessed snapshot in `dir`. Returns
/// `None` if no snapshots exist. Returns an error if the newest
/// one is corrupt — we don't silently skip to an older snapshot
/// because that would mask data loss.
pub fn load_latest_snapshot(
    dir: &Path,
) -> Result<Option<(SnapshotHeader, HnswSnapshot)>> {
    let Some(ok_path) = newest_snapshot(dir)? else {
        return Ok(None);
    };
    let stem = ok_path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| IndexError::Invalid("bad snapshot filename".into()))?;
    // stem includes the nested extension: "snapshot-<seq>.nsnap"
    let data_path = dir.join(stem);
    let file = File::open(&data_path).map_err(io_to_index)?;
    let reader = BufReader::new(file);
    let mut dec = zstd::Decoder::new(reader).map_err(io_to_index)?;
    let mut header_len_buf = [0u8; 8];
    dec.read_exact(&mut header_len_buf).map_err(io_to_index)?;
    let header_len = u64::from_le_bytes(header_len_buf) as usize;
    let mut header_bytes = vec![0u8; header_len];
    dec.read_exact(&mut header_bytes).map_err(io_to_index)?;
    let header: SnapshotHeader = bincode::deserialize(&header_bytes)
        .map_err(|e| IndexError::Invalid(format!("snapshot header decode: {e}")))?;

    let mut hnsw_bytes = Vec::new();
    dec.read_to_end(&mut hnsw_bytes).map_err(io_to_index)?;
    let hnsw: HnswSnapshot = bincode::deserialize(&hnsw_bytes)
        .map_err(|e| IndexError::Invalid(format!("snapshot hnsw decode: {e}")))?;

    Ok(Some((header, hnsw)))
}

/// Return the path of the newest committed snapshot's `.ok`
/// marker, or `None` if no complete snapshots exist.
fn newest_snapshot(dir: &Path) -> Result<Option<PathBuf>> {
    if !dir.exists() {
        return Ok(None);
    }
    let mut best: Option<(u64, PathBuf)> = None;
    for entry in fs::read_dir(dir).map_err(io_to_index)? {
        let entry = entry.map_err(io_to_index)?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        // Commit marker: `snapshot-<seq>.nsnap.ok`.
        let Some(stem) = name.strip_suffix(".nsnap.ok") else { continue };
        let Some(seq_str) = stem.strip_prefix("snapshot-") else { continue };
        if let Ok(seq) = seq_str.parse::<u64>() {
            if best.as_ref().map_or(true, |(s, _)| seq > *s) {
                best = Some((seq, entry.path()));
            }
        }
    }
    Ok(best.map(|(_, p)| p))
}

/// Delete snapshot files older than the newest committed one.
/// Called after a successful snapshot to reclaim disk.
pub fn prune_old_snapshots(dir: &Path) -> Result<usize> {
    let Some(newest) = newest_snapshot(dir)? else {
        return Ok(0);
    };
    let newest_seq = newest
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.strip_suffix(".nsnap.ok"))
        .and_then(|n| n.strip_prefix("snapshot-"))
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);

    let mut removed = 0;
    for entry in fs::read_dir(dir).map_err(io_to_index)? {
        let entry = entry.map_err(io_to_index)?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.starts_with("snapshot-") {
            continue;
        }
        // Extract the numeric seq from any snapshot-related
        // filename (main data, .part, .ok).
        let without_prefix = &name["snapshot-".len()..];
        let seq_str = without_prefix
            .split('.')
            .next()
            .unwrap_or("");
        let Ok(seq) = seq_str.parse::<u64>() else { continue };
        if seq < newest_seq {
            fs::remove_file(entry.path()).map_err(io_to_index)?;
            removed += 1;
        }
    }
    Ok(removed)
}

fn sync_dir(dir: &Path) -> Result<()> {
    // Linux guarantees `fsync(dir)` commits rename visibility; on
    // macOS it's a no-op but we still issue it for forward
    // portability. Skip silently if the FS refuses.
    if let Ok(f) = File::open(dir) {
        let _ = f.sync_data();
    }
    Ok(())
}

fn io_to_index(e: std::io::Error) -> IndexError {
    IndexError::Core(nebula_core::NebulaError::Io(e))
}

/// Convenience: convert the current `Inner` maps into their
/// serializable form. Used by the snapshot path.
pub fn serialize_docs(
    next_id: u64,
    docs: &AHashMap<Id, Document>,
    parents: &AHashMap<(String, String), AHashSet<String>>,
) -> SerializedDocState {
    let mut serialized_docs = Vec::with_capacity(docs.len());
    for (id, doc) in docs {
        serialized_docs.push(SerializedDoc {
            internal_id: id.0,
            bucket: doc.bucket.clone(),
            external_id: doc.external_id.clone(),
            text: doc.text.clone(),
            metadata_json: doc.metadata.to_string(),
            parent_doc_id: doc.parent_doc_id.clone(),
            chunk_index: doc.chunk_index,
        });
    }
    let parents_vec = parents
        .iter()
        .map(|((bucket, parent), kids)| ParentEntry {
            bucket: bucket.clone(),
            parent_doc_id: parent.clone(),
            external_ids: kids.iter().cloned().collect(),
        })
        .collect();
    SerializedDocState {
        next_id,
        docs: serialized_docs,
        parents: parents_vec,
    }
}

/// Convert a WAL error into our `IndexError` without a second
/// transitive `From`.
pub fn wal_err(e: WalError) -> IndexError {
    IndexError::Invalid(format!("wal: {e}"))
}
