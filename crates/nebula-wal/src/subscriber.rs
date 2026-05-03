//! WAL subscriber — tail the log from an arbitrary cursor forward.
//!
//! The WAL is single-writer and append-only. A subscriber turns it
//! into a stream that a follower can consume: "give me every record
//! from cursor X onward, live as they arrive."
//!
//! # Why a cursor (not a seq number)
//!
//! Records don't carry their own seq — they're identified by *where
//! they sit on disk*: `(segment_seq, byte_offset_of_record_start)`.
//! That's opaque to callers but trivial for us to compute during
//! replay and append. Crucially it survives crash-truncation: if a
//! torn-tail record is discarded on reopen, its cursor simply never
//! existed, and the next appended record takes the slot.
//!
//! # Two-phase subscribe
//!
//! 1. **Catch-up**: read historical records from disk starting at
//!    the caller's cursor, up to whatever the live tip is right now.
//!    This reads from a snapshot of segment files, so writes racing
//!    in parallel don't confuse it — they just end up in the live
//!    phase.
//!
//! 2. **Live tail**: swap to a `tokio::sync::broadcast` receiver
//!    that the writer pushes to on every `append`. Dropped messages
//!    (slow consumer) surface as `Lagged` — the subscriber can
//!    either reconnect with its last-seen cursor or fail hard.
//!
//! # Ack model
//!
//! Subscribers call `ack(cursor)` when a record is durably applied
//! downstream. The WAL tracks the minimum ack across all live
//! subscribers so compaction can be bounded: "don't delete a
//! segment if any follower still needs records from it." Without
//! this, compacting after a snapshot would silently orphan a
//! follower that's just behind the snapshot point.

use std::cmp::Ordering;
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::{
    list_segments, segment_path, WalError, WalRecord, Result, MAGIC, MAX_RECORD_BYTES,
};

/// Opaque position of a record in the WAL. Ordering is lexical on
/// `(segment_seq, byte_offset)` — within a segment, later records
/// have larger offsets; across segments, later segments win.
///
/// `byte_offset` points at the **start of the frame** (the `len`
/// field), so `seek(offset); read_frame()` is a valid resume.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalCursor {
    pub segment_seq: u64,
    pub byte_offset: u64,
}

impl WalCursor {
    /// Cursor that sorts before every real cursor — use this as the
    /// starting point when a follower wants "everything from scratch".
    pub const BEGIN: WalCursor = WalCursor {
        segment_seq: 0,
        byte_offset: 0,
    };
}

impl PartialOrd for WalCursor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WalCursor {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.segment_seq.cmp(&other.segment_seq) {
            Ordering::Equal => self.byte_offset.cmp(&other.byte_offset),
            o => o,
        }
    }
}

/// A record plus its cursor. Subscribers receive these; they ack by
/// calling [`WalSubscriber::ack`] with the cursor of the last
/// durably-applied entry.
#[derive(Debug, Clone)]
pub struct WalEntry {
    pub cursor: WalCursor,
    /// Cursor pointing *past* this record — where a resume would
    /// pick up the next one. Handing this back to `subscribe` after
    /// processing `record` gives at-least-once semantics without the
    /// follower having to decode the cursor format.
    pub next_cursor: WalCursor,
    pub record: WalRecord,
}

/// Broadcast capacity. 1024 entries ≈ ~6 MB of live backpressure
/// for 1536-dim vector writes; enough that a transient consumer
/// pause doesn't trip `Lagged`, small enough that a stalled
/// subscriber doesn't balloon memory.
pub(crate) const BROADCAST_CAPACITY: usize = 1024;

/// Shared state between the Wal writer and its subscribers.
///
/// The broadcast channel is the live-tail fanout; the ack registry
/// lets compaction know how far every follower has got. Both must
/// be cheap to touch on every append (we hold the writer mutex at
/// that moment), so: one atomic send + one cheap min over a small
/// Vec behind its own mutex.
pub(crate) struct SubscriberHub {
    pub(crate) tx: broadcast::Sender<WalEntry>,
    acks: Mutex<Vec<AckSlot>>,
}

struct AckSlot {
    id: u64,
    last_ack: WalCursor,
}

impl SubscriberHub {
    pub(crate) fn new() -> Self {
        let (tx, _rx) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            tx,
            acks: Mutex::new(Vec::new()),
        }
    }

    /// Publish a live entry to every active subscriber. A send
    /// error just means nobody's listening — fine, not an error.
    pub(crate) fn publish(&self, entry: WalEntry) {
        let _ = self.tx.send(entry);
    }

    /// Oldest cursor still needed by any follower. Returns `None`
    /// when nothing is subscribed — callers (compaction) use that
    /// as "no constraint, compact freely up to your own snapshot
    /// boundary."
    pub fn min_ack(&self) -> Option<WalCursor> {
        let acks = self.acks.lock();
        acks.iter().map(|a| a.last_ack).min()
    }

    fn register(&self, start: WalCursor) -> u64 {
        let mut acks = self.acks.lock();
        // Simple monotonic ID. We re-use slots on unsubscribe
        // rather than growing forever — subscribers are long-lived
        // followers, not a hot path.
        let id = acks.iter().map(|a| a.id).max().unwrap_or(0) + 1;
        acks.push(AckSlot {
            id,
            last_ack: start,
        });
        id
    }

    fn unregister(&self, id: u64) {
        let mut acks = self.acks.lock();
        acks.retain(|a| a.id != id);
    }

    fn record_ack(&self, id: u64, cursor: WalCursor) {
        let mut acks = self.acks.lock();
        if let Some(slot) = acks.iter_mut().find(|a| a.id == id) {
            if cursor > slot.last_ack {
                slot.last_ack = cursor;
            }
        }
    }
}

/// Live handle a follower holds. Dropping it unregisters from the
/// ack registry so compaction isn't held back by a dead consumer.
pub struct WalSubscriber {
    id: u64,
    hub: Arc<SubscriberHub>,
    rx: broadcast::Receiver<WalEntry>,
    catchup: std::collections::VecDeque<WalEntry>,
    /// Once we're done draining `catchup`, cursor at which we
    /// expect the next broadcast message to start. If the broadcast
    /// gives us something earlier (historical replay racing with a
    /// live append that arrived during catch-up), we skip it.
    live_start: WalCursor,
}

impl WalSubscriber {
    /// Internal: build a subscriber already primed with its
    /// catch-up queue and a live receiver. Public entry point is
    /// [`crate::Wal::subscribe`].
    pub(crate) fn new(
        hub: Arc<SubscriberHub>,
        catchup: Vec<WalEntry>,
        live_start: WalCursor,
        start: WalCursor,
    ) -> Self {
        let rx = hub.tx.subscribe();
        let id = hub.register(start);
        Self {
            id,
            hub,
            rx,
            catchup: catchup.into(),
            live_start,
        }
    }

    /// Next entry, or `None` if the WAL is closed and we've drained
    /// everything. Historical catch-up is served first; when
    /// exhausted, we switch to the broadcast tail.
    pub async fn next(&mut self) -> Option<Result<WalEntry>> {
        if let Some(e) = self.catchup.pop_front() {
            return Some(Ok(e));
        }
        loop {
            match self.rx.recv().await {
                Ok(entry) => {
                    // Skip anything that was already in our catch-up
                    // slice — this happens when an append lands
                    // between the catch-up read and the subscribe.
                    if entry.cursor < self.live_start {
                        continue;
                    }
                    return Some(Ok(entry));
                }
                Err(broadcast::error::RecvError::Closed) => return None,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    return Some(Err(WalError::Format(format!(
                        "subscriber lagged; missed {n} live records — reconnect with last-seen cursor"
                    ))));
                }
            }
        }
    }

    /// Record durable application downstream. The WAL uses this to
    /// bound compaction so a follower never sees an orphaned gap.
    pub fn ack(&self, cursor: WalCursor) {
        self.hub.record_ack(self.id, cursor);
    }
}

impl Drop for WalSubscriber {
    fn drop(&mut self) {
        self.hub.unregister(self.id);
    }
}

// ---------------------------------------------------------------------------
// Historical catch-up: read records with cursors from disk.
// ---------------------------------------------------------------------------

/// Read every record with a cursor `>= start` from `dir`. Stops
/// cleanly at the first bad frame — matching the crash-safety
/// discipline of the writer.
///
/// The returned `live_start` is the cursor *past* the last entry
/// we produced: hand it to the subscriber so it knows which live
/// messages to skip (anything the catch-up already saw).
pub(crate) fn read_from(
    dir: &Path,
    start: WalCursor,
) -> Result<(Vec<WalEntry>, WalCursor)> {
    let mut out = Vec::new();
    let mut live_start = start;

    for seg in list_segments(dir)? {
        if seg.seq < start.segment_seq {
            continue;
        }

        // Starting offset within this segment: either the caller's
        // offset (if resuming inside seg.seq) or just past the
        // magic header for later segments.
        let mut off = if seg.seq == start.segment_seq {
            start.byte_offset.max(MAGIC.len() as u64)
        } else {
            MAGIC.len() as u64
        };

        let path = segment_path(dir, seg.seq);
        let entries = scan_segment(&path, seg.seq, off)?;
        for e in entries {
            off = e.next_cursor.byte_offset;
            live_start = e.next_cursor;
            out.push(e);
        }
        // If we didn't reach the end of this segment (cursor past
        // the file), live_start still points into this segment — a
        // later read picks up from there.
        let _ = off;
    }

    Ok((out, live_start))
}

/// Scan one segment from `start_off` to EOF (or first bad record),
/// producing entries tagged with their cursors.
fn scan_segment(
    path: &PathBuf,
    seq: u64,
    start_off: u64,
) -> Result<Vec<WalEntry>> {
    let mut file = File::open(path)?;
    // Verify magic if we're at the top of the file.
    if start_off < MAGIC.len() as u64 {
        let mut magic = [0u8; 8];
        if file.read_exact(&mut magic).is_err() {
            return Ok(Vec::new());
        }
        if &magic != MAGIC {
            return Err(WalError::Format(format!(
                "unknown magic in {}",
                path.display()
            )));
        }
    } else {
        use std::io::Seek;
        file.seek(io::SeekFrom::Start(start_off))?;
    }

    let mut reader = BufReader::new(file);
    let mut cur_off = start_off.max(MAGIC.len() as u64);
    let mut out = Vec::new();
    loop {
        let mut header = [0u8; 8];
        match reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let len =
            u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let crc =
            u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
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
        let frame_len = 8 + len as u64;
        let cursor = WalCursor {
            segment_seq: seq,
            byte_offset: cur_off,
        };
        let next = WalCursor {
            segment_seq: seq,
            byte_offset: cur_off + frame_len,
        };
        out.push(WalEntry {
            cursor,
            next_cursor: next,
            record: rec,
        });
        cur_off += frame_len;
    }
    Ok(out)
}
