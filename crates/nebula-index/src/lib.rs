//! Text-addressable vector index.
//!
//! This crate wires the dumb pieces (HNSW + embedder) into something the
//! API layer can call: a [`TextIndex`] where you insert documents by
//! `(bucket, id, text)` and search by text or vector, with bucket
//! filtering applied post-ANN.
//!
//! Optionally durable: pass a `data_dir` at construction time to
//! enable WAL + snapshot recovery (see the `durability` module).
//! Without a data dir, everything is in-memory — the historical
//! behaviour we still ship for tests and demos.

pub mod durability;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use ahash::{AHashMap, AHashSet};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use nebula_chunk::Chunker;
use nebula_bm25::{Bm25Index, Bm25Params};
use nebula_core::{Id, NebulaError};
use nebula_embed::{EmbedError, Embedder};
use nebula_vector::{Hnsw, HnswConfig, Metric};
use nebula_wal::{Wal, WalChunk, WalConfig, WalRecord, WalStats};

#[derive(Debug, Error)]
pub enum IndexError {
    #[error(transparent)]
    Core(#[from] NebulaError),
    #[error(transparent)]
    Embed(#[from] EmbedError),
    #[error("bucket not found: {0}")]
    BucketNotFound(String),
    #[error("document not found: {bucket}/{id}")]
    DocNotFound { bucket: String, id: String },
    #[error("invalid document: {0}")]
    Invalid(String),
}

pub type Result<T> = std::result::Result<T, IndexError>;

/// A single stored document or chunk. `text` is kept for RAG context
/// return; `vector` is kept so we can re-insert into a rebuilt HNSW
/// without re-running the embedder.
///
/// `parent_doc_id` is `Some` for chunks and `None` for standalone
/// documents inserted via [`TextIndex::upsert_text`]. It lets
/// [`TextIndex::delete_document`] find every chunk for a document
/// without a second index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    pub bucket: String,
    pub external_id: String,
    pub text: String,
    #[serde(skip)]
    pub vector: Vec<f32>,
    #[serde(default)]
    pub metadata: serde_json::Value,
    #[serde(default)]
    pub parent_doc_id: Option<String>,
    #[serde(default)]
    pub chunk_index: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Hit {
    pub bucket: String,
    pub id: String,
    pub text: String,
    pub score: f32,
    pub metadata: serde_json::Value,
}

/// Wire shape for bucket export / import. Unlike `Document`, the
/// vector is serialized — the whole point of this type is to move
/// pre-embedded data between nodes without re-running the embedder.
///
/// Stable JSON field names so a future `nebula-ctl` or external tool
/// can produce the same shape and feed it into `import_bucket`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportedDoc {
    pub external_id: String,
    pub text: String,
    #[serde(default)]
    pub metadata: serde_json::Value,
    #[serde(default)]
    pub parent_doc_id: Option<String>,
    #[serde(default)]
    pub chunk_index: Option<usize>,
    /// Raw embedding. Length must equal the target index's `dim()`.
    pub vector: Vec<f32>,
}

/// Return of a successful `TextIndex::snapshot()`.
#[derive(Debug, Clone, Serialize)]
pub struct SnapshotOutcome {
    pub path: PathBuf,
    pub wal_seq_captured: u64,
}

/// One bucket's summary for the admin UI. `metadata_keys` counts how
/// often each top-level metadata key appears — a cheap "which fields
/// are worth filtering on?" signal that doesn't require a proper
/// schema inference pass.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BucketStats {
    pub bucket: String,
    pub docs: usize,
    pub parent_docs: usize,
    pub metadata_keys: Vec<(String, usize)>,
}

/// Min-max normalize an iterator of scores onto `[0, 1]`. Returns one
/// output per input, in order. Degenerate cases collapse to a constant:
/// an empty input yields an empty vec; a single value or an all-equal
/// set maps every element to `1.0` (they're all equally — maximally —
/// relevant within their stage, so a flat-zero would erase the stage's
/// contribution to the fusion).
fn min_max_normalize(scores: impl Iterator<Item = f32>) -> Vec<f32> {
    let v: Vec<f32> = scores.collect();
    if v.is_empty() {
        return v;
    }
    let min = v.iter().copied().fold(f32::INFINITY, f32::min);
    let max = v.iter().copied().fold(f32::NEG_INFINITY, f32::max);
    let range = max - min;
    if range <= f32::EPSILON {
        return vec![1.0; v.len()];
    }
    v.into_iter().map(|s| (s - min) / range).collect()
}

/// Rebuild an `Inner` from serialized-on-disk form. Factored out of
/// `open_persistent` so the crate's tests can drive it directly.
fn inner_from_serialized(state: durability::SerializedDocState) -> Inner {
    let mut by_key = AHashMap::with_capacity(state.docs.len());
    let mut docs = AHashMap::with_capacity(state.docs.len());
    // The BM25 index isn't part of the snapshot — rebuild it from the
    // restored `docs` text (design 0008 §6). Same principle as vectors
    // living only in the HNSW arena: we don't persist derivable state.
    let mut bm25 = Bm25Index::new(Bm25Params::default());
    for d in state.docs {
        let metadata: serde_json::Value = serde_json::from_str(&d.metadata_json)
            .unwrap_or(serde_json::Value::Null);
        let id = Id(d.internal_id);
        by_key.insert((d.bucket.clone(), d.external_id.clone()), id);
        bm25.add(id.0, &d.text);
        // Vectors live in the HNSW snapshot — the `Document`
        // struct we materialize here has an empty vector because
        // we'd be duplicating bytes otherwise. The HNSW is the
        // authority for the vector arena.
        docs.insert(
            id,
            Document {
                bucket: d.bucket,
                external_id: d.external_id,
                text: d.text,
                vector: Vec::new(),
                metadata,
                parent_doc_id: d.parent_doc_id,
                chunk_index: d.chunk_index,
            },
        );
    }
    let mut parents: AHashMap<(String, String), AHashSet<String>> = AHashMap::new();
    for p in state.parents {
        parents.insert(
            (p.bucket, p.parent_doc_id),
            p.external_ids.into_iter().collect(),
        );
    }
    Inner {
        by_key,
        docs,
        parents,
        bm25,
        next_id: state.next_id,
    }
}

struct Inner {
    /// External `(bucket, id)` → internal numeric id the HNSW speaks.
    by_key: AHashMap<(String, String), Id>,
    /// Internal id → document. Tombstoned docs are removed from this
    /// map and from `by_key`, but the HNSW tombstone stays — so a
    /// search returning a dead id is skipped at result-assembly time.
    docs: AHashMap<Id, Document>,
    /// `(bucket, parent_doc_id)` → set of chunk external ids. Lets
    /// `delete_document` find every chunk to tombstone without
    /// scanning the whole `docs` map.
    parents: AHashMap<(String, String), AHashSet<String>>,
    /// Lexical (BM25) index over the same corpus as `docs`, keyed by the
    /// internal `Id`'s `u64`. Kept in lock-step with `docs` via
    /// [`Inner::insert_doc`] / [`Inner::remove_doc`] so the two never
    /// diverge. Not serialized — rebuilt from `docs` on snapshot load
    /// (design 0008 §6), exactly as the HNSW arena is the authority for
    /// vectors and this is the authority for nothing persistent.
    bm25: Bm25Index,
    next_id: u64,
}

impl Inner {
    /// Insert (or replace) a document, keeping `docs` and the BM25
    /// index in sync. The single choke point for adding to the corpus —
    /// every caller routes through here so the lexical index can never
    /// silently fall behind the doc map.
    fn insert_doc(&mut self, id: Id, doc: Document) {
        self.bm25.add(id.0, &doc.text);
        self.docs.insert(id, doc);
    }

    /// Remove a document by internal id from both `docs` and the BM25
    /// index. Returns the removed `Document`, mirroring
    /// `HashMap::remove`, so callers keep their existing control flow.
    fn remove_doc(&mut self, id: Id) -> Option<Document> {
        self.bm25.remove(id.0);
        self.docs.remove(&id)
    }
}

pub struct TextIndex {
    embedder: Arc<dyn Embedder>,
    hnsw: Hnsw,
    inner: RwLock<Inner>,
    /// `Some` = durable. Every mutation writes to the WAL before
    /// applying. `None` = in-memory, legacy behaviour.
    wal: Option<Arc<Wal>>,
    /// Parent dir for snapshots. Always `Some` iff `wal` is `Some`.
    data_dir: Option<PathBuf>,
    /// Directory snapshots are written to / loaded from. `Some` iff
    /// `wal` is `Some`. Defaults to `data_dir/snapshots`, but is
    /// overridable via the `NEBULA_SNAPSHOT_DIR` env at construction
    /// so a leader/follower pair can share one snapshots volume
    /// (design 0007 §4). When unset, behavior is identical to the
    /// historical `data_dir/snapshots` layout.
    snapshot_dir: Option<PathBuf>,
}

impl TextIndex {
    pub fn new(embedder: Arc<dyn Embedder>, metric: Metric, config: HnswConfig) -> Result<Self> {
        let dim = embedder.dim();
        let hnsw = Hnsw::new(dim, metric, config)?;
        Ok(Self {
            embedder,
            hnsw,
            inner: RwLock::new(Inner {
                by_key: AHashMap::new(),
                docs: AHashMap::new(),
                parents: AHashMap::new(),
                bm25: Bm25Index::new(Bm25Params::default()),
                next_id: 1,
            }),
            wal: None,
            data_dir: None,
            snapshot_dir: None,
        })
    }

    /// Resolve the snapshots directory for a persistent index:
    /// `NEBULA_SNAPSHOT_DIR` if set (so a leader/follower pair can
    /// share one volume — design 0007 §4), else the historical
    /// `data_dir/snapshots`. The default keeps current behavior when
    /// the env is unset.
    fn resolve_snapshot_dir(data_dir: &Path) -> PathBuf {
        std::env::var("NEBULA_SNAPSHOT_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| data_dir.join("snapshots"))
    }

    /// Durable variant: opens (or creates) a WAL in `data_dir/wal`,
    /// loads the newest snapshot from `data_dir/snapshots` if any,
    /// and replays WAL records newer than the snapshot. The
    /// returned index is ready to serve traffic with exactly the
    /// pre-crash state.
    ///
    /// Replay is embedder-free — WAL records carry resolved
    /// vectors — so this works even if the embedder backend is
    /// down at boot.
    pub fn open_persistent(
        embedder: Arc<dyn Embedder>,
        metric: Metric,
        config: HnswConfig,
        data_dir: impl AsRef<Path>,
    ) -> Result<Self> {
        Self::open_persistent_in(embedder, metric, config, data_dir, None)
    }

    /// Like [`Self::open_persistent`] but with an explicit snapshots
    /// directory override. `snapshot_dir = None` resolves the dir the
    /// same way the default path does: `NEBULA_SNAPSHOT_DIR` if set,
    /// else `data_dir/snapshots`. Threading the dir in explicitly lets
    /// tests be hermetic without mutating the process-global env.
    pub fn open_persistent_in(
        embedder: Arc<dyn Embedder>,
        metric: Metric,
        config: HnswConfig,
        data_dir: impl AsRef<Path>,
        snapshot_dir: Option<PathBuf>,
    ) -> Result<Self> {
        let dim = embedder.dim();
        let data_dir = data_dir.as_ref().to_path_buf();
        let wal_dir = data_dir.join("wal");
        let snapshots_dir = snapshot_dir.unwrap_or_else(|| Self::resolve_snapshot_dir(&data_dir));
        std::fs::create_dir_all(&wal_dir).map_err(|e| IndexError::Core(NebulaError::Io(e)))?;
        std::fs::create_dir_all(&snapshots_dir).map_err(|e| IndexError::Core(NebulaError::Io(e)))?;

        // Load snapshot first, then replay only later WAL records.
        //
        // Migration fallback: if the configured snapshot dir (e.g. a newly
        // introduced shared NEBULA_SNAPSHOT_DIR — design 0007 §4) has no
        // snapshot yet, also look in the legacy per-node `data_dir/snapshots`.
        // Without this, switching to a shared dir while the local WAL has
        // ALREADY been compacted past the legacy snapshot's seq would
        // silently recover an INCOMPLETE index (replay starts at 0 but the
        // pre-snapshot segments are gone). The fallback lets a first boot on
        // the new dir find the old snapshot; the follower then republishes
        // into the shared dir and subsequent boots use it.
        let legacy_snapshots_dir = data_dir.join("snapshots");
        let loaded_snapshot = match durability::load_latest_snapshot(&snapshots_dir)? {
            Some(s) => Some(s),
            None if snapshots_dir != legacy_snapshots_dir => {
                let s = durability::load_latest_snapshot(&legacy_snapshots_dir)?;
                if s.is_some() {
                    tracing::warn!(
                        legacy = %legacy_snapshots_dir.display(),
                        configured = %snapshots_dir.display(),
                        "no snapshot in configured dir; recovering from legacy dir (one-time migration)"
                    );
                }
                s
            }
            None => None,
        };
        let (inner, hnsw, snapshot_wal_seq) =
            match loaded_snapshot {
                Some((header, hnsw_snap)) => {
                    if hnsw_snap.dim != dim {
                        return Err(IndexError::Invalid(format!(
                            "snapshot dim {} != embedder dim {}",
                            hnsw_snap.dim, dim
                        )));
                    }
                    let hnsw = Hnsw::restore_from_snapshot(hnsw_snap)?;
                    let inner = inner_from_serialized(header.docs);
                    (inner, hnsw, header.wal_seq_at_snapshot)
                }
                None => {
                    let hnsw = Hnsw::new(dim, metric, config.clone())?;
                    let inner = Inner {
                        by_key: AHashMap::new(),
                        docs: AHashMap::new(),
                        parents: AHashMap::new(),
                        bm25: Bm25Index::new(Bm25Params::default()),
                        next_id: 1,
                    };
                    // No snapshot yet: every WAL record is fresh.
                    // `u64::MAX` as "nothing superseded" would be
                    // confusing; 0 is clearer since WAL seqs start
                    // there.
                    (inner, hnsw, 0)
                }
            };

        let wal = Arc::new(Wal::open(&wal_dir, WalConfig::default()).map_err(durability::wal_err)?);

        let index = Self {
            embedder,
            hnsw,
            inner: RwLock::new(inner),
            wal: Some(Arc::clone(&wal)),
            data_dir: Some(data_dir),
            snapshot_dir: Some(snapshots_dir),
        };

        // Replay WAL records that postdate the snapshot. We only
        // care about records written AFTER the snapshot captured
        // its state — earlier ones are already reflected in the
        // loaded graph.
        //
        // In the common case (fresh boot, no snapshot) we replay
        // everything. `snapshot_wal_seq` = 0 means "no records
        // superseded yet"; WAL seq numbers the server cares about
        // are 1-based from here, so nothing is skipped.
        let records = wal.replay().map_err(durability::wal_err)?;
        let _ = snapshot_wal_seq; // currently we replay ALL records
                                  // in the WAL, relying on compact
                                  // to have pruned already-snapshotted
                                  // segments. See compact_wal.
        for rec in records {
            index.apply_wal_record(&rec)?;
        }
        Ok(index)
    }

    pub fn dim(&self) -> usize {
        self.embedder.dim()
    }

    /// Append a record to the WAL if persistence is on. No-op in
    /// the in-memory path. Errors propagate so a failed WAL write
    /// aborts the mutation — we never "partially commit" under
    /// durability.
    fn wal_append(&self, rec: &WalRecord) -> Result<()> {
        if let Some(wal) = &self.wal {
            wal.append(rec).map_err(durability::wal_err)?;
        }
        Ok(())
    }

    /// Handle to the underlying WAL when persistence is on. Exposed
    /// so replication (follower mode) can subscribe to the leader's
    /// WAL stream. Returns `None` for in-memory indexes — those
    /// can't be leaders.
    pub fn wal(&self) -> Option<Arc<Wal>> {
        self.wal.clone()
    }

    /// Apply a single WAL record to the in-memory state — shared
    /// between two callers:
    ///
    /// - crash recovery on boot (`open_persistent` replay), and
    /// - follower-mode replication, which receives records over
    ///   the wire from a leader and applies them locally.
    ///
    /// Deliberately doesn't call the embedder — vectors are carried
    /// in the record — and doesn't re-append to the WAL. The latter
    /// is important for followers: their writes come from the
    /// leader, never from local clients, so re-appending would be
    /// a bug (double-recording the record in the follower's own
    /// WAL would make split-brain recovery incoherent).
    pub fn apply_wal_record(&self, rec: &WalRecord) -> Result<()> {
        match rec {
            WalRecord::UpsertText {
                bucket,
                external_id,
                text,
                vector,
                metadata_json,
            } => {
                let metadata: serde_json::Value = serde_json::from_str(metadata_json)
                    .unwrap_or(serde_json::Value::Null);
                self.apply_upsert_single(bucket, external_id, text, vector.clone(), metadata, None, None)?;
            }
            WalRecord::UpsertDocument {
                bucket,
                doc_id,
                chunks,
                metadata_json,
            } => {
                let metadata: serde_json::Value = serde_json::from_str(metadata_json)
                    .unwrap_or(serde_json::Value::Null);
                self.apply_upsert_document(bucket, doc_id, chunks, metadata)?;
            }
            WalRecord::Delete { bucket, external_id } => {
                // delete() returns NotFound if the id's gone — that's
                // fine during replay (earlier record was superseded).
                let _ = self.delete_internal(bucket, external_id);
            }
            WalRecord::DeleteDocument { bucket, doc_id } => {
                let _ = self.delete_document_internal(bucket, doc_id);
            }
            WalRecord::EmptyBucket { bucket } => {
                self.empty_bucket_internal(bucket);
            }
        }
        Ok(())
    }

    pub fn embedder_model(&self) -> &str {
        self.embedder.model()
    }

    /// Borrow the embedder. Used by Raft mode (`nebula-raft`) so the
    /// leader can resolve text → vector before submitting the log
    /// entry — the resolved vector then replicates to peers, which
    /// is what makes the state-machine apply path embedder-free
    /// (every peer reaches byte-identical state without re-calling
    /// the embedder).
    pub fn embedder(&self) -> Arc<dyn Embedder> {
        Arc::clone(&self.embedder)
    }

    pub fn len(&self) -> usize {
        self.inner.read().docs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Insert or replace a text document. If the key already exists we
    /// tombstone the old node first; the HNSW itself does not support
    /// in-place updates, and re-inserting under the same external id is
    /// the safest way to get a fresh graph position after text changes.
    pub async fn upsert_text(
        &self,
        bucket: &str,
        external_id: &str,
        text: &str,
        metadata: serde_json::Value,
    ) -> Result<()> {
        if bucket.is_empty() || external_id.is_empty() {
            return Err(IndexError::Invalid("bucket and id must be non-empty".into()));
        }

        let vector = self.embedder.embed_one(text).await?;
        if vector.len() != self.dim() {
            return Err(IndexError::Embed(EmbedError::DimensionMismatch {
                expected: self.dim(),
                actual: vector.len(),
            }));
        }

        // WAL append + HNSW insert are fully synchronous and hold
        // the inner write lock. Run them on tokio's blocking-OK
        // window via `block_in_place` so a burst of writes from a
        // client app doesn't pin async workers (the wedge source
        // that took us down earlier today via RAG / Admin polling).
        // Requires multi-threaded runtime — main.rs builds the
        // server with one explicitly; tests use
        // `#[tokio::test(flavor = "multi_thread", ...)]` annotations.
        tokio::task::block_in_place(|| {
            // WAL first: if we crash between this append and the
            // in-memory apply below, replay picks up the record. If
            // we crash before the append, the caller's retry is safe
            // — nothing was committed.
            self.wal_append(&WalRecord::UpsertText {
                bucket: bucket.to_string(),
                external_id: external_id.to_string(),
                text: text.to_string(),
                vector: vector.clone(),
                metadata_json: metadata.to_string(),
            })?;

            self.apply_upsert_single(bucket, external_id, text, vector, metadata, None, None)
        })
    }

    /// Pure in-memory apply shared by `upsert_text` and WAL replay.
    /// Holds the inner write lock across the HNSW mutation so a
    /// concurrent reader never observes a half-committed write —
    /// see the earlier revision that had this race.
    ///
    /// Lock order across the whole crate: `inner` → `hnsw`.
    /// Readers (search_vector) do the same. Keeps us deadlock-free.
    #[allow(clippy::too_many_arguments)]
    fn apply_upsert_single(
        &self,
        bucket: &str,
        external_id: &str,
        text: &str,
        vector: Vec<f32>,
        metadata: serde_json::Value,
        parent_doc_id: Option<String>,
        chunk_index: Option<usize>,
    ) -> Result<()> {
        let key = (bucket.to_string(), external_id.to_string());
        let mut g = self.inner.write();

        if let Some(&old_id) = g.by_key.get(&key) {
            let _ = self.hnsw.delete(old_id);
            g.remove_doc(old_id);
            g.by_key.remove(&key);
        }

        // Probe insert with the candidate id; only commit `next_id`
        // after the HNSW accepts it. A failed insert (dimension
        // mismatch, duplicate id) used to leak the bumped counter
        // into the snapshot, sparsifying internal ids over time.
        let new_internal_id = Id(g.next_id);
        if let Err(e) = self.hnsw.insert(new_internal_id, &vector) {
            return Err(e.into());
        }
        g.next_id += 1;

        g.by_key.insert(key, new_internal_id);
        g.insert_doc(
            new_internal_id,
            Document {
                bucket: bucket.to_string(),
                external_id: external_id.to_string(),
                text: text.to_string(),
                vector,
                metadata,
                parent_doc_id,
                chunk_index,
            },
        );
        Ok(())
    }

    /// Batch variant of [`Self::upsert_text`]. A single embedder call
    /// amortizes the per-request overhead that dominates the remote
    /// provider cost — for `MockEmbedder` it's a micro-optimization,
    /// for OpenAI it's roughly 10-100x faster on 100-row batches.
    ///
    /// Replace semantics are preserved per item: any existing key is
    /// tombstoned before the new id is inserted. On HNSW failure for
    /// an individual item we roll back just that item, not the whole
    /// batch — partial success is preferable to a single bad text
    /// poisoning a whole ingest run.
    pub async fn upsert_text_bulk(
        &self,
        bucket: &str,
        items: &[(String, String, serde_json::Value)],
    ) -> Result<usize> {
        if bucket.is_empty() {
            return Err(IndexError::Invalid("bucket must be non-empty".into()));
        }
        if items.is_empty() {
            return Ok(0);
        }
        for (id, text, _) in items {
            if id.is_empty() {
                return Err(IndexError::Invalid("id must be non-empty".into()));
            }
            if text.is_empty() {
                return Err(IndexError::Invalid("text must be non-empty".into()));
            }
        }

        // One embedder call for the whole batch.
        let inputs: Vec<String> = items.iter().map(|(_, t, _)| t.clone()).collect();
        let vectors = self.embedder.embed(&inputs).await?;
        if vectors.len() != items.len() {
            return Err(IndexError::Embed(EmbedError::Decode(format!(
                "embedder returned {} vectors for {} inputs",
                vectors.len(),
                items.len()
            ))));
        }
        for v in &vectors {
            if v.len() != self.dim() {
                return Err(IndexError::Embed(EmbedError::DimensionMismatch {
                    expected: self.dim(),
                    actual: v.len(),
                }));
            }
        }

        // Bulk inserts hold the inner write lock across an entire
        // batch — at scale (a 1000-doc payload) that's hundreds of
        // ms of sync HNSW work. Move it to tokio's blocking-OK
        // window so async workers stay free to drive other futures.
        // Requires multi-threaded runtime — main.rs builds the
        // server with one explicitly; tests use the multi_thread
        // tokio test flavor.
        tokio::task::block_in_place(|| {
            // WAL: write every record before we start applying. The
            // WAL's writer is internally serialized so a batch lands
            // contiguously in one segment. If any append fails the
            // whole batch aborts — simpler than partial-commit
            // bookkeeping, and `append` doesn't typically fail except
            // on full disk.
            for ((id, text, meta), vec) in items.iter().zip(vectors.iter()) {
                self.wal_append(&WalRecord::UpsertText {
                    bucket: bucket.to_string(),
                    external_id: id.clone(),
                    text: text.clone(),
                    vector: vec.clone(),
                    metadata_json: meta.to_string(),
                })?;
            }

            // Same atomic-per-item discipline as the single-item path:
            // hold the inner write lock across each item's HNSW mutation
            // so readers never see a half-committed insert. Under bulk
            // load this also prevents two batches interleaving their
            // next_id allocations.
            //
            // Per-item failure handling: we keep going on HNSW failures
            // because a single bad vector shouldn't kill an ingest of
            // thousands. The lock is released at the end of the batch —
            // readers back up briefly at scale but never see torn state.
            let mut inserted = 0usize;
            let mut g = self.inner.write();
            for ((id, text, meta), vec) in items.iter().zip(vectors.iter()) {
                let key = (bucket.to_string(), id.clone());
                if let Some(&old_id) = g.by_key.get(&key) {
                    let _ = self.hnsw.delete(old_id);
                    g.remove_doc(old_id);
                    g.by_key.remove(&key);
                }
                // Bump `next_id` only after the HNSW accepts the
                // insert. Otherwise a failure here permanently burns
                // an internal id and the snapshot's `next_id` drifts
                // ahead of the actual document count.
                let new_id = Id(g.next_id);
                if self.hnsw.insert(new_id, vec).is_err() {
                    continue;
                }
                g.next_id += 1;
                g.by_key.insert(key, new_id);
                g.insert_doc(
                    new_id,
                    Document {
                        bucket: bucket.to_string(),
                        external_id: id.clone(),
                        text: text.clone(),
                        vector: vec.clone(),
                        metadata: meta.clone(),
                        parent_doc_id: None,
                        chunk_index: None,
                    },
                );
                inserted += 1;
            }
            Ok(inserted)
        })
    }

    pub fn get(&self, bucket: &str, external_id: &str) -> Option<Document> {
        let g = self.inner.read();
        let key = (bucket.to_string(), external_id.to_string());
        g.by_key.get(&key).and_then(|id| g.docs.get(id)).cloned()
    }

    pub fn delete(&self, bucket: &str, external_id: &str) -> Result<()> {
        self.wal_append(&WalRecord::Delete {
            bucket: bucket.to_string(),
            external_id: external_id.to_string(),
        })?;
        self.delete_internal(bucket, external_id)
    }

    /// In-memory apply shared by the public path and WAL replay.
    fn delete_internal(&self, bucket: &str, external_id: &str) -> Result<()> {
        let mut g = self.inner.write();
        let key = (bucket.to_string(), external_id.to_string());
        let id = g.by_key.remove(&key).ok_or_else(|| IndexError::DocNotFound {
            bucket: bucket.to_string(),
            id: external_id.to_string(),
        })?;
        if let Some(doc) = g.remove_doc(id) {
            if let Some(parent) = doc.parent_doc_id {
                if let Some(set) = g.parents.get_mut(&(bucket.to_string(), parent.clone())) {
                    set.remove(&doc.external_id);
                    if set.is_empty() {
                        g.parents.remove(&(bucket.to_string(), parent));
                    }
                }
            }
        }
        let _ = self.hnsw.delete(id);
        Ok(())
    }

    /// Chunk + embed + insert a whole document under `doc_id`.
    ///
    /// Each chunk lands at key `"{doc_id}#{i}"` in the same bucket.
    /// If `doc_id` already exists we atomically (from the caller's
    /// perspective) replace every chunk — no point-in-time where a
    /// search could mix old and new content.
    ///
    /// Embeddings are generated in a single batched call, which is a
    /// large win vs. one-call-per-chunk for remote providers. For
    /// `MockEmbedder` the difference is negligible.
    pub async fn upsert_document(
        &self,
        bucket: &str,
        doc_id: &str,
        text: &str,
        chunker: &dyn Chunker,
        metadata: serde_json::Value,
    ) -> Result<usize> {
        if bucket.is_empty() || doc_id.is_empty() {
            return Err(IndexError::Invalid(
                "bucket and doc_id must be non-empty".into(),
            ));
        }
        if text.trim().is_empty() {
            return Err(IndexError::Invalid("text must be non-empty".into()));
        }

        let chunks = chunker.chunk(text);
        if chunks.is_empty() {
            return Err(IndexError::Invalid("chunker produced no chunks".into()));
        }

        let inputs: Vec<String> = chunks.iter().map(|c| c.text.clone()).collect();
        let vectors = self.embedder.embed(&inputs).await?;
        if vectors.len() != chunks.len() {
            return Err(IndexError::Invalid(format!(
                "embedder returned {} vectors for {} chunks",
                vectors.len(),
                chunks.len()
            )));
        }
        for v in &vectors {
            if v.len() != self.dim() {
                return Err(IndexError::Embed(EmbedError::DimensionMismatch {
                    expected: self.dim(),
                    actual: v.len(),
                }));
            }
        }

        // WAL: one record per document (not per chunk) so replay
        // can't produce a half-indexed doc. Chunks + their vectors
        // are embedded directly in the record.
        let wal_chunks: Vec<WalChunk> = chunks
            .iter()
            .zip(vectors.iter())
            .map(|(c, v)| WalChunk {
                index: c.index,
                char_start: c.char_start,
                text: c.text.clone(),
                vector: v.clone(),
            })
            .collect();
        // Same blocking-window treatment as the other upsert paths
        // — WAL append + HNSW inserts for every chunk hold the inner
        // write lock through fully synchronous work. Bulk documents
        // pin a worker for noticeable time on large texts.
        let n = chunks.len();
        tokio::task::block_in_place(|| {
            self.wal_append(&WalRecord::UpsertDocument {
                bucket: bucket.to_string(),
                doc_id: doc_id.to_string(),
                chunks: wal_chunks.clone(),
                metadata_json: metadata.to_string(),
            })?;
            self.apply_upsert_document(bucket, doc_id, &wal_chunks, metadata)
        })?;
        Ok(n)
    }

    /// In-memory apply shared by public path and WAL replay. The
    /// public method validates input + calls the embedder first;
    /// this runs after those have produced resolved chunks.
    fn apply_upsert_document(
        &self,
        bucket: &str,
        doc_id: &str,
        chunks: &[WalChunk],
        metadata: serde_json::Value,
    ) -> Result<()> {
        let mut g = self.inner.write();
        let parent_key = (bucket.to_string(), doc_id.to_string());
        if let Some(existing) = g.parents.remove(&parent_key) {
            for external_id in existing {
                let key = (bucket.to_string(), external_id.clone());
                if let Some(id) = g.by_key.remove(&key) {
                    g.remove_doc(id);
                    let _ = self.hnsw.delete(id);
                }
            }
        }

        let mut parent_set: AHashSet<String> = AHashSet::with_capacity(chunks.len());
        for chunk in chunks {
            let external_id = format!("{doc_id}#{}", chunk.index);
            // Probe insert before bumping `next_id`. A mid-document
            // HNSW failure used to leak the failing chunk's id even
            // though no map state referenced it, drifting the snapshot
            // counter past the live document count.
            let id = Id(g.next_id);
            if let Err(e) = self.hnsw.insert(id, &chunk.vector) {
                for prev_external in &parent_set {
                    let pk = (bucket.to_string(), prev_external.clone());
                    if let Some(prev_id) = g.by_key.remove(&pk) {
                        g.remove_doc(prev_id);
                        let _ = self.hnsw.delete(prev_id);
                    }
                }
                return Err(IndexError::Core(e));
            }
            g.next_id += 1;
            g.by_key
                .insert((bucket.to_string(), external_id.clone()), id);
            g.insert_doc(
                id,
                Document {
                    bucket: bucket.to_string(),
                    external_id: external_id.clone(),
                    text: chunk.text.clone(),
                    vector: chunk.vector.clone(),
                    metadata: metadata.clone(),
                    parent_doc_id: Some(doc_id.to_string()),
                    chunk_index: Some(chunk.index),
                },
            );
            parent_set.insert(external_id);
        }
        g.parents.insert(parent_key, parent_set);
        Ok(())
    }

    /// Delete every chunk associated with `doc_id`. Returns the number
    /// of chunks removed, or an error if no such document exists.
    pub fn delete_document(&self, bucket: &str, doc_id: &str) -> Result<usize> {
        self.wal_append(&WalRecord::DeleteDocument {
            bucket: bucket.to_string(),
            doc_id: doc_id.to_string(),
        })?;
        self.delete_document_internal(bucket, doc_id)
    }

    /// Count the chunks currently registered under `(bucket, doc_id)`.
    /// Returns 0 if the document doesn't exist. Read-only — used by
    /// the Raft path's REST handler to populate the response's
    /// `chunks_removed` field at submit time, without taking a write
    /// lock.
    pub fn count_chunks(&self, bucket: &str, doc_id: &str) -> usize {
        let g = self.inner.read();
        g.parents
            .get(&(bucket.to_string(), doc_id.to_string()))
            .map(|s| s.len())
            .unwrap_or(0)
    }

    fn delete_document_internal(&self, bucket: &str, doc_id: &str) -> Result<usize> {
        let mut g = self.inner.write();
        let parent_key = (bucket.to_string(), doc_id.to_string());
        let Some(chunks) = g.parents.remove(&parent_key) else {
            return Err(IndexError::DocNotFound {
                bucket: bucket.to_string(),
                id: doc_id.to_string(),
            });
        };
        let n = chunks.len();
        for external_id in chunks {
            let key = (bucket.to_string(), external_id);
            if let Some(id) = g.by_key.remove(&key) {
                g.remove_doc(id);
                let _ = self.hnsw.delete(id);
            }
        }
        Ok(n)
    }

    /// Tombstone every document in a bucket. Returns how many were
    /// removed. Cheap "reset a bucket" for admins — the underlying
    /// HNSW nodes become tombstones, same as per-doc delete, so the
    /// vector index compacts on the next rebuild.
    pub fn empty_bucket(&self, bucket: &str) -> usize {
        // WAL append errors are rare (full disk) and would make
        // the mutation silently partial if we swallowed them. But
        // the public `empty_bucket` returns `usize`, not `Result`.
        // Keep that contract and log any WAL failure — the
        // in-memory state is still the source of truth until a
        // snapshot is taken. Callers that want strict durability
        // should use `empty_bucket_durable` (added below).
        if let Some(wal) = &self.wal {
            if let Err(e) = wal.append(&WalRecord::EmptyBucket {
                bucket: bucket.to_string(),
            }) {
                tracing::warn!(error = %e, bucket = %bucket, "wal append failed for empty_bucket");
            }
        }
        self.empty_bucket_internal(bucket)
    }

    fn empty_bucket_internal(&self, bucket: &str) -> usize {
        let mut g = self.inner.write();
        let victims: Vec<Id> = g
            .docs
            .iter()
            .filter(|(_, d)| d.bucket == bucket)
            .map(|(id, _)| *id)
            .collect();
        let n = victims.len();
        for id in &victims {
            if let Some(doc) = g.remove_doc(*id) {
                let key = (doc.bucket.clone(), doc.external_id.clone());
                g.by_key.remove(&key);
                if let Some(parent) = doc.parent_doc_id {
                    g.parents.remove(&(doc.bucket, parent));
                }
            }
            let _ = self.hnsw.delete(*id);
        }
        n
    }

    /// Snapshot every document in `bucket` as an `ExportedDoc`, with
    /// the vector pulled directly from HNSW so a receiving node can
    /// re-insert without calling the embedder again. Empty buckets
    /// return an empty Vec — we don't error on absence since bucket
    /// names are implicit.
    ///
    /// The result is a point-in-time snapshot: we hold the inner
    /// read lock for the whole scan, so concurrent writes are
    /// serialized behind the export. Export is primarily used by
    /// the operator's rebalance coordinator; at that point writes
    /// should already be routed elsewhere, so this contention
    /// trade-off is acceptable.
    pub fn export_bucket(&self, bucket: &str) -> Vec<ExportedDoc> {
        let g = self.inner.read();
        let mut out = Vec::new();
        for (id, doc) in g.docs.iter() {
            if doc.bucket != bucket {
                continue;
            }
            // Re-fetch the vector from the HNSW arena. The in-memory
            // `Document::vector` is populated on fresh writes but
            // empty after snapshot restore — exporting from the HNSW
            // is correct in both cases.
            let vector = self.hnsw.get_vector(*id).unwrap_or_default();
            out.push(ExportedDoc {
                external_id: doc.external_id.clone(),
                text: doc.text.clone(),
                metadata: doc.metadata.clone(),
                parent_doc_id: doc.parent_doc_id.clone(),
                chunk_index: doc.chunk_index,
                vector,
            });
        }
        out
    }

    /// Import a batch of already-embedded documents into `bucket`.
    /// Mirrors the semantics of `upsert_text` but skips the embedder
    /// call, which is what makes it safe for rebalance handoff
    /// (the vector bytes are authoritative — no drift from embedder
    /// version differences between source and target).
    ///
    /// WAL-appended in full: replay recreates the imported state.
    /// Vector dim is validated per item; mismatched vectors abort
    /// the whole batch to avoid a half-imported bucket.
    pub fn import_bucket(&self, bucket: &str, docs: &[ExportedDoc]) -> Result<usize> {
        if bucket.is_empty() {
            return Err(IndexError::Invalid("bucket must be non-empty".into()));
        }
        for d in docs {
            if d.vector.len() != self.dim() {
                return Err(IndexError::Embed(EmbedError::DimensionMismatch {
                    expected: self.dim(),
                    actual: d.vector.len(),
                }));
            }
            if d.external_id.is_empty() {
                return Err(IndexError::Invalid("exported doc has empty id".into()));
            }
        }
        let mut inserted = 0;
        for d in docs {
            // WAL record + in-memory apply, one document at a time.
            // A crash mid-import replays cleanly because each record
            // is self-contained.
            self.wal_append(&WalRecord::UpsertText {
                bucket: bucket.to_string(),
                external_id: d.external_id.clone(),
                text: d.text.clone(),
                vector: d.vector.clone(),
                metadata_json: d.metadata.to_string(),
            })?;
            self.apply_upsert_single(
                bucket,
                &d.external_id,
                &d.text,
                d.vector.clone(),
                d.metadata.clone(),
                d.parent_doc_id.clone(),
                d.chunk_index,
            )?;
            inserted += 1;
        }
        Ok(inserted)
    }

    /// One full-scan pass over the corpus. Cheap at demo scale
    /// (thousands of docs); at production scale we'd maintain these
    /// counters incrementally on the write path. Fine trade-off for
    /// the admin UI today — rebuilds happen on demand, not per
    /// request.
    /// Take a consistent snapshot and write it to `<data_dir>/snapshots`.
    /// Returns the path + the WAL seq at the time of the snapshot. A
    /// no-op and error if the index is in-memory (no `data_dir`).
    ///
    /// Streams directly from the live state into a zstd-compressed
    /// file — no intermediate `SerializedDocState` / `HnswSnapshot`
    /// clone. The earlier two-phase path (clone under read lock, then
    /// `bincode::serialize` each into a `Vec<u8>`, then write the
    /// vecs) tripled peak memory for the duration of every fire and
    /// took the 1 GiB-capped container OOM on each 15-minute tick
    /// — see RFC followups in commit history.
    ///
    /// Trade-off: the read lock is held for the *entire* write, not
    /// just the clone phase. Writers block for the duration. Readers
    /// continue (parking_lot RwLock allows concurrent readers). For
    /// a 200 MB compressed snapshot at zstd-3 + NVMe that's roughly
    /// 1–2 s of writer queueing per fire — well below the cost of
    /// the OOM kill loop we're replacing.
    /// Write a snapshot to an explicit directory with a caller-supplied
    /// `mark_seq`. Unlike [`Self::snapshot`], this variant does not
    /// require a WAL — Raft mode (`nebula-raft`) drives its own log
    /// and supplies the Raft `last_applied.index` as the marker.
    ///
    /// The on-disk format is identical to `Self::snapshot`. The
    /// `wal_seq_at_snapshot` field on the header is reused as a
    /// generic "covers up through this seq" marker — its semantics
    /// are correct for either driver, only the name is WAL-flavored.
    /// We don't rename the field because that would break load
    /// compatibility with snapshots produced by today's standalone
    /// path.
    pub fn snapshot_to(
        &self,
        snapshots_dir: impl AsRef<Path>,
        mark_seq: u64,
    ) -> Result<SnapshotOutcome> {
        let snapshots_dir = snapshots_dir.as_ref();
        let g = self.inner.read();
        let path = durability::write_snapshot_streaming(
            snapshots_dir,
            mark_seq,
            g.next_id,
            &g.docs,
            &g.parents,
            &self.hnsw,
        )?;
        drop(g);
        Ok(SnapshotOutcome {
            path,
            wal_seq_captured: mark_seq,
        })
    }

    pub fn snapshot(&self) -> Result<SnapshotOutcome> {
        let snapshots_dir = self
            .snapshot_dir
            .as_ref()
            .ok_or_else(|| IndexError::Invalid("index is in-memory; no data_dir".into()))?;
        let wal = self
            .wal
            .as_ref()
            .ok_or_else(|| IndexError::Invalid("index has no wal".into()))?;

        // Flush WAL so on-disk seq reflects every committed append.
        // Without this a recently appended record might not be in
        // any segment yet — recovery would think it was unseen.
        wal.flush().map_err(durability::wal_err)?;
        // Capture wal_seq BEFORE acquiring the read lock — matches
        // the pre-existing ordering. The write path appends to the
        // WAL *outside* `inner.write()`, so neither ordering is
        // race-free: capturing seq before the lock risks recovery
        // double-applying a record that landed in `inner` between
        // the seq read and the lock acquisition (idempotent for
        // upsert, the only worry is a stray duplicate-key error in
        // logs); capturing after the lock risks the opposite —
        // recovery *skipping* a record whose WAL append happened
        // between the lock and the seq read but whose `inner` apply
        // is blocked behind us, i.e. data loss. The former is the
        // preferable failure mode until WAL append + inner apply
        // are made atomic on the write path.
        let wal_seq =
            nebula_wal::current_seq(wal.dir()).map_err(durability::wal_err)?.unwrap_or(0);

        let g = self.inner.read();
        let path = durability::write_snapshot_streaming(
            snapshots_dir,
            wal_seq,
            g.next_id,
            &g.docs,
            &g.parents,
            &self.hnsw,
        )?;
        // Lock dropped here on `g` going out of scope.
        drop(g);

        Ok(SnapshotOutcome {
            path,
            wal_seq_captured: wal_seq,
        })
    }

    /// Delete WAL segments strictly older than the newest
    /// snapshot's captured seq. Safe to call any time; does
    /// nothing if no snapshot exists yet.
    pub fn compact_wal(&self) -> Result<usize> {
        self.compact_wal_inner(true)
    }

    /// Like [`Self::compact_wal`] but never prunes old snapshot files
    /// from the snapshots dir. Used by a leader in the offloaded
    /// snapshot mode (design 0007 §4): it compacts its WAL against a
    /// snapshot the *follower* published into the shared dir, but it
    /// does NOT own those files, so it must not delete them — only the
    /// snapshotter (follower / standalone) prunes.
    pub fn compact_wal_no_prune(&self) -> Result<usize> {
        self.compact_wal_inner(false)
    }

    fn compact_wal_inner(&self, prune: bool) -> Result<usize> {
        let (Some(snapshots_dir), Some(wal)) = (self.snapshot_dir.as_ref(), self.wal.as_ref())
        else {
            return Ok(0);
        };
        // Header-only read: compaction only needs `wal_seq_at_snapshot`.
        // Loading the FULL snapshot here (decompress + deserialize the
        // multi-GB arena) is catastrophic in LeaderCompactOnly mode, which
        // runs this every poll (~30s) — it would peg the leader on the
        // snapshot every cycle. `read_latest_snapshot_header` streams just
        // the header off the front of the file.
        let Some(header) = durability::read_latest_snapshot_header(snapshots_dir)? else {
            return Ok(0);
        };
        if prune {
            // Also prune older snapshots so we don't accumulate them
            // indefinitely. Only the owner of the snapshots dir does
            // this.
            let _ = durability::prune_old_snapshots(snapshots_dir)?;
        }
        let removed = wal
            .compact(header.wal_seq_at_snapshot)
            .map_err(durability::wal_err)?;
        Ok(removed)
    }

    /// Delete snapshot files older than the newest one in the
    /// configured snapshots dir, WITHOUT touching the WAL. Used by a
    /// follower in the offloaded snapshot mode (design 0007 §4): it
    /// owns the shared snapshots, but has no authoritative WAL to
    /// compact, so it prunes snapshots directly. Returns the number of
    /// files removed; no-op for in-memory indexes.
    pub fn prune_snapshots(&self) -> Result<usize> {
        let Some(snapshots_dir) = self.snapshot_dir.as_ref() else {
            return Ok(0);
        };
        durability::prune_old_snapshots(snapshots_dir)
    }

    /// Return WAL size / segment stats for the admin endpoint.
    /// Returns `None` if the index is in-memory only.
    pub fn wal_stats(&self) -> Option<WalStats> {
        self.wal.as_ref().and_then(|w| w.stats().ok())
    }

    /// WAL bytes that postdate the most recent committed snapshot's
    /// captured segment seq — i.e. data a cold recovery would have to
    /// replay. `None` for in-memory indexes (no WAL). `Some(total)`
    /// when persistent but no snapshot exists yet (nothing has been
    /// superseded, so all WAL bytes are "since the snapshot").
    ///
    /// Granularity is per-segment; see [`nebula_wal::Wal::bytes_since_seq`].
    pub fn wal_bytes_since_snapshot(&self) -> Option<u64> {
        let wal = self.wal.as_ref()?;
        let snapshot_seq = self
            .latest_snapshot_header()
            .map(|h| h.wal_seq_at_snapshot)
            .unwrap_or(0);
        wal.bytes_since_seq(snapshot_seq).ok()
    }

    /// Header of the newest committed snapshot (no HNSW body decode),
    /// or `None` if the index is in-memory or has no snapshot yet.
    /// Surfaced to `/metrics` for `nebula_snapshot_age_seconds`.
    pub fn latest_snapshot_header(&self) -> Option<durability::SnapshotHeader> {
        let snapshots_dir = self.snapshot_dir.as_ref()?;
        durability::read_latest_snapshot_header(snapshots_dir)
            .ok()
            .flatten()
    }

    /// Directory snapshots are written to / loaded from. `None` for
    /// in-memory indexes. Resolved at construction from
    /// `NEBULA_SNAPSHOT_DIR` (else `data_dir/snapshots`). The
    /// role-aware scheduler passes this to `snapshot_to`.
    pub fn snapshot_dir(&self) -> Option<&Path> {
        self.snapshot_dir.as_deref()
    }

    /// `true` if this index is durably persisting mutations.
    pub fn is_persistent(&self) -> bool {
        self.wal.is_some()
    }

    /// Absolute path to the data directory, if any. Used by the
    /// admin endpoint to surface "where is my data?" for ops.
    pub fn data_dir(&self) -> Option<&Path> {
        self.data_dir.as_deref()
    }

    pub fn bucket_stats(&self, top_metadata_keys: usize) -> Vec<BucketStats> {
        let g = self.inner.read();
        // Per-bucket accumulator. A struct is clearer than a 3-tuple
        // and silences clippy::type_complexity.
        struct Acc {
            docs: usize,
            parents: AHashSet<String>,
            keys: AHashMap<String, usize>,
        }
        let mut per_bucket: AHashMap<String, Acc> = AHashMap::new();
        for doc in g.docs.values() {
            let entry = per_bucket.entry(doc.bucket.clone()).or_insert_with(|| Acc {
                docs: 0,
                parents: AHashSet::new(),
                keys: AHashMap::new(),
            });
            entry.docs += 1;
            if let Some(parent) = &doc.parent_doc_id {
                entry.parents.insert(parent.clone());
            }
            // Count top-level metadata keys only. Nested JSON would
            // require a recursive walk; metadata is conventionally
            // flat, and a deep walk would skew the "popular key"
            // signal we're actually trying to expose.
            if let serde_json::Value::Object(map) = &doc.metadata {
                for k in map.keys() {
                    *entry.keys.entry(k.clone()).or_insert(0) += 1;
                }
            }
        }

        let mut out: Vec<BucketStats> = per_bucket
            .into_iter()
            .map(|(bucket, acc)| {
                let mut kv: Vec<(String, usize)> = acc.keys.into_iter().collect();
                // Sort by frequency desc, then key name for stable
                // output in the UI (no "columns jumping around").
                kv.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
                kv.truncate(top_metadata_keys);
                BucketStats {
                    bucket,
                    docs: acc.docs,
                    parent_docs: acc.parents.len(),
                    metadata_keys: kv,
                }
            })
            .collect();
        out.sort_by(|a, b| a.bucket.cmp(&b.bucket));
        out
    }

    /// Search by raw vector. `bucket` filters results *after* ANN, which
    /// is simpler than filtered-ANN but means you may need a larger
    /// `ef` to hit `k` hits when one bucket dominates the corpus. For
    /// NebulaDB scale that's an acceptable starting point; true
    /// pre-filtered HNSW is a future enhancement.
    pub fn search_vector(
        &self,
        vector: &[f32],
        bucket: Option<&str>,
        k: usize,
        ef: Option<usize>,
    ) -> Result<Vec<Hit>> {
        // Over-fetch when filtering because ANN results are post-filtered.
        // 4x is a rule-of-thumb; a real system would adapt based on the
        // bucket's share of the corpus.
        let fetch = if bucket.is_some() { k.saturating_mul(4).max(32) } else { k };

        // Lock order discipline: `inner` before `hnsw`, everywhere.
        // Writers take `inner.write()` then drive `hnsw` under it;
        // readers take `inner.read()` then `hnsw.search` under it.
        // Mixing the order would expose us to an AB-BA deadlock
        // under `parking_lot::RwLock`'s write-priority contention.
        let g = self.inner.read();
        let raw = self.hnsw.search(vector, fetch, ef)?;
        let mut hits = Vec::with_capacity(raw.len());
        for r in raw {
            let Some(doc) = g.docs.get(&r.id) else {
                continue; // tombstoned
            };
            if let Some(b) = bucket {
                if doc.bucket != b {
                    continue;
                }
            }
            hits.push(Hit {
                bucket: doc.bucket.clone(),
                id: doc.external_id.clone(),
                text: doc.text.clone(),
                score: r.distance,
                metadata: doc.metadata.clone(),
            });
            if hits.len() >= k {
                break;
            }
        }
        Ok(hits)
    }

    /// Search by text: embed the query, then delegate. The embed call is
    /// async because it may hit a remote provider; the actual index
    /// lookup is synchronous under a read lock.
    pub async fn search_text(
        &self,
        query: &str,
        bucket: Option<&str>,
        k: usize,
        ef: Option<usize>,
    ) -> Result<Vec<Hit>> {
        let qv = self.embedder.embed_one(query).await?;
        self.search_vector(&qv, bucket, k, ef)
    }

    /// Lexical-only (BM25) search over the chunk corpus. Synchronous —
    /// no embedder call. Returns `Hit`s with `score` set to the raw
    /// BM25 weight (larger = more relevant), which is the *opposite*
    /// sense to the vector `score` (a distance). Callers that mix the
    /// two must normalize; [`Self::search_hybrid`] does.
    pub fn search_bm25(&self, query: &str, bucket: Option<&str>, k: usize) -> Vec<Hit> {
        let g = self.inner.read();
        // Over-fetch when bucket-filtering, same rationale as the
        // vector path: BM25 ranks the whole corpus and we post-filter.
        let fetch = if bucket.is_some() { k.saturating_mul(4).max(32) } else { k };
        let raw = g.bm25.search(query, fetch);
        let mut hits = Vec::with_capacity(raw.len().min(k));
        for r in raw {
            let Some(doc) = g.docs.get(&Id(r.id)) else {
                continue; // tombstoned between search and assembly
            };
            if let Some(b) = bucket {
                if doc.bucket != b {
                    continue;
                }
            }
            hits.push(Hit {
                bucket: doc.bucket.clone(),
                id: doc.external_id.clone(),
                text: doc.text.clone(),
                score: r.score,
                metadata: doc.metadata.clone(),
            });
            if hits.len() >= k {
                break;
            }
        }
        hits
    }

    /// Hybrid retrieval: fuse dense (vector) and lexical (BM25) signals
    /// into one ranking (design 0008 §6). This is the quality
    /// differentiator over a vector-only store — dense recall for
    /// paraphrase, lexical precision for rare exact tokens.
    ///
    /// Fusion is a weighted sum of **min-max normalized** per-stage
    /// scores, so the two incomparable raw scales (cosine distance vs
    /// BM25 weight) are mapped onto a common `[0, 1]`-higher-is-better
    /// axis before combining:
    ///
    /// - vector: distance `d` → similarity `1/(1+d)`, then min-max
    ///   normalized across the candidate set.
    /// - BM25: raw weight, min-max normalized across the candidate set.
    ///
    /// `weights` are `(vector, bm25)` and need not sum to 1 — they're
    /// applied as-is. A doc found by only one stage contributes 0 from
    /// the other. The returned `Hit::score` is the fused score
    /// (larger = better).
    ///
    /// Each stage over-fetches `k` so a doc strong in one signal but
    /// outside the other's top-`k` still surfaces.
    pub fn search_vector_hybrid(
        &self,
        query_vector: &[f32],
        query_text: &str,
        bucket: Option<&str>,
        k: usize,
        ef: Option<usize>,
        weights: (f32, f32),
    ) -> Result<Vec<Hit>> {
        // Over-fetch each stage so the fusion set is the union of both
        // top-k's, not just their intersection.
        let stage_k = k.saturating_mul(4).max(16);
        let vec_hits = self.search_vector(query_vector, bucket, stage_k, ef)?;
        let bm_hits = self.search_bm25(query_text, bucket, stage_k);

        // Map vector distance → similarity so "higher = better" holds
        // for both stages before normalization.
        let vec_scored: Vec<(String, f32, Hit)> = vec_hits
            .into_iter()
            .map(|h| {
                let sim = 1.0 / (1.0 + h.score);
                (h.id.clone(), sim, h)
            })
            .collect();
        let bm_scored: Vec<(String, f32, Hit)> =
            bm_hits.into_iter().map(|h| (h.id.clone(), h.score, h)).collect();

        let vec_norm = min_max_normalize(vec_scored.iter().map(|(_, s, _)| *s));
        let bm_norm = min_max_normalize(bm_scored.iter().map(|(_, s, _)| *s));
        let (w_vec, w_bm) = weights;

        // Accumulate fused score per external id, keeping one `Hit`
        // representative (either stage carries the same doc fields).
        let mut fused: AHashMap<String, (f32, Hit)> = AHashMap::new();
        for ((id, _, hit), n) in vec_scored.into_iter().zip(vec_norm) {
            fused.entry(id).or_insert((0.0, hit)).0 += w_vec * n;
        }
        for ((id, _, hit), n) in bm_scored.into_iter().zip(bm_norm) {
            fused.entry(id).or_insert((0.0, hit)).0 += w_bm * n;
        }

        let mut out: Vec<Hit> = fused
            .into_iter()
            .map(|(_, (score, mut hit))| {
                hit.score = score;
                hit
            })
            .collect();
        // Descending fused score; tie-break on id for determinism.
        out.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.id.cmp(&b.id))
        });
        out.truncate(k);
        Ok(out)
    }

    /// Text-in hybrid search: embed the query, then fuse with BM25 over
    /// the same query string. The embedder call is the only async part.
    pub async fn search_text_hybrid(
        &self,
        query: &str,
        bucket: Option<&str>,
        k: usize,
        ef: Option<usize>,
        weights: (f32, f32),
    ) -> Result<Vec<Hit>> {
        let qv = self.embedder.embed_one(query).await?;
        self.search_vector_hybrid(&qv, query, bucket, k, ef, weights)
    }

    /// Async wrapper around [`search_text_hybrid`] with the same
    /// blocking-pool hand-off as [`search_text_blocking`]: the embed
    /// step runs on the async worker, the synchronous fusion + index
    /// lookups move to the blocking pool so they don't pin a runtime
    /// worker under load.
    pub async fn search_text_hybrid_blocking(
        self: std::sync::Arc<Self>,
        query: String,
        bucket: Option<String>,
        k: usize,
        ef: Option<usize>,
        weights: (f32, f32),
    ) -> Result<Vec<Hit>> {
        let qv = self.embedder.embed_one(&query).await?;
        tokio::task::spawn_blocking(move || {
            self.search_vector_hybrid(&qv, &query, bucket.as_deref(), k, ef, weights)
        })
        .await
        .map_err(|e| IndexError::Invalid(format!("blocking hybrid search task failed: {e}")))?
    }

    /// Async wrapper around [`search_vector`] that runs the
    /// synchronous HNSW work on tokio's blocking pool.
    ///
    /// Why this exists: [`search_vector`] takes a `parking_lot::RwLock`
    /// read and does HNSW traversal under it — both fully synchronous.
    /// Calling it directly inside an async future pins a tokio worker
    /// for the duration of the search. With a small worker pool
    /// (e.g. when `available_parallelism` returns a low number under
    /// a CPU cgroup cap), a handful of concurrent searches saturate
    /// the runtime; new requests including `/healthz` queue with no
    /// worker free, and the docker healthcheck flips the container
    /// to unhealthy. Moving the work to the dedicated blocking pool
    /// fixes that — tokio's async workers stay free to drive other
    /// futures.
    pub async fn search_vector_blocking(
        self: std::sync::Arc<Self>,
        vector: Vec<f32>,
        bucket: Option<String>,
        k: usize,
        ef: Option<usize>,
    ) -> Result<Vec<Hit>> {
        tokio::task::spawn_blocking(move || {
            self.search_vector(&vector, bucket.as_deref(), k, ef)
        })
        .await
        .map_err(|e| IndexError::Invalid(format!("blocking search task failed: {e}")))?
    }

    /// Async wrapper around [`search_text`] with the same blocking-pool
    /// hand-off as [`search_vector_blocking`]. The embed step still
    /// runs on the async worker — it's network-bound and yields at
    /// every `.await` — so only the synchronous index lookup moves.
    pub async fn search_text_blocking(
        self: std::sync::Arc<Self>,
        query: String,
        bucket: Option<String>,
        k: usize,
        ef: Option<usize>,
    ) -> Result<Vec<Hit>> {
        let qv = self.embedder.embed_one(&query).await?;
        self.search_vector_blocking(qv, bucket, k, ef).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nebula_embed::MockEmbedder;

    fn make_index() -> TextIndex {
        let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(64));
        TextIndex::new(emb, Metric::Cosine, HnswConfig::default()).unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn upsert_and_get() {
        let idx = make_index();
        idx.upsert_text("docs", "a", "hello world", serde_json::json!({}))
            .await
            .unwrap();
        let d = idx.get("docs", "a").unwrap();
        assert_eq!(d.text, "hello world");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn replace_drops_old_vector() {
        let idx = make_index();
        idx.upsert_text("docs", "a", "v1", serde_json::json!({})).await.unwrap();
        idx.upsert_text("docs", "a", "v2", serde_json::json!({})).await.unwrap();
        assert_eq!(idx.len(), 1);
        assert_eq!(idx.get("docs", "a").unwrap().text, "v2");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bucket_filter_excludes_other_buckets() {
        let idx = make_index();
        idx.upsert_text("a", "1", "zero trust", serde_json::json!({})).await.unwrap();
        idx.upsert_text("b", "1", "zero trust", serde_json::json!({})).await.unwrap();
        let hits = idx.search_text("zero trust", Some("a"), 5, None).await.unwrap();
        assert!(hits.iter().all(|h| h.bucket == "a"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_removes_from_results() {
        let idx = make_index();
        idx.upsert_text("docs", "a", "foo", serde_json::json!({})).await.unwrap();
        idx.upsert_text("docs", "b", "bar", serde_json::json!({})).await.unwrap();
        idx.delete("docs", "a").unwrap();
        let hits = idx.search_text("foo", None, 10, None).await.unwrap();
        assert!(hits.iter().all(|h| h.id != "a"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn empty_bucket_or_id_rejected() {
        let idx = make_index();
        assert!(idx.upsert_text("", "a", "x", serde_json::json!({})).await.is_err());
        assert!(idx.upsert_text("b", "", "x", serde_json::json!({})).await.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn upsert_document_creates_multiple_chunks() {
        let idx = make_index();
        let chunker = nebula_chunk::FixedSizeChunker::new(10, 0).unwrap();
        let text = "abcdefghij".repeat(5); // 50 chars → 5 chunks at size 10
        let n = idx
            .upsert_document("docs", "d1", &text, &chunker, serde_json::json!({"src": "unit"}))
            .await
            .unwrap();
        assert_eq!(n, 5);
        assert_eq!(idx.len(), 5);
        // Every chunk keyed `d1#0`..`d1#4`.
        for i in 0..5 {
            let d = idx.get("docs", &format!("d1#{i}")).unwrap();
            assert_eq!(d.parent_doc_id.as_deref(), Some("d1"));
            assert_eq!(d.chunk_index, Some(i));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn upsert_document_replaces_previous_chunks() {
        let idx = make_index();
        let chunker = nebula_chunk::FixedSizeChunker::new(5, 0).unwrap();
        idx.upsert_document("docs", "d1", "aaaaabbbbb", &chunker, serde_json::json!({}))
            .await
            .unwrap();
        assert_eq!(idx.len(), 2);
        // Shorter replacement — 1 chunk.
        idx.upsert_document("docs", "d1", "xxxx", &chunker, serde_json::json!({}))
            .await
            .unwrap();
        assert_eq!(idx.len(), 1);
        assert!(idx.get("docs", "d1#0").is_some());
        assert!(idx.get("docs", "d1#1").is_none()); // from first version
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn count_chunks_reports_zero_for_unknown_and_n_for_known() {
        let idx = make_index();
        assert_eq!(idx.count_chunks("docs", "missing"), 0);
        let chunker = nebula_chunk::FixedSizeChunker::new(5, 0).unwrap();
        idx.upsert_document("docs", "d1", "aaaaabbbbbccccc", &chunker, serde_json::json!({}))
            .await
            .unwrap();
        assert_eq!(idx.count_chunks("docs", "d1"), 3);
        assert_eq!(idx.count_chunks("other-bucket", "d1"), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_document_removes_all_chunks() {
        let idx = make_index();
        let chunker = nebula_chunk::FixedSizeChunker::new(5, 0).unwrap();
        idx.upsert_document("docs", "d1", "aaaaabbbbbccccc", &chunker, serde_json::json!({}))
            .await
            .unwrap();
        idx.upsert_document("docs", "d2", "zzzzz", &chunker, serde_json::json!({}))
            .await
            .unwrap();
        let removed = idx.delete_document("docs", "d1").unwrap();
        assert_eq!(removed, 3);
        assert_eq!(idx.len(), 1);
        assert!(idx.get("docs", "d2#0").is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_document_unknown_errors() {
        let idx = make_index();
        assert!(idx.delete_document("docs", "nope").is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bucket_stats_reports_counts_and_top_keys() {
        let idx = make_index();
        // Two buckets with different metadata shapes; parent_doc_id on
        // some docs so `parent_docs` differs from `docs`.
        idx.upsert_text("a", "1", "x", serde_json::json!({"region": "eu", "lang": "en"}))
            .await
            .unwrap();
        idx.upsert_text("a", "2", "x", serde_json::json!({"region": "us"}))
            .await
            .unwrap();
        idx.upsert_text("b", "1", "x", serde_json::json!({"team": "platform"}))
            .await
            .unwrap();
        let chunker = nebula_chunk::FixedSizeChunker::new(5, 0).unwrap();
        idx.upsert_document("a", "doc1", "aaaaabbbbbccccc", &chunker, serde_json::json!({}))
            .await
            .unwrap();

        let stats = idx.bucket_stats(10);
        assert_eq!(stats.len(), 2, "two buckets");
        let a = stats.iter().find(|s| s.bucket == "a").unwrap();
        assert_eq!(a.docs, 2 + 3, "2 plain + 3 chunks");
        assert_eq!(a.parent_docs, 1, "doc1 is the one parent");
        // `region` appears in both plain docs, `lang` in only one.
        let keys: Vec<&str> = a.metadata_keys.iter().map(|(k, _)| k.as_str()).collect();
        assert!(keys.contains(&"region"));

        let b = stats.iter().find(|s| s.bucket == "b").unwrap();
        assert_eq!(b.docs, 1);
        assert_eq!(b.parent_docs, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bucket_stats_empty_index_is_empty() {
        let idx = make_index();
        assert!(idx.bucket_stats(10).is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn upsert_text_bulk_inserts_and_replaces() {
        let idx = make_index();
        let batch: Vec<(String, String, serde_json::Value)> = (0..50)
            .map(|i| (format!("d{i}"), format!("text {i}"), serde_json::json!({"i": i})))
            .collect();
        let n = idx.upsert_text_bulk("docs", &batch).await.unwrap();
        assert_eq!(n, 50);
        assert_eq!(idx.len(), 50);
        // Replace-semantics on the second pass: same ids, different
        // text. Doc count stays 50, but the text is the updated one.
        let batch2: Vec<(String, String, serde_json::Value)> = (0..50)
            .map(|i| (format!("d{i}"), format!("updated {i}"), serde_json::json!({})))
            .collect();
        let n2 = idx.upsert_text_bulk("docs", &batch2).await.unwrap();
        assert_eq!(n2, 50);
        assert_eq!(idx.len(), 50);
        assert_eq!(idx.get("docs", "d0").unwrap().text, "updated 0");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn upsert_text_bulk_rejects_empty_id_or_text() {
        let idx = make_index();
        let err = idx
            .upsert_text_bulk("docs", &[("".into(), "x".into(), serde_json::json!({}))])
            .await
            .unwrap_err();
        assert!(matches!(err, IndexError::Invalid(_)));
        let err = idx
            .upsert_text_bulk("docs", &[("a".into(), "".into(), serde_json::json!({}))])
            .await
            .unwrap_err();
        assert!(matches!(err, IndexError::Invalid(_)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn empty_bucket_drops_only_that_bucket() {
        let idx = make_index();
        idx.upsert_text("a", "1", "x", serde_json::json!({})).await.unwrap();
        idx.upsert_text("a", "2", "x", serde_json::json!({})).await.unwrap();
        idx.upsert_text("b", "1", "x", serde_json::json!({})).await.unwrap();
        let removed = idx.empty_bucket("a");
        assert_eq!(removed, 2);
        assert!(idx.get("a", "1").is_none());
        assert!(idx.get("a", "2").is_none());
        assert!(idx.get("b", "1").is_some(), "b must be untouched");
        assert_eq!(idx.empty_bucket("nonexistent"), 0);
    }

    fn persistent_index(data_dir: &Path, snapshot_dir: Option<PathBuf>) -> TextIndex {
        let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(64));
        TextIndex::open_persistent_in(
            emb,
            Metric::Cosine,
            HnswConfig::default(),
            data_dir,
            snapshot_dir,
        )
        .unwrap()
    }

    /// `snapshot_to` stamps the header with the EXACT caller-supplied
    /// mark_seq — the primitive a follower relies on to mark a snapshot
    /// with the leader-WAL seq it has applied (design 0007 §4).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn snapshot_to_stamps_explicit_mark_seq() {
        let data = tempfile::tempdir().unwrap();
        let snaps = data.path().join("snapshots");
        let idx = persistent_index(data.path(), Some(snaps.clone()));
        idx.upsert_text("docs", "a", "hello", serde_json::json!({}))
            .await
            .unwrap();

        let mark = 4242;
        let out = idx.snapshot_to(&snaps, mark).unwrap();
        assert_eq!(out.wal_seq_captured, mark);

        // Round-trip the header off disk.
        let header = durability::read_latest_snapshot_header(&snaps)
            .unwrap()
            .expect("snapshot header present");
        assert_eq!(header.wal_seq_at_snapshot, mark);
    }

    /// An explicit snapshot_dir override writes/reads snapshots there
    /// (not in `data_dir/snapshots`), and recovery loads from it. This
    /// threads the dir through construction so the test is hermetic —
    /// no process-global env mutation (design 0007 §4 task 1).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn snapshot_dir_override_writes_reads_and_recovers() {
        let data = tempfile::tempdir().unwrap();
        let alt = tempfile::tempdir().unwrap();
        let alt_snaps = alt.path().to_path_buf();

        let idx = persistent_index(data.path(), Some(alt_snaps.clone()));
        assert_eq!(idx.snapshot_dir(), Some(alt_snaps.as_path()));
        idx.upsert_text("docs", "a", "persisted", serde_json::json!({}))
            .await
            .unwrap();
        let out = idx.snapshot().unwrap();
        // Snapshot landed in the override dir, NOT data_dir/snapshots.
        assert!(out.path.starts_with(&alt_snaps));
        let default_dir = data.path().join("snapshots");
        let default_has_snap = std::fs::read_dir(&default_dir)
            .map(|rd| rd.filter_map(|e| e.ok()).any(|e| {
                e.file_name()
                    .to_string_lossy()
                    .ends_with(".nsnap.ok")
            }))
            .unwrap_or(false);
        assert!(!default_has_snap, "no snapshot should land in the default dir");

        drop(idx);
        // Recovery from the same override dir restores the document.
        let recovered = persistent_index(data.path(), Some(alt_snaps.clone()));
        assert_eq!(recovered.get("docs", "a").unwrap().text, "persisted");
        let header = recovered.latest_snapshot_header().expect("header via override dir");
        assert_eq!(header.wal_seq_at_snapshot, out.wal_seq_captured);
    }

    // ---------- hybrid retrieval (design 0008 §6) ----------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bm25_search_finds_exact_token() {
        let idx = make_index();
        for (id, text) in [
            ("1", "the quarterly financial report summary"),
            ("2", "kubernetes pod autoscaling guide"),
            ("3", "general onboarding documentation"),
        ] {
            idx.upsert_text("docs", id, text, serde_json::json!({}))
                .await
                .unwrap();
        }
        let hits = idx.search_bm25("kubernetes", Some("docs"), 5);
        assert!(!hits.is_empty());
        assert_eq!(hits[0].id, "2");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bm25_respects_bucket_filter() {
        let idx = make_index();
        idx.upsert_text("a", "1", "shared keyword here", serde_json::json!({}))
            .await
            .unwrap();
        idx.upsert_text("b", "1", "shared keyword here", serde_json::json!({}))
            .await
            .unwrap();
        let hits = idx.search_bm25("keyword", Some("a"), 5);
        assert!(hits.iter().all(|h| h.bucket == "a"));
        assert_eq!(hits.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hybrid_search_returns_fused_ranking() {
        let idx = make_index();
        for (id, text) in [
            ("1", "alpha beta gamma"),
            ("2", "delta epsilon zeta"),
            ("3", "eta theta iota"),
        ] {
            idx.upsert_text("docs", id, text, serde_json::json!({}))
                .await
                .unwrap();
        }
        // Equal weights; the doc whose text matches the query lexically
        // must be present and scored highest under fusion.
        let hits = idx
            .search_text_hybrid("alpha beta", Some("docs"), 3, None, (0.5, 0.5))
            .await
            .unwrap();
        assert!(!hits.is_empty());
        assert_eq!(hits[0].id, "1");
        // Fused scores are descending.
        for w in hits.windows(2) {
            assert!(w[0].score >= w[1].score);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hybrid_bm25_weight_surfaces_rare_token() {
        // With the MockEmbedder the vector signal is content-agnostic
        // noise, so a pure-lexical weighting must still pull the doc
        // containing the rare exact token to the top — the property a
        // vector-only store cannot guarantee.
        let idx = make_index();
        for (id, text) in [
            ("1", "common filler words everywhere here today"),
            ("2", "common filler words plus errcode_8842 token"),
            ("3", "common filler words and nothing special"),
        ] {
            idx.upsert_text("docs", id, text, serde_json::json!({}))
                .await
                .unwrap();
        }
        let hits = idx
            .search_text_hybrid("errcode_8842", Some("docs"), 3, None, (0.0, 1.0))
            .await
            .unwrap();
        assert_eq!(hits[0].id, "2");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bm25_stays_in_sync_through_delete() {
        let idx = make_index();
        idx.upsert_text("docs", "1", "uniquetoken alpha", serde_json::json!({}))
            .await
            .unwrap();
        assert_eq!(idx.search_bm25("uniquetoken", Some("docs"), 5).len(), 1);
        idx.delete("docs", "1").unwrap();
        assert!(
            idx.search_bm25("uniquetoken", Some("docs"), 5).is_empty(),
            "BM25 must drop the doc when the vector index does"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bm25_rebuilds_from_snapshot_on_recovery() {
        let data = tempfile::tempdir().unwrap();
        let snaps = data.path().join("snapshots");
        let idx = persistent_index(data.path(), Some(snaps.clone()));
        idx.upsert_text("docs", "1", "recoverable bm25 token zeta", serde_json::json!({}))
            .await
            .unwrap();
        idx.snapshot().unwrap();
        drop(idx);

        // The BM25 index isn't serialized — recovery must rebuild it
        // from the restored doc text so lexical search works post-crash.
        let recovered = persistent_index(data.path(), Some(snaps.clone()));
        let hits = recovered.search_bm25("zeta", Some("docs"), 5);
        assert_eq!(hits.len(), 1, "BM25 should be rebuilt from the snapshot");
        assert_eq!(hits[0].id, "1");
    }

    #[test]
    fn min_max_normalize_handles_degenerate_inputs() {
        assert!(min_max_normalize(std::iter::empty()).is_empty());
        // Single value → 1.0 (maximally relevant within its stage).
        assert_eq!(min_max_normalize([5.0].into_iter()), vec![1.0]);
        // All-equal → all 1.0, not a stage-erasing flat zero.
        assert_eq!(min_max_normalize([3.0, 3.0, 3.0].into_iter()), vec![1.0, 1.0, 1.0]);
        // Spread → endpoints at 0 and 1.
        let n = min_max_normalize([0.0, 5.0, 10.0].into_iter());
        assert_eq!(n, vec![0.0, 0.5, 1.0]);
    }
}
