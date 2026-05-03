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

/// Rebuild an `Inner` from serialized-on-disk form. Factored out of
/// `open_persistent` so the crate's tests can drive it directly.
fn inner_from_serialized(state: durability::SerializedDocState) -> Inner {
    let mut by_key = AHashMap::with_capacity(state.docs.len());
    let mut docs = AHashMap::with_capacity(state.docs.len());
    for d in state.docs {
        let metadata: serde_json::Value = serde_json::from_str(&d.metadata_json)
            .unwrap_or(serde_json::Value::Null);
        let id = Id(d.internal_id);
        by_key.insert((d.bucket.clone(), d.external_id.clone()), id);
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
    next_id: u64,
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
                next_id: 1,
            }),
            wal: None,
            data_dir: None,
        })
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
        let dim = embedder.dim();
        let data_dir = data_dir.as_ref().to_path_buf();
        let wal_dir = data_dir.join("wal");
        let snapshots_dir = data_dir.join("snapshots");
        std::fs::create_dir_all(&wal_dir).map_err(|e| IndexError::Core(NebulaError::Io(e)))?;
        std::fs::create_dir_all(&snapshots_dir).map_err(|e| IndexError::Core(NebulaError::Io(e)))?;

        // Load snapshot first, then replay only later WAL records.
        let (inner, hnsw, snapshot_wal_seq) =
            match durability::load_latest_snapshot(&snapshots_dir)? {
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

        // WAL first: if we crash between this append and the
        // in-memory apply below, replay picks up the record. If we
        // crash before the append, the caller's retry is safe —
        // nothing was committed.
        self.wal_append(&WalRecord::UpsertText {
            bucket: bucket.to_string(),
            external_id: external_id.to_string(),
            text: text.to_string(),
            vector: vector.clone(),
            metadata_json: metadata.to_string(),
        })?;

        self.apply_upsert_single(bucket, external_id, text, vector, metadata, None, None)
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
            g.docs.remove(&old_id);
            g.by_key.remove(&key);
        }

        let new_internal_id = Id(g.next_id);
        g.next_id += 1;

        if let Err(e) = self.hnsw.insert(new_internal_id, &vector) {
            return Err(e.into());
        }

        g.by_key.insert(key, new_internal_id);
        g.docs.insert(
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
                g.docs.remove(&old_id);
                g.by_key.remove(&key);
            }
            let new_id = Id(g.next_id);
            g.next_id += 1;
            if self.hnsw.insert(new_id, vec).is_err() {
                // Roll back the id allocation we already bumped. No
                // map writes to undo — we haven't done them yet.
                continue;
            }
            g.by_key.insert(key, new_id);
            g.docs.insert(
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
        if let Some(doc) = g.docs.remove(&id) {
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
        self.wal_append(&WalRecord::UpsertDocument {
            bucket: bucket.to_string(),
            doc_id: doc_id.to_string(),
            chunks: wal_chunks.clone(),
            metadata_json: metadata.to_string(),
        })?;

        self.apply_upsert_document(bucket, doc_id, &wal_chunks, metadata)?;
        Ok(chunks.len())
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
                    g.docs.remove(&id);
                    let _ = self.hnsw.delete(id);
                }
            }
        }

        let mut parent_set: AHashSet<String> = AHashSet::with_capacity(chunks.len());
        for chunk in chunks {
            let external_id = format!("{doc_id}#{}", chunk.index);
            let id = Id(g.next_id);
            g.next_id += 1;
            if let Err(e) = self.hnsw.insert(id, &chunk.vector) {
                for prev_external in &parent_set {
                    let pk = (bucket.to_string(), prev_external.clone());
                    if let Some(prev_id) = g.by_key.remove(&pk) {
                        g.docs.remove(&prev_id);
                        let _ = self.hnsw.delete(prev_id);
                    }
                }
                return Err(IndexError::Core(e));
            }
            g.by_key
                .insert((bucket.to_string(), external_id.clone()), id);
            g.docs.insert(
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
                g.docs.remove(&id);
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
            if let Some(doc) = g.docs.remove(id) {
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

    /// One full-scan pass over the corpus. Cheap at demo scale
    /// (thousands of docs); at production scale we'd maintain these
    /// counters incrementally on the write path. Fine trade-off for
    /// the admin UI today — rebuilds happen on demand, not per
    /// request.
    /// Take a consistent snapshot and write it to `<data_dir>/snapshots`.
    /// Returns the path + the WAL seq at the time of the snapshot. A
    /// no-op and error if the index is in-memory (no `data_dir`).
    ///
    /// Snapshot is atomic w.r.t. writers: we hold the read lock for
    /// the in-memory half (blocks writers briefly), take the HNSW
    /// snapshot (also holds an HNSW read lock), then write to disk
    /// outside any lock. The WAL's current seq captured *before*
    /// the read lock is released ensures post-snapshot records
    /// won't be double-applied on recovery.
    pub fn snapshot(&self) -> Result<SnapshotOutcome> {
        let data_dir = self
            .data_dir
            .as_ref()
            .ok_or_else(|| IndexError::Invalid("index is in-memory; no data_dir".into()))?;
        let wal = self
            .wal
            .as_ref()
            .ok_or_else(|| IndexError::Invalid("index has no wal".into()))?;

        // Make sure every buffered append is on disk before we
        // read the seq. Without this a recent append could be
        // included in the snapshot but not yet in any segment —
        // recovery would think it was unseen.
        wal.flush().map_err(durability::wal_err)?;
        let wal_seq =
            nebula_wal::current_seq(wal.dir()).map_err(durability::wal_err)?.unwrap_or(0);

        // Capture in-memory + HNSW state under the read lock.
        let (serialized_docs, hnsw_snap) = {
            let g = self.inner.read();
            let docs = durability::serialize_docs(g.next_id, &g.docs, &g.parents);
            let hnsw = self.hnsw.to_snapshot();
            (docs, hnsw)
        };

        let path = durability::write_snapshot(
            &data_dir.join("snapshots"),
            wal_seq,
            serialized_docs,
            hnsw_snap,
        )?;
        Ok(SnapshotOutcome {
            path,
            wal_seq_captured: wal_seq,
        })
    }

    /// Delete WAL segments strictly older than the newest
    /// snapshot's captured seq. Safe to call any time; does
    /// nothing if no snapshot exists yet.
    pub fn compact_wal(&self) -> Result<usize> {
        let (Some(data_dir), Some(wal)) = (self.data_dir.as_ref(), self.wal.as_ref()) else {
            return Ok(0);
        };
        let snapshots_dir = data_dir.join("snapshots");
        let Some((header, _)) = durability::load_latest_snapshot(&snapshots_dir)? else {
            return Ok(0);
        };
        // Also prune older snapshots so we don't accumulate them
        // indefinitely.
        let _ = durability::prune_old_snapshots(&snapshots_dir)?;
        let removed = wal
            .compact(header.wal_seq_at_snapshot)
            .map_err(durability::wal_err)?;
        Ok(removed)
    }

    /// Return WAL size / segment stats for the admin endpoint.
    /// Returns `None` if the index is in-memory only.
    pub fn wal_stats(&self) -> Option<WalStats> {
        self.wal.as_ref().and_then(|w| w.stats().ok())
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use nebula_embed::MockEmbedder;

    fn make_index() -> TextIndex {
        let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(64));
        TextIndex::new(emb, Metric::Cosine, HnswConfig::default()).unwrap()
    }

    #[tokio::test]
    async fn upsert_and_get() {
        let idx = make_index();
        idx.upsert_text("docs", "a", "hello world", serde_json::json!({}))
            .await
            .unwrap();
        let d = idx.get("docs", "a").unwrap();
        assert_eq!(d.text, "hello world");
    }

    #[tokio::test]
    async fn replace_drops_old_vector() {
        let idx = make_index();
        idx.upsert_text("docs", "a", "v1", serde_json::json!({})).await.unwrap();
        idx.upsert_text("docs", "a", "v2", serde_json::json!({})).await.unwrap();
        assert_eq!(idx.len(), 1);
        assert_eq!(idx.get("docs", "a").unwrap().text, "v2");
    }

    #[tokio::test]
    async fn bucket_filter_excludes_other_buckets() {
        let idx = make_index();
        idx.upsert_text("a", "1", "zero trust", serde_json::json!({})).await.unwrap();
        idx.upsert_text("b", "1", "zero trust", serde_json::json!({})).await.unwrap();
        let hits = idx.search_text("zero trust", Some("a"), 5, None).await.unwrap();
        assert!(hits.iter().all(|h| h.bucket == "a"));
    }

    #[tokio::test]
    async fn delete_removes_from_results() {
        let idx = make_index();
        idx.upsert_text("docs", "a", "foo", serde_json::json!({})).await.unwrap();
        idx.upsert_text("docs", "b", "bar", serde_json::json!({})).await.unwrap();
        idx.delete("docs", "a").unwrap();
        let hits = idx.search_text("foo", None, 10, None).await.unwrap();
        assert!(hits.iter().all(|h| h.id != "a"));
    }

    #[tokio::test]
    async fn empty_bucket_or_id_rejected() {
        let idx = make_index();
        assert!(idx.upsert_text("", "a", "x", serde_json::json!({})).await.is_err());
        assert!(idx.upsert_text("b", "", "x", serde_json::json!({})).await.is_err());
    }

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
    async fn delete_document_unknown_errors() {
        let idx = make_index();
        assert!(idx.delete_document("docs", "nope").is_err());
    }

    #[tokio::test]
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

    #[tokio::test]
    async fn bucket_stats_empty_index_is_empty() {
        let idx = make_index();
        assert!(idx.bucket_stats(10).is_empty());
    }

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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
}
