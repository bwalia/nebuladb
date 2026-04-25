//! Text-addressable vector index.
//!
//! This crate wires the dumb pieces (HNSW + embedder) into something the
//! API layer can call: a [`TextIndex`] where you insert documents by
//! `(bucket, id, text)` and search by text or vector, with bucket
//! filtering applied post-ANN.
//!
//! Everything is in-memory. Persistence is a separate concern — the
//! `Arc<dyn Embedder>` dependency and the bucket map are both designed
//! so that swapping in a disk-backed store later doesn't change the
//! public API.

use std::sync::Arc;

use ahash::{AHashMap, AHashSet};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use nebula_chunk::Chunker;
use nebula_core::{Id, NebulaError};
use nebula_embed::{EmbedError, Embedder};
use nebula_vector::{Hnsw, HnswConfig, Metric};

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
        })
    }

    pub fn dim(&self) -> usize {
        self.embedder.dim()
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

        let key = (bucket.to_string(), external_id.to_string());

        // Step 1: claim an internal id and snapshot the doc under the
        // lock. Holding the lock across the HNSW insert is fine because
        // HNSW has its own internal lock — we are not nesting on the
        // same mutex.
        let new_internal_id = {
            let mut g = self.inner.write();

            if let Some(&old_id) = g.by_key.get(&key) {
                // Replace: tombstone old, drop from maps. The HNSW node
                // for `old_id` stays for connectivity but is filtered
                // from results.
                let _ = self.hnsw.delete(old_id);
                g.docs.remove(&old_id);
                g.by_key.remove(&key);
            }

            let id = Id(g.next_id);
            g.next_id += 1;
            g.by_key.insert(key.clone(), id);
            g.docs.insert(
                id,
                Document {
                    bucket: bucket.to_string(),
                    external_id: external_id.to_string(),
                    text: text.to_string(),
                    vector: vector.clone(),
                    metadata,
                    parent_doc_id: None,
                    chunk_index: None,
                },
            );
            id
        };

        // Step 2: push into HNSW. If this fails we roll back the maps
        // so the index stays consistent; otherwise a failed insert
        // would leave a `docs` entry the graph can never return.
        if let Err(e) = self.hnsw.insert(new_internal_id, &vector) {
            let mut g = self.inner.write();
            g.docs.remove(&new_internal_id);
            g.by_key.remove(&key);
            return Err(e.into());
        }
        Ok(())
    }

    pub fn get(&self, bucket: &str, external_id: &str) -> Option<Document> {
        let g = self.inner.read();
        let key = (bucket.to_string(), external_id.to_string());
        g.by_key.get(&key).and_then(|id| g.docs.get(id)).cloned()
    }

    pub fn delete(&self, bucket: &str, external_id: &str) -> Result<()> {
        let mut g = self.inner.write();
        let key = (bucket.to_string(), external_id.to_string());
        let id = g.by_key.remove(&key).ok_or_else(|| IndexError::DocNotFound {
            bucket: bucket.to_string(),
            id: external_id.to_string(),
        })?;
        // If this was a chunk, also drop it from the parent's set.
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
        drop(g);
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

        // Replace semantics: drop any existing chunks for this doc.
        // Done under one lock so we never have a half-old, half-new
        // state visible to a concurrent searcher.
        let chunk_count = chunks.len();
        {
            let mut g = self.inner.write();
            let parent_key = (bucket.to_string(), doc_id.to_string());
            if let Some(existing) = g.parents.remove(&parent_key) {
                for external_id in existing {
                    let key = (bucket.to_string(), external_id.clone());
                    if let Some(id) = g.by_key.remove(&key) {
                        g.docs.remove(&id);
                        // HNSW has its own lock; safe to call here.
                        let _ = self.hnsw.delete(id);
                    }
                }
            }

            // Register every new chunk before touching HNSW so a
            // partial failure rolls back both maps together.
            let mut registered: Vec<(Id, String)> = Vec::with_capacity(chunks.len());
            let mut parent_set: AHashSet<String> = AHashSet::with_capacity(chunks.len());
            for (chunk, vector) in chunks.iter().zip(vectors.iter()) {
                let external_id = format!("{doc_id}#{}", chunk.index);
                let id = Id(g.next_id);
                g.next_id += 1;
                g.by_key
                    .insert((bucket.to_string(), external_id.clone()), id);
                g.docs.insert(
                    id,
                    Document {
                        bucket: bucket.to_string(),
                        external_id: external_id.clone(),
                        text: chunk.text.clone(),
                        vector: vector.clone(),
                        metadata: metadata.clone(),
                        parent_doc_id: Some(doc_id.to_string()),
                        chunk_index: Some(chunk.index),
                    },
                );
                parent_set.insert(external_id.clone());
                registered.push((id, external_id));
            }
            g.parents.insert(parent_key, parent_set);

            // Drop the write lock before the HNSW inserts to minimize
            // the blast radius. The chunks are already visible in the
            // maps under tombstone-safe semantics: an HNSW insert
            // failure will undo both sides.
            drop(g);

            for ((id, external_id), vector) in registered.iter().zip(vectors.iter()) {
                if let Err(e) = self.hnsw.insert(*id, vector) {
                    // Rollback every chunk for this doc so we don't
                    // leave a half-inserted document.
                    let mut g = self.inner.write();
                    for (rid, rkey) in &registered {
                        g.docs.remove(rid);
                        g.by_key.remove(&(bucket.to_string(), rkey.clone()));
                        let _ = self.hnsw.delete(*rid);
                    }
                    g.parents.remove(&(bucket.to_string(), doc_id.to_string()));
                    return Err(IndexError::Core(e));
                }
                // Suppress unused warning when the iteration is loop-final.
                let _ = external_id;
            }
        }
        Ok(chunk_count)
    }

    /// Delete every chunk associated with `doc_id`. Returns the number
    /// of chunks removed, or an error if no such document exists.
    pub fn delete_document(&self, bucket: &str, doc_id: &str) -> Result<usize> {
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
        let mut g = self.inner.write();
        // Collect first so we can mutate the map without fighting the
        // borrow checker over the iteration.
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
        }
        // HNSW deletes are soft-tombstones; safe to call outside the
        // write lock since the HNSW has its own internal lock.
        drop(g);
        for id in victims {
            let _ = self.hnsw.delete(id);
        }
        n
    }

    /// One full-scan pass over the corpus. Cheap at demo scale
    /// (thousands of docs); at production scale we'd maintain these
    /// counters incrementally on the write path. Fine trade-off for
    /// the admin UI today — rebuilds happen on demand, not per
    /// request.
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
        let raw = self.hnsw.search(vector, fetch, ef)?;
        let g = self.inner.read();
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
