//! Okapi BM25 lexical retrieval over an in-memory inverted index.
//!
//! # Why a crate
//!
//! NebulaDB already does dense (vector) retrieval via HNSW. Dense
//! retrieval is strong on paraphrase and weak on rare exact tokens —
//! product codes, error strings, function names, acronyms — which is
//! exactly where a lexical signal shines. Design 0008 §6 fuses the two.
//! BM25 is the lexical half. Isolating it here keeps the scoring math
//! testable without an index, a WAL, or an embedder in the loop.
//!
//! # Model
//!
//! Documents are keyed by an opaque `u64` (the index layer's internal
//! id). The caller owns the mapping from that id to a real document;
//! this crate only knows ids, tokens, and lengths. Bucket filtering,
//! tombstones, and attribution all live above this layer.
//!
//! Scoring is standard Okapi BM25:
//!
//! ```text
//! score(D, Q) = Σ_t  IDF(t) · ( f(t,D)·(k1+1) )
//!                            / ( f(t,D) + k1·(1 - b + b·|D|/avgdl) )
//!
//! IDF(t) = ln( 1 + (N - n(t) + 0.5) / (n(t) + 0.5) )
//! ```
//!
//! where `f(t,D)` is the term frequency in the doc, `|D|` the doc
//! length in tokens, `avgdl` the mean doc length, `N` the live doc
//! count, and `n(t)` the number of live docs containing `t`. The `+1`
//! inside the IDF log keeps it non-negative (Lucene's variant), so a
//! term appearing in every document contributes ~0 rather than a
//! negative score.
//!
//! # Tokenizer
//!
//! Deliberately simple and dependency-free: lowercase, split on any
//! non-alphanumeric Unicode boundary. This matches the char-oriented
//! posture of [`nebula_chunk`] and avoids pulling a stemmer / language
//! model into the core. A future change can swap [`tokenize`] for a
//! real analyzer behind the same `add`/`search` surface.

use ahash::{AHashMap, AHashSet};

/// Tunable BM25 parameters. Defaults (`k1 = 1.2`, `b = 0.75`) are the
/// long-standing Okapi values and a sane out-of-the-box choice; design
/// 0008 §9 exposes these per-collection later.
#[derive(Debug, Clone, Copy)]
pub struct Bm25Params {
    /// Term-frequency saturation. Higher = term frequency keeps mattering
    /// for longer; lower = saturates fast. Typical range 1.2–2.0.
    pub k1: f32,
    /// Length normalization. `0.0` disables it (long and short docs
    /// scored alike); `1.0` fully normalizes by `|D|/avgdl`.
    pub b: f32,
}

impl Default for Bm25Params {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

/// One scored hit. `score` is the summed BM25 weight; larger is more
/// relevant (the opposite sense to a vector *distance*).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bm25Hit {
    pub id: u64,
    pub score: f32,
}

/// A postings entry: a document and the raw term frequency within it.
#[derive(Debug, Clone, Copy)]
struct Posting {
    id: u64,
    tf: u32,
}

/// In-memory BM25 index. Not thread-safe by itself — the embedding
/// host ([`nebula_index`]) already serializes mutations under its own
/// write lock, so adding one here would just nest locks. Callers that
/// need concurrency wrap it the same way they wrap the rest of the
/// corpus.
#[derive(Debug, Default)]
pub struct Bm25Index {
    params: Bm25Params,
    /// term → postings list. Postings are unsorted; search scans them.
    postings: AHashMap<String, Vec<Posting>>,
    /// doc id → token count (its `|D|`). Also the authoritative set of
    /// live doc ids — a removed doc is gone from here.
    doc_len: AHashMap<u64, u32>,
    /// Σ of all `doc_len` values, kept incrementally so `avgdl` is O(1).
    total_len: u64,
}

impl Bm25Index {
    pub fn new(params: Bm25Params) -> Self {
        Self {
            params,
            ..Default::default()
        }
    }

    /// Number of live documents (`N`).
    pub fn len(&self) -> usize {
        self.doc_len.len()
    }

    pub fn is_empty(&self) -> bool {
        self.doc_len.is_empty()
    }

    /// Mean document length in tokens. `0.0` when empty — search
    /// short-circuits before this is used, so the value is never a
    /// divisor in that state.
    fn avgdl(&self) -> f32 {
        if self.doc_len.is_empty() {
            0.0
        } else {
            self.total_len as f32 / self.doc_len.len() as f32
        }
    }

    /// Index `text` under `id`. If `id` already exists it is replaced
    /// (remove-then-add), so re-indexing a changed chunk is correct and
    /// idempotent — the same contract as the vector index's upsert.
    pub fn add(&mut self, id: u64, text: &str) {
        if self.doc_len.contains_key(&id) {
            self.remove(id);
        }

        let tokens = tokenize(text);
        let len = tokens.len() as u32;

        // Collapse to per-term frequencies so each posting list gets at
        // most one entry per document.
        let mut tf: AHashMap<String, u32> = AHashMap::new();
        for tok in tokens {
            *tf.entry(tok).or_insert(0) += 1;
        }
        for (term, freq) in tf {
            self.postings
                .entry(term)
                .or_default()
                .push(Posting { id, tf: freq });
        }

        self.doc_len.insert(id, len);
        self.total_len += len as u64;
    }

    /// Remove a document. No-op if the id is unknown. Empty posting
    /// lists are dropped so `n(t)` (the IDF document frequency) stays
    /// exact and memory doesn't grow with churn.
    pub fn remove(&mut self, id: u64) {
        let Some(len) = self.doc_len.remove(&id) else {
            return;
        };
        self.total_len -= len as u64;

        // We don't track which terms a doc held, so we scan posting
        // lists. Acceptable: removals are far rarer than searches, and
        // this keeps the per-doc memory footprint to a single length
        // entry rather than a full term set.
        self.postings.retain(|_term, list| {
            list.retain(|p| p.id != id);
            !list.is_empty()
        });
    }

    /// Score the corpus against `query` and return the top `k` by
    /// descending BM25 score. Ties break by ascending id for
    /// determinism. Returns fewer than `k` when the corpus is smaller
    /// or no document matches any query term.
    pub fn search(&self, query: &str, k: usize) -> Vec<Bm25Hit> {
        if k == 0 || self.doc_len.is_empty() {
            return Vec::new();
        }

        let q_terms: AHashSet<String> = tokenize(query).into_iter().collect();
        if q_terms.is_empty() {
            return Vec::new();
        }

        let n = self.doc_len.len() as f32;
        let avgdl = self.avgdl();
        let k1 = self.params.k1;
        let b = self.params.b;

        let mut acc: AHashMap<u64, f32> = AHashMap::new();
        for term in &q_terms {
            let Some(list) = self.postings.get(term) else {
                continue;
            };
            let df = list.len() as f32;
            // Lucene-style IDF: the inner `1 +` floors it at 0 so a
            // term present in every doc adds nothing instead of pulling
            // scores negative.
            let idf = (1.0 + (n - df + 0.5) / (df + 0.5)).ln();

            for p in list {
                let dl = self.doc_len[&p.id] as f32;
                let tf = p.tf as f32;
                let denom = tf + k1 * (1.0 - b + b * dl / avgdl);
                let weight = idf * (tf * (k1 + 1.0)) / denom;
                *acc.entry(p.id).or_insert(0.0) += weight;
            }
        }

        let mut hits: Vec<Bm25Hit> = acc
            .into_iter()
            .map(|(id, score)| Bm25Hit { id, score })
            .collect();
        // Descending score, ascending id on ties.
        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.id.cmp(&b.id))
        });
        hits.truncate(k);
        hits
    }
}

/// Lowercase + split on non-alphanumeric boundaries. Empty tokens are
/// dropped. Unicode-aware: `char::is_alphanumeric` keeps accented and
/// non-Latin scripts as token characters.
pub fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_lowercase())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build(docs: &[(u64, &str)]) -> Bm25Index {
        let mut idx = Bm25Index::new(Bm25Params::default());
        for (id, text) in docs {
            idx.add(*id, text);
        }
        idx
    }

    #[test]
    fn empty_index_returns_nothing() {
        let idx = Bm25Index::new(Bm25Params::default());
        assert!(idx.search("anything", 10).is_empty());
        assert!(idx.is_empty());
    }

    #[test]
    fn shorter_doc_with_same_term_ranks_higher() {
        let idx = build(&[
            (1, "cat"),
            (2, "cat with many other unrelated filler words here today"),
            (3, "dogs are loyal pets"),
        ]);
        let hits = idx.search("cat", 10);
        // Docs 1 and 2 mention cat; doc 1 is far shorter, so BM25
        // length-normalization ranks it first. Doc 3 has no match.
        assert_eq!(hits[0].id, 1);
        let ids: Vec<u64> = hits.iter().map(|h| h.id).collect();
        assert_eq!(ids, vec![1, 2]);
    }

    #[test]
    fn rare_term_outranks_common_term() {
        // "database" appears in every doc (low IDF); "kubernetes" in one
        // (high IDF). A query for both should surface the kubernetes doc.
        let idx = build(&[
            (1, "database tuning guide"),
            (2, "database backup and restore"),
            (3, "database on kubernetes cluster"),
        ]);
        let hits = idx.search("database kubernetes", 10);
        assert_eq!(hits[0].id, 3);
    }

    #[test]
    fn multi_term_query_accumulates() {
        let idx = build(&[
            (1, "alpha"),
            (2, "alpha beta"),
            (3, "alpha beta gamma"),
        ]);
        let hits = idx.search("alpha beta gamma", 10);
        // Doc 3 matches all three query terms → highest.
        assert_eq!(hits[0].id, 3);
    }

    #[test]
    fn remove_drops_doc_and_updates_stats() {
        let mut idx = build(&[(1, "cat dog"), (2, "cat fish")]);
        assert_eq!(idx.len(), 2);
        idx.remove(1);
        assert_eq!(idx.len(), 1);
        let hits = idx.search("cat", 10);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, 2);
        // Removing again is a no-op.
        idx.remove(1);
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn add_same_id_replaces() {
        let mut idx = build(&[(1, "cat")]);
        idx.add(1, "dog");
        assert_eq!(idx.len(), 1);
        assert!(idx.search("cat", 10).is_empty());
        assert_eq!(idx.search("dog", 10)[0].id, 1);
    }

    #[test]
    fn term_in_every_doc_has_nonnegative_score() {
        let idx = build(&[(1, "common"), (2, "common"), (3, "common")]);
        for h in idx.search("common", 10) {
            assert!(h.score >= 0.0, "score went negative: {}", h.score);
        }
    }

    #[test]
    fn unknown_term_yields_no_hits() {
        let idx = build(&[(1, "cat dog")]);
        assert!(idx.search("kubernetes", 10).is_empty());
    }

    #[test]
    fn k_caps_result_count() {
        let idx = build(&[(1, "x"), (2, "x"), (3, "x"), (4, "x")]);
        assert_eq!(idx.search("x", 2).len(), 2);
    }

    #[test]
    fn tokenizer_is_case_and_punctuation_insensitive() {
        assert_eq!(tokenize("Hello, World!"), vec!["hello", "world"]);
        assert_eq!(tokenize("a_b-c.d"), vec!["a", "b", "c", "d"]);
        assert!(tokenize("   ").is_empty());
    }

    #[test]
    fn ties_break_by_ascending_id() {
        let idx = build(&[(5, "same"), (2, "same"), (9, "same")]);
        let ids: Vec<u64> = idx.search("same", 10).iter().map(|h| h.id).collect();
        assert_eq!(ids, vec![2, 5, 9]);
    }
}
