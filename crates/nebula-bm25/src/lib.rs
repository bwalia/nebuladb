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

/// One query term's share of a document's BM25 score — see
/// [`Bm25Index::explain_doc`].
#[derive(Debug, Clone, PartialEq)]
pub struct TermContribution {
    pub term: String,
    /// Occurrences of `term` in the document.
    pub tf: u32,
    /// Live documents containing `term` (`n(t)`).
    pub df: u32,
    pub idf: f32,
    /// This term's weight in the document's score; the score is the sum.
    pub contribution: f32,
}

/// A query term's corpus statistics — see [`Bm25Index::query_terms`].
#[derive(Debug, Clone, PartialEq)]
pub struct QueryTerm {
    pub term: String,
    /// Live documents containing the term; 0 = not in the vocabulary.
    pub df: u32,
    pub idf: f32,
}

/// A postings entry: a document and the raw term frequency within it.
///
/// `generation` identifies the [`Bm25Index::add`] call that created the
/// posting. A posting is live only while its doc is indexed under that
/// same generation, so a removed-then-re-added id never resurrects the
/// old doc's postings. It fits in what was padding, so a `Posting` is
/// still 16 bytes.
#[derive(Debug, Clone, Copy)]
struct Posting {
    id: u64,
    tf: u32,
    generation: u32,
}

/// Postings for one term. Removal is lazy: [`Bm25Index::remove_text`]
/// only decrements `live`, leaving the dead entry in `list` for search
/// to skip, and the list is compacted once dead entries pass half the
/// live count — amortized O(1) per removal.
///
/// Eager removal scanned the whole list, and a term in every doc has a
/// list as long as the corpus. Prod leads are templated text ("County:
/// …", "Region: …", "Contact 1 Title: …"), so ~16 terms sit in all 2.3M
/// docs and every replace scanned ~37M postings: ~130ms per upsert under
/// the index write lock, capping writes and WAL replay at ~7/s.
#[derive(Debug, Default)]
struct PostingList {
    list: Vec<Posting>,
    /// Live postings in `list` — exactly `n(t)`, the IDF doc frequency.
    live: u32,
}

/// A live document's BM25 bookkeeping.
#[derive(Debug, Clone, Copy)]
struct DocEntry {
    /// Token count, `|D|`.
    len: u32,
    /// The generation its postings carry.
    generation: u32,
}

/// In-memory BM25 index. Not thread-safe by itself — the embedding
/// host ([`nebula_index`]) already serializes mutations under its own
/// write lock, so adding one here would just nest locks. Callers that
/// need concurrency wrap it the same way they wrap the rest of the
/// corpus.
#[derive(Debug, Default)]
pub struct Bm25Index {
    params: Bm25Params,
    /// term → postings. Postings are unsorted; search scans them and
    /// skips entries that are no longer live.
    postings: AHashMap<String, PostingList>,
    /// doc id → length + generation. The authoritative set of live doc
    /// ids — a removed doc is gone from here even while its postings
    /// await compaction.
    docs: AHashMap<u64, DocEntry>,
    /// Σ of all live doc lengths, kept incrementally so `avgdl` is O(1).
    total_len: u64,
    /// Generation stamped on the next [`Self::add`]. Wraps; a stale
    /// posting could only be mistaken for live if its id were re-added
    /// exactly 2^32 adds later without its list being compacted once.
    next_generation: u32,
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
        self.docs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.docs.is_empty()
    }

    /// Mean document length in tokens. `0.0` when empty — search
    /// short-circuits before this is used, so the value is never a
    /// divisor in that state.
    fn avgdl(&self) -> f32 {
        if self.docs.is_empty() {
            0.0
        } else {
            self.total_len as f32 / self.docs.len() as f32
        }
    }

    /// Index `text` under `id`. If `id` already exists it is replaced
    /// (remove-then-add), so re-indexing a changed chunk is correct and
    /// idempotent — the same contract as the vector index's upsert.
    pub fn add(&mut self, id: u64, text: &str) {
        if self.docs.contains_key(&id) {
            // Replacing a live id without its old text: we can't tell
            // which `live` counts to decrement, so take the exact slow
            // path. `nebula_index` never does this — it gives every
            // upsert a fresh id and removes the old one via
            // `remove_text`.
            self.remove(id);
        }

        let tokens = tokenize(text);
        let len = tokens.len() as u32;
        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1);

        // Collapse to per-term frequencies so each posting list gets at
        // most one entry per document.
        let mut tf: AHashMap<String, u32> = AHashMap::new();
        for tok in tokens {
            *tf.entry(tok).or_insert(0) += 1;
        }
        for (term, freq) in tf {
            let pl = self.postings.entry(term).or_default();
            pl.list.push(Posting {
                id,
                tf: freq,
                generation,
            });
            pl.live += 1;
        }

        self.docs.insert(id, DocEntry { len, generation });
        self.total_len += len as u64;
    }

    /// Remove a document. No-op if the id is unknown. Empty posting
    /// lists are dropped so `n(t)` (the IDF document frequency) stays
    /// exact and memory doesn't grow with churn.
    ///
    /// This scans *every* posting list — O(total postings). Prefer
    /// [`Self::remove_text`] whenever the caller still has the text
    /// the doc was indexed with.
    pub fn remove(&mut self, id: u64) {
        let Some(doc) = self.docs.remove(&id) else {
            return;
        };
        self.total_len -= doc.len as u64;

        // We don't track which terms a doc held, so we scan posting
        // lists. Keeps the per-doc footprint to a single length entry.
        self.postings.retain(|_term, pl| {
            let before = pl.list.len();
            pl.list
                .retain(|p| !(p.id == id && p.generation == doc.generation));
            if pl.list.len() < before {
                pl.live -= 1;
            }
            pl.live > 0
        });
    }

    /// Remove a document given the exact `text` it was [`Self::add`]ed
    /// with. Re-tokenizing tells us which terms' live counts to
    /// decrement; the postings themselves are dropped lazily (see
    /// [`PostingList`]). O(terms in the doc), amortized — independent
    /// of corpus size.
    ///
    /// Upserting an existing key removes the old doc, so this is the
    /// write hot path, not a rare admin operation. An O(corpus) removal
    /// here capped prod writes — and WAL replay on boot — at a few per
    /// second; replay of one 64MB segment then outlasted the 60-minute
    /// startup probe and the pod restart-looped for ~4h.
    ///
    /// `text` must be what the doc was indexed with; anything else
    /// skews `n(t)` for the terms that differ.
    pub fn remove_text(&mut self, id: u64, text: &str) {
        let Some(doc) = self.docs.remove(&id) else {
            return;
        };
        self.total_len -= doc.len as u64;

        let terms: AHashSet<String> = tokenize(text).into_iter().collect();
        for term in terms {
            let Some(pl) = self.postings.get_mut(&term) else {
                continue;
            };
            pl.live = pl.live.saturating_sub(1);
            if pl.live == 0 {
                self.postings.remove(&term);
                continue;
            }
            let dead = pl.list.len() - pl.live as usize;
            if dead > pl.live as usize / 2 {
                let docs = &self.docs;
                pl.list.retain(|p| {
                    docs.get(&p.id).is_some_and(|d| d.generation == p.generation)
                });
                pl.live = pl.list.len() as u32;
            }
        }
    }

    /// Score the corpus against `query` and return the top `k` by
    /// descending BM25 score. Ties break by ascending id for
    /// determinism. Returns fewer than `k` when the corpus is smaller
    /// or no document matches any query term.
    pub fn search(&self, query: &str, k: usize) -> Vec<Bm25Hit> {
        self.search_counted(query, k).0
    }

    /// [`Self::search`] plus the number of documents that matched at
    /// least one query term, before truncation to `k`.
    pub fn search_counted(&self, query: &str, k: usize) -> (Vec<Bm25Hit>, usize) {
        if k == 0 || self.docs.is_empty() {
            return (Vec::new(), 0);
        }

        let q_terms: AHashSet<String> = tokenize(query).into_iter().collect();
        if q_terms.is_empty() {
            return (Vec::new(), 0);
        }

        let avgdl = self.avgdl();

        let mut acc: AHashMap<u64, f32> = AHashMap::new();
        for term in &q_terms {
            let Some(pl) = self.postings.get(term) else {
                continue;
            };
            let idf = self.idf(pl.live);

            for p in &pl.list {
                let Some(doc) = self.docs.get(&p.id) else {
                    continue;
                };
                if doc.generation != p.generation {
                    continue;
                }
                *acc.entry(p.id).or_insert(0.0) += self.term_weight(idf, p.tf, doc.len, avgdl);
            }
        }

        let matched = acc.len();
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
        (hits, matched)
    }
}

impl Bm25Index {
    /// Lucene-style IDF: the inner `1 +` floors it at 0 so a term
    /// present in every doc adds nothing instead of pulling scores
    /// negative.
    fn idf(&self, df: u32) -> f32 {
        let n = self.docs.len() as f32;
        let df = df as f32;
        (1.0 + (n - df + 0.5) / (df + 0.5)).ln()
    }

    /// One term's BM25 weight in one document. Shared by
    /// [`Self::search`] and [`Self::explain_doc`] so an explanation can
    /// never drift from the score it explains.
    fn term_weight(&self, idf: f32, tf: u32, doc_len: u32, avgdl: f32) -> f32 {
        let (k1, b) = (self.params.k1, self.params.b);
        let tf = tf as f32;
        let denom = tf + k1 * (1.0 - b + b * doc_len as f32 / avgdl);
        idf * (tf * (k1 + 1.0)) / denom
    }

    /// The query's distinct terms, in query order, with their corpus
    /// document frequency and IDF. A term with `df == 0` isn't in the
    /// vocabulary and can't match anything.
    pub fn query_terms(&self, query: &str) -> Vec<QueryTerm> {
        let mut seen = AHashSet::new();
        tokenize(query)
            .into_iter()
            .filter(|t| seen.insert(t.clone()))
            .map(|term| {
                let df = self.postings.get(&term).map_or(0, |pl| pl.live);
                QueryTerm {
                    idf: self.idf(df),
                    term,
                    df,
                }
            })
            .collect()
    }

    /// Break document `id`'s score for `query` down by term. `text` must
    /// be the text it was indexed with: term frequencies come from
    /// re-tokenizing it, which is exact (the index was built from the
    /// same tokenizer) and O(doc length) — reading them from posting
    /// lists would mean scanning lists as long as the corpus. Only
    /// terms the document contains are returned; they sum to the score
    /// [`Self::search`] gives it. Empty if `id` isn't live.
    pub fn explain_doc(&self, query: &str, id: u64, text: &str) -> Vec<TermContribution> {
        let Some(doc) = self.docs.get(&id) else {
            return Vec::new();
        };
        let mut tf: AHashMap<String, u32> = AHashMap::new();
        for tok in tokenize(text) {
            *tf.entry(tok).or_insert(0) += 1;
        }
        let avgdl = self.avgdl();
        self.query_terms(query)
            .into_iter()
            .filter_map(|q| {
                let &n = tf.get(&q.term)?;
                Some(TermContribution {
                    contribution: self.term_weight(q.idf, n, doc.len, avgdl),
                    tf: n,
                    df: q.df,
                    idf: q.idf,
                    term: q.term,
                })
            })
            .collect()
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
    fn remove_text_matches_full_scan_remove() {
        let docs = [(1, "Cat dog, cat!"), (2, "cat fish"), (3, "dog bird")];
        let mut scanned = build(&docs);
        let mut targeted = build(&docs);
        scanned.remove(1);
        targeted.remove_text(1, docs[0].1);

        assert_eq!(targeted.len(), scanned.len());
        assert_eq!(targeted.total_len, scanned.total_len);
        let mut a: Vec<_> = targeted.postings.keys().cloned().collect();
        let mut b: Vec<_> = scanned.postings.keys().cloned().collect();
        a.sort();
        b.sort();
        assert_eq!(a, b, "same posting lists survive, empty ones dropped");
        for q in ["cat", "dog", "fish", "bird"] {
            let ids = |i: &Bm25Index| i.search(q, 10).iter().map(|h| h.id).collect::<Vec<_>>();
            assert_eq!(ids(&targeted), ids(&scanned), "query {q}");
        }
        // Removing again is a no-op.
        targeted.remove_text(1, docs[0].1);
        assert_eq!(targeted.len(), 2);
    }

    /// Lazily removed docs must be invisible to scoring: every score
    /// (which depends on N, avgdl and n(t)) must equal that of an index
    /// that never contained them.
    #[test]
    fn lazy_removal_scores_like_never_indexed() {
        let all: Vec<(u64, String)> = (0..200u64)
            .map(|i| (i, format!("county region contact title lead{i} town{}", i % 7)))
            .collect();
        let mut lazy = Bm25Index::new(Bm25Params::default());
        for (id, t) in &all {
            lazy.add(*id, t);
        }
        for (id, t) in all.iter().filter(|(id, _)| id % 3 == 0) {
            lazy.remove_text(*id, t);
        }
        let fresh = build(
            &all.iter()
                .filter(|(id, _)| id % 3 != 0)
                .map(|(id, t)| (*id, t.as_str()))
                .collect::<Vec<_>>(),
        );
        assert_eq!(lazy.len(), fresh.len());
        for q in ["county", "town3", "lead5 county", "lead6", "title town0"] {
            let (a, b) = (lazy.search(q, 500), fresh.search(q, 500));
            assert_eq!(a.len(), b.len(), "query {q}");
            for (x, y) in a.iter().zip(&b) {
                assert_eq!(x.id, y.id, "query {q}");
                assert!((x.score - y.score).abs() < 1e-5, "query {q}");
            }
        }
    }

    #[test]
    fn readding_a_removed_id_does_not_resurrect_old_postings() {
        let mut idx = build(&[(1, "old words"), (2, "other")]);
        idx.remove_text(1, "old words");
        idx.add(1, "new words");
        assert!(idx.search("old", 10).is_empty(), "stale posting came back");
        assert_eq!(idx.search("new", 10)[0].id, 1);
        let words = idx.search("words", 10);
        assert_eq!(words.len(), 1);
        assert_eq!(idx.postings["words"].live, 1);
    }

    /// Removal must not scan lists as long as the corpus: dead postings
    /// are dropped in amortized batches, so a list never holds more
    /// than ~1.5x its live entries.
    #[test]
    fn universal_term_list_stays_bounded_under_churn() {
        let mut idx = Bm25Index::new(Bm25Params::default());
        let text = |i: u64| format!("county region lead{i}");
        for i in 0..1_000u64 {
            idx.add(i, &text(i));
        }
        // Replace every doc 5 times over (fresh id each time, like
        // nebula_index's upsert).
        let mut live: Vec<u64> = (0..1_000).collect();
        let mut next = 1_000u64;
        for _ in 0..5 {
            for slot in live.iter_mut() {
                idx.remove_text(*slot, &text(*slot));
                idx.add(next, &text(next));
                *slot = next;
                next += 1;
            }
        }
        let pl = &idx.postings["county"];
        assert_eq!(pl.live, 1_000);
        assert!(pl.list.len() <= 1_501, "list grew to {}", pl.list.len());
        assert_eq!(idx.search("county", 5_000).len(), 1_000);
    }

    #[test]
    fn explain_doc_sums_to_search_score() {
        let docs = [
            (1, "County: Kent\nRegion: South East florist"),
            (2, "County: Kent\nRegion: London florist florist"),
            (3, "County: Essex\nRegion: East builder"),
        ];
        let idx = build(&docs);
        let q = "florist in Kent";
        for hit in idx.search(q, 10) {
            let text = docs.iter().find(|(id, _)| *id == hit.id).unwrap().1;
            let parts = idx.explain_doc(q, hit.id, text);
            let sum: f32 = parts.iter().map(|p| p.contribution).sum();
            assert!((sum - hit.score).abs() < 1e-5, "doc {}: {sum} vs {}", hit.id, hit.score);
            assert!(parts.iter().all(|p| p.term != "in"), "absent term listed");
        }
        let two = idx.explain_doc(q, 2, docs[1].1);
        assert_eq!(two.iter().find(|p| p.term == "florist").unwrap().tf, 2);

        let terms = idx.query_terms("Florist in florist Kent");
        let names: Vec<_> = terms.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(names, vec!["florist", "in", "kent"], "dedup, query order");
        assert_eq!(terms[1].df, 0, "'in' is not in the vocabulary");
        assert_eq!(terms[2].df, 2);
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
