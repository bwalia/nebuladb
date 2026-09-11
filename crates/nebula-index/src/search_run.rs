//! Retrieval cores shared by plain search and EXPLAIN.
//!
//! `search_vector`, `search_bm25` and `search_vector_hybrid` are thin
//! wrappers over the `run_*` functions here, which also record what
//! happened along the way (candidate counts, what each filter dropped,
//! per-stage timings, HNSW traversal stats). Plain searches drop that
//! record; `search_text_explained` turns it into [`SearchTrace`]
//! stages. Sharing one code path is what guarantees an explanation
//! describes the search that actually runs.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ahash::AHashMap;
use nebula_vector::HnswSearchStats;

use crate::explain::{
    Bm25Part, HitExplain, Note, ScoreKind, SearchTrace, Stage, TermPart, VectorPart,
};
use crate::{min_max_normalize, Hit, IndexError, Result, TextIndex};

/// Share of the HNSW beam taken by deleted nodes above which EXPLAIN
/// warns that recall is suffering.
const TOMBSTONE_BEAM_WARN_PCT: usize = 25;

pub(crate) struct VectorRun {
    pub hits: Vec<Hit>,
    /// Results the caller asked for.
    k: usize,
    stats: HnswSearchStats,
    /// Candidates requested from HNSW (`k`, or over-fetched when a
    /// bucket filter follows).
    fetch: usize,
    /// Live candidates HNSW returned.
    raw: usize,
    /// Candidates looked at before `k` results were collected.
    examined: usize,
    /// Candidates whose document vanished between search and assembly.
    missing: usize,
    /// Candidates dropped by the bucket filter.
    other_bucket: usize,
    hnsw_took: Duration,
    assemble_took: Duration,
}

pub(crate) struct Bm25Run {
    pub hits: Vec<Hit>,
    fetch: usize,
    /// Documents containing at least one query term.
    matched: usize,
    raw: usize,
    examined: usize,
    missing: usize,
    other_bucket: usize,
    took: Duration,
}

/// Per-document fusion inputs, kept for EXPLAIN.
#[derive(Default, Clone, Copy)]
struct FuseParts {
    /// (1-based rank, distance, similarity, normalized)
    vector: Option<(usize, f32, f32, f32)>,
    /// (1-based rank, raw bm25, normalized)
    bm25: Option<(usize, f32, f32)>,
}

pub(crate) struct HybridRun {
    pub hits: Vec<Hit>,
    vector: VectorRun,
    bm25: Bm25Run,
    weights: (f32, f32),
    parts: AHashMap<String, FuseParts>,
    union: usize,
    overlap: usize,
    fuse_took: Duration,
}

/// When every candidate from a fusion stage has the same score, min-max
/// normalization has no range to spread and gives each the full 1.0 —
/// so that stage adds `weight` to every candidate and contributes no
/// ranking signal, while lifting candidates the other stage never
/// matched up to tie with its best. Worth saying out loud.
fn flat_stage_note(stage: &str, scores: &[f32], weight: f32) -> Option<Note> {
    // Same degeneracy test as `min_max_normalize`.
    let (min, max) = scores
        .iter()
        .fold((f32::INFINITY, f32::NEG_INFINITY), |(lo, hi), &s| (lo.min(s), hi.max(s)));
    (scores.len() > 1 && max - min <= f32::EPSILON).then(|| {
        Note::warn(format!(
            "All {} {stage} candidates scored the same, so min-max normalization gave \
             each the full {stage} weight ({weight}). The {stage} stage added no ranking signal \
             and lifted candidates the other stage never matched up to tie with its best.",
            scores.len()
        ))
    })
}

fn bucket_fetch(bucket: Option<&str>, k: usize) -> usize {
    // Over-fetch when filtering because results are post-filtered. 4x is
    // a rule-of-thumb; a real system would adapt based on the bucket's
    // share of the corpus.
    if bucket.is_some() {
        k.saturating_mul(4).max(32)
    } else {
        k
    }
}

impl TextIndex {
    pub(crate) fn run_vector(
        &self,
        vector: &[f32],
        bucket: Option<&str>,
        k: usize,
        ef: Option<usize>,
    ) -> Result<VectorRun> {
        let fetch = bucket_fetch(bucket, k);

        // Lock order discipline: `inner` before `hnsw`, everywhere.
        // Writers take `inner.write()` then drive `hnsw` under it;
        // readers take `inner.read()` then `hnsw.search` under it.
        // Mixing the order would expose us to an AB-BA deadlock
        // under `parking_lot::RwLock`'s write-priority contention.
        let g = self.inner.read();
        let started = Instant::now();
        let (raw, stats) = self.hnsw.search_with_stats(vector, fetch, ef)?;
        let hnsw_took = started.elapsed();

        let started = Instant::now();
        let mut hits = Vec::with_capacity(raw.len().min(k));
        let (mut examined, mut missing, mut other_bucket) = (0, 0, 0);
        for r in &raw {
            if hits.len() >= k {
                break;
            }
            examined += 1;
            let Some(doc) = g.docs.get(&r.id) else {
                missing += 1; // tombstoned
                continue;
            };
            if let Some(b) = bucket {
                if doc.bucket != b {
                    other_bucket += 1;
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
        }
        Ok(VectorRun {
            hits,
            k,
            stats,
            fetch,
            raw: raw.len(),
            examined,
            missing,
            other_bucket,
            hnsw_took,
            assemble_took: started.elapsed(),
        })
    }

    pub(crate) fn run_bm25(&self, query: &str, bucket: Option<&str>, k: usize) -> Bm25Run {
        let g = self.inner.read();
        // Over-fetch when bucket-filtering, same rationale as the
        // vector path: BM25 ranks the whole corpus and we post-filter.
        let fetch = bucket_fetch(bucket, k);
        let started = Instant::now();
        let (raw, matched) = g.bm25.search_counted(query, fetch);
        let mut hits = Vec::with_capacity(raw.len().min(k));
        let (mut examined, mut missing, mut other_bucket) = (0, 0, 0);
        for r in &raw {
            if hits.len() >= k {
                break;
            }
            examined += 1;
            let Some(doc) = g.docs.get(&nebula_core::Id(r.id)) else {
                missing += 1; // tombstoned between search and assembly
                continue;
            };
            if let Some(b) = bucket {
                if doc.bucket != b {
                    other_bucket += 1;
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
        }
        Bm25Run {
            hits,
            fetch,
            matched,
            raw: raw.len(),
            examined,
            missing,
            other_bucket,
            took: started.elapsed(),
        }
    }

    pub(crate) fn run_hybrid(
        &self,
        query_vector: &[f32],
        query_text: &str,
        bucket: Option<&str>,
        k: usize,
        ef: Option<usize>,
        weights: (f32, f32),
    ) -> Result<HybridRun> {
        // Over-fetch each stage so the fusion set is the union of both
        // top-k's, not just their intersection.
        let stage_k = k.saturating_mul(4).max(16);
        let vector = self.run_vector(query_vector, bucket, stage_k, ef)?;
        let bm25 = self.run_bm25(query_text, bucket, stage_k);

        let started = Instant::now();
        // Map vector distance → similarity so "higher = better" holds
        // for both stages before normalization.
        let vec_sim: Vec<f32> = vector.hits.iter().map(|h| 1.0 / (1.0 + h.score)).collect();
        let vec_norm = min_max_normalize(vec_sim.iter().copied());
        let bm_norm = min_max_normalize(bm25.hits.iter().map(|h| h.score));
        let (w_vec, w_bm) = weights;

        // Accumulate fused score per external id, keeping one `Hit`
        // representative (either stage carries the same doc fields).
        let mut fused: AHashMap<String, (f32, Hit)> = AHashMap::new();
        let mut parts: AHashMap<String, FuseParts> = AHashMap::new();
        for (i, ((hit, sim), n)) in vector.hits.iter().zip(&vec_sim).zip(&vec_norm).enumerate() {
            fused.entry(hit.id.clone()).or_insert((0.0, hit.clone())).0 += w_vec * n;
            parts.entry(hit.id.clone()).or_default().vector = Some((i + 1, hit.score, *sim, *n));
        }
        let mut overlap = 0;
        for (i, (hit, n)) in bm25.hits.iter().zip(&bm_norm).enumerate() {
            fused.entry(hit.id.clone()).or_insert((0.0, hit.clone())).0 += w_bm * n;
            let p = parts.entry(hit.id.clone()).or_default();
            overlap += p.vector.is_some() as usize;
            p.bm25 = Some((i + 1, hit.score, *n));
        }
        let union = fused.len();

        let mut out: Vec<Hit> = fused
            .into_values()
            .map(|(score, mut hit)| {
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
        Ok(HybridRun {
            hits: out,
            vector,
            bm25,
            weights,
            parts,
            union,
            overlap,
            fuse_took: started.elapsed(),
        })
    }

    /// EXPLAIN ANALYZE for text search: runs exactly the search
    /// [`Self::search_text_blocking`] (or, with `hybrid` weights,
    /// [`Self::search_text_hybrid_blocking`]) runs, and reports how it
    /// went — embed, HNSW traversal, bucket filtering, BM25 and fusion
    /// — plus why each hit scored what it did.
    pub async fn search_text_explained(
        self: Arc<Self>,
        query: String,
        bucket: Option<String>,
        k: usize,
        ef: Option<usize>,
        hybrid: Option<(f32, f32)>,
    ) -> Result<(Vec<Hit>, SearchTrace)> {
        let started = Instant::now();
        let qv = self.embedder.embed_one(&query).await?;
        let model = self.embedder.model().to_string();
        let embed = Stage::new(
            "embed",
            "Embed query",
            format!(
                "Turned the query into a {}-dimensional vector with the '{model}' embedder.",
                qv.len()
            ),
        )
        .took(started.elapsed())
        .attr("model", model.clone())
        .attr("dim", qv.len())
        .attr("query", query.clone());

        let (hits, mut trace) = tokio::task::spawn_blocking(move || {
            self.explain_retrieval(&qv, Some(&query), bucket.as_deref(), k, ef, hybrid)
        })
        .await
        .map_err(|e| IndexError::Invalid(format!("explain search task failed: {e}")))??;

        trace.stages.insert(0, embed);
        if model.starts_with("mock") {
            trace.notes.insert(
                0,
                Note::warn(format!(
                    "The embedder is '{model}': it hashes each text into a pseudo-random \
                     vector instead of running a language model, so vector distance carries \
                     no meaning — texts are close only if they are identical. Vector-ranked \
                     results are effectively arbitrary; use hybrid search (BM25) or \
                     configure a real embedding model."
                )),
            );
        }
        Ok((hits, trace))
    }

    /// EXPLAIN ANALYZE for a raw-vector search (SQL `vector_distance`).
    pub async fn search_vector_explained(
        self: Arc<Self>,
        vector: Vec<f32>,
        bucket: Option<String>,
        k: usize,
        ef: Option<usize>,
    ) -> Result<(Vec<Hit>, SearchTrace)> {
        tokio::task::spawn_blocking(move || {
            self.explain_retrieval(&vector, None, bucket.as_deref(), k, ef, None)
        })
        .await
        .map_err(|e| IndexError::Invalid(format!("explain search task failed: {e}")))?
    }

    fn explain_retrieval(
        &self,
        qv: &[f32],
        query_text: Option<&str>,
        bucket: Option<&str>,
        k: usize,
        ef: Option<usize>,
        hybrid: Option<(f32, f32)>,
    ) -> Result<(Vec<Hit>, SearchTrace)> {
        let mut trace = SearchTrace::default();
        match (hybrid, query_text) {
            (Some(weights), Some(text)) => {
                let run = self.run_hybrid(qv, text, bucket, k, ef, weights)?;
                vector_stages(&run.vector, bucket, true, &mut trace);
                self.bm25_stages(&run.bm25, text, bucket, &mut trace);
                let (w_vec, w_bm) = run.weights;
                trace.stages.push(
                    Stage::new(
                        "fuse",
                        "Hybrid fusion",
                        format!(
                            "Combined {} vector and {} keyword candidates ({} unique, {} found by \
                             both): min-max normalized each score to 0–1, fused = {w_vec} × vector \
                             + {w_bm} × keyword, and kept the top {}.",
                            run.vector.hits.len(),
                            run.bm25.hits.len(),
                            run.union,
                            run.overlap,
                            run.hits.len()
                        ),
                    )
                    .rows(Some(run.union), Some(run.hits.len()))
                    .took(run.fuse_took)
                    .attr("vector_weight", w_vec)
                    .attr("bm25_weight", w_bm)
                    .attr("unique_candidates", run.union)
                    .attr("found_by_both", run.overlap),
                );
                if run.bm25.matched == 0 {
                    trace.notes.push(Note::warn(
                        "No document contains any query term, so ranking is vector-only.",
                    ));
                }
                for (stage, scores, weight) in [
                    // The exact values fusion normalizes: similarity for
                    // the vector stage, raw weight for BM25.
                    ("vector", run.vector.hits.iter().map(|h| 1.0 / (1.0 + h.score)).collect::<Vec<_>>(), w_vec),
                    ("keyword", run.bm25.hits.iter().map(|h| h.score).collect(), w_bm),
                ] {
                    if let Some(note) = flat_stage_note(stage, &scores, weight) {
                        trace.notes.push(note);
                    }
                }
                trace.hits = self.hybrid_hit_explains(&run, text);
                Ok((run.hits, trace))
            }
            _ => {
                let run = self.run_vector(qv, bucket, k, ef)?;
                vector_stages(&run, bucket, false, &mut trace);
                trace.hits = run
                    .hits
                    .iter()
                    .enumerate()
                    .map(|(i, h)| HitExplain {
                        id: h.id.clone(),
                        rank: i + 1,
                        final_score: h.score,
                        score_kind: ScoreKind::Distance,
                        vector: Some(VectorPart {
                            distance: h.score,
                            similarity: 1.0 / (1.0 + h.score),
                            normalized: None,
                            weight: None,
                            weighted: None,
                            rank: Some(i + 1),
                        }),
                        bm25: None,
                        filters: None,
                    })
                    .collect();
                Ok((run.hits, trace))
            }
        }
    }

    fn bm25_stages(&self, run: &Bm25Run, query: &str, bucket: Option<&str>, trace: &mut SearchTrace) {
        let (terms, n_docs) = {
            let g = self.inner.read();
            (g.bm25.query_terms(query), g.bm25.len())
        };
        let term_list = terms
            .iter()
            .map(|t| format!("'{}' (in {} docs)", t.term, t.df))
            .collect::<Vec<_>>()
            .join(", ");
        let mut detail = format!(
            "Scored the {} documents containing at least one query term — {term_list} — and kept \
             the top {}",
            run.matched, run.raw
        );
        if bucket.is_some() {
            detail.push_str(&format!(" (asked for {} to leave room for the bucket filter)", run.fetch));
        }
        detail.push('.');
        let term_attrs: Vec<serde_json::Value> = terms
            .iter()
            .map(|t| serde_json::json!({ "term": t.term, "df": t.df, "idf": t.idf }))
            .collect();
        trace.stages.push(
            Stage::new("bm25", "Keyword search (BM25)", detail)
                .rows(Some(n_docs), Some(run.raw))
                .took(run.took)
                .attr("terms", term_attrs)
                .attr("matched_docs", run.matched)
                .attr("fetch", run.fetch),
        );
        if let Some(b) = bucket {
            trace.stages.push(bucket_stage(
                "bm25",
                b,
                run.raw,
                run.examined,
                run.fetch.min(run.raw).max(run.hits.len()),
                run.hits.len(),
                run.other_bucket,
                run.missing,
            ));
        }
        for t in &terms {
            if t.df == 0 {
                trace.notes.push(Note::info(format!(
                    "'{}' is not in the index vocabulary and matched nothing.",
                    t.term
                )));
            } else if n_docs > 0 && (t.df as usize) * 2 >= n_docs {
                trace.notes.push(Note::info(format!(
                    "'{}' appears in {}% of documents, so its IDF is {:.3} and it barely \
                     influences ranking.",
                    t.term,
                    (t.df as usize) * 100 / n_docs,
                    t.idf
                )));
            }
        }
    }

    fn hybrid_hit_explains(&self, run: &HybridRun, query: &str) -> Vec<HitExplain> {
        let (w_vec, w_bm) = run.weights;
        let g = self.inner.read();
        run.hits
            .iter()
            .enumerate()
            .map(|(i, h)| {
                let p = run.parts.get(&h.id).copied().unwrap_or_default();
                let terms = g
                    .by_key
                    .get(&(h.bucket.clone(), h.id.clone()))
                    .map(|id| g.bm25.explain_doc(query, id.0, &h.text))
                    .unwrap_or_default()
                    .into_iter()
                    .map(|t| TermPart {
                        term: t.term,
                        tf: t.tf,
                        df: t.df,
                        idf: t.idf,
                        contribution: t.contribution,
                    })
                    .collect();
                HitExplain {
                    id: h.id.clone(),
                    rank: i + 1,
                    final_score: h.score,
                    score_kind: ScoreKind::Fused,
                    vector: p.vector.map(|(rank, distance, similarity, n)| VectorPart {
                        distance,
                        similarity,
                        normalized: Some(n),
                        weight: Some(w_vec),
                        weighted: Some(w_vec * n),
                        rank: Some(rank),
                    }),
                    bm25: Some(match p.bm25 {
                        Some((rank, score, n)) => Bm25Part {
                            score,
                            normalized: Some(n),
                            weight: Some(w_bm),
                            weighted: Some(w_bm * n),
                            rank: Some(rank),
                            terms,
                        },
                        // Not in the keyword top-k; show its terms anyway
                        // so "why didn't the keywords count?" is answerable.
                        None => Bm25Part {
                            score: 0.0,
                            normalized: None,
                            weight: Some(w_bm),
                            weighted: Some(0.0),
                            rank: None,
                            terms,
                        },
                    }),
                    filters: None,
                }
            })
            .collect()
    }
}

fn vector_stages(run: &VectorRun, bucket: Option<&str>, hybrid: bool, trace: &mut SearchTrace) {
    let s = &run.stats;
    let live_nodes = s.nodes_total.saturating_sub(s.tombstones_total);
    let mut detail = format!(
        "Walked {} graph layer{} and distance-computed {} nodes, kept the {} closest (ef={})",
        s.levels,
        if s.levels == 1 { "" } else { "s" },
        s.visited,
        s.pool,
        s.ef
    );
    if s.tombstoned_in_pool > 0 {
        detail.push_str(&format!(
            ", {} of which were deleted documents and were discarded",
            s.tombstoned_in_pool
        ));
    }
    detail.push_str(&format!(", and returned {} candidates", run.raw));
    if bucket.is_some() {
        detail.push_str(&format!(
            " (asked for {} — over-fetched because the bucket filter runs after the search)",
            run.fetch
        ));
    }
    detail.push('.');
    trace.stages.push(
        Stage::new("hnsw", "Vector search (HNSW)", detail)
            .rows(Some(live_nodes), Some(run.raw))
            .took(run.hnsw_took)
            .attr("ef", s.ef)
            .attr("requested", run.fetch)
            .attr("layers", s.levels)
            .attr("visited", s.visited)
            .attr("beam", s.pool)
            .attr("deleted_in_beam", s.tombstoned_in_pool)
            .attr("graph_nodes", s.nodes_total)
            .attr("graph_deleted", s.tombstones_total),
    );
    if let Some(b) = bucket {
        trace.stages.push(
            bucket_stage("vector", b, run.raw, run.examined, run.k, run.hits.len(), run.other_bucket, run.missing)
                .took(run.assemble_took),
        );
        let wanted = if hybrid {
            format!("{} candidates the vector stage needed", run.k)
        } else {
            format!("{} requested results", run.k)
        };
        if run.hits.is_empty() && run.raw > 0 {
            trace.notes.push(Note::warn(format!(
                "None of the {} nearest candidates is in bucket '{b}'. Check the bucket name — it \
                 may not exist or be empty — or, if it is a small share of the corpus, raise \
                 top_k or ef.",
                run.raw
            )));
        } else if run.hits.len() < run.k && run.raw >= run.fetch {
            // HNSW returned everything asked for, yet the bucket filter
            // left fewer than k: more matches exist further out. (If
            // HNSW returned less than asked, the graph was exhausted.)
            trace.notes.push(Note::warn(format!(
                "Only {} of the {wanted} are in bucket '{b}' among the {} nearest candidates. The \
                 bucket filter runs after the vector search, so a bucket that is a small share of \
                 the corpus starves results; raise top_k or ef to look further.",
                run.hits.len(),
                run.raw
            )));
        }
    }
    if s.pool > 0 && s.tombstoned_in_pool * 100 / s.pool >= TOMBSTONE_BEAM_WARN_PCT {
        trace.notes.push(Note::warn(format!(
            "{} of the {} best HNSW candidates ({}%) were deleted documents. Deleted nodes are \
             never removed from the graph, so they take beam slots from live results and cost \
             recall; raise ef or rebuild the index.",
            s.tombstoned_in_pool,
            s.pool,
            s.tombstoned_in_pool * 100 / s.pool
        )));
    } else if s.nodes_total > 0 && s.tombstones_total * 5 >= s.nodes_total {
        trace.notes.push(Note::info(format!(
            "The graph holds {} deleted nodes ({}% of {}); every search still walks through them.",
            s.tombstones_total,
            s.tombstones_total * 100 / s.nodes_total,
            s.nodes_total
        )));
    }
}

/// Rows chain from the retrieval stage (`candidates` in → `kept` out),
/// and the detail says how far down the list the filter had to look.
#[allow(clippy::too_many_arguments)]
fn bucket_stage(
    source: &str,
    bucket: &str,
    candidates: usize,
    examined: usize,
    wanted: usize,
    kept: usize,
    other: usize,
    missing: usize,
) -> Stage {
    let mut detail = format!(
        "Walked the {candidates} {source} candidates in rank order keeping those in bucket \
         '{bucket}': kept {kept} after examining {examined}, {other} belonged to other buckets"
    );
    if missing > 0 {
        detail.push_str(&format!(", {missing} were deleted mid-search"));
    }
    if kept >= wanted && examined < candidates {
        detail.push_str(&format!(
            "; stopped once {wanted} were found, leaving {} unexamined",
            candidates - examined
        ));
    }
    detail.push('.');
    Stage::new("bucket_filter", "Bucket filter", detail)
        .rows(Some(candidates), Some(kept))
        .attr("examined", examined)
        .attr("bucket", bucket)
        .attr("source", source)
        .attr("other_bucket", other)
}

#[cfg(test)]
mod tests {
    use super::flat_stage_note;

    #[test]
    fn flat_stage_note_only_when_scores_cannot_discriminate() {
        assert!(flat_stage_note("vector", &[0.88, 0.88, 0.88], 0.5).is_some());
        assert!(flat_stage_note("vector", &[0.88, 0.91], 0.5).is_none());
        assert!(flat_stage_note("vector", &[0.88], 0.5).is_none(), "one candidate is not a tie");
        assert!(flat_stage_note("vector", &[], 0.5).is_none());
    }
}
