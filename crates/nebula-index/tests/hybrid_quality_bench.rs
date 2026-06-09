//! Retrieval-quality benchmark: hybrid (BM25 + vector) vs vector-only
//! vs BM25-only (design 0008 §6/§9).
//!
//! # What this measures
//!
//! Recall@k and MRR on a synthetic labeled corpus. Each "relevant" doc
//! is tagged with a rare token (e.g. `errcode_4821`); the query for
//! that doc is the rare token plus common filler. A retrieval method
//! "hits" if the relevant doc is in its top-k.
//!
//! # The honest caveat (read before trusting the numbers)
//!
//! This test uses [`MockEmbedder`], which is a **hash**, not a language
//! model (see `nebula-embed/src/mock.rs`). A query string therefore
//! embeds to a vector unrelated to a document that merely *contains*
//! the query's tokens. Consequences:
//!
//! - **Vector-only recall here is ~chance.** That is NOT a claim that
//!   real dense retrieval is useless — it's the expected behaviour of a
//!   content-agnostic hash embedder on a lexical-overlap task.
//! - **BM25 / hybrid recall here reflects the lexical signal only.**
//!   With a real embedder, the vector half would add *semantic* recall
//!   (paraphrase, synonymy) on top.
//!
//! So this bench isolates and quantifies the **lexical contribution**
//! that hybrid adds over a pure-vector store on exact-token queries —
//! the gap that motivated design 0008 §6. To benchmark semantic recall,
//! re-run against a real embedder (point `NEBULA_BENCH_EMBED` at an
//! OpenAI-compatible endpoint — not wired here to keep the test offline)
//! — left as a follow-up since CI must stay network-free.
//!
//! # How to run
//!
//! ```text
//! cargo test --release -p nebula-index --test hybrid_quality_bench -- --ignored --nocapture
//!
//! # Larger corpus:
//! NEBULA_BENCH_DOCS=5000 NEBULA_BENCH_QUERIES=500 \
//!   cargo test --release -p nebula-index --test hybrid_quality_bench -- --ignored --nocapture
//! ```

use std::sync::Arc;

use nebula_embed::{Embedder, MockEmbedder};
use nebula_index::TextIndex;
use nebula_vector::{HnswConfig, Metric};

const COMMON_WORDS: &[&str] = &[
    "the", "system", "configuration", "guide", "service", "network",
    "policy", "access", "update", "deploy", "cluster", "node", "data",
    "request", "response", "error", "status", "metric", "log", "trace",
];

fn filler(seed: usize, n: usize) -> String {
    (0..n)
        .map(|i| COMMON_WORDS[(seed.wrapping_mul(31).wrapping_add(i * 7)) % COMMON_WORDS.len()])
        .collect::<Vec<_>>()
        .join(" ")
}

struct Metrics {
    recall_at_k: f64,
    mrr: f64,
}

/// Accumulate recall@k and MRR given a slice of (ranked result ids) per
/// query, aligned to `queries`. Pure and synchronous — retrieval is
/// done by the caller so this stays async-free.
fn score(queries: &[(String, String)], results: &[Vec<String>]) -> Metrics {
    let mut hits = 0usize;
    let mut rr_sum = 0.0f64;
    for ((_, want_id), ranked) in queries.iter().zip(results) {
        if let Some(rank) = ranked.iter().position(|id| id == want_id) {
            hits += 1;
            rr_sum += 1.0 / (rank as f64 + 1.0);
        }
    }
    let n = queries.len() as f64;
    Metrics {
        recall_at_k: hits as f64 / n,
        mrr: rr_sum / n,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "quality benchmark — run explicitly with --ignored --nocapture"]
async fn hybrid_vs_vector_vs_bm25_recall() {
    let n_docs: usize = std::env::var("NEBULA_BENCH_DOCS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2000);
    let n_queries: usize = std::env::var("NEBULA_BENCH_QUERIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);
    let k = 10usize;

    let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::new(64));
    let index = Arc::new(TextIndex::new(emb, Metric::Cosine, HnswConfig::default()).unwrap());

    // Build a corpus. Every doc is mostly common filler; a subset carry
    // a unique rare token that a query will later search for.
    let mut queries: Vec<(String, String)> = Vec::with_capacity(n_queries);
    let query_stride = (n_docs / n_queries).max(1);
    for i in 0..n_docs {
        let id = format!("doc{i}");
        let body = filler(i, 20);
        let text = if i % query_stride == 0 && queries.len() < n_queries {
            // Plant a rare token and register the matching query.
            let rare = format!("errcode_{:05}", i);
            queries.push((format!("{rare} {}", filler(i + 1, 3)), id.clone()));
            format!("{body} {rare}")
        } else {
            body
        };
        index
            .upsert_text("bench", &id, &text, serde_json::json!({}))
            .await
            .unwrap();
    }

    println!(
        "\n=== hybrid quality bench: {n_docs} docs, {} labeled queries, k={k} ===",
        queries.len()
    );
    println!("(MockEmbedder is a hash — vector-only recall reflects chance, not real dense retrieval; see file header)\n");

    // Helper: run all queries through a search closure, awaiting each.
    async fn run_all<'a, Fut>(
        queries: &'a [(String, String)],
        k: usize,
        mut f: impl FnMut(&'a str, usize) -> Fut,
    ) -> Vec<Vec<String>>
    where
        Fut: std::future::Future<Output = Vec<String>>,
    {
        let mut out = Vec::with_capacity(queries.len());
        for (q, _) in queries {
            out.push(f(q, k).await);
        }
        out
    }

    // Vector-only.
    let vec_results = run_all(&queries, k, |q, k| {
        let idx = Arc::clone(&index);
        async move {
            idx.search_text(q, Some("bench"), k, None)
                .await
                .unwrap()
                .into_iter()
                .map(|h| h.id)
                .collect()
        }
    })
    .await;
    let vec_m = score(&queries, &vec_results);

    // BM25-only (synchronous).
    let bm_results: Vec<Vec<String>> = queries
        .iter()
        .map(|(q, _)| {
            index
                .search_bm25(q, Some("bench"), k)
                .into_iter()
                .map(|h| h.id)
                .collect()
        })
        .collect();
    let bm_m = score(&queries, &bm_results);

    // Hybrid at a few weight settings to show the §9 tuning knob.
    let mut hybrid_default_recall = 0.0;
    for (label, w) in [
        ("hybrid 0.5/0.5", (0.5f32, 0.5f32)),
        ("hybrid 0.7/0.3", (0.7, 0.3)),
        ("hybrid 0.3/0.7", (0.3, 0.7)),
    ] {
        let results = run_all(&queries, k, |q, k| {
            let idx = Arc::clone(&index);
            async move {
                idx.search_text_hybrid(q, Some("bench"), k, None, w)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|h| h.id)
                    .collect()
            }
        })
        .await;
        let m = score(&queries, &results);
        if w == (0.5, 0.5) {
            hybrid_default_recall = m.recall_at_k;
        }
        report(label, &m);
    }

    report("vector-only", &vec_m);
    report("bm25-only", &bm_m);

    // The point of the bench: hybrid must not regress below vector-only
    // on this lexical task — it strictly adds the lexical signal.
    assert!(
        hybrid_default_recall >= vec_m.recall_at_k,
        "hybrid ({hybrid_default_recall}) should not lose to vector-only ({}) on a lexical task",
        vec_m.recall_at_k
    );
}

fn report(label: &str, m: &Metrics) {
    println!(
        "{label:>16}:  recall@k = {:.3}   MRR = {:.3}",
        m.recall_at_k, m.mrr
    );
}
