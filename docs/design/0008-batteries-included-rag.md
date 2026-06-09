# Design 0008: Batteries-Included RAG

- **Status**: Proposed — Phase 1 in progress
- **Author**: Claude + @bwalia
- **Created**: 2026-06-09
- **Tracks**: product differentiator — "upload documents → query immediately → relevant answers, no config"
- **Relates to**: existing `nebula-{chunk,embed,vector,llm,index,sql}` crates; the `/ai/rag` SSE path and `semantic_match()` SQL already shipped

---

## 1. Why this exists

Every vector-DB competitor (pgvector, Mongo Atlas Search, Couchbase Capella)
makes the developer assemble the retrieval pipeline by hand: chunking,
embeddings, an index, hybrid fusion, reranking, prompt construction. Weeks of
glue before the first useful answer.

NebulaDB already owns the hard primitives that glue normally sits on top of:

- `Embedder` trait + multi-tier embedding cache (`nebula-embed`, `nebula-cache`)
- HNSW vector index, sub-50ms search (`nebula-vector`, `nebula-index`)
- `Chunker` trait + chunk-aware document upsert (`nebula-chunk`,
  `TextIndex::upsert_document`)
- Streaming RAG over `LlmClient` with `build_rag_prompt` (`nebula-llm`,
  `/ai/rag`)
- `semantic_match()` / `vector_distance()` in SQL (`nebula-sql`)

What's missing is the **convention layer** that turns those primitives into a
one-call experience, plus the **retrieval-quality** pieces (lexical fusion,
rerank) that separate "stores vectors" from "returns the right answer."

This design adds that layer **without bloating the DB binary**: every heavy or
native-dependency stage (PDF/DOCX/HTML extraction, neural cross-encoder rerank)
runs as a **pluggable HTTP stage or an out-of-process ingestion worker**,
mirroring how `Embedder` and `LlmClient` already keep vendor code out of the
core.

## 2. Goal

A developer does:

```sql
CREATE BUCKET docs;
INSERT INTO docs VALUES ('company_handbook.pdf');
SELECT ai_answer('What is the company holiday policy?');
```

and gets a grounded answer with source attribution, having configured nothing.

## 3. Design principles

| Principle | Consequence |
|---|---|
| **Convention over configuration** | Sensible defaults at every stage; config is optional, per-collection. |
| **Keep the DB binary lean** | PDF/DOCX/HTML parsing and neural rerank are **out-of-process** (sidecar worker) or **pluggable HTTP** stages. No native parsing/ONNX deps linked into `nebula-server`. |
| **Reuse the trait-swap pattern** | New capabilities (`Reranker`, `QueryExpander`, hybrid retriever) are traits with a zero-dep default impl + an HTTP-backed impl, exactly like `Embedder`/`LlmClient`. |
| **No storage churn** | Attribution and metadata ride the existing `Hit { bucket, id, text, score, metadata }` and the document's `metadata` JSON column. The `doc_id#chunk_index` external-id convention already exists. |

## 4. Target architecture

```
                 ┌──────────────── nebula-server (lean core) ────────────────┐
ingest-worker    │                                                            │
(sidecar) ──POST─┼─► /documents ─► Chunker ─► Embedder ─► HNSW + BM25 index   │
 - file detect   │                                                            │
 - extract       │   query path:                                             │
 - metadata LLM  │   ai_answer / /rag/answer                                  │
                 │     └─► QueryExpander ─► [BM25 ∪ Vector] ─► Fusion         │
                 │            ─► Reranker(HTTP|noop) ─► context ─► LlmClient   │
                 └────────────────────────────────────────────────────────────┘
                       Reranker sidecar (cross-encoder) ◄── HTTP, optional
```

The single retrieval contract every stage speaks:

```rust
pub struct RetrievedChunk {
    pub doc_id: String,        // parent doc (external_id before '#')
    pub chunk_index: usize,    // ordinal within the doc
    pub text: String,
    pub score: f32,            // stage-local; meaning depends on stage
    pub source: String,        // human-facing doc name for attribution
    pub metadata: serde_json::Value,
}
```

This is the seam. Phases 2–5 add stages that consume and produce
`RetrievedChunk`; Phase 1's answer layer is the first consumer.

## 5. Phase 1 — `ai_answer()` + `POST /rag/answer` (this PR)

Highest leverage, ~80% reuse. No index/storage changes.

- New **non-streaming** answer path: retrieve via existing
  `TextIndex::search_text_blocking` → `build_rag_prompt` → drain the
  `LlmClient` stream to a `String` → return
  `{ answer, sources: [{ doc, chunk, score }] }`.
- Attribution is free: `Hit` already carries `id` (=`doc_id#chunk_index`),
  `score`, and `metadata`. Split the id on `'#'` for doc + chunk.
- SQL: add `ai_answer('...')` as a scalar retrieval clause alongside
  `semantic_match` in `nebula-sql` (`plan.rs` parse, `executor.rs` dispatch).
- REST: `POST /rag/answer` in `router.rs`, beside the existing streaming
  `/ai/rag`.

Out of scope for Phase 1 (later phases): BM25, fusion, rerank, query
expansion, file parsing, `CREATE RAG COLLECTION`.

## 6. Phase 2 — Hybrid retrieval (BM25 + vector fusion)

The real quality moat vs pgvector. **Lexical + vector only — no neural model.**

- BM25 inverted index over the same chunk corpus held in `inner.docs`, built
  on the existing WAL-replay path so recovery rebuilds it for free.
- Fusion: min-max normalize each stage, weighted sum
  `0.4·BM25 + 0.4·vector + 0.2·metadata`, **weights per-collection config**,
  not hardcoded.
- `semantic_match` and `/rag/answer` opt into fusion behind a flag;
  vector-only stays the default until benchmarked.

## 7. Phase 3 — Pluggable rerank + query expansion

- `Reranker` trait: `NoopReranker` (default) + `HttpReranker` (POSTs
  query+candidates to a cross-encoder sidecar). Slots between fusion and
  context assembly.
- `QueryExpander` trait: cheap synonym/abbreviation map default (e.g.
  `VPN → Virtual Private Network`), optional LLM-backed expander reusing
  `LlmClient`.
- Both are zero native deps in-process.

## 8. Phase 4 — Ingestion sidecar (file types + extraction + metadata)

- `apps/ingest-worker` (or an operator job): detect type → extract text →
  pick chunk strategy → POST `/documents`. Keeps PDF/DOCX/HTML CVE surface
  out of the DB binary. Reuses the existing K8s operator + multi-region compose.
- `nebula-chunk`: add `HeadingAwareChunker`, `CodeAwareChunker`, and a pure-Rust
  `select_strategy(doc_type)` (safe to keep in-process).
- AI metadata (summary/keywords/entities) generated in the worker via
  `LlmClient`, written to the existing document `metadata` JSON — no schema
  change.

## 9. Phase 5 — `CREATE RAG COLLECTION` + dashboard + metrics

- SQL DDL binding a bucket to `{ embedding model, chunk strategy, fusion
  weights, rerank stage }`, stored as bucket config.
- Metrics: extend the existing Prometheus block in `router.rs` (already emits
  embed-cache counters) with `chunks_created`, `rerank_latency`, `rag_latency`.
- Showcase React app: RAG overview panel.

## 10. Explicit deviations from the source spec

- **`answer_accuracy_score` as a live metric is dropped.** There is no
  ground truth at query time; accuracy needs an offline eval harness, tracked
  separately, not a Prometheus gauge.
- **No new Redis-compatible cache layer.** Reuse `nebula-redis-cache` for
  retrieval/prompt caching.
- **Fusion weights are per-collection, not a hardcoded `0.4/0.4/0.2`** from
  Phase 2 onward.
- **PDF/DOCX/neural-rerank are out-of-process**, against the spec's implied
  in-DB processing, to keep the single binary lean and reduce native CVE
  surface.

## 11. Sequencing

Phase 0 (this doc) → Phase 1 (fast win, all reuse) → Phase 2 (the moat).
Phases 3–5 are independent and may be reordered by demo priority.
