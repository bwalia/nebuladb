import type { Explain } from "./api";

/**
 * Hand-written `Explain` payloads covering every rendering branch of
 * `ExplainPanel` (SQL analyze + plan-only, hybrid search with BM25 terms,
 * RAG with a prompt). A dev aid: render them to eyeball the panel without
 * a server, and they fail `tsc` if the contract types drift.
 */

export const sqlAnalyzeFixture: Explain = {
  kind: "sql",
  analyzed: true,
  summary:
    "Embedded 'flowers', fetched the 32 nearest leads from the HNSW index, kept 3 where city = 'London', and returned 3 of the requested 5.",
  total_us: 5840,
  stages: [
    { name: "parse", label: "Parse SQL", detail: "sqlparser GenericDialect", rows_in: null, rows_out: null, took_us: 38, attrs: {} },
    {
      name: "plan",
      label: "Plan",
      detail: "Single-bucket scan: semantic_match + 1 residual filter",
      rows_in: null,
      rows_out: null,
      took_us: 12,
      attrs: { node: "scan", bucket: "leads" },
    },
    { name: "cache", label: "Result cache", detail: "Bypassed — EXPLAIN always executes", rows_in: null, rows_out: null, took_us: 0, attrs: { status: "bypassed" } },
    { name: "embed", label: "Embed query", detail: "mock-384 → 384-dim vector", rows_in: null, rows_out: null, took_us: 410, attrs: { model: "mock-384", dim: 384 } },
    {
      name: "hnsw",
      label: "Vector search (HNSW)",
      detail: "Fetched 32 nearest candidates (LIMIT 5 × 4 over-fetch because 1 filter follows), ef=64",
      rows_in: 2417950,
      rows_out: 32,
      took_us: 4920,
      attrs: { fetch_k: 32, ef: 64, visited: 1843, pool: 64, tombstoned_in_pool: 41, entry_level: 4, metric: "cosine" },
    },
    { name: "filter", label: "Filter city = 'London'", detail: "Residual predicate on metadata", rows_in: 32, rows_out: 3, took_us: 21, attrs: { predicate: "city = 'London'", path: ["city"] } },
    { name: "sort", label: "Sort", detail: "score ASC (distance, closest first)", rows_in: 3, rows_out: 3, took_us: 2, attrs: {} },
    { name: "limit", label: "Limit", detail: "LIMIT 5", rows_in: 3, rows_out: 3, took_us: 1, attrs: { limit: 5 } },
    { name: "project", label: "Project", detail: "id, company_name, city", rows_in: 3, rows_out: 3, took_us: 4, attrs: { columns: ["id", "company_name", "city"] } },
  ],
  notes: [
    { level: "warn", message: "Filter starvation: 32 candidates → 3 rows, fewer than LIMIT 5. Raise LIMIT to widen the candidate net." },
    { level: "warn", message: "41 of the 64 HNSW pool slots were deleted (tombstoned) nodes." },
    { level: "info", message: "Result cache bypassed so the timings are real." },
  ],
  hits: [
    {
      id: "02354940",
      rank: 1,
      final_score: 0.1234,
      score_kind: "distance",
      vector: { distance: 0.1234, similarity: 0.8902, normalized: null, weight: null, weighted: null, rank: 4 },
      bm25: null,
      filters: [{ predicate: "city = 'London'", actual: "London", passed: true }],
    },
    {
      id: "02354941",
      rank: 2,
      final_score: 0.2011,
      score_kind: "distance",
      vector: { distance: 0.2011, similarity: 0.8326, normalized: null, weight: null, weighted: null, rank: 9 },
      bm25: null,
      filters: [{ predicate: "status IN ('1', '2')", actual: null, passed: false }],
    },
  ],
  plan: {
    node: "scan",
    bucket: "leads",
    semantic: { kind: "match", column: "text", query: "flowers" },
    filters: [{ op: "eq", path: ["city"], value: "London" }],
    projection: { kind: "columns", cols: ["id", "company_name", "city"] },
    order_by: null,
    limit: 5,
  },
  prompt: null,
  text: [
    "Limit  (limit=5)  (rows=3) (time=0.001ms)",
    "  ->  Filter  city = 'London'  (rows 32 → 3) (time=0.021ms)",
    "        ->  HNSW Scan on leads  semantic_match(text, 'flowers')  k=32 ef=64  (rows=32) (time=4.920ms)",
    "Planning Time: 0.050 ms",
    "Execution Time: 5.840 ms",
  ],
};

export const sqlPlanOnlyFixture: Explain = {
  kind: "sql",
  analyzed: false,
  summary: "Would embed 'training', fetch 40 candidates from leads, filter on city, sort by score and return up to 10 rows.",
  total_us: 0,
  stages: [
    { name: "hnsw", label: "Vector search (HNSW)", detail: "k=40 (LIMIT 10 × 4 over-fetch because 1 filter follows)", rows_in: null, rows_out: null, took_us: 0, attrs: { fetch_k: 40 } },
    { name: "filter", label: "Filter city IN (...)", detail: "Residual predicate on metadata", rows_in: null, rows_out: null, took_us: 0, attrs: {} },
  ],
  notes: [],
  hits: [],
  plan: { node: "scan", bucket: "leads" },
  prompt: null,
  text: ["Limit  (limit=10)", "  ->  Filter  city IN ('London', 'Leeds')", "        ->  HNSW Scan on leads  k=40"],
};

export const hybridSearchFixture: Explain = {
  kind: "search",
  analyzed: true,
  summary: "Hybrid: 20 vector candidates and 20 BM25 keyword candidates were min-max normalized and fused 0.7 × vector + 0.3 × keywords.",
  total_us: 12900,
  stages: [
    { name: "embed", label: "Embed query", detail: "mock-384", rows_in: null, rows_out: null, took_us: 300, attrs: { model: "mock-384" } },
    { name: "hnsw", label: "Vector search (HNSW)", detail: "20 candidates", rows_in: null, rows_out: 20, took_us: 4100, attrs: { fetch_k: 20 } },
    {
      name: "bm25",
      label: "Keyword search (BM25)",
      detail: "Scored docs containing any query term",
      rows_in: null,
      rows_out: 20,
      took_us: 8300,
      attrs: { terms: [{ term: "florist", df: 812, idf: 7.9 }, { term: "london", df: 402113, idf: 1.8 }], k1: 1.2, b: 0.75 },
    },
    { name: "fuse", label: "Fuse", detail: "0.7 × vector + 0.3 × BM25", rows_in: 38, rows_out: 5, took_us: 200, attrs: { weights: [0.7, 0.3] } },
  ],
  notes: [{ level: "info", message: "'london' appears in 17% of docs, so it barely moves BM25 scores (idf 1.8)." }],
  hits: [
    {
      id: "lead-9",
      rank: 1,
      final_score: 0.93,
      score_kind: "fused",
      vector: { distance: 0.21, similarity: 0.8264, normalized: 0.9, weight: 0.7, weighted: 0.63, rank: 2 },
      bm25: {
        score: 11.42,
        normalized: 1.0,
        weight: 0.3,
        weighted: 0.3,
        rank: 1,
        terms: [
          { term: "florist", tf: 2, df: 812, idf: 7.9, contribution: 10.1 },
          { term: "london", tf: 1, df: 402113, idf: 1.8, contribution: 1.32 },
        ],
      },
      filters: null,
    },
    {
      id: "lead-3",
      rank: 2,
      final_score: 0.3,
      score_kind: "fused",
      vector: null,
      bm25: { score: 9.0, normalized: 1.0, weight: 0.3, weighted: 0.3, rank: 2, terms: [] },
      filters: null,
    },
  ],
  plan: null,
  prompt: null,
  text: [],
};

export const ragFixture: Explain = {
  kind: "rag",
  analyzed: true,
  summary: "Retrieved 3 chunks by vector similarity, built a 1,204-char prompt, and llama3.1:8b answered in 2.1s (first token 380ms).",
  total_us: 2_140_000,
  stages: [
    { name: "embed", label: "Embed query", detail: "mock-384", rows_in: null, rows_out: null, took_us: 350, attrs: {} },
    { name: "hnsw", label: "Vector search (HNSW)", detail: "3 nearest chunks", rows_in: null, rows_out: 3, took_us: 3900, attrs: { fetch_k: 3 } },
    { name: "prompt", label: "Build prompt", detail: "3 chunks numbered [0]..[2] + question", rows_in: 3, rows_out: null, took_us: 40, attrs: { chars: 1204 } },
    { name: "llm", label: "Generate answer", detail: "llama3.1:8b via Ollama", rows_in: null, rows_out: null, took_us: 2_135_000, attrs: { model: "llama3.1:8b", ttft_us: 380000, answer_chars: 412 } },
  ],
  notes: [],
  hits: [
    { id: "lead-1", rank: 1, final_score: 0.11, score_kind: "distance", vector: { distance: 0.11, similarity: 0.9009, normalized: null, weight: null, weighted: null, rank: 1 }, bm25: null, filters: null },
  ],
  plan: null,
  prompt: "Use the context to answer.\n\n[0] County: Kent\nTypeOfBusiness: Florists\n\nQuestion: florist in London?",
  text: ["RAG  (time=2140.000ms)", "  ->  LLM llama3.1:8b  (time=2135.000ms)", "  ->  HNSW Scan  k=3  (rows=3)"],
};

export const allExplainFixtures: Explain[] = [
  sqlAnalyzeFixture,
  sqlPlanOnlyFixture,
  hybridSearchFixture,
  ragFixture,
];
