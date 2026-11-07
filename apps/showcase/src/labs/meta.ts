import type { LabId } from "./types.ts";

export type Concept = "search" | "models" | "filters" | "rag" | "reasoning" | "safety" | "scale";

export interface LabMeta {
  id: LabId;
  /** Short, plain hook shown on the card. */
  hook: string;
  title: string;
  lead: string;
  /** What actually runs — named model, metric, endpoint. */
  method: string;
  /** What the result does not show. */
  limits: string;
  concept: Concept;
  minutes: 2 | 5;
  /** What the lab needs to be meaningful. */
  needs: ("embedder" | "llm" | "browser")[];
}

export const LABS: LabMeta[] = [
  {
    id: "semantic",
    hook: "Words or meaning?",
    title: "Meaning vs words",
    lead: "Ask one question three ways — exact words (BM25), meaning (vectors) and both fused — over the same business documents, and see which wins and why.",
    method:
      "Keyword: BM25 (k1 1.2, b 0.75) over the pack's documents, computed in your browser. Vector and hybrid: POST /api/v1/ai/search on the pack's NebulaDB bucket with the server embedder and HNSW, hybrid fusing BM25 and vector scores (explain: true shows both parts per hit).",
    limits:
      "A top score means close, not correct. The pack is a few dozen documents, so keyword search does better here than it would on millions of messy ones. With the mock embedder, vector scores are a hash, not meaning.",
    concept: "search",
    minutes: 2,
    needs: ["embedder"],
  },
  {
    id: "models",
    hook: "Which model for this business?",
    title: "Model shoot-out",
    lead: "Run every labelled question for the pack through each retriever, score them, and get a recommendation with the numbers behind it.",
    method:
      "Retrievers: BM25 (browser), the server's embedder via NebulaDB vector search, NebulaDB hybrid, and MiniLM-L6-v2 running in your browser (transformers.js, cosine over the same docs). Scores: hit@1, hit@5, MRR and recall@10 against the pack's labelled answers, plus median latency.",
    limits:
      "Twenty-odd questions per pack is a smoke test, not a benchmark — differences of a few points are noise. Browser MiniLM latency is your laptop, not a server. Labels were written by hand and can be wrong.",
    concept: "models",
    minutes: 5,
    needs: ["embedder", "browser"],
  },
  {
    id: "filters",
    hook: "Meaning, inside the rules",
    title: "Filters that matter",
    lead: "Combine semantic search with hard business rules — site, tenant, crop, stock, price — in one SQL query, and watch what a filter does to recall.",
    method:
      "One NebulaDB SQL query: semantic_match(text, …) plus metadata predicates (=, array_contains, <=). NebulaDB retrieves LIMIT × 4 candidates (at least 32) by vector, then applies the filters. The 'should have found' list is computed in your browser from the pack's metadata.",
    limits:
      "Filters run after retrieval, so a very narrow filter with a small LIMIT can return fewer rows than exist — that's the point of the recall meter, not a bug in the meter. Raising LIMIT fixes it at the cost of more candidates.",
    concept: "filters",
    minutes: 2,
    needs: ["embedder"],
  },
  {
    id: "rag",
    hook: "With and without the documents",
    title: "RAG, step by step",
    lead: "The same model answers twice: once from memory, once grounded in retrieved passages. Every sentence of the grounded answer links to the passage it came from.",
    method:
      "Retrieve top-k from the pack bucket (vector or hybrid, optional persona filter) → assemble numbered sources within a token budget → POST /api/v1/ai/chat with the same model for both answers. Citations [n] are parsed from the reply; sentences without one are flagged.",
    limits:
      "A citation shows where a sentence claims to come from — it doesn't prove the passage supports it. Read the passage. The no-retrieval answer can be right by luck; the mock LLM echoes rather than reasons.",
    concept: "rag",
    minutes: 5,
    needs: ["embedder", "llm"],
  },
  {
    id: "reason",
    hook: "Plan, query, then answer",
    title: "Reason over the data",
    lead: "Questions that need numbers and documents together. See the plan, every SQL and search call it runs against NebulaDB, and an answer whose numbers are checked against the rows.",
    method:
      "Plan: from the LLM (asked for a JSON list of sql/search steps) or the pack's reference plan. Each step runs live: SQL via /api/v1/query, search via /api/v1/ai/search. The final answer is generated from the step outputs, then every number in it is matched against the returned rows.",
    limits:
      "The trace shows the steps and their outputs — not the model's private reasoning. The number check catches invented figures, not wrong conclusions drawn from right figures.",
    concept: "reasoning",
    minutes: 5,
    needs: ["embedder", "llm"],
  },
  {
    id: "refuse",
    hook: "Knowing when not to answer",
    title: "When to say no",
    lead: "Unsafe, off-topic and other people's data. See which guard stops each question — a rule in code, an empty retrieval, or an access filter — and score them all.",
    method:
      "Safety rules: phrase triggers in code, checked before any model call, returning a fixed card. Out of scope: retrieval returns nothing relevant above the threshold (hybrid search; keyword-overlap check), so the app says so. Access: the persona's filter is in the SQL, so other rows never leave the database.",
    limits:
      "Phrase triggers miss rewordings and can fire on innocent text; real deployments layer classifiers and human review on top. The threshold is tuned on this pack only.",
    concept: "safety",
    minutes: 2,
    needs: ["embedder"],
  },
  {
    id: "scale",
    hook: "From 80 documents to millions",
    title: "Scale it up",
    lead: "Run the same searches against the small pack and against the largest bucket on this server. Sweep the HNSW ef setting and watch latency trade against agreement.",
    method:
      "Repeated POST /api/v1/ai/search calls with ef ∈ {16, 32, 64, 128, 256, 512}. Latency is measured in your browser (network included) and as the server's took_ms. Agreement is the overlap@10 with the ef = 512 result.",
    limits:
      "Agreement with ef 512 is not recall against exact search — it's a proxy, honest about being one. Latency includes your network and whatever else the server is doing; run it more than once.",
    concept: "scale",
    minutes: 5,
    needs: ["embedder"],
  },
  {
    id: "map",
    hook: "See the shape of the data",
    title: "See the space",
    lead: "Every document of the pack as a point, coloured by a business field. Your question lands among its nearest neighbours.",
    method:
      "MiniLM-L6-v2 embeddings computed in your browser (384-d), projected to 2-D with PCA (power iteration). Nearest neighbours are by cosine in the full 384 dimensions, not by distance on the picture.",
    limits:
      "Squashing 384 dimensions into 2 distorts distances: points that look close may not be. Hover a point for its true cosine score.",
    concept: "search",
    minutes: 2,
    needs: ["browser"],
  },
];

export const LAB_BY_ID = Object.fromEntries(LABS.map((l) => [l.id, l])) as Record<LabId, LabMeta>;

export const START_PATH: LabId[] = ["semantic", "filters", "rag", "reason", "scale"];
