# NebulaDB Labs

Short (2–5 minute) experiments in the showcase. Each one teaches a single retrieval or reasoning idea, runs on one business's own (invented) data, and talks to a real NebulaDB.

- Open them from **Labs** in the sidebar, or at `#/labs`.
- Code lives in `apps/showcase/src/labs/`.
- The build prompt is `prompts/demos/07-labs.md`.

Every lab card states the same four things:
- **how it works**: the exact model, metric and endpoint;
- **what it doesn't show**: the limits of the result;
- **a challenge** for the chosen business;
- **the API calls**: every call it made, viewable under "Show the API calls behind this lab".

Every view is shareable, because the full state is in the URL:

```
#/labs/rag?pack=carehome&q=…&k=5&persona=carer_elm_lodge&models=ollama
#/labs/filters?pack=source&f.in_stock=true&f.price_gbp=30&k=50
```

## Labs

| Lab | What runs | Needs |
|---|---|---|
| `semantic` — Meaning vs words | BM25 in the browser, then vector and hybrid `/api/v1/ai/search` (with `explain`) on the pack bucket | embedder |
| `models` — Model shoot-out | Runs every labelled question through BM25, the server vector search, server hybrid, and optionally MiniLM-L6-v2 in the browser (transformers.js). Reports hit@1/5, MRR, recall@10, median latency, a breakdown by question type and language, a recommendation, and JSON export | embedder (+ browser model) |
| `filters` — Filters that matter | One SQL query: `semantic_match` + metadata predicates, with EXPLAIN ANALYZE. A recall meter compares the result with the docs that pass the filters, computed in the browser | embedder |
| `rag` — RAG, step by step | Safety rules → retrieve (hybrid/vector, or persona-filtered SQL) → token-budgeted numbered sources → `/api/v1/ai/chat` twice with the same model, without and with sources. Citations are checked per sentence, and every number is checked against the sources | embedder, LLM |
| `reason` — Reason over the data | Plan (pack reference plan, or written by the LLM as JSON) → each SQL/search step runs live → answer from the step outputs only. Numbers not in any output are flagged. Any step can be edited and re-run | embedder, LLM |
| `refuse` — When to say no | Guards run in order: phrase rules in code → persona access filter → relevance threshold. Covers the pack's should-refuse set plus 12 should-answer controls, and shows a confusion matrix with live threshold sliders | embedder |
| `scale` — Scale it up | `ef` sweep (16–512) on the pack and on the largest bucket on the server: browser and server latency, plus overlap@10 with ef 512 | embedder |
| `map` — See the space | MiniLM embeddings in the browser → 2-D PCA → the question placed among its true-cosine nearest neighbours | browser model |

## Business packs

Each pack lives in `apps/showcase/src/labs/packs/<id>.ts` and seeds bucket `lab_<id>` through `ensureSeeded` on first open.

| Pack | Business | Docs | Labelled questions |
|---|---|---|---|
| `campus` | Northfield Learning Trust + University of Wexley | 88 | 27 |
| `carehome` | Harbour Care Group | 94 | 26 |
| `chat` | Orbital Logistics team and support chat | 78 | 28 |
| `cms` | Greenway Media (3 sites, drafts and scheduled) | 74 | 27 |
| `joyai` | Three small UK businesses + tax notes | 95 | 26 |
| `kisaan` | Punjab/UP farmers — English, Hindi, Punjabi, Hinglish | 90 | 31 |
| `source` | Source Supplies (B2B cleaning) | 93 | 26 |

Each pack also has:
- ≥ 3 metadata filters, one of them carrying an apostrophe to test `''` escaping;
- personas;
- ≥ 2 safety rules with fixed cards;
- a should-refuse set (safety, out of scope, access);
- reasoning tasks with reference SQL plans;
- one challenge per lab.

**All data is invented.**

### Adding a pack

1. Copy a pack file, change `id`, set `bucket` to `lab_<id>`, and write the docs, questions, refusals, rules, personas, plans and challenges.
2. Register it in `packs/index.ts`.
3. Run `npm test`. `validatePack` checks ids, filters, labels, safety false positives, persona slices, and the SQL rules (`checkSql`).
4. Run the reference SQL against a live server. The tests check syntax only, not execution.

## Rules the labs follow

- **SQL**: exactly one `semantic_match`; filters apply after retrieval over `LIMIT × 4` candidates (at least 32); no `OR` / `JOIN` / subqueries / CTEs. Aggregates need `GROUP BY`; to get a single row, group by the filtered column.
- **Vector `score` is a cosine distance**: lower is closer. The labs show similarity as 1 − distance. Hybrid scores are fused, so higher is better.
- **Safety rules run in code before any model call**, and their cards are fixed text.
- **Personas filter inside the SQL.** Rows outside the persona never leave the database. Personas match on exact equality, so group-wide docs (for example "Group-wide" policies) are hidden from single-home personas. Fixing that needs `OR` or `IN` in NebulaDB SQL.
- **No invented numbers.** Every score is measured in the visitor's session. Each mock embedder or mock LLM run is labelled, and the model lab's JSON export carries the model, doc count and timestamp.

## Known limits

- Labelled sets are about 25 questions per pack: a smoke test, not a benchmark.
- **Out-of-scope detection is a threshold.** With the mock embedder only the keyword threshold applies, so paraphrases look out of scope.
- **Scale agreement is measured against ef 512**, not exact search.
- **MiniLM in the browser is English-only.** On Kisaan it struggles with Hindi and Hinglish, which is visible in the models and map labs; a multilingual server embedder (for example `bge-m3`) is the fix.
