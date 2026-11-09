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
| `visa` | Lotus Gate Visa Desk (fictional Thai visa agency) — Visa Finder / immigration assistant | see `packs/visa.ts` | see `packs/visa.ts` |

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

## Visa AI Agent (`#/tab/visaagent`)

A conversational Thai visa assistant built on the `visa` pack, under **RAG apps → Visa AI Agent** in the sidebar. Code lives in `apps/showcase/src/labs/visa/`.

Small local models (prod runs `llama3.2:3b`) don't follow JSON or tool-calling instructions reliably. So the agent never lets the model decide what to query.

**What code does** (`agent.ts`, unit-tested):
- Pulls the applicant's age, savings, monthly income, purpose and any visa named from plain text. Purposes in the latest message take priority.
- Picks the tools:
  - **visa_finder:** SQL over visa types: `min_age <= age AND min_funds_thb <= funds AND array_contains(purpose, …)`. If nothing matches, it drops the funds limit and says so.
  - **search_official:** semantic SQL over `lab_visa_gov`, the passages from official Thai government websites, narrowed with `AND visa = '<route>'` when the question names a visa. After the Visa Finder runs, one more `search_official` per shortlisted route (up to 3) fetches that route's official requirements.
  - **case_status:** reads case notes, only for the signed-in demo client.
- Runs the safety rules (false documents, detention) before anything else. Those return a fixed card, and the model is never called.

**What the model does:** writes the reply through `/api/v1/ai/chat` from the numbered passages only: official government text, plus the signed-in client's own case notes. The agency's notes (the `visa` pack) only build the Visa Finder shortlist; they never reach the model. When the passages don't cover a question, the model says "The official sources I checked don't cover that" and points to the closest official site. If no official passage matches at all, the reply is a fixed message and the model isn't called.

**What the page shows for each reply:**
- the shortlisted visa routes as cards, built from the SQL rows rather than the model and labelled as the agency's notes;
- a citation per sentence, and an **Official sources** list with the authority and a link for each page;
- a check that every number appears in an official passage. If the first draft has a number that isn't, the model is asked once to rewrite from the passages, and the page says so;
- an expandable trace of each SQL call.

### Official sources (`gov-corpus.json`)

`apps/showcase/scripts/visa-gov-fetch.mjs` builds `apps/showcase/src/labs/visa/gov-corpus.json`. The showcase loads it as a separate chunk and seeds `lab_visa_gov` when its `version` (a hash of the passages) changes.

| Source | Authority | What it gives |
|---|---|---|
| Thai e-Visa master data (`thaievisa.go.th/ApiGateway/masterdata/…`) | Ministry of Foreign Affairs | One passage per visa sub-type and purpose of visit: the exact documents, funds, insurance and age rules the e-Visa system asks for |
| `thaievisa.go.th` | Ministry of Foreign Affairs | How to apply online |
| `tdac.immigration.go.th` | Immigration Bureau | Thailand Digital Arrival Card, 90-day notification |
| `ltr.boi.go.th` | Board of Investment | LTR types, criteria, fees, 1-year reporting, FAQ |
| `smart-visa.boi.go.th` | Board of Investment | SMART visa types, documents, how to manage it |
| `thailandprivilege.co.th` | Thailand Privilege Card Co. (TAT) | Living-in-Thailand guidance |
| `tourismthailand.org` | Tourism Authority of Thailand | Visa exemption list, visa on arrival, DTV, ED Plus |

To refresh it: install `playwright-core` somewhere, then run `CHROME=<chrome binary> node apps/showcase/scripts/visa-gov-fetch.mjs` from that directory, run `npm test` (it checks every passage links to a government page and every route tag has a passage) and commit the JSON.

Not included: `www.immigration.go.th` answers automated clients with a bot challenge, and we don't work around it. Two TAT articles refused automated fetches too. So questions such as the overstay fine per day get "The official sources I checked don't cover that" rather than an answer from the agency's notes.

**Measured on prod (2026-11-09, `llama3.2:3b`):** the same 8 questions answered from the agency's notes (before) and from official passages (after):

| Question | Before: agency notes | After: official sources |
|---|---|---|
| Health insurance for the O-A visa? | "health insurance covering your stay"; the minimum "is not explicitly stated" | "minimum insured sum of 100,000 USD or 3,000,000 THB" (thaievisa.go.th) |
| Money for the O-X visa? | "3 million THB" | "3,000,000 THB, or at least 1,800,000 THB together with an income of at least 1,200,000 THB per year in a Thai bank" (thaievisa.go.th) |
| Arrival card before I fly? | the paper TM.6 "has since been replaced by the online digital arrival card" | "submit their arrival card information 3 days in advance of their arrival date" (tdac.immigration.go.th) |
| Overstay fine per day? | "500 THB" (from an agency note) | "The official sources I checked don't cover that" |
| VOA, LTR fee, DTV funds and fee | same figures as the notes | same figures, now each linked to TAT, BOI or the e-Visa page |

The Claude-driven agent teams in `examples/nebula-agents` need `ANTHROPIC_API_KEY` on the examples pod. It isn't set on int or prod, so this agent runs on NebulaDB's own LLM gateway instead.

## When the official rules change (RAG freshness)

Some packs ship `updates`, which are simulated changes to an official source. The `visa` pack has three: changes to a Thai government visa page.

The RAG lab's **"The rules changed — does the answer?"** panel does four things:
1. Asks the update's question against the current documents.
2. Re-ingests the changed documents under the **same ids**, which is exactly what a page-watcher would do on a real change.
3. Asks again.
4. Shows the two answers side by side, marking which sources changed.

No model is retrained; only the retrieved text changed.

The demo bucket is shared, so the panel shows whether an update is currently applied, for everyone viewing it, and always offers **Revert**.

In production, the same loop is a crawler on the official sources that runs on a schedule:
1. Fetch each page and hash its content.
2. On a change, chunk and upsert into NebulaDB under the page's stable id, recording `source` and `as_of`.
3. RAG cites the new text on the next question.

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
