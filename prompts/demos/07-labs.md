# Prompt 07 — NebulaDB Labs: hands-on retrieval, reasoning and RAG experiments per business

> Can run alone or after 01–06. Where 00 applies (data rules, honesty, safety cards), follow it.
> Read `apps/showcase/src/App.tsx` (nav + hash routing), `usecases/UseCaseDemo.tsx`,
> `rag/PipelineTrace.tsx`, `rag/GroundedChat.tsx`, `components/UnderTheHood.tsx`,
> `components/Provenance.tsx` and `explain.tsx` first. **Build on these instead of re-inventing them.**

## Why: what we learned from trust-agent.ai/labs (analysed 7 Nov 2026)

MICKAI's Labs (trust-agent.ai/labs) has 35 small "experiments", one page per lab under
`/labs/experiments/<id>/`. Every lab uses the same card:
- a hook line ("Search for meaning.");
- a title and a lead;
- **How this experiment works** (the method, in one or two sentences);
- **What it does not prove** (the limits);
- **Try this challenge** (one concrete task);
- a link to a related course.

The models run **in the browser** through transformers.js:
- `Xenova/all-MiniLM-L6-v2` for semantic search and the 3-D "embedding constellation" (PCA);
- DistilBERT SST-2 for sentiment;
- YOLOS, SlimSAM, MODNet, Depth-Anything and Whisper-tiny for vision and speech;
- Tesseract for OCR.

The semantic lab embeds a **9-sentence editable corpus**. It includes deliberate distractors
("A gardener plants roses in the spring") and ranks by dot product. "Memory studio" asks one
question of **four retrieval systems side by side** (identity facts, recent events, TF-IDF
vectors and a relationship graph). It then scores all four on **four labelled questions** and
says plainly what the score does and does not measure.

**What works, and we should match it:**
- Each lab has one idea, an editable input and an immediate result.
- The method and the limits are stated on every lab.
- A "challenge" turns browsing into thinking.
- Compare modes run several systems on the same question.
- A tiny labelled benchmark is shown with honest framing.
- Deep links take the form `#lab=<id>`.

**Gaps that are our opening:**
- Toy corpora: 9 sentences and 4 questions, with no scale and no latency.
- No business or industry context. Every lab is generic teaching material.
- **No RAG at all.** Retrieval stops at a ranked list, with no generation, citations,
  grounding or refusal.
- No hybrid search, no metadata or SQL filters, no reranking. Their "vector" memory mode is
  TF-IDF.
- No model comparison. Each lab hard-codes one model, and no lab says which model is better
  for which job.
- No persistence and no shareable state beyond the lab id. Results are not exportable.
- Nothing runs against a real database, which is exactly our strength.

**Rule:** do **not** copy their visual style, layout, copy or lab list. Use the showcase's own
design tokens and components. Take the *ideas* (card structure, honesty, challenges, compare
modes) and go further: real data at scale, real NebulaDB queries, RAG, and business context.

## Goal

Add a **Labs** area to the showcase. Each lab is a focused, 2–5 minute experiment that teaches
one retrieval or reasoning idea **on a business's own data**, runs against **real NebulaDB**,
and lets the user **pick and compare models**. Labs complement the full apps (Source Supplies,
Campus, CareHub and others), which stay as they are. A lab is the "why it works"; an app is
the "what you'd ship".

## Structure

### Lab × business-pack matrix
A lab is a reusable experiment. A **business pack** supplies the data, sample questions,
labelled answers and safety rules. The URL holds the full state, so any view can be shared:

```
#/labs/<lab>?pack=<pack>&q=<question>&models=<a,b>&mode=<...>&k=10&ef=256
```

Packs reuse the buckets from the vertical prompts where they exist. Otherwise each pack seeds a
small (200–2,000 doc) dataset with `ensureSeeded`:

| Pack | Source | Example question |
|---|---|---|
| `campus` (schools & universities) | 01 | "Can my child take a term-time holiday?" |
| `carehome` | 02 | "How does Mrs D like to be supported with eating?" |
| `chat` | 03 | "What did we decide about the Rotterdam SLA?" |
| `cms` | 04 | "Do we already have an article on composting in small flats?" |
| `joyai` (small business + tax) | 05 | "Can I claim my van as an expense?" |
| `kisaan` (farming, multilingual) | 06 | "gehu me peela rog aa gaya" |
| `source` (B2B supplies) | existing `source/` | "low odour floor cleaner for a school canteen" |
| `custom` | user pastes text / uploads a file | anything |

### Lab card (every lab, rendered from one `LabSpec`)
`hook` · `title` · `lead` · **How it works** (method, naming the exact model, dimension, metric,
index and SQL) · **What it doesn't prove** (limits) · **Challenge** (one concrete task per pack;
the app detects when it is completed and shows a tick) · **Under the hood** (existing
`UnderTheHood`: SQL, REST call, latency, `ef`, candidates scanned) · **Data badge** (pack, doc
count, embedder; "Mock embedder" warning when `mock-384`).

Write fresh copy for every lab. Never reuse their text.

## The labs (v1: build all eight, each working with every pack)

1. **Meaning vs words** (`semantic`)
   - Same question over three modes side by side: BM25 only, vector only, and hybrid.
   - The corpus is the pack's data, or the user's own sentences in `custom`.
   - Each result shows its lexical score, vector score and fused score, plus a bar showing why
     it ranked there.
   - Challenge: "find a paraphrase with no shared words that hybrid still finds."

2. **Model shoot-out** (`models`)
   - Run the same question set through 2–3 embedders side by side.
   - Models: `mock-384` as a baseline, `nomic-embed-text` (768), and a multilingual model such
     as `bge-m3` via Ollama. Optionally `all-MiniLM-L6-v2` in the browser via transformers.js,
     so it works with no server model.
   - Show recall@k and MRR on the pack's **labelled questions**, embed latency, vector size and
     memory per million docs.
   - **Recommend a model for this pack, with the reason** ("Kisaan: multilingual model wins on
     Hinglish, +X recall").
   - Pre-embedded copies of each pack per model live in buckets `lab_<pack>_<model>`.

3. **Filters that matter** (`filters`)
   - Semantic plus SQL hard filters in one query (`semantic_match` + `WHERE`).
   - The user toggles filters (site, year group, crop + state, tenant, locale) and watches the
     results change.
   - Show the post-filter `LIMIT × 4` candidate behaviour honestly: the "filtered recall" meter
     drops when a filter is very selective, and the `exhaustiveLimit` fix is shown.
   - Challenge: "make a filter so narrow recall drops, then fix it."

4. **RAG, step by step** (`rag`)
   - The pipeline runs: question → retrieve → rerank → assemble context (token budget) →
     answer with citations.
   - It reuses `PipelineTrace` and `GroundedChat`, and each stage can be paused and inspected.
   - **Side by side:** the same LLM answers *without* retrieval and *with* retrieval. Every
     claim is highlighted and linked to its source chunk, and unsupported sentences are
     flagged.
   - The LLM picker covers models available on the configured Ollama / OpenAI-compatible
     endpoint, listed by the server and never hard-coded.

5. **Reason over the data** (`reason`)
   - Multi-step questions that need SQL *and* retrieval, for example:
     - "Which pupils in 9X2 are below 90% attendance and what does the policy say we do?"
     - "What's mustard's 14-day price trend at Khanna and should I wait given the rain
       forecast?"
   - The model plans the steps, then each tool call (SQL, `semantic_match`, aggregate) runs
     visibly against NebulaDB, then the model answers with numbers quoted from rows.
   - It shows the plan and the trace, **not hidden chain-of-thought**. Every number in the
     answer must appear in a returned row; check this in code and flag any number that
     doesn't.
   - Option: let the user edit a step and re-run from there.

6. **When to say no** (`refuse`)
   - Out-of-scope, unanswerable and unsafe questions for each pack, for example:
     - "write my essay";
     - "should I double the dose";
     - pesticide poisoning;
     - another tenant's data.
   - Show what happens: no retrieval match → "not in the documents", a safety-rule trigger →
     the fixed card (rules in code, from 00), an ACL filter → zero rows.
   - Score refusal accuracy on a labelled set.

7. **Scale it up** (`scale`)
   - The same query against 1k → 100k → 1M → full-corpus versions of the pack (bench scale
     from 00, or the real prod buckets read-only).
   - Show p50/p95 latency, recall@10 against exact search and candidates scanned.
   - An `ef` slider (64–512) shows the recall vs latency trade-off live.
   - This is what browser-only labs cannot do; it is our headline lab.

8. **See the space** (`map`)
   - A 2-D/3-D projection (PCA or UMAP, computed server-side or in a worker) of the pack's
     embeddings, coloured by a metadata field (category, crop, unit).
   - The query point and its top-k neighbours are drawn.
   - "What it doesn't prove": projections distort distance; the true cosine score is shown
     on hover.

## Labs home

- A grid of lab cards filterable by **business pack**, by **concept** (search, models, filters,
  RAG, reasoning, safety, scale) and by **time** (2 or 5 min).
- A pack selector at the top is remembered in localStorage (wrapped in try/catch).
- "Start here" path: semantic → filters → rag → reason → scale.
- Each lab's card shows whether it needs a real embedder or LLM, and degrades with a clear
  banner if not.

## Evaluation built in

Every pack ships `labels.ts`: 20–40 questions, each with expected doc ids, plus the
should-refuse set. Labs 2, 3, 4, 6 and 7 compute hit@k, MRR, citation accuracy and refusal
accuracy live from these, with an "Export results (JSON/CSV)" button. Results use the same
schema as the 00 benchmark JSON so the Benchmarks tab can show them. Label every number as
measured, with model, `ef`, doc count and timestamp. Never show invented numbers.

## Engineering

- `apps/showcase/src/labs/`: `LabSpec` type, `LabCard`, `LabsHome`, one file per lab, and
  `packs/<pack>/{data,labels,challenges}.ts`.
- Add a `labs` nav group in `App.tsx` and extend the existing hash router for
  `#/labs/<lab>?…` without breaking `#/blog`.
- Server: add only what is missing. Candidates are `GET /api/v1/models` (embedders and LLMs
  available plus dims), per-request `embed_model` and `ef` on search, and a projection
  endpoint. Check `crates/nebula-server/src/router.rs` first, and document any new route in
  `docs/openapi.yaml`.
- Browser-only fallback: transformers.js MiniLM in a Web Worker for `semantic` and `models`, so
  a lab works even without a server-side model. Label it "runs in your browser".
- Tests (Node runner):
  - URL state round-trips.
  - Every pack's labels resolve to real doc ids.
  - The numbers-in-answer checker.
  - The refusal rules.
  - Every lab renders with the mock embedder.
- Accessibility: keyboard-operable controls, and charts with text equivalents.
- Responsive to 360 px.

## Acceptance

- `#/labs` lists 8 labs × 8 packs. Every combination loads, runs and shows method, limits and a
  challenge.
- A shared URL reproduces the exact view (pack, question, models, `ef`, mode).
- The `models` lab recommends a model per pack from measured scores.
- The `rag` lab shows answers with and without retrieval, each claim linked to its chunk.
- The `reason` lab shows a visible tool plan, and every number in the answer is traced to a
  row.
- The `scale` lab runs against 1M docs and shows the recall/latency trade-off.
- `docs/labs.md` explains each lab, each pack, how to add a pack, and the honesty rules.
