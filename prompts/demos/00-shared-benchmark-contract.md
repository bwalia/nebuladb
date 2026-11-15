# Shared contract for every NebulaDB benchmark demo

> Paste this file **above** any of the demo prompts in this folder (01–06). It sets the
> rules every demo follows so their benchmark numbers can be compared side by side.
> Read the repo before you write code. The conventions below come from
> `docs/source-supplies-demo.md` and `apps/showcase/src/source/`. Check them against the
> code and follow the code if it disagrees.

## Role

You are building one vertical demo for **NebulaDB**, a Rust vector + document + SQL database
(`crates/nebula-*`). The demo has two jobs:

1. **Product demo.** A believable app for one industry that a non-technical buyer understands
   in two minutes.
2. **Benchmark harness.** A repeatable load test for that industry's real query shapes that
   reports honest numbers.

## Where things go

| Piece | Location |
|---|---|
| Showcase tabs (React/TS, Vite) | `apps/showcase/src/<demo>/` and a sidebar group, the same as `source/` |
| Seed data (deterministic TS) | `apps/showcase/src/<demo>/data/*.ts`, each exporting docs and `SEED_VERSION` |
| Agents (optional) | `examples/nebula-agents` as a new `--team <demo>` |
| Benchmark runner | `bench/<demo>/` (Go or Python, no heavy deps), with a `Makefile` target `bench-<demo>` |
| Docs | `docs/<demo>-demo.md` with the same sections as `docs/source-supplies-demo.md` |
| Tests | Node built-in runner (`npm test`) for data and rule tests, plus `unittest` for agent guards |

## Data rules

- **All data is invented.** Every person, school, care home, farm, phone number and ID is
  fictional and generated from a fixed seed. No real personal data, and no scraped content.
  Each tab shows a "Demo data" banner.
- Seed with the existing `ensureSeeded` helper (versioned, batched upserts keyed by id).
  Opening a tab again must write 0 docs.
- Provide **two scales**:
  - `demo`: a few hundred to a few thousand docs, loaded in the browser on first open.
  - `bench`: a generator CLI that writes N docs (default 1M, configurable up to 10M) with the
    same schema and the same distributions, loaded through the REST bulk path.
- Text must look real: varied sentence length, domain vocabulary, typos where users would make
  them, and near-duplicates where the real domain has them. Real domains repeat themselves, so
  give the bench generator a duplicate-text ratio knob (`--dup-ratio`). Recent leads work
  showed that unique text share changes recall a lot.
- Every bucket documents its filter fields in a table (bucket | docs | one doc is | filtered on).

## Query rules (current NebulaDB SQL limits — verify before you rely on them)

- One `semantic_match(text, …)` per query. No `OR`, subqueries or CTEs.
- Metadata filters apply **after** retrieval, over `LIMIT × 4` candidates (at least 32).
  Exhaustive filtered lookups need a big enough `LIMIT` (see `exhaustiveLimit()` in
  `apps/showcase/src/source/data/common.ts`). The benchmark must report filtered recall
  separately for this reason.
- Every literal goes through `lit()` (`''` escaping). Seed at least one value with an apostrophe.
- HNSW `ef` defaults to 256 (`NEBULA_EF_SEARCH`) and can be overridden per request. The
  benchmark sweeps it.
- With the mock embedder (`mock-384`), similarity is a hash, not meaning. Show the
  "Mock embedder" banner. The hard filters must still return the right rows. Benchmarks that
  report recall must run with a real embedder (`NEBULA_OPENAI_*` → `nomic-embed-text` via
  Ollama) and print the model name.

## Benchmark contract (identical for all six demos)

Each demo defines **5–8 named workloads** taken from its real user journeys, for example
`school.timetable_lookup` or `kisaan.crop_advice_rag`. Each workload has:

```yaml
name: <demo>.<workload>
kind: point_read | filtered_scan | vector | hybrid | rag | write | mixed
endpoint: /api/v1/query | /api/v1/ai/search | /api/v1/rag/answer | /api/v1/bucket/{b}/doc ...
mix_weight: 0.30          # share in the demo's mixed profile
query_templates: [...]    # parameterised; values drawn from the seed distributions
ground_truth: how recall@k is computed (brute-force exact kNN over the same filter, or labelled set)
slo: { p95_ms: ..., p99_ms: ... }   # targets, marked as targets and not as results
```

The runner must:

1. **Load phase.** Bulk-load the `bench` scale and report docs/s, embed/s, bytes on disk and
   RSS after load (from `/metrics`).
2. **Warm-up.** Run 60 s, discarded.
3. **Per-workload runs.** Use fixed concurrency steps (1, 8, 32, 128) and report QPS and
   p50/p95/p99/max latency, plus the error rate split by status code.
4. **Mixed profile.** Use the `mix_weight`s with an open-loop arrival rate to avoid coordinated
   omission, then step the rate up until p99 breaks the SLO. Report that knee.
5. **Recall.** Report recall@10 for vector and hybrid workloads at `ef` ∈ {64, 128, 256, 512}
   against exact ground truth, both unfiltered and filtered.
6. **RAG quality** (rag workloads only). Run a labelled set of 30–50 questions per demo with
   expected source ids and report hit@k, MRR, citation accuracy and refusal accuracy (questions
   that must be refused, or routed to a fixed safety card).
7. **Write results** to `bench/results/<demo>/<timestamp>.json` with the git sha, hardware,
   embedder, `ef`, doc count and NebulaDB version, then render a summary table to stdout.
8. **Benchmarks tab.** A showcase tab reads those JSON files and charts them. **Never show
   invented numbers.** If no result file exists, the tab says "No benchmark run yet" and
   shows the command.

The runner must also be able to target any base URL (local, int, prod read pool) with
`--base-url` and `--token`, and it must refuse to run write workloads against a URL containing
`prod` unless `--allow-prod-writes` is passed.

## Safety rules for the in-app assistant (all demos)

- Answer only from retrieved, cited docs. If the docs don't cover the question, say so and stop.
- Fixed, non-model responses for emergencies (medical, safeguarding, self-harm, crop poisoning,
  and so on). Each demo lists its trigger words and the fixed card text. Rules live in a pure TS
  module with tests.
- The model drafts and a person confirms. No demo sends a message, places an order or changes
  a care plan by itself.

## Acceptance (every demo)

- `./scripts/start.sh` brings the demo up. The tab seeds itself and every journey works with
  the mock embedder.
- `make bench-<demo> SCALE=bench DOCS=1000000` completes on a laptop and writes a result JSON.
- `npm test` passes, including the data-consistency test (every foreign key in the seed
  resolves).
- `docs/<demo>-demo.md` contains: run it, buckets, SQL for each journey, workloads with SLO
  targets, how to read the results, and known limits.
