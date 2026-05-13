# NebulaDB — Use Cases for AI Applications

Companion to [`USE_CASES.md`](USE_CASES.md) (operational use cases),
[`PROBLEMS_SOLVED.md`](PROBLEMS_SOLVED.md) (pain → fix list), and the
architecture diagram at
[`docs/durability-architecture.svg`](docs/durability-architecture.svg).

`USE_CASES.md` is operator-flavored — durability, replication, pgwire.
This document is for the engineer building an **AI application** —
chatbot, copilot, agent, eval harness, recommender — and asking the
honest question: *what does NebulaDB give me that a stack of
[vector DB] + [embedder service] + [LLM gateway] doesn't?*

## How to read this document

Each use case follows the same shape so you can scan them:

1. **The AI problem** — the failure mode of a typical stack.
2. **Why NebulaDB fits** — the components that make it one box.
3. **Worked example** — REST / SQL you can paste into a running
   stack (`./scripts/start.sh` boots one in a minute).
4. **Limits today** — what it doesn't do. No hand-waving.

---

## 1. Retrieval-augmented chat over private documents

**The AI problem.** Building a chatbot over internal docs / tickets
/ runbooks needs a vector store, an embedder, a chunker, an LLM
gateway, and a streaming layer that interleaves retrieved context
with generated tokens. Five processes, five auth surfaces, five
deploy stories — and the chunker is usually a half-broken Python
script nobody maintains.

**Why NebulaDB fits.** One binary carries the embedder
(`crates/nebula-embed` — OpenAI-compatible or Ollama), chunking
(`crates/nebula-chunk`), the HNSW index (`crates/nebula-index`),
and the LLM client (`crates/nebula-llm`). `POST /api/v1/ai/rag` is
SSE: `context` events fire first (so the UI can render citations
immediately), then `answer_delta` token groups, then `done`.
Time-to-first-token is measurable client-side from the SSE stream
and is what the showcase RAG-chat tab displays.

**Worked example.**

```bash
# Ingest a doc; chunking + embedding happens server-side
curl -s -X POST http://localhost:8080/api/v1/bucket/handbook/document \
  -H 'content-type: application/json' \
  -d '{
        "doc_id":"onboarding",
        "text":"New hires are paired with a buddy on day one. The buddy owns first-week onboarding..."
      }'

# Stream a grounded answer
curl -N -X POST http://localhost:8080/api/v1/ai/rag \
  -H 'content-type: application/json' \
  -d '{"query":"what happens on day one?","top_k":4,"bucket":"handbook","stream":true}'
```

**Limits today.** No prompt-template library — the prompt is your
query plus retrieved hits, assembled by the server. No per-chunk
ACLs; bearer auth or JWT grants access to the whole bucket.

---

## 2. Long-term memory for agents and copilots

**The AI problem.** An agent's context window is finite. After a
few turns the model forgets what the user said an hour ago, last
week, or in a previous session. Bolting on "memory" usually means
adding a separate vector DB just for chat history and writing a
custom retrieve-then-prepend layer.

**Why NebulaDB fits.** Memory is just documents in a bucket. Each
conversation turn becomes an upsert with `metadata` (user id,
session id, timestamp). Recall is a `top_k` semantic search
filtered by user. The persistent durability path
(`NEBULA_DATA_DIR`) means memory survives restarts — a vector
store that loses everything on `kubectl delete pod` is unusable
as agent memory.

**Worked example.**

```bash
# Persist a turn into per-user memory
curl -s -X POST http://localhost:8080/api/v1/bucket/agent_memory/document \
  -H 'content-type: application/json' \
  -d '{
        "doc_id":"u-42:turn-128",
        "text":"User said they prefer dark mode and TypeScript over JavaScript.",
        "metadata":{"user_id":"u-42","session_id":"s-7","ts":1715600000}
      }'

# Recall: SQL with metadata filter + semantic match
curl -s -X POST http://localhost:8080/api/v1/query \
  -H 'content-type: application/json' \
  -d '{
        "sql":"SELECT id, text FROM agent_memory WHERE semantic_match(content, ?) AND user_id = ? LIMIT 5",
        "params":["editor preferences","u-42"]
      }'
```

**Limits today.** No cross-document transactions, so writing two
turns atomically isn't possible — write each, treat the agent
loop as idempotent. No automatic forgetting / TTL on documents;
prune via `DELETE` with a metadata filter on a schedule.

---

## 3. Tool / function-call selection for agents

**The AI problem.** A capable agent has dozens to hundreds of
tools. Stuffing every JSON schema into the system prompt blows
the context budget; hard-coding a router is brittle. The clean
approach is *retrieve the right tools by the user's intent*, then
expose only those to the model.

**Why NebulaDB fits.** Tool descriptions are documents. The
agent embeds the user's request, top-k searches the `tools`
bucket, attaches only the matching schemas. Same mechanism as
RAG; different bucket. The two-tier embedding cache
(`crates/nebula-cache` + `crates/nebula-redis-cache`) means the
per-turn embedding cost is one cheap LRU hit on warm prompts.

**Worked example.**

```bash
# One-time: seed your tool catalog
curl -s -X POST http://localhost:8080/api/v1/bucket/tools/document \
  -H 'content-type: application/json' \
  -d '{
        "doc_id":"calendar.create_event",
        "text":"Create a calendar event. Inputs: title, start_iso, end_iso, attendees[].",
        "metadata":{"name":"calendar.create_event","schema_url":"s3://schemas/calendar.json"}
      }'

# At runtime: pick top-3 tools for this user request
curl -s -X POST http://localhost:8080/api/v1/ai/search \
  -H 'content-type: application/json' \
  -d '{"query":"book a meeting with alex tomorrow at 2pm","top_k":3,"bucket":"tools"}'
```

**Limits today.** Pure semantic match — no learned routing on
top of historical tool-call success. If two tools have similar
descriptions they will both retrieve; disambiguation is the
agent's job.

---

## 4. Eval harness for prompt and model regressions

**The AI problem.** You changed the system prompt or bumped the
model version. Did quality regress? Most teams answer this with a
spreadsheet, a hand-written test runner, and a pile of saved
JSONL files nobody can query.

**Why NebulaDB fits.** Each eval run is a row of documents:
`{prompt, expected, actual, score, model_version, prompt_hash}`.
Because the SQL engine supports `GROUP BY`, comparison across
runs is one query. Because the same engine is reachable over
pgwire (`crates/nebula-pgwire`), Metabase or a notebook can graph
regressions without an export step.

**Worked example.**

```sql
-- in psql -h localhost -p 5433 -U any -d any
-- average score per (model, prompt template), latest 7 days
SELECT model_version, prompt_hash, COUNT(*) AS n, AVG(score) AS avg_score
  FROM evals
 WHERE ts > 1715000000
 GROUP BY model_version, prompt_hash
 ORDER BY avg_score ASC
 LIMIT 20;
```

```bash
# semantic dig: find evals whose prompts looked like the failing one
curl -s -X POST http://localhost:8080/api/v1/query \
  -H 'content-type: application/json' \
  -d '{
        "sql":"SELECT id, score FROM evals WHERE semantic_match(content, ?) AND model_version = ? ORDER BY score ASC LIMIT 20",
        "params":["multi-step math word problem","claude-opus-4-7"]
      }'
```

**Limits today.** No `OR` in `WHERE`, no outer joins, no CTEs —
the SQL engine targets simple analytical queries. Complex
slice-and-dice belongs in a notebook reading the same data over
pgwire.

---

## 5. Hybrid retrieval: semantic + structured filter in one query

**The AI problem.** "Find docs semantically related to 'incident
postmortem', but only `region = 'eu'` from the last quarter."
Most vector DBs make you pick one side: either a metadata filter
*before* the ANN search (kills recall) or *after* (over-fetches).
The bigger pain is that hybrid retrieval typically lives in
glue code, not the database.

**Why NebulaDB fits.** `semantic_match` is a SQL predicate. The
planner pushes it through `WHERE` alongside ordinary filters,
and the executor combines them. The same query runs over REST
and pgwire.

**Worked example.**

```sql
SELECT id, region, score
  FROM docs
 WHERE semantic_match(content, 'incident postmortem')
   AND region = 'eu'
 ORDER BY score ASC
 LIMIT 10;
```

```bash
curl -s -X POST http://localhost:8080/api/v1/query \
  -H 'content-type: application/json' \
  -d '{
        "sql":"SELECT id, region, score FROM docs WHERE semantic_match(content, ?) AND region = ? ORDER BY score ASC LIMIT 10",
        "params":["incident postmortem","eu"]
      }'
```

**Limits today.** No `OR` in `WHERE` (use two queries and union
client-side). No JOINs across buckets yet — joins work *within*
a bucket. `semantic_match` is the only vector predicate; no
distance-operator overload.

---

## 6. Embedding cost control for production AI apps

**The AI problem.** Embedding spend creeps. Every retry, every
re-rank, every "let me re-embed the corpus with the new model"
is a fresh OpenAI charge. Most stacks have *a* cache somewhere
but no visibility into whether it's actually working.

**Why NebulaDB fits.** Embedder calls are wrapped in a two-tier
cache: in-process LRU (sub-microsecond hits) on top of an
optional Redis layer (shared across replicas). Prometheus
metrics expose `nebula_embed_cache_hits`, `_misses`,
`_evictions`, `_inserts`. The provisioned Grafana dashboard in
`deploy/grafana/` plots the hit ratio so you can prove the cache
is paying for itself before the next finance review.

**Worked example.**

```bash
NEBULA_OPENAI_API_KEY=sk-... \
NEBULA_EMBED_CACHE_SIZE=20000 \
NEBULA_REDIS_URL=redis://redis:6379 \
  nebula-server &

# Run real workload, then observe
curl -s localhost:8080/metrics | grep embed_cache
# nebula_embed_cache_hits   1834
# nebula_embed_cache_misses 142
```

**Limits today.** No per-model partitioning. Mixing two
embedders in one process pollutes each other's cache — run
separate processes per model. No semantic equivalence in the
cache key; "hello world" and "Hello, world!" miss each other.

---

## 7. Streaming chat UI without a polling loop

**The AI problem.** A browser chat UI that polls `GET /rag?query=`
every 500 ms while the LLM thinks is bad UX, worse on mobile,
and makes intermediate state (which chunks were retrieved?)
invisible to the user.

**Why NebulaDB fits.** `POST /api/v1/ai/rag` is SSE. The wire
order is:

1. `event: context` — one event per retrieved chunk, with id
   and snippet, so the UI can render citations *before* the
   LLM has produced a single token.
2. `event: answer_delta` — LLM token groups as they arrive.
3. `event: done` — terminal.

The gRPC side mirrors this with a server-streaming `AIService.Rag`
RPC for non-browser clients.

**Worked example.**

```bash
curl -N -X POST http://localhost:8080/api/v1/ai/rag \
  -H 'content-type: application/json' \
  -d '{"query":"explain dns failover","top_k":3,"stream":true}'
# event: context
# data: {"id":"zt-overview#1","snippet":"DNS failover relies on..."}
# event: answer_delta
# data: {"delta":"DNS failover works by"}
# ...
# event: done
```

**Limits today.** Retrieval is one shot — all `context` events
fire before the first `answer_delta`. No partial-retrieval
streaming where chunks trickle in alongside tokens; if retrieval
is slow the client waits for it.

---

## 8. Audit trail and prompt-debugging for AI features

**The AI problem.** A user reports a bad answer from your
copilot. You need to know: which prompt was sent, what was
retrieved, what model version, who asked, when. Most stacks
either log nothing structured or log so much that finding the
incident takes an hour.

**Why NebulaDB fits.** Two complementary surfaces:

- `GET /admin/audit` — 1024-entry ring buffer of every mutating
  request (method, path, principal fingerprint, status,
  timestamp). Lives in
  [`crates/nebula-server/src/audit.rs`](crates/nebula-server/src/audit.rs).
- `GET /admin/slow` — top-N slowest SQL queries since boot,
  floor-thresholded by `NEBULA_SLOW_QUERY_THRESHOLD_MS`. Useful
  when "AI feels slow today" turns out to be one expensive
  hybrid query.
- `GET /admin/logs/stream` — SSE log tail with `?level=` and
  `?target=` filters, so you can watch RAG calls live from the
  admin UI without `kubectl logs`.

**Worked example.**

```bash
# Live tail of LLM-related events at warn or above
curl -N 'http://localhost:8080/api/v1/admin/logs/stream?level=warn&target=nebula_llm'

# Last 1024 mutations
curl -s http://localhost:8080/api/v1/admin/audit | jq '.entries[-20:]'

# What's been slow lately
curl -s http://localhost:8080/api/v1/admin/slow | jq
```

**Limits today.** Audit is in-memory (1024 entries or one
process lifetime). For long-term retention ship logs to a SIEM
via the SSE tail or a sidecar; the audit ring is for
debugging, not compliance archives.

---

## Not buildable on NebulaDB today

These are real AI-application needs. NebulaDB does not solve
them today; listed here so nobody adopts the stack expecting
otherwise.

- **Active-active multi-region writes for global agents.**
  Single-leader only. Use the read-replica pattern in
  [`USE_CASES.md`](USE_CASES.md#use-case-3--read-scaling-with-a-follower-replica)
  for read scaling.
- **Per-user / per-chunk ACLs on retrieval.** Auth grants full
  bucket access. A retrieval-time filter that respects ACLs
  would need to land in the index, not the router.
- **Built-in prompt template / chain library.** No LangChain-
  style abstractions; bring your own templating.
- **Automatic re-embedding on model change.** Today: bump
  `NEBULA_EMBED_DIM`, wipe the index, reingest. A migration
  command is a future addition.
- **Online schema evolution for buckets.** Metadata is freeform;
  there is no "alter bucket" tool.
- **Cold-storage tiering of rarely-accessed vectors.** The full
  index is RAM + one snapshot on disk. Multi-TB AI corpora need
  a different architecture.

---

## Related reading

- [`USE_CASES.md`](USE_CASES.md) — operational use cases
  (durability, replication, pgwire, observability).
- [`PROBLEMS_SOLVED.md`](PROBLEMS_SOLVED.md) — the pain → fix
  list, written from an operator's perspective.
- [`docs/durability-architecture.svg`](docs/durability-architecture.svg)
  — component diagram. Open alongside this doc for the "how"
  behind the durability story used by use cases 1, 2, and 8.
- `README.md` — quick start, endpoint reference, env-var table.
- `docs/openapi.yaml` — machine-readable REST schema.
