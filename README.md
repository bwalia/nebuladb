# NebulaDB

AI-native hybrid database written in Rust. Single binary serves three
transports — REST, gRPC, and the Postgres wire protocol — over a
shared in-memory corpus with an HNSW vector index, a pluggable
embedder, a multi-tier embedding cache, and a streaming RAG endpoint.

## Features

- **HNSW** vector index (Cosine / L2² / negative dot), soft-delete
  tombstones, concurrent reads under `RwLock`.
- **Document pipeline** — UTF-8-safe chunker (fixed / sentence),
  batched embedder trait with `MockEmbedder` (offline default) and
  OpenAI-compatible HTTP backend, stored with `{doc_id}#{i}` keys and
  replace-on-upsert semantics.
- **Two-tier embedding cache** — in-process LRU (`nebula-cache`) over
  an optional Redis layer (`nebula-redis-cache`). Redis failure is
  transparent; the server always falls back to the upstream embedder.
- **AI SQL** — extend standard SELECT with `semantic_match(col, 'q')`
  and `vector_distance(col, '[...]')`. Supports residual filters
  (`=`, `IN`), `GROUP BY` (`COUNT / SUM / AVG / MIN / MAX`), INNER
  `JOIN`, `ORDER BY`, and `LIMIT`.
- **Streaming RAG** — SSE endpoint forwards LLM tokens from Ollama
  or any OpenAI-compatible chat server. Context events emit first so
  clients can render citations before the answer completes.
- **Auth + rate limit** — bearer-token allowlist and HS256 JWT
  coexist; token-bucket limiter keyed by authenticated principal
  (not IP), 429 with `Retry-After`.
- **Observability** — `/metrics` in Prometheus text format; Grafana
  dashboard covers traffic, cache hit ratio, rate-limit / auth events.

## Quick start

```bash
docker compose up --build
```

First run pulls `llama3` (chat) and `nomic-embed-text` (embeddings)
into a persistent volume — slow the first time, fast thereafter.

| Endpoint                                 | What it is               |
|------------------------------------------|--------------------------|
| `http://localhost:8080/healthz`          | liveness                 |
| `http://localhost:8080/metrics`          | Prometheus scrape target |
| `http://localhost:8080/api/v1/*`         | REST API                 |
| `grpc://localhost:50051`                 | gRPC (`DocumentService`, `SearchService`, `AIService`) |
| `psql -h localhost -p 5433 -U any -d any`| Postgres wire protocol   |
| `http://localhost:9090`                  | Prometheus UI            |
| `http://localhost:3000`                  | Grafana (`admin`/`admin`)|

## RAG end-to-end, with `curl`

```bash
# 1. Ingest a document. The server chunks + embeds server-side.
curl -s -X POST http://localhost:8080/api/v1/bucket/docs/document \
  -H 'content-type: application/json' \
  -d '{
    "doc_id": "zt-overview",
    "text": "Zero trust networking replaces perimeter trust with per-request verification. DNS failover relies on short TTLs and health checks to steer traffic when a region becomes degraded."
  }'

# 2. Semantic search.
curl -s -X POST http://localhost:8080/api/v1/ai/search \
  -H 'content-type: application/json' \
  -d '{"query": "how does dns failover work?", "top_k": 3}'

# 3. Streaming RAG. Emits:
#    event: context   one per retrieved chunk
#    event: answer_delta  one per LLM token group
#    event: done
curl -N -X POST http://localhost:8080/api/v1/ai/rag \
  -H 'content-type: application/json' \
  -d '{"query": "explain dns failover", "top_k": 3, "stream": true}'

# 4. Same thing via SQL.
curl -s -X POST http://localhost:8080/api/v1/query \
  -H 'content-type: application/json' \
  -d '{"sql": "SELECT id, text FROM docs WHERE semantic_match(content, '\''dns failover'\'') LIMIT 3"}'
```

## Knowledge Ops showcase app

A React UI that demos every NebulaDB capability end-to-end lives at
`apps/showcase`. Build and run it with the rest of the stack:

```bash
docker compose up --build      # brings up showcase at http://localhost:5173
./scripts/seed.sh              # populates a small knowledge corpus
```

Or dev-mode, proxying to a local nebula-server at :8080:

```bash
cd apps/showcase
npm install
npm run dev                    # http://localhost:5173
```

Tabs: Documents (ingest), SQL (console with presets), Semantic search,
RAG chat (streaming), Hybrid (SQL + retrieval side-by-side).

## SQL dialect — examples

```sql
-- semantic search + residual metadata filter
SELECT id, region FROM docs
 WHERE semantic_match(content, 'zero trust')
   AND region = 'eu'
 LIMIT 5;

-- aggregation
SELECT region, COUNT(*) AS n FROM docs
 WHERE semantic_match(content, 'incident')
 GROUP BY region
 ORDER BY n DESC;

-- inner join, each side with its own retrieval
SELECT u.id, o.id, o.total
  FROM users AS u JOIN orders AS o ON u.region = o.region
 WHERE semantic_match(u.content, 'developer')
   AND semantic_match(o.content, 'subscription');
```

Explicitly unsupported (return `400` with `code: sql_unsupported`):
OR in WHERE, OUTER joins, subqueries, CTEs, HAVING, `JOIN` combined
with `GROUP BY`. The pgwire front end speaks only the **simple** query
protocol — `psql` works; `tokio-postgres::query()` (extended) does not.

## Configuration

Every knob is an environment variable; none are required.

| Variable                   | Default                            | Purpose                                     |
|----------------------------|------------------------------------|---------------------------------------------|
| `NEBULA_BIND`              | `127.0.0.1:8080`                   | REST bind address                           |
| `NEBULA_GRPC_BIND`         | *(unset → gRPC disabled)*          | gRPC bind                                   |
| `NEBULA_PG_BIND`           | *(unset → pgwire disabled)*        | Postgres wire bind                          |
| `NEBULA_API_KEYS`          | *(empty → auth off)*               | Comma-separated allowlist                   |
| `NEBULA_JWT_SECRET`        | *(unset → JWT off)*                | HS256 secret                                |
| `NEBULA_JWT_ISS` / `_AUD`  | *(unset → unchecked)*              | Optional JWT claim checks                   |
| `NEBULA_RATE_LIMIT`        | `on`                               | Set to `off` to disable                     |
| `NEBULA_RATE_LIMIT_BURST`  | `120`                              | Token-bucket capacity                       |
| `NEBULA_RATE_LIMIT_RPS`    | `20`                               | Token refill per second                     |
| `NEBULA_EMBED_DIM`         | `384` / `1536`                     | Required for non-Mock embedders             |
| `NEBULA_EMBED_CACHE_SIZE`  | `10000`                            | In-proc LRU entries (0 = off)               |
| `NEBULA_REDIS_URL`         | *(unset → Redis layer off)*        | e.g. `redis://redis:6379`                   |
| `NEBULA_REDIS_PREFIX`      | `nebula:embed:`                    | Key prefix                                  |
| `NEBULA_REDIS_TTL_SECS`    | `0` (no expiry)                    | Per-entry TTL                               |
| `NEBULA_OPENAI_API_KEY`    | *(unset → MockEmbedder)*           | Use OpenAI-compat embeddings                |
| `NEBULA_OPENAI_BASE_URL`   | `https://api.openai.com/v1`        | Works with vLLM, Ollama `/v1`, Azure        |
| `NEBULA_LLM_OLLAMA_URL`    | *(unset → MockLlm)*                | Ollama base URL for RAG                     |
| `NEBULA_LLM_OPENAI_KEY`    | *(unset)*                          | Overrides Ollama; OpenAI chat for RAG       |
| `NEBULA_CHUNK_CHARS`       | `500`                              | Chunker window                              |
| `NEBULA_CHUNK_OVERLAP`     | `50`                               | Chunker overlap                             |
| `NEBULA_SQL_CACHE`         | `on`                               | SQL result cache                            |
| `NEBULA_SQL_CACHE_SIZE`    | `512`                              |                                             |
| `NEBULA_SQL_CACHE_TTL_SECS`| `30`                               |                                             |

## Running without Docker

```bash
cargo run --release -p nebula-server
```

Same env vars apply. With nothing set, the server boots with the
MockEmbedder + MockLlm — completely offline, deterministic, and
suitable for integration tests.

## Running the tests

```bash
cargo test --workspace     # 120+ tests across all crates
cargo clippy --workspace --all-targets -- -D warnings
```

## Workspace layout

```
crates/
  nebula-core/         IDs, error type, shared primitives
  nebula-vector/       HNSW + distance metrics
  nebula-embed/        Embedder trait + MockEmbedder + OpenAI client
  nebula-cache/        In-process LRU embedding cache
  nebula-redis-cache/  Redis-backed embedding cache (failure-transparent)
  nebula-chunk/        Chunker trait + fixed / sentence implementations
  nebula-llm/          LlmClient trait + MockLlm, OllamaLlm, OpenAiChatLlm
  nebula-index/        TextIndex: buckets, chunks, parent-child, delete
  nebula-sql/          Parser → typed plan tree → executor + result cache
  nebula-grpc/         Tonic services mirroring the REST surface
  nebula-pgwire/       pgwire SimpleQueryHandler over SqlEngine
  nebula-server/       Axum router + auth + rate limit + metrics + wiring
```
