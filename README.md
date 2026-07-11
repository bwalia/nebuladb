# NebulaDB

AI-native hybrid database written in Rust. One binary serves REST,
gRPC, and the Postgres wire protocol over a shared corpus with an
HNSW vector index, a pluggable embedder, a multi-tier embedding
cache, and a streaming RAG endpoint — plus a React showcase app
that demos the whole thing end-to-end.

Runs standalone or as a Raft cluster with quorum-durable writes,
automatic leader failover, **self-healing membership** (take a node
down, bring a new one up, and the cluster reconfigures itself), and
active-active multi-region replication with automatic failover.
See [High availability & clustering](#high-availability--clustering).

![NebulaDB end-to-end architecture](docs/architecture.png)

> See [`docs/architecture.svg`](docs/architecture.svg) for the
> source diagram, and [`docs/durability-architecture.svg`](docs/durability-architecture.svg)
> for a deeper view of the WAL + snapshot + recovery path.

- **493 tests passing**, clippy-clean on Rust stable.
- **Nightly GitHub Actions** run the unit suite *and* the full
  docker-compose stack against real Ollama models; see
  `.github/workflows/nightly.yml`.

## Quick start

One-command boot — picks free host ports, builds everything, seeds
a knowledge corpus, prints URLs:

```bash
./scripts/start.sh                  # default (tinyllama chat model)
./scripts/start.sh --model llama3   # use a realistic chat model instead
./scripts/start.sh --no-showcase    # backend only, skip the React app
./scripts/start.sh --down           # stop + wipe volumes
./scripts/start.sh --help           # all flags
```

First run pulls `tinyllama` (chat, ~640 MB) and `nomic-embed-text`
(embeddings, ~275 MB) into a persistent Docker volume — slow the
first time, fast thereafter.

**If port 8080 (or any default) is already taken**, `start.sh`
auto-picks a free one and the URL table at the end reflects the
actual bindings. To pin a specific port instead:

```bash
NEBULA_REST_HOST_PORT=18080 ./scripts/start.sh
# Or any combination of:
NEBULA_REST_HOST_PORT=18080 \
NEBULA_SHOWCASE_PORT=15173 \
NEBULA_PG_HOST_PORT=15433 \
  ./scripts/start.sh
```


What you get (ports shown are defaults; the script auto-picks free
ones when they're already taken on your machine):

| Endpoint                                  | What it is                                             |
|-------------------------------------------|--------------------------------------------------------|
| `http://localhost:5173`                   | **Knowledge Ops showcase** — 5-tab demo UI             |
| `http://localhost:8080/healthz`           | REST liveness                                          |
| `http://localhost:8080/api/v1/*`          | REST API                                               |
| `http://localhost:8080/metrics`           | Prometheus scrape target                               |
| `grpc://localhost:50051`                  | gRPC (`DocumentService`, `SearchService`, `AIService`) |
| `psql -h localhost -p 5433 -U any -d any` | Postgres wire protocol                                 |
| `http://localhost:9090`                   | Prometheus UI                                          |
| `http://localhost:3000`                   | Grafana (admin / admin)                                |

Open the showcase and try the **Hybrid** or **RAG chat** tab after
`start.sh` finishes — the corpus is already seeded.

## RAG end-to-end, with `curl`

```bash
# 1. Ingest. Chunk + embed happens server-side.
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
#    event: context      one per retrieved chunk
#    event: answer_delta one per LLM token group
#    event: done
curl -N -X POST http://localhost:8080/api/v1/ai/rag \
  -H 'content-type: application/json' \
  -d '{"query": "explain dns failover", "top_k": 3, "stream": true}'

# 4. Same thing via SQL.
curl -s -X POST http://localhost:8080/api/v1/query \
  -H 'content-type: application/json' \
  -d '{"sql": "SELECT id, text FROM docs WHERE semantic_match(content, '\''dns failover'\'') LIMIT 3"}'

# 5. Same again over pgwire (psql or any PG driver speaking simple query).
psql -h localhost -p 5433 -U any -d any \
  -c "SELECT id, text FROM docs WHERE semantic_match(content, 'dns failover') LIMIT 3"
```

## Knowledge Ops showcase app

A React UI at `apps/showcase` that demos every capability end-to-end.
Five tabs:

- **Documents** — ingest single or chunked documents, with live
  chunk counts and metadata JSON.
- **SQL** — console with 4 presets (semantic, filter, `GROUP BY`,
  `JOIN`), `Ctrl+Enter` to run, click a row for its raw JSON.
- **Semantic search** — top-k with distance scores + expandable
  metadata per hit.
- **RAG chat** — SSE streaming with time-to-first-token measurement;
  context events land before tokens so citations are visible first.
- **Hybrid** — three curated presets that run a SQL+semantic query
  side-by-side with a plain semantic query, so the filter/re-rank
  effect is visible.

Run it with `./scripts/start.sh` (includes the app), or in dev mode:

```bash
cd apps/showcase
npm install
npm run dev   # http://localhost:5173, proxying to localhost:8080
```

## Seed corpus

`scripts/seed.sh` inserts 10 realistic "knowledge ops" docs (zero
trust, DNS failover, incident postmortems, subscription tiers) with
`region` and `owner` metadata, so every tab in the showcase has
interesting content to work with.

```bash
BASE_URL=http://localhost:8080 ./scripts/seed.sh
```

`start.sh` runs it automatically unless you pass `--no-seed`.

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

Explicitly unsupported (return `400` with `code: sql_unsupported` on
REST, SQLSTATE `42601` on pgwire): OR in WHERE, OUTER joins,
subqueries, CTEs, HAVING, `JOIN` combined with `GROUP BY`. The
pgwire front end speaks only the **simple** query protocol — `psql`
works; `tokio-postgres::query()` (extended) does not.

## Configuration

Every knob is an environment variable; none are required.

| Variable                   | Default                         | Purpose                                 |
|----------------------------|---------------------------------|-----------------------------------------|
| `NEBULA_BIND`              | `127.0.0.1:8080`                | REST bind address                       |
| `NEBULA_GRPC_BIND`         | *(unset → gRPC disabled)*       | gRPC bind                               |
| `NEBULA_PG_BIND`           | *(unset → pgwire disabled)*     | Postgres wire bind                      |
| `NEBULA_API_KEYS`          | *(empty → auth off)*            | Comma-separated bearer allowlist        |
| `NEBULA_JWT_SECRET`        | *(unset → JWT off)*             | HS256 secret                            |
| `NEBULA_JWT_ISS` / `_AUD`  | *(unset → unchecked)*           | Optional JWT claim checks               |
| `NEBULA_RATE_LIMIT`        | `on`                            | Set to `off` to disable                 |
| `NEBULA_RATE_LIMIT_BURST`  | `120`                           | Token-bucket capacity                   |
| `NEBULA_RATE_LIMIT_RPS`    | `20`                            | Token refill per second                 |
| `NEBULA_EMBED_DIM`         | `384` / `1536`                  | Required for non-Mock embedders         |
| `NEBULA_EMBED_CACHE_SIZE`  | `10000`                         | In-proc LRU entries (0 = off)           |
| `NEBULA_REDIS_URL`         | *(unset → Redis layer off)*     | e.g. `redis://redis:6379`               |
| `NEBULA_REDIS_PREFIX`      | `nebula:embed:`                 | Key prefix                              |
| `NEBULA_REDIS_TTL_SECS`    | `0` (no expiry)                 | Per-entry TTL                           |
| `NEBULA_OPENAI_API_KEY`    | *(unset → MockEmbedder)*        | Use OpenAI-compat embeddings            |
| `NEBULA_OPENAI_BASE_URL`   | `https://api.openai.com/v1`     | Works with vLLM, Ollama `/v1`, Azure    |
| `NEBULA_LLM_OLLAMA_URL`    | *(unset → MockLlm)*             | Ollama base URL for RAG                 |
| `NEBULA_LLM_OPENAI_KEY`    | *(unset)*                       | Overrides Ollama; OpenAI chat for RAG   |
| `NEBULA_CHUNK_CHARS`       | `500`                           | Chunker window                          |
| `NEBULA_CHUNK_OVERLAP`     | `50`                            | Chunker overlap                         |
| `NEBULA_SQL_CACHE`         | `on`                            | SQL result cache                        |
| `NEBULA_SQL_CACHE_SIZE`    | `512`                           |                                         |
| `NEBULA_SQL_CACHE_TTL_SECS`| `30`                            |                                         |

Compose-only overrides for host-side ports (the container-side
ports never change, so your env can move just one side):

| Variable                  | Default | Purpose                                         |
|---------------------------|---------|-------------------------------------------------|
| `NEBULA_REST_HOST_PORT`   | `8080`  | Host → container 8080                           |
| `NEBULA_GRPC_HOST_PORT`   | `50051` | Host → container 50051                          |
| `NEBULA_PG_HOST_PORT`     | `5433`  | Host → container 5432                           |
| `NEBULA_SHOWCASE_PORT`    | `5173`  | Host → container 80 (nginx)                     |
| `OLLAMA_CHAT_MODEL`       | `llama3`| Chat model pulled by `ollama-init`              |
| `OLLAMA_EMBED_MODEL`      | `nomic-embed-text` | Embedding model pulled                 |

## Running without Docker

```bash
cargo run --release -p nebula-server
```

Same env vars apply. With nothing set, the server boots with the
MockEmbedder + MockLlm — fully offline, deterministic, and suitable
for integration tests.

## High availability & clustering

NebulaDB scales from a single node to a multi-region, self-healing
cluster. All of it is opt-in — a default deployment is a single node
and behaves exactly as it always has.

### Raft mode (within-region HA)

Set `NEBULA_RAFT_PEERS` (with `NEBULA_RAFT_NODE_ID` and
`NEBULA_RAFT_DATA_DIR`) to run the node as part of a Raft cluster
(built on [openraft](https://github.com/databendlabs/openraft)):

- **Quorum-durable writes.** A write is acknowledged only after a
  majority of nodes have persisted it (fsync'd Raft log). A committed
  write survives the loss of any minority of nodes, including the
  leader — **no data loss**.
- **Automatic leader failover.** If the leader dies, the surviving
  nodes elect a new one automatically. Writes sent to a non-leader
  return `421 Misdirected Request` with the new leader's address, so
  a smart client re-targets without downtime.

```bash
NEBULA_RAFT_NODE_ID=1 \
NEBULA_RAFT_PEERS=1=node1:50052,2=node2:50052,3=node3:50052 \
NEBULA_RAFT_DATA_DIR=/var/lib/nebula/raft \
  cargo run --release -p nebula-server
# Form the cluster once, on any node:
curl -X POST http://node1:8080/api/v1/admin/raft/initialize
```

### Self-healing membership — take a node down, bring a new one up

The cluster reconfigures itself at runtime with no write outage and
no data loss, using a **learner catch-up → promote → evict** flow.
To replace a dead node `2` with a fresh node `4`:

```bash
# 1. Add the new node as a non-voting learner. openraft streams it a
#    snapshot + log tail and BLOCKS until it is caught up. Quorum is
#    unchanged during this phase, so writes keep flowing.
curl -X POST http://leader:8080/api/v1/admin/raft/learner \
  -H 'content-type: application/json' \
  -d '{"node_id": 4, "addr": "node4:50052"}'

# 2. Promote the caught-up learner to a voter (joint consensus).
curl -X POST http://leader:8080/api/v1/admin/raft/voter/4

# 3. Evict the dead node from the quorum.
curl -X DELETE http://leader:8080/api/v1/admin/raft/node/2

# Inspect membership at any time:
curl http://leader:8080/api/v1/admin/raft/membership
```

`scripts/replace_node.sh` drives this whole sequence and refuses to
run if evicting the old node would break quorum:

```bash
scripts/replace_node.sh --leader http://leader:8080 \
  --new-id 4 --new-addr node4:50052 --old-id 2
```

At every step a quorum of the *current* voter set stays available,
so committed writes are never lost and the leader never stalls.

| Endpoint                                | Method   | Action                                    |
|-----------------------------------------|----------|-------------------------------------------|
| `/api/v1/admin/raft/membership`         | `GET`    | Current voters / learners / leader        |
| `/api/v1/admin/raft/initialize`         | `POST`   | One-time cluster formation (idempotent)   |
| `/api/v1/admin/raft/learner`            | `POST`   | Add a learner, block until caught up      |
| `/api/v1/admin/raft/voter/:id`          | `POST`   | Promote a caught-up learner to voter      |
| `/api/v1/admin/raft/node/:id`           | `DELETE` | Evict a voter from the quorum             |

### Multi-region active-active with automatic failover

Across regions, NebulaDB runs active-active: each bucket has a
*home region* (a monotonic `home_epoch` fences out a deposed region's
stale writes) and regions tail each other's WAL. A **region failover
monitor** detects a dead home region and self-promotes the local
region automatically — no operator required:

```bash
NEBULA_REGION=us-west-2 \
NEBULA_REGION_FAILOVER_CANDIDATE=true \
NEBULA_REGION_FAILOVER_BUCKETS=catalog=us-east-1,orders=us-east-1 \
NEBULA_REGION_FAILOVER_GRACE_SECS=60 \
  cargo run --release -p nebula-server
```

When `us-east-1` is unreachable for the grace period, this region
bumps the bucket's epoch and takes over as the new home. Anti-flap by
design: continuous-unreachability grace + hysteresis, a single opt-in
candidate per home, monotonic epochs (no auto-failback / oscillation),
and a post-promotion cooldown.

The full design — including the two pre-existing multi-node Raft bugs
this work surfaced and fixed — is documented in
[`docs/design/0011-dynamic-membership-and-failover.md`](docs/design/0011-dynamic-membership-and-failover.md).

## Testing

```bash
cargo test --workspace                                   # 493 tests
cargo clippy --workspace --all-targets -- -D warnings    # clippy gate
```

End-to-end smoke scripts — usable locally and wired into CI:

```bash
./scripts/test_ollama.sh     # model pulls + generation + embedding APIs
./scripts/test_rest.sh       # CRUD + chunked upsert + search + RAG + SQL
./scripts/test_pgwire.sh     # psql SELECT, GROUP BY, parse errors
./scripts/test_rag.sh        # SSE streaming assertions, TTFT check
./scripts/test_metrics.sh    # /metrics counters + Prometheus scrape
./scripts/smoke_load.sh      # 60 concurrent requests, p95 budget
```

## Deployment

### Docker Hub images

`.github/workflows/publish.yml` builds multi-arch images
(linux/amd64 + linux/arm64) and pushes to Docker Hub on every main
push and every `v*.*.*` tag:

```bash
docker pull bwalia/nebula-server:latest
docker pull bwalia/nebula-showcase:latest
```

Skips gracefully on forks that don't have `DOCKERHUB_USERNAME` and
`DOCKERHUB_TOKEN` repo secrets set.

### Helm chart

`deploy/helm/nebuladb/` is a single chart covering the server,
showcase UI, optional bundled Redis, and an optional Prometheus
Operator `ServiceMonitor`. Chart docs live alongside the chart in
[`deploy/helm/nebuladb/README.md`](deploy/helm/nebuladb/README.md).

```bash
helm dependency update deploy/helm/nebuladb
helm install nebula deploy/helm/nebuladb

# Production-ish: external Redis, ingress on, persistence hook
helm install nebula deploy/helm/nebuladb \
  --set redis.enabled=false \
  --set externalRedisUrl=redis://prod-redis:6379 \
  --set server.ingress.enabled=true \
  --set server.ingress.hosts[0].host=nebuladb.example.com
```

### Host watchdog (compose deployments)

`restart: unless-stopped` (set on every service in `docker-compose.yml`)
covers crashes and Docker daemon restarts, but **not** containers
stuck in `Created` state — which is what you get when a `compose up`
run is interrupted, or when a CI deploy job gets cancelled
mid-`compose create`. Those containers never auto-start.

`scripts/reconcile.sh` closes that gap. It's an idempotent
`docker compose up -d --no-recreate --no-build` — a no-op when
everything is Up, a fast start for anything that isn't. Wire it to
cron or a systemd timer:

```bash
# user crontab — runs every 2 min
( crontab -l 2>/dev/null; \
  echo '*/2 * * * * /path/to/nebuladb/scripts/reconcile.sh >> /path/to/nebuladb/.reconcile.log 2>&1' \
) | crontab -
```

Logs to `.reconcile.log` (gitignored).

## CI

`.github/workflows/nightly.yml` runs:

1. **`unit`** — `cargo fmt`, `cargo clippy -D warnings`, `cargo test
   --workspace`, on every push / PR / nightly cron.
2. **`e2e`** — builds the compose stack, pulls real Ollama models,
   runs every `scripts/test_*.sh`, uploads container logs +
   `/metrics` as artifacts. Nightly cron + manual dispatch only.
3. **`notify`** — Slack webhook on scheduled failure, no-ops without
   a `SLACK_WEBHOOK_URL` secret.

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
  nebula-wal/          Write-ahead log: CRC-framed, fsync, snapshots
  nebula-raft/         openraft log/state-machine/snapshot/gRPC + membership
  nebula-sql/          Parser → typed plan tree → executor + result cache
  nebula-grpc/         Tonic services + WAL streaming + cross-region + failover
  nebula-pgwire/       pgwire SimpleQueryHandler over SqlEngine
  nebula-server/       Axum router + auth + rate limit + metrics + wiring

apps/
  showcase/            React + Vite + Tailwind demo UI (nginx in prod)

scripts/               Bash test + ops scripts (start.sh, seed.sh, test_*.sh)
deploy/                Prometheus + Grafana provisioning
docs/openapi.yaml      REST surface description
.github/workflows/     Nightly CI
```
