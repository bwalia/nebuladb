# NebulaDB — Use Cases and Worked Examples

Companion to the `README.md` and the architecture diagram at
[`docs/durability-architecture.svg`](docs/durability-architecture.svg).
For the operational story behind the durability + replication
layers this document leans on, open the SVG alongside this file —
the numbered flows (write / snapshot / recovery / compact / UI) map
directly onto the sections below.

> A PDF export of this document paired with the SVG is a planned
> follow-up. Until then, opening the SVG in any browser next to
> this markdown gives the same "diagram + narrative" view a PDF
> would.

## How to read this document

Each use case is structured the same way so you can scan them:

1. **Who it's for** — the engineering role that would adopt it.
2. **Why NebulaDB** — the honest fit; what we do and don't cover.
3. **Architecture** — what this looks like on top of today's
   components. References the SVG where a layer is relevant.
4. **Worked example** — a real REST / gRPC / SQL sequence you
   can paste into a running stack.
5. **Limits today** — what isn't built yet. No hand-waving.

We keep this document honest: it only describes use cases
buildable on components that currently exist in the repo. See the
final section for deferred use cases that are explicitly not yet
supported.

---

## Use case 1 — RAG over an internal knowledge base

**Who it's for.** Platform and product teams that want retrieval-
augmented generation over docs / tickets / runbooks without
gluing together a vector store, an embedding service, and an LLM
gateway.

**Why NebulaDB.** One binary carries the embedder (OpenAI-compatible
or Ollama), the HNSW index, chunking, and the RAG streaming
endpoint. The SSE `/ai/rag` response interleaves retrieved context
and LLM token deltas on one wire, which is exactly how a browser
chat UI wants to consume it.

**Architecture.**

- Embedder layer (Ollama or OpenAI-compatible) generates vectors.
- `nebula-chunk` splits incoming docs; each chunk becomes a doc
  in the index with a parent-id pointer so deletes cascade.
- `nebula-llm` talks to Ollama / OpenAI for generation.
- The write path goes through the WAL (see SVG layer 3, WAL box)
  so ingested docs survive a restart.

**Worked example.**

```bash
# Ingest one document (chunked server-side)
curl -s -X POST http://localhost:8080/api/v1/bucket/handbook/document \
  -H 'Content-Type: application/json' \
  -d '{
        "id":"onboarding",
        "text":"New hires are assigned a buddy on day one...",
        "metadata":{"team":"people-ops"}
      }'

# Ask a question; stream tokens + context
curl -N http://localhost:8080/api/v1/ai/rag \
  -H 'Content-Type: application/json' \
  -d '{"query":"what happens on day one?","top_k":4,"bucket":"handbook"}'
```

**Limits today.**

- No per-user access control on retrieved chunks beyond bearer
  auth / JWT.
- Re-embedding on model change is manual — bump `NEBULA_EMBED_DIM`,
  wipe the index, reingest.

---

## Use case 2 — Durable vector store with crash recovery

**Who it's for.** Engineers who need the vector store to survive
a `kubectl delete pod` without an export/import loop.

**Why NebulaDB.** `NEBULA_DATA_DIR` turns on the WAL + snapshot
path. Every mutation is framed, CRC-checked and fsynced before
being applied in RAM. On boot the server loads the newest
committed snapshot, then replays the WAL from the snapshot
boundary. A torn WAL tail (mid-fsync crash) is truncated to the
last good record automatically. See SVG layer 4 (disk layout) and
the **WRITE / SNAPSHOT / RECOVERY** flows at the bottom of the
diagram.

**Architecture.** Layered exactly as the SVG shows:

- `nebula-wal` — append-only log, 64 MiB segment rotation.
- `nebula-index::durability` — atomic snapshot commit via
  `.part` → fsync → rename → `.ok` marker.
- `TextIndex::open_persistent` orchestrates load + replay.

**Worked example.**

```bash
# Boot with durability on
NEBULA_DATA_DIR=/var/lib/nebuladb \
  /usr/local/bin/nebula-server

# Ingest, then ask for a snapshot explicitly
curl -X POST http://localhost:8080/api/v1/admin/snapshot
# {"path":"/var/lib/nebuladb/snapshots/snapshot-....nsnap","wal_seq_captured":0}

# Later: free disk by compacting WAL segments the snapshot
# already covers
curl -X POST http://localhost:8080/api/v1/admin/wal/compact
```

A restart now replays from the newest snapshot + remaining WAL.
Verify with `GET /healthz` — the `docs` count returns with the
same value it had before the restart.

**Limits today.**

- Snapshots are triggered manually or by an operator job, not on
  a schedule. A cron inside the binary is a future addition.
- Snapshot size scales with the index; multi-GB snapshots need a
  dedicated disk budget.

---

## Use case 3 — Read-scaling with a follower replica

**Who it's for.** Teams whose read QPS is about to saturate one
box and who want a second node mirroring the first.

**Why NebulaDB.** The session that added `ReplicationService.TailWal`
ships a follower mode that subscribes to the leader's WAL over
gRPC, applies every record into a local `TextIndex`, and persists
its cursor across restarts.

**Architecture.**

- Leader opens with `NEBULA_DATA_DIR=...`; its `Wal::subscribe`
  fans appends out over gRPC.
- Follower opens with `NEBULA_FOLLOW_LEADER=http://leader:50051`
  and (optionally) its own `NEBULA_DATA_DIR` so the cursor file
  survives restart.
- The router on a follower rejects writes with 409
  `read_only_follower` — see the cluster-awareness session for
  the middleware that enforces this.

**Worked example.**

```bash
# Leader
NEBULA_DATA_DIR=/var/lib/nebuladb \
NEBULA_NODE_ID=leader-1 \
NEBULA_NODE_ROLE=leader \
nebula-server &

# Follower (different host / pod)
NEBULA_DATA_DIR=/var/lib/nebuladb-follower \
NEBULA_FOLLOW_LEADER=http://leader:50051 \
NEBULA_LEADER_REST_URL=http://leader:8080 \
NEBULA_NODE_ID=follower-1 \
NEBULA_NODE_ROLE=follower \
nebula-server &

# Ingest on the leader
curl -X POST http://leader:8080/api/v1/bucket/b/doc \
  -d '{"id":"d0","text":"mirrored","metadata":{}}'

# Read on the follower — docs count reflects replicated state
curl http://follower:8080/healthz
# {"status":"ok","docs":1,...}

# Observe lag
curl http://follower:8080/api/v1/admin/replication
# {"role":"follower","follower_applied":{...},"lag_bytes":0,"behind":false,...}
```

**Limits today.**

- Single-leader only. Active-active / multi-master is **not**
  supported and will not be in the near term — see *Not supported*
  below.
- The follower's index state is RAM-only — on restart it
  re-replays from the persisted cursor position, not from its own
  WAL. This is deliberate: it makes split-brain recovery
  impossible because there is no local write history to
  reconcile.

---

## Use case 4 — Operational observability + live log tailing

**Who it's for.** On-call engineers debugging a stuck ingestion
or a flaky LLM response without shelling into a pod.

**Why NebulaDB.** Every `tracing::info!` / `warn!` / `error!` in
the server goes through a capturing subscriber that fans events
out over a broadcast channel. `GET /admin/logs/stream` is an SSE
endpoint that replays the last 256 events and then tails live.
Supports `?level=` and `?target=` filters.

**Worked example.**

```bash
# kubectl-logs style: snapshot + live tail
curl -N 'http://localhost:8080/api/v1/admin/logs/stream?level=warn'

# event: log
# data: {"ts_ms":...,"level":"warn","target":"nebula_grpc","message":"..."}
# event: snapshot_done
# ... live events follow ...

# Scope to one subsystem
curl -N 'http://localhost:8080/api/v1/admin/logs/stream?target=nebula_grpc'

# Pure tail (no historical replay)
curl -N 'http://localhost:8080/api/v1/admin/logs/stream?replay=false'
```

Combine with `/admin/replication` + `/admin/cluster/nodes` to get
a complete live operator view across a two-node setup without
touching Grafana or jumping between logs.

**Limits today.**

- Only the current process's logs. No leader + follower log
  aggregation yet; an operator tails each node separately.
- No download-to-file endpoint; redirect to a file client-side
  (`curl -N ... > session.log`).

---

## Use case 5 — Postgres-compatible hybrid search from BI tools

**Who it's for.** Analysts who want to run vector-aware queries
from `psql`, Metabase, DBeaver, or their existing Postgres client
library — without learning a bespoke SQL dialect.

**Why NebulaDB.** `nebula-pgwire` speaks the simple-query subset
of the Postgres wire protocol. The same SQL engine backs
`POST /api/v1/query` and the pgwire listener, so a query reads
identical results over both transports.

**Worked example.**

```bash
psql -h localhost -p 5433 -U any -d any <<'SQL'
SELECT id, score FROM docs
WHERE SEMANTIC_MATCH('onboarding buddy system')
ORDER BY score ASC
LIMIT 5;
SQL
```

The same query over REST:

```bash
curl -s -X POST http://localhost:8080/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT id, score FROM docs WHERE SEMANTIC_MATCH(?) ORDER BY score ASC LIMIT 5","params":["onboarding buddy system"]}'
```

**Limits today.**

- Simple-query only; no prepared statements / extended query over
  pgwire.
- No JOINs across buckets yet — one bucket per query.
- `SEMANTIC_MATCH` is the only vector predicate; no `<->` distance
  operator overload yet.

---

## Use case 6 — Embedding cost control with tiered caching

**Who it's for.** Teams whose monthly OpenAI bill is climbing
faster than their corpus is growing.

**Why NebulaDB.** Embedder calls are wrapped in a two-tier cache:
an in-process LRU on top, optional Redis layer beneath. The
Prometheus `/metrics` endpoint exposes hit/miss counters so you
can actually see where the money is going.

**Architecture.** Embedder composition done at boot in
`main.rs`:

```
raw OpenAI embedder
   → Redis layer (shared across replicas)
     → in-proc LRU (sub-microsecond hits)
```

A cache miss on the in-proc layer checks Redis before calling
OpenAI. A Redis failure is transparent — the server logs a
`warn` and keeps serving from the raw embedder.

**Worked example.**

```bash
NEBULA_OPENAI_API_KEY=sk-... \
NEBULA_EMBED_CACHE_SIZE=20000 \
NEBULA_REDIS_URL=redis://redis:6379 \
  nebula-server

# Then scrape:
curl -s localhost:8080/metrics | grep embed_cache
# nebula_embed_cache_hits   1834
# nebula_embed_cache_misses 142
```

A dashboard in Grafana (provisioned by `deploy/grafana/`) visualizes
the hit ratio over time.

**Limits today.**

- No per-model cache partitioning — mixing two embedders in one
  process pollutes each other's cache. Run separate processes per
  model.

---

## Use case 7 — Rate-limited public API in front of an LLM

**Who it's for.** Teams exposing NebulaDB's RAG endpoint directly
to end users (internal chatbot on an intranet, partner API, etc.)
who want burst protection without a sidecar proxy.

**Why NebulaDB.** A token-bucket rate limiter runs as middleware
on every `/api/v1/*` route. Keys are either the authenticated
principal or the peer IP. Defaults (120 burst, 20 rps) are sane
for a dev stack; tune via env.

**Worked example.**

```bash
NEBULA_API_KEYS=secret1,secret2 \
NEBULA_RATE_LIMIT_BURST=60 \
NEBULA_RATE_LIMIT_RPS=10 \
  nebula-server
```

A flood of requests beyond the configured rate returns 429 with a
`retry_after_ms` field; the Prometheus counter
`nebula_rate_limited_total` tracks rejections per label.

**Limits today.**

- One global bucket per key; no per-route tuning. Hot
  `/api/v1/ai/rag` shares a budget with cheap `/healthz` — in
  practice `/healthz` is mounted outside the limiter, so this
  rarely matters, but something to watch if you add more
  expensive routes.

---

## Not supported today (honest list)

These use cases get asked about. They are **not** buildable on the
current codebase without new components.

- **Active-active multi-region replication.** Single-writer only.
  No HLCs, no conflict resolution, no merge for HNSW. See the
  read-replica use case above for the sanctioned path.
- **Automatic sharding / partitioning.** One index per process.
  Horizontal scale comes from running multiple NebulaDB instances
  per dataset, each responsible for its own bucket slice, wired
  up application-side.
- **Transactions across documents.** Every upsert is its own
  commit. Cross-document atomicity would need a new commit
  protocol on top of the WAL.
- **Row-level / chunk-level ACLs.** Auth is coarse — bearer token
  or JWT grants full API access. A filtering layer on top of
  search hits would need to land in the index, not the router.
- **Cross-node log aggregation.** Logs stream per-node only. A
  fan-in aggregator (or Loki / CloudWatch adapter) is the
  realistic path, not a feature.
- **Retry / rollback for a rebalance engine.** The rebalance
  engine itself doesn't exist — there's nothing to retry. See the
  session notes on why active-active is a multi-month project.

---

## Related reading

- [`docs/durability-architecture.svg`](docs/durability-architecture.svg)
  — component diagram with numbered write / snapshot / recovery
  / compact / UI flows. Open next to this file for the "how" to
  match each "what" above.
- `README.md` — quick start, endpoint reference, Docker Compose
  topology.
- `docs/openapi.yaml` — machine-readable REST schema.
- `crates/nebula-wal/src/lib.rs` — WAL wire format and crash-
  safety discipline documented in the module header. Worth
  reading before relying on the durability story for anything
  load-bearing.
