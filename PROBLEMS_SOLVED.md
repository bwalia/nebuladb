# NebulaDB — Problems Solved by This Stack

Companion to [`USE_CASES.md`](USE_CASES.md) and the architecture
diagram at [`docs/durability-architecture.svg`](docs/durability-architecture.svg).

`USE_CASES.md` answers *"what would I build with this?"*
This document answers *"what pain does this remove?"* — one
problem per section, honest about where the solution ends.

Each entry has the same shape so you can scan:

1. **The pain** — the headache in one sentence.
2. **What's in the box** — the components that actually solve it.
3. **Where it lives** — code references and SVG layer.
4. **What it doesn't solve** — so you know where this stack stops.

---

## 1. "I'm gluing four services together for RAG"

**The pain.** A typical RAG stack is a vector DB + an embedder
service + a chunker script + an LLM gateway + a streaming layer.
Five processes, five deploy stories, five places auth can leak.

**What's in the box.** One binary (`nebula-server`) carries the
embedder (OpenAI-compatible or Ollama), `nebula-chunk`, the HNSW
index, and `nebula-llm`. `POST /api/v1/ai/rag` streams retrieved
context *and* generated tokens over a single SSE response.

**Where it lives.** `crates/nebula-llm`, `crates/nebula-chunk`,
`crates/nebula-server/src/router.rs` (`ai_rag` handler).

**What it doesn't solve.** No built-in prompt-template library —
the prompt is assembled from your query + hits. No per-tenant
LLM quotas beyond the global rate limiter.

---

## 2. "My vector DB loses everything when the pod restarts"

**The pain.** In-memory HNSW is fast; the ops team sees every
restart as a regression. A `kubectl delete pod` drops the index,
and someone has to re-ingest from source.

**What's in the box.** `NEBULA_DATA_DIR` turns on a WAL + snapshot
path. Every mutation is framed, CRC-checked, and fsynced before
being applied in RAM. On boot the newest committed snapshot
loads, then the WAL is replayed from the snapshot boundary. A
torn WAL tail (mid-fsync crash) is truncated to the last good
record automatically.

**Where it lives.** `crates/nebula-wal`,
`crates/nebula-index/src/durability.rs`,
`TextIndex::open_persistent`. See SVG layers 3 + 4 and the
**WRITE** / **RECOVERY** flows.

**What it doesn't solve.** Snapshots are operator-triggered — no
scheduler in the binary yet. Multi-TB datasets need a dedicated
disk budget and snapshot cadence you own.

---

## 3. "Analysts can't query my vector store from psql"

**The pain.** BI tools speak Postgres wire protocol. Every
bespoke SQL-over-REST dialect turns into an adapter nobody
maintains.

**What's in the box.** `nebula-pgwire` serves the simple-query
subset of the Postgres wire protocol on a configurable port. The
same `SqlEngine` backs REST and pgwire, so `SELECT ... WHERE
SEMANTIC_MATCH(...)` produces identical results over either
transport.

**Where it lives.** `crates/nebula-pgwire`, `crates/nebula-sql`.

**What it doesn't solve.** No prepared statements / extended
query. No JOIN across buckets yet. `SEMANTIC_MATCH` is the only
vector predicate — no `<->` operator overload.

---

## 4. "I can't tell which OpenAI calls are actually cached"

**The pain.** Embedding spend creeps up. The stack claims "it
caches" but there's no visibility into whether the cache is
doing anything useful.

**What's in the box.** Two-tier cache: in-process LRU on top,
optional Redis beneath. Prometheus metrics expose
`nebula_embed_cache_hits`, `_misses`, `_evictions`, `_inserts`.
A Grafana dashboard in `deploy/grafana/dashboards/` plots the
hit ratio.

**Where it lives.** `crates/nebula-cache`,
`crates/nebula-redis-cache`, `crates/nebula-server/src/router.rs`
(`metrics_handler`).

**What it doesn't solve.** No per-model partitioning. Running
two embedders in one process pollutes each other's cache — use
separate deployments.

---

## 5. "A slow follower quietly breaks my WAL compaction"

**The pain.** You add a replica for read-scaling. Six hours
later, the leader's disk is full because compaction kept cleaning
up segments the follower still needed.

**What's in the box.** Followers ack cursors back through the
subscriber hub. `Wal::compact` clamps to
`min(all_follower_acks)` — never deletes a segment a live
follower still needs. A dropped follower's ack slot is released
automatically, so compaction catches up as soon as the follower
disconnects.

**Where it lives.** `crates/nebula-wal/src/subscriber.rs`
(`SubscriberHub::min_ack`), `Wal::compact`.

**What it doesn't solve.** An *abandoned but still connected*
follower pins segments forever. The compacted WAL view in
`/admin/durability` flags when this is happening; teardown is
the operator's call.

---

## 6. "I don't know if my replica is keeping up"

**The pain.** The replica's `docs` count is lower than the
leader's. Is that lag, or is something broken?

**What's in the box.** `GET /admin/replication` returns a role-
aware view:

- On a leader: `local_newest` (WAL tip).
- On a follower: `follower_applied` (where we are),
  `leader_newest_probed` (where the leader is, fetched over
  HTTP), `lag_bytes` and `behind` when both cursors live in the
  same segment.

**Where it lives.** `crates/nebula-server/src/router.rs`
(`admin_replication`), `state::FollowerCursor`.

**What it doesn't solve.** Cross-segment lag is reported as
`null` — the byte delta is meaningless when cursors live in
different files. The UI should fall back to the `behind` boolean
in that case.

---

## 7. "I can't tail server logs without shelling into a pod"

**The pain.** An operator hits a bug at 2am. `kubectl logs` is
fine for the fleet cases, but when you're poking at one node
you want live output without leaving the admin UI.

**What's in the box.** A capturing `tracing::Subscriber` pushes
every event into a `LogBus` — a broadcast channel with a 256-
event ring buffer for late connecters. `GET /admin/logs/stream`
is an SSE endpoint that replays the ring then tails live.
Supports `?level=`, `?target=substring`, and `?replay=false`.

**Where it lives.** `crates/nebula-server/src/log_stream.rs`,
`router.rs` (`admin_logs_stream`).

**What it doesn't solve.** Current process only. Aggregating
across leader + follower needs a fan-in proxy or an external
log backend — out of scope for this stack.

---

## 8. "A write on a follower would silently diverge"

**The pain.** Someone aims their client at the wrong URL. The
follower accepts the write, commits it locally, and now the two
nodes disagree forever.

**What's in the box.** A middleware
(`guard_writes_on_follower`) returns 409
`read_only_follower` on any mutating HTTP method when
`NEBULA_NODE_ROLE=follower`. Runs *before* auth (no point
authenticating a wrong-node request) and *after* audit
(rejections still show up in the audit log).

**Where it lives.** `crates/nebula-server/src/middleware.rs`
(`guard_writes_on_follower`).

**What it doesn't solve.** gRPC and pgwire listeners don't have
the same guard yet. In practice followers disable those
listeners by not setting the env; adding guards there is a
follow-up.

---

## 9. "A chat UI polling for RAG answers feels bad"

**The pain.** `GET /rag?query=...` polled every 500 ms while the
LLM thinks is bad UX and worse on mobile.

**What's in the box.** `/api/v1/ai/rag` is SSE. The response
streams `context` events (retrieved chunks), `answer_delta`
events (LLM tokens), and a terminal `done`. The gRPC side
mirrors this with a server-streaming `AIService.Rag`.

**Where it lives.** `crates/nebula-server/src/router.rs`
(`ai_rag`), `crates/nebula-grpc/src/lib.rs` (`AiSvc::rag`).

**What it doesn't solve.** No partial-retrieval streaming —
all chunks are emitted up front, then tokens. If retrieval
itself is slow, the client waits for the first `answer_delta`.

---

## 10. "A noisy client can starve my health probes"

**The pain.** Rate-limit middleware typically covers everything.
A misbehaving client eats the global budget and Kubernetes
starts killing pods that were otherwise healthy.

**What's in the box.** `/healthz` and `/metrics` are mounted
*outside* the rate-limit and auth layers. Only `/api/v1/*` goes
through the token bucket. Probes and Prometheus scrapes keep
working during abuse events.

**Where it lives.** `crates/nebula-server/src/router.rs`
(`build_router` layer composition).

**What it doesn't solve.** The rate limiter is one global
bucket per key, not per-route. A hot `/ai/rag` shares budget
with a cheap `/bucket/:b/doc/:id`; split into separate
deployments if this becomes a contention.

---

## 11. "I can't tell which SQL queries are slow"

**The pain.** The query log is either too noisy (every query)
or too quiet (none). You want a "hall of fame" for the worst
offenders.

**What's in the box.** `SlowQueryLog` — a bounded priority
queue of the top-N slowest queries since boot, with a floor
threshold (`NEBULA_SLOW_QUERY_THRESHOLD_MS`). Surfaced as
`GET /admin/slow`; the showcase Admin tab renders it.

**Where it lives.** `crates/nebula-server/src/slow_log.rs`,
`router.rs` (`admin_slow`).

**What it doesn't solve.** No per-table / per-bucket slicing.
The SQL layer is simple enough today that a flat top-N is
enough information; this may change as the SQL engine grows.

---

## 12. "I need an audit trail without a separate service"

**The pain.** Compliance asks "who did what write when?" and
the honest answer is "it's not recorded anywhere structured."

**What's in the box.** `AuditLog` is a 1024-entry ring buffer
populated by middleware on every mutating request. Records
method, path, principal fingerprint (hashed token prefix + IP),
response status, timestamp. Surfaced at `GET /admin/audit`.

**Where it lives.** `crates/nebula-server/src/audit.rs`,
`middleware.rs` (`audit_writes`).

**What it doesn't solve.** In-memory ring only — 1024 entries
or one process lifetime, whichever comes first. A persistent
audit trail needs a log shipper to SIEM / CloudTrail.

---

## Not in scope (deliberately)

These are real problems, and NebulaDB *doesn't* solve them.
Listed here so nobody adopts this stack expecting them:

- **Active-active multi-region writes.** See the dedicated
  section in `USE_CASES.md`.
- **Transactions across documents.** Every upsert is its own
  commit.
- **Row-level / field-level ACLs.** Auth is coarse — bearer
  token or JWT grants API access; no filtering inside search
  hits.
- **Online schema evolution.** Buckets and metadata shapes are
  freeform; a migration tool is not part of this stack.
- **Cold storage tiering.** The full index lives in RAM + one
  snapshot on disk. Offloading rarely-accessed vectors to cold
  storage is a different architecture.
- **Geo-partitioned retrieval** (routing queries to the nearest
  region's index). Single-node today.

---

## Related reading

- [`USE_CASES.md`](USE_CASES.md) — what you'd build on top of
  these solved problems.
- [`docs/durability-architecture.svg`](docs/durability-architecture.svg)
  — component diagram with the flows that back problems 2, 5, 6.
- `README.md` — quick start + endpoint reference.
- `docs/openapi.yaml` — REST schema.
