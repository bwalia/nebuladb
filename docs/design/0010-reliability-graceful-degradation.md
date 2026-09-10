# Design 0010: Reliability, resource awareness, and graceful degradation

- **Status**: Proposed — Phase 1 implemented in this PR
- **Author**: Claude + @bwalia
- **Created**: 2026-07-04
- **Tracks**: mission requirement — "NebulaDB should continue serving queries when other databases are degraded"
- **Relates to**: 0003 (write-availability), 0006 (WAL replay), 0007 (zero-downtime reads/writes), 0008 (RAG pipeline), 0009 (rolling upgrade)

---

## 1. Why this exists

NebulaDB already has strong durability primitives (CRC-framed fsync'd WAL with
torn-tail truncation, `.ok`-blessed atomic snapshots, cursor-gated WAL
compaction, a runtime-stall watchdog) and a working replication story
(WAL-streaming followers, operator-triggered promotion, cross-region
consumers). What it does **not** have is any runtime awareness of the
resources it is consuming, and no concept of *degrading* instead of *failing*:

1. **No resource manager.** Every limit is a static env var read once at
   boot (`NEBULA_WORKER_THREADS`, `NEBULA_EMBED_CACHE_SIZE`, …). The only
   runtime resource reading is the cgroup memory scrape on `/metrics`
   (`crates/nebula-server/src/router.rs` `render_memory_metrics`) — it feeds
   an alert, not a behavior. Nothing observes CPU or disk at all.
2. **Disk-full is an unhandled failure.** `Wal::append` propagates raw
   `io::Error`; a full volume surfaces as opaque 500s while the process keeps
   accepting writes it cannot make durable. ADR 0007's history shows what a
   near-cap *memory* incident looks like; disk has the same shape and no
   mitigation.
3. **AI availability is coupled to database availability.** Every write path
   (`TextIndex::upsert_text` / `upsert_text_bulk` / `upsert_document` in
   `crates/nebula-index/src/lib.rs`) awaits `embedder.embed(...)` inline
   *before* WAL append, with **no retry, no queue, no fallback**. A down or
   slow embedding provider makes the database write-unavailable (HTTP 502),
   which directly violates "AI workloads must never prevent database
   availability."
4. **No admission control.** One flat token-bucket per principal
   (`ratelimit.rs`). A burst of analytics-grade queries competes equally with
   writes and replication for the same tokio workers.
5. **Declared-but-unimplemented operator behavior.** `AutoscalingSpec` and
   `ReplicationSpec.RestartPolicyOnLag` exist in the `NebulaCluster` CRD but
   the controller never acts on them — the CRD advertises reliability the
   system doesn't deliver.

This ADR defines the target reliability architecture and a phased plan.
Phase 1 (this PR) lands the foundation everything else hangs off: the
**resource manager** and the **operating-mode state machine**.

## 2. Goal

Under memory, disk, CPU, or AI-provider pressure, NebulaDB transitions
through explicit, observable, hysteresis-guarded operating modes that shed
the *right* work first — background before interactive, AI before
transactional, analytics before writes — and recovers automatically when
pressure clears. An operator can always answer "what mode is the node in,
why, and since when" from `/metrics` or one admin endpoint.

## 3. Operating-mode state machine

One authoritative enum, owned by the new `nebula-resource` crate, derived
from per-resource pressure levels — never set directly by handlers:

| Mode | Entered when | Behavior contract |
|---|---|---|
| `Normal` | all resources below high watermark | everything enabled |
| `MemoryPressure` | resident ≥ 85% of cgroup cap | shrink embed/SQL caches, pause snapshot *scheduling* (never an in-flight snapshot), defer non-critical embedding |
| `CpuPressure` | sustained CPU ≥ 90% | reduce background concurrency, deprioritize analytics-class queries |
| `DiskPressure` | free space on data volume ≤ 10% | force snapshot+compact, prune old snapshots, throttle ingestion |
| `DiskCritical` | free space ≤ 3% or < 2× WAL segment size | **refuse mutations** (503 `write_unavailable` + `Retry-After`), reads unaffected — refusing a write is recoverable, a half-written WAL frame on a full disk is an incident |
| `AiDegraded` | embedding/LLM provider failing | serve vector/BM25/SQL reads normally; text-write behavior per §5 |

Rules, all enforced in one place (`nebula_resource::derive_mode`):

- **Severity-ordered**: when several apply, the most severe wins
  (`DiskCritical` > `DiskPressure` > `MemoryPressure` > `CpuPressure`).
  `AiDegraded` is an orthogonal flag, not a rung — a node can be
  `Normal + AiDegraded`.
- **Hysteresis**: enter at the high watermark, exit only below a lower one
  (e.g. memory: enter ≥ 85%, exit < 78%) so the mode can't flap with the
  sampler period.
- **Debounce**: a level must hold for N consecutive samples (default 2)
  before a transition fires. One anomalous statvfs read must not gate writes.
- **Every transition is an event**: `tracing` warn/info, a monotonically
  increasing `nebula_mode_transitions_total` counter, and a bounded in-memory
  ring readable at `GET /api/v1/admin/reliability`.

## 4. Resource manager (`crates/nebula-resource`, Phase 1)

A sampler + state machine, deliberately dependency-light (no `sysinfo`;
direct file/syscall reads like the existing cgroup scrape):

- **Memory**: cgroup v2 `memory.current`/`memory.max`, v1 fallback — same
  sources as `render_memory_metrics`, which moves into this crate as the
  single owner of that logic.
- **Disk**: `statvfs` on the data dir (free bytes + fraction). None when the
  index is in-memory.
- **CPU**: process CPU time delta between samples vs wall-clock ×
  `available_parallelism` (`/proc/self/stat` on Linux; `getrusage` fallback).
- Sampling every 5s from a `spawn_blocking` tick (same pattern as the
  existing `DurabilityMetricsCache` sampler in `main.rs`).
- Consumers read lock-free: mode and pressure levels are atomics; handlers on
  the write path pay one `Ordering::Relaxed` load.

The same struct is the future home of the *actuator* side (§6): components
register adjustable knobs (cache capacity, background concurrency) and the
manager retunes them on transitions. Phase 1 ships the observe/derive/gate
half only.

## 5. AI decoupling (Phase 2)

The single highest-leverage reliability fix after the gate:

- **Embedding queue**: a bounded, WAL-like durable queue
  (`nebula-embedq`). `upsert_*` with `embed_mode=deferred` (or when
  `AiDegraded` and the operator enables fallback) appends the document with a
  vector-pending marker; a background worker pool embeds in batches with
  exponential-backoff retries and inserts into HNSW on completion. BM25 and
  metadata search work immediately; vector search covers the doc once
  embedded. Synchronous embedding stays the default for compatibility.
- **Retries in `OpenAiEmbedder`**: bounded retry with jittered backoff on
  429/5xx/connect errors (today: zero retries anywhere in the stack).
- **Circuit breaker** per provider: after K consecutive failures flip
  `AiDegraded`, probe half-open in the background, recover automatically.
  While open, `search_text*` fails fast with a distinct code
  (`embedder_unavailable`) instead of burning a 30s timeout per request;
  `POST /vector/search` (client-supplied vectors) and BM25 are unaffected.
- **Embedding versioning**: stamp `(model, dim)` per document at write time;
  refuse mixed-space inserts unless a re-embed migration job is running.
  Today a model swap silently mixes incompatible vector spaces.

## 6. Adaptive scheduling & workload classes (Phase 3)

- Classify at the router: `Critical` (writes, replication, promote),
  `High` (interactive search/SQL), `Medium` (document search), `Low`
  (analytics/aggregates), `Background` (indexing, embedding, snapshots).
- Enforce with weighted semaphores (per-class concurrency budgets), not a
  custom runtime — tokio remains the scheduler; under `CpuPressure` the
  manager shrinks `Low`/`Background` budgets first, `Critical` never shrinks.
- Convert the flat rate limiter to class-aware admission: 429 `Low` before
  `High`, never 429 `Critical` on resource grounds (only on abuse grounds).
- Actuator registry: `CachingEmbedder` capacity, `SemanticCache` capacity,
  snapshot scheduler pause/force, follower batch size become runtime-tunable
  knobs the resource manager drives on mode transitions.

## 7. Replication & storage adaptivity (Phase 4)

- Adaptive follower catch-up batching (batch size/ack cadence scale with
  observed lag) and optional zstd frame compression on the WAL stream for
  cross-region links.
- `DiskPressure` integration: force snapshot+compact immediately, prune
  superseded snapshots, then throttle ingestion (per-class) before ever
  reaching `DiskCritical`.
- Resumability audit: backup/restore already stage-and-commit via manifest;
  add job-level checkpoints so an interrupted restore resumes at the last
  verified artifact instead of restarting.

## 8. Kubernetes operator (Phase 5)

- Implement the already-declared `RestartPolicyOnLag` (restart a follower
  stuck beyond `LagByteThreshold` with no progress for N minutes).
- Implement `AutoscalingSpec`: scale followers on the CRD's declared signals;
  scale *AI workers* (embedding worker Deployment, once §5 lands)
  independently of database StatefulSets.
- Surface the node operating mode in `NodeStatus` (probe
  `/api/v1/admin/reliability`); map any node in `DiskCritical`/persistent
  `MemoryPressure` to cluster `Phase=Degraded` with a reason.

## 9. Observability & reliability dashboard

Phase 1 adds to `/metrics`:

```
nebula_operating_mode          # gauge: 0=normal 1=cpu 2=memory 3=disk 4=disk_critical
nebula_ai_degraded             # gauge: 0/1 (orthogonal flag)
nebula_resource_pressure{resource="memory|disk|cpu"}   # 0=ok 1=high 2=critical
nebula_disk_free_bytes / nebula_disk_total_bytes
nebula_cpu_utilization_ratio
nebula_mode_transitions_total
nebula_writes_rejected_total   # 503s from the DiskCritical gate
```

plus `GET /api/v1/admin/reliability` (mode, per-resource readings/levels,
recent transitions with timestamps and causes) and the mode in `/healthz`.
A "Nebula Reliability" Grafana dashboard (mode timeline, pressure gauges,
rejected writes, transition annotations) joins the four existing dashboards;
alerts on `DiskCritical` and mode-flap frequency join `alerts.yml`.

## 10. Reliability testing (continuous)

Grow the existing nightly e2e job into a chaos lane, one scenario per PR
that touches the relevant subsystem: disk-full (small tmpfs data dir →
assert 503 gate then recovery after freeing space), memory pressure
(tight cgroup → assert mode entry + cache shrink, no OOM), embedder outage
(kill mock sidecar → assert reads unaffected + circuit-breaker recovery),
partition/lag (existing `replication.rs` tests extended with mode
assertions), pod eviction under load (operator kind job). The unit layer
tests `derive_mode` exhaustively with injected samples — hysteresis,
debounce, severity ordering are pure functions and cheap to cover.

## 11. Phasing

| Phase | Contents | Status |
|---|---|---|
| 1 | `nebula-resource` crate, mode state machine, disk-critical write gate, metrics + `/admin/reliability` | done (PR #90) |
| 2 | AI decoupling: embed retries, circuit breaker, deferred embedding (WAL-durable), embedder-identity guard | **this PR** |
| 3 | Workload classes, class-aware admission, actuator registry (adaptive caches/concurrency) | |
| 4 | Adaptive replication, disk-pressure compaction/throttling, restore checkpoints | |
| 5 | Operator: lag-restart, autoscaling, mode-aware cluster phase | |

## 12. Non-goals

- No custom thread scheduler; tokio + semaphores are sufficient and debuggable.
- No auto-promotion on leader loss (unchanged from ADR 0007 §6 — 2-node
  clusters can't safely self-elect; Raft mode is the eventual answer).
- No memory-based write gate in Phase 1: memory pressure is survivable
  (shed caches, spill) and half of it is addressed by int8 quantization
  already; a wrong 503 on memory grounds is worse than the risk. Disk is
  gated because a full WAL volume is unrecoverable without intervention.
