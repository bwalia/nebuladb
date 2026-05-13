# HA + multi-region active-active + XDCR vision prompt

Paste the section below into a fresh Claude Code session against this
repo when you want to scope and ship the end-state: NebulaDB as a
**highly-available, multi-region, active-active vector database with
Couchbase-style cross-data-center replication (XDCR), zero downtime
on any single-component failure, and the lowest-achievable client
latency.**

This is the **vision** doc. It defines what "done" looks like, the
success criteria you'll be measured against, and the feature map
from Couchbase XDCR (a useful mental model) to NebulaDB. It does
**not** redesign anything that's already designed — RFC 0001
(multi-region active-active) and RFC 0003 (write availability) cover
the architecture. For the **sequenced rollout** that gets you here,
see `docs/active-active-rollout-prompt.md` — that's the tactical
companion to this strategic doc.

---

You are scoping the end-state of NebulaDB's HA + multi-region story.
Read everything in §0 before forming an opinion. Then map the
features in §3 to existing primitives, identify gaps, and propose
exactly what's missing — don't rewrite what's already designed.

## 0. Read first

In this order:

1. **`docs/active-active-rollout-prompt.md`** — the sequenced rollout
   that gets you to the end-state described here. Has the phase
   table, the verification gates, and the anti-patterns. This vision
   doc is the "what" / "why"; the rollout prompt is the "in what
   order."
2. `docs/design/0001-multi-region-active-active.md` — multi-region
   architecture. Per-bucket `home_region` + `home_epoch`,
   cross-region WAL tail, client-side routing, operator failover.
3. `docs/design/0003-write-availability.md` — within-region write
   availability. Pick exactly one of A/B/C/D.
4. `crates/nebula-grpc/src/cross_region.rs` and
   `crates/nebula-server/src/cross_region_status.rs` — the existing
   cross-region scaffold. Already ~600 lines; don't reinvent it.
5. `crates/nebula-server/src/cluster.rs` — `ClusterConfig`,
   `CrossRegionPeer`, env-var parsing. The shape of "peers and
   regions" already exists in code.
6. The Couchbase XDCR docs at
   <https://docs.couchbase.com/server/current/learn/clusters-and-availability/xdcr-overview.html>
   — for the mental model. NebulaDB doesn't have to match XDCR
   feature-for-feature, but XDCR is the right reference point and
   the user explicitly compared against it.

## 1. Definition of done — measurable success criteria

The system is "done" when **every** item below is true and verified
in production on `192.168.1.193` (single-host dev stack) plus at
least one second region:

### 1.1 Availability

| Failure scenario | Required behavior |
|---|---|
| One node in a region dies | Zero write or read downtime; surviving node accepts traffic within 1s. |
| Entire region goes offline | Other regions accept writes for their home buckets immediately; deposed-region buckets failover within 60s via operator action. |
| Cross-region link partitions for ≥ 10 min | Each region keeps serving local reads + writes for its home buckets; converge on heal with no data loss. |
| WAL recovery on a cold-start node | Other nodes serve the load. The recovering node returns 503 on `/healthz` (not `/healthz/live`) so client pools skip it; once recovered, it rejoins the rotation without operator action. |
| Rolling binary upgrade | Per-region rolling restart; no observable client error. |
| Snapshot-in-progress on any node | Reads on that node continue (parking_lot read-RW lock allows it); writers wait ≤ 5s for the snapshot to drain. |

### 1.2 Latency

| Operation | Target p50 | Target p99 |
|---|---|---|
| Local-region REST `GET /bucket/.../doc/...` | ≤ 5 ms | ≤ 20 ms |
| Local-region REST `POST /bucket/.../doc` (single-doc upsert, embedding cached) | ≤ 15 ms | ≤ 50 ms |
| Local-region `POST /api/v1/vector/search` (k=10, dim=384) | ≤ 25 ms | ≤ 100 ms |
| Local-region SQL `SELECT ... WHERE semantic_match(...)` | ≤ 50 ms | ≤ 200 ms |
| Cross-region read of a foreign-home bucket | ≤ 10 ms (served from local replica via cross-region tail) |
| Cross-region replication lag, steady-state | ≤ 1 s p99 |
| Cross-region replication lag, after 1-min link outage | ≤ 30 s back to steady-state |

These are the floor — not the goal. The goal is "lower." Establish
baselines today; track them as Prometheus histograms; alert on p99
regression > 50%.

### 1.3 Durability

| Failure | RPO (data loss) | RTO (time to serve) |
|---|---|---|
| Single node crash | 0 (WAL is durable, fsynced before ack) | ≤ 10 s (existing node picks up) |
| Region power loss | 0 for home-region writes; cross-region lag bounded by §1.2 | ≤ 60 s (operator failover) |
| Disk corruption on one node | 0 (rebuild from snapshot + WAL on a peer) | minutes |
| Snapshot corruption | 0 (next snapshot supersedes; WAL replay covers gap) | seconds |

### 1.4 Throughput (on the dev box `192.168.1.193`, indicative not guaranteed)

| Workload | Sustained |
|---|---|
| Single-doc upserts, MockEmbedder | ≥ 1000 req/s |
| Bulk upserts (1000 docs per request) | ≥ 50 req/s (i.e. 50k docs/s) |
| Vector search k=10 | ≥ 500 req/s on a 1M-doc index |

Production targets scale with hardware. The point is to have
numbers, not folklore.

## 2. The mental model: think Couchbase XDCR for vectors

Couchbase XDCR has six features that directly map to what NebulaDB
needs. The mental model is "every region has a full local copy;
writes propagate asynchronously; conflicts resolve deterministically;
operators manage the topology, not the data plane."

## 3. Couchbase XDCR feature map → NebulaDB

| Couchbase XDCR feature | NebulaDB equivalent | Status |
|---|---|---|
| **Continuous, async replication** between clusters | Cross-region WAL tail over gRPC. `cross_region.rs` skeleton already exists. | partly built (Phase 2 of RFC 0001) |
| **Bucket-level granularity** (replicate some, not all) | Per-bucket `home_region` + `replicated_to` in seed doc per RFC 0001. | designed, not implemented |
| **Bidirectional / multi-way topology** | Each region is leader for its home buckets, follower for everyone else's. Full mesh of cross-region tails. | designed |
| **Filter expressions** on what to replicate (Couchbase: by key prefix) | Per-bucket replication filter — already implicit in `replicated_to`. Per-doc filter is a future-stretch goal. | partial |
| **Conflict resolution** | Two choices: (a) LWW by `(wal_seq, node_id)` per RFC 0003 Option B, (b) home-region wins (no concurrent writes possible) per RFC 0001's design. Decide before shipping. | decision pending |
| **Pause / resume / drain** replication | Add `POST /admin/cross-region/{peer}/pause` / `/resume` REST endpoints; persist state in the cluster config. | not designed |
| **Lost-and-found** / persisted cursor | Already implemented for local follower (`FollowerCursor` in `nebula-server/src/state.rs`). Extend the same primitive to cross-region. | partial |
| **Optimistic XDCR** — send without waiting for target ack | NebulaDB's cross-region tail is push-based with cursor ack; matches the model. Tune ack batching for throughput. | yes (by design) |
| **Compression on the wire** | gRPC supports gzip / zstd compression natively. Enable on the cross-region service. | trivial follow-up |
| **TLS / mTLS between clusters** | Use rustls (already in the dep tree via reqwest). | not configured |
| **Per-link lag observability** | `/admin/replication` already reports lag for one peer. Extend to many. | partial |
| **Topology change ops** (add peer, remove peer, swap home) | `NebulaRegionFailover` CRD per RFC 0001. CLI: `nebulactl region failover ...`. | designed, not implemented |

If you're reading this and any item shows "designed, not
implemented," your first task is to **finish the implementation**,
not write a new design.

## 4. HA topology — what to deploy

The minimum production topology that satisfies §1:

```
        Region A (e.g. us-east-1)         Region B (us-west-2)        Region C (eu-west-1)
        ┌─────────────────────────┐       ┌────────────────────────┐  ┌────────────────────────┐
        │  ┌─────┐ ┌─────┐ ┌─────┐│       │ ┌─────┐ ┌─────┐ ┌─────┐│  │ ┌─────┐ ┌─────┐ ┌─────┐│
        │  │node1│ │node2│ │node3││  ←→   │ │node1│ │node2│ │node3││  │ │node1│ │node2│ │node3││
        │  └─────┘ └─────┘ └─────┘│       │ └─────┘ └─────┘ └─────┘│  │ └─────┘ └─────┘ └─────┘│
        │  3-node cluster         │       │  3-node cluster        │  │  3-node cluster        │
        │  (RFC 0003 Option B     │       │                        │  │                        │
        │   multi-leader OR Raft) │       │                        │  │                        │
        └─────────────────────────┘       └────────────────────────┘  └────────────────────────┘
                  ▲                                    ▲                          ▲
                  └─────────────────── cross-region WAL tails (full mesh) ──────────┘
                  │
                  │  GET /admin/bucket/:b/home-region (cached 30s)
                  │
            ┌─────┴──────┐
            │ nebula-    │
            │ client SDK │
            │ in app     │
            └────────────┘
```

Three regions × three nodes each = nine total. Each region:
- Tolerates **one node failure** without write disruption (Option B
  multi-leader: any of the three keeps accepting; Raft: quorum of
  two-of-three).
- Tolerates **one cross-region link** failure without data loss
  (writes continue locally; converge on heal).

Two regions is the minimum to claim "multi-region"; three is the
minimum to claim "no SPOF region" (one region dies → two left, still
quorum).

## 5. Latency budget breakdown — how to actually hit §1.2

This is what's NOT in any existing RFC. The architecture in
RFC 0001 says "reads are local"; this section says what that buys
you and where the remaining ms go.

### 5.1 The local-region read budget (target p99 ≤ 20 ms)

```
TCP accept + axum routing                  0.5 ms
  middleware chain (auth + rate limit)     1.0 ms
  bucket lookup (AHashMap read)            0.1 ms
  HNSW search (k=10, 1M docs)              5–15 ms
  serialize response + write               1.0 ms
                                          ────────
                                          ~10–20 ms
```

The HNSW search dominates. Optimization opportunities (ordered by
impact):
- **Lower `ef_search`** at the cost of recall — already tunable.
- **Per-bucket HNSWs** instead of one global graph — splits the
  search space.
- **GPU acceleration** for the distance computation — out of scope
  for now; flag if anyone proposes it.

### 5.2 The local-region write budget (target p99 ≤ 50 ms)

```
TCP accept + axum routing                  0.5 ms
  middleware chain                         1.0 ms
  embedder call (cached)                   1.0 ms
  embedder call (cold OpenAI)              [50–500 ms — busts budget]
  wal.append + fsync                       3–10 ms
  inner.write() + HNSW insert              5–20 ms
  serialize response                       0.5 ms
                                          ────────
                                          ~10–30 ms (warm)
                                          ~50–500 ms (cold embedder)
```

The embedder is the single biggest latency variable. Production
deployments must:
- Run the embedder co-located (same pod or local sidecar — Ollama
  is already in the compose).
- Warm the embed cache on cluster startup with a known-good vocab.
- Set a max embedder timeout (`NEBULA_LLM_TIMEOUT_SECS=30`
  already exists; consider lowering for online traffic).

### 5.3 The cross-region read budget (target p99 ≤ 10 ms)

Cross-region reads are **NOT supposed to cross regions** in the
steady state — they're served from the local replica that's been
tailing the home region's WAL. The budget is the local-region read
budget plus the freshness check (~0.1 ms). If you find yourself
proposing a synchronous cross-region call to serve a read, stop —
that's a design mistake; the architecture is built so this never
happens.

### 5.4 The cross-region write budget — N/A

Writes go to the **home region only** (client SDK routes there).
There is no cross-region write budget by design. The
"cross-region write latency" the user might worry about is actually
the **client → home-region** round trip — same as any geographically
distributed system. Mitigations:
- Put the home region geographically near the writer (per-bucket
  pinning).
- Allow per-bucket `homeRegion: auto` mode that picks the region
  with the most recent writers (future stretch).

## 6. What's NOT in scope, even at the end-state

Be explicit so reviewers don't ask:

- **Global transactions.** No 2PC, no Paxos across regions, no
  global locking. RFC 0001 §10 is explicit on this.
- **Strong consistency on cross-region reads.** Reads are eventually
  consistent unless you set `If-Match-Home-Epoch` to force a check.
- **Vector-level CRDT merge.** Two writers can't merge HNSW state.
  Conflicts resolve at the document layer (LWW or home-region-wins).
- **Per-document home region.** Granularity stops at the bucket.
  Anything finer makes the routing layer 10× more complex with no
  realistic use case to justify it.
- **Automatic, no-human failback** to the original home after a
  region recovers. The operator decides. (Open question #5 in
  RFC 0001; leaning toward manual.)
- **A new storage engine.** Everything sits on top of today's
  WAL + HNSW + replication.
- **A custom consensus protocol.** If RFC 0003 picks (A), use
  `openraft` or `raft-rs`. Don't roll your own.

## 7. The five non-negotiables

The work isn't shippable without these — if any one is missing,
push back:

1. **Idempotent `apply_wal_record`.** The same record applied twice
   must produce the same end state. Already true for upserts (LWW
   on `(bucket, external_id)`). Extend the property to every WAL
   record type; add a unit test that re-applies every record kind
   and asserts state-equality.
2. **Persisted cursors per replication link.** Local follower
   already has `FollowerCursor`. Every cross-region tail needs the
   same primitive, so a restart resumes where it left off — not at
   the WAL beginning.
3. **Bounded WAL growth.** With PR #25 (streaming snapshot) the
   scheduler can actually keep WAL compacted. Don't ship without
   the scheduler enabled in production. If the scheduler is
   disabled for any reason, alert on WAL size > 500 MB.
4. **Wire-format compatibility with `git main` snapshots.** New
   binaries must read old `.nsnap` and `.nwal` files. The
   byte-identity test in
   `crates/nebula-vector/src/hnsw.rs::streaming_serialize_matches_owned_serialize`
   is the model — apply the same pattern to any new format you
   introduce.
5. **Observable replication state.** `/admin/replication` returns
   per-peer lag, per-peer last-applied-seq, and per-peer
   error-rate. A peer with > 30 s lag pages on-call. No silent
   stalls.

## 8. The 13-page summary in one paragraph

NebulaDB end-state: three or more regions, three or more nodes per
region, each region accepts writes for its home buckets and serves
reads from a local replica of every bucket. Cross-region WAL
tailing keeps replicas eventually consistent within 1s p99.
Failure of a node is invisible; failure of a region triggers a
60s operator-driven bucket-home failover, no data loss. Clients
use an SDK that caches the bucket→region map and routes writes to
the right home. The control plane (failover, peer add/remove,
config) goes through the Kubernetes operator's CRDs. The data
plane is gRPC + REST + pgwire, no router in the hot path. The
mental model is **Couchbase XDCR for vectors** — same async
multi-master semantics, different storage primitives.

## 9. Next steps if you're picking this up cold

1. Read the two RFCs and the rollout prompt (§0 of this doc).
2. Confirm the success criteria in §1 match the user's
   expectations — they're aggressive; the user may want to soften
   one. **Ask before relaxing any.**
3. Open the issues called out in `docs/active-active-rollout-prompt.md`
   §1 — the two RFC-decision issues. Block on those.
4. Once decisions land, follow the rollout prompt's §2 phase table.
5. Treat §1 (definition of done) as your acceptance criteria. Every
   phase ends with measurements against the numbers in §1.2 and
   §1.4. No vibes-based "looks fast."

## 10. When to escalate to the user

Same triggers as the rollout prompt, plus:

- Any latency target in §1.2 looks unreachable on the chosen
  topology — propose a relaxation explicitly, don't paper over.
- Any throughput target in §1.4 requires hardware the user
  doesn't have — surface the gap before promising the number.
- The chosen RFC 0003 option turns out to be wrong mid-flight
  (e.g. you started B, hit a fundamental issue, want to switch to
  D). Stop. Surface. Don't quietly pivot.
- Anyone proposes "let's just use $OTHER_VECTOR_DB instead." Out
  of scope of this prompt; route to product.

Your first output when starting from this prompt is: "I've read
this vision, the active-active rollout prompt, and RFCs 0001 / 0003.
Here's my read of where the gap between today and the §1 success
criteria is, and the smallest sequence of PRs that closes it.
Decisions pending: [list]." Then proceed per the rollout prompt.

Do not start any implementation in this prompt's scope without:
- PR #25 (streaming snapshot) deployed to production and verified.
- PR #27 (deploy follower-port fix) merged so future deploys
  actually ship.
- The two RFC decisions (RFC 0003 option, RFC 0001 open
  questions §9) made.
