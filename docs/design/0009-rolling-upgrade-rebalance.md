# Design 0009: Zero-downtime rolling upgrade & rebalance

- **Status**: Implemented (validated on a real 2-node stack)
- **Author**: Claude + @bwalia
- **Created**: 2026-06-14
- **Tracks**: production requirement — "upgrade a node without downtime; reads
  and writes survive a cluster-wide version bump"
- **Relates to**: 0007 (zero-downtime reads/writes — this is its Phase 3 made
  concrete + the orchestration on top), 0003 (write-availability), 0006
  (parallel WAL replay — recovery), 0005 (WAL CDC streaming — the follower tail)

---

## 1. Why this exists

ADR 0007 fixed *snapshots* (Phase 1: snapshot off the standby, leader
compact-only) so the WAL stays small and recovery is minutes, not hours. It
left the **upgrade path** broken in two distinct ways, both of which cause an
outage on every version bump:

1. **The deploy recreates BOTH data nodes at once.** `deploy.yml`'s
   `recreate_db=1` branch runs `docker compose up -d --build --remove-orphans`
   (`.github/workflows/deploy.yml`), which recreates `nebula-server` *and*
   `nebula-follower` in the same step. Both go through WAL recovery
   simultaneously → there is a window with **no serving node**. nginx correctly
   returns a clean 503 + `Retry-After` during it (`apps/showcase/nginx.conf`
   error_page intercept), but a clean 503 is still a failed read. This is the
   opposite of a rolling upgrade.
2. **Writes have no failover at all.** Reads already fail over node↔node on 503
   (read pool, `max_fails=0` + `http_503`), but writes are pinned to
   `${NEBULA_SERVER}` (`nginx.conf` — "writes can't fail over today"). The
   follower 409s every write (`middleware.rs::guard_writes_on_follower`,
   plus the gRPC `failed_precondition` and pgwire SQLSTATE `25006` guards), and
   role is a **boot-time static** env var (`NEBULA_NODE_ROLE`,
   `cluster.rs`). So even a perfect one-at-a-time recreate still drops **writes**
   for the entire leader-recovery window.

There's a quieter third problem that makes a promoted follower unsafe today:

3. **The follower never persists replicated writes to its own WAL.**
   `TextIndex::apply_wal_record` (`crates/nebula-index/src/lib.rs`) deliberately
   mutates only the in-memory arena and **does not re-append to the WAL** — the
   comment there says so explicitly. The follower tracks a *cursor*
   (`FollowerCursor`, `state.rs`; durable `FileCursorStore`,
   `nebula-grpc/src/follower.rs`) but the records themselves are never written
   to its local WAL. If we promote that follower and it later restarts, its WAL
   tail is empty — it would recover only to its last *snapshot*, silently losing
   every write applied since. Promotion is unsafe until this gap closes.

## 2. Goal

A full cluster-wide version upgrade completes with **0 failed reads and 0
failed writes** observed by a continuous client, **at most one data node
offline at any moment**, each upgraded node verified caught-up before the next
proceeds, driven automatically by the deploy with a working abort path.

Non-goal: automatic leader election. A 2-node cluster cannot safely auto-elect
without an external arbiter (split-brain) — promotion stays **operator/
orchestrator-triggered**, exactly as ADR 0007 §6 states. The orchestrator *is*
that deliberate trigger; it is not a consensus protocol.

## 3. Target architecture

| Concern | Design |
|---|---|
| **Order** | The deploy upgrades data nodes **strictly one at a time**: drain → recreate → wait-until-ready-AND-caught-up → only then touch the next. An explicit guard aborts the rollout rather than ever recreating the last serving node. |
| **Writes during leader upgrade** | Before the leader is recreated, the orchestrator **promotes the warm follower at runtime** (`POST /admin/promote`) and points nginx writes at it. The old leader comes back as a follower of the new leader. |
| **Reads during any upgrade** | The existing read pool already fails over on 503. We additionally make the leader-preference (`weight=100`) *drain-aware* so a node being upgraded sheds its read share **before** it goes down, instead of relying on 503-retry after the fact. |
| **Rejoin = rebalance** | An upgraded node restores from the shared snapshot (ADR 0007 Phase 1), replays only its WAL tail, then **WAL-tails the current leader to catch up**. It does not count as "serving" for rollout purposes until its replication cursor ≈ leader head. |
| **Durability of a promoted node** | The follower now **persists replicated writes into its own WAL** as it applies them (closes gap #3), so a promoted node has a complete, durable log and is a safe leader. |
| **Bounded recovery** | Snapshots already run on the standby (ADR 0007 Phase 1) so the un-snapshotted WAL is small → each rejoin is minutes. The rollout enforces a per-node catch-up budget and fails **loud** if exceeded. |

### 3.1 Does this need N≥3?

**No — N=2 is sufficient and is the target**, because promotion is
orchestrator-triggered, not quorum-based. The invariant we enforce is "at most
one node offline," which with 2 nodes means "exactly one serving node at all
times." The sequence for a 2-node bump:

1. Both up: `A`=leader, `B`=follower (caught up).
2. Promote `B`→leader; repoint nginx writes to `B`. (`A` still serving reads.)
3. Recreate `A` on the new image; it rejoins as follower of `B`, restores
   snapshot, tails `B` until caught-up.
4. Promote `A`→leader again (or leave `B` as leader — both are valid; we
   re-promote `A` only to keep role/hostname stable for ops). Repoint writes.
5. Recreate `B`; it rejoins as follower of `A`, catches up. Done.

At no step are both nodes offline, and writes are always accepted by whichever
node currently holds the leader role. **N≥3 is not required and does not fit
the host** (125 GiB, two ~60 GiB arenas — ADR 0007 §1). If a future deployment
wants quorum auto-election it must pair with ADR 0007 Phase 2 (int8
quantization, ~4× smaller) or go cross-host; that is explicitly out of scope
here and called out in §7.

## 4. Phase A — durable follower WAL (correctness prerequisite)

**The smallest, highest-leverage change, and a hard prerequisite for safe
promotion.** Today `apply_wal_record` skips the WAL by design. The follower's
`TextIndex` already owns an open `Wal` instance (constructed in
`open_persistent` when `NEBULA_DATA_DIR` is set; `wal_stats()` is `Some` on a
follower — the scheduler guard already relies on this). Nothing writes to it.

Change: in the follower drain loop (`nebula-grpc/src/follower.rs`, right after
`index.apply_wal_record(&rec)` succeeds), durably append the same record to the
follower's local WAL via a new `TextIndex::append_replicated(&rec)` — a thin
wrapper over the existing private `wal_append` that skips the leader-only path.
The cursor `store.save(applied_next)` call that follows is unchanged: it still
tracks the **leader-WAL** position, which is what the snapshot scheduler stamps
via `snapshot_to(dir, applied_seq)` (ADR 0007 Phase 1). The follower's *own*
WAL seq space is independent and only matters after promotion.

### 4.1 Correctness

- Apply-then-append ordering: we append to the local WAL **after** the
  in-memory apply succeeds, so a record that fails to apply is never logged.
  Crash between apply and append loses at most the last record, which the
  follower re-tails from its (not-yet-advanced) cursor on restart — at-least-
  once, idempotent applies (upsert/delete are idempotent by id).
- Snapshot interaction: the follower's snapshot is still stamped with the
  **leader** seq (unchanged). Its local WAL compaction stays disabled while it
  is a follower (`SnapshotMode::FollowerSnapshot` "never compacts" —
  `snapshot_scheduler.rs`); it has an authoritative WAL only **after**
  promotion, at which point the leader compact-path applies normally.

## 5. Phase B — runtime promotion (`POST /admin/promote`)

ADR 0007 Phase 3, made concrete. The crux discovered during design: **role is
read in three independent places that do not share state.**

- REST reads `AppState.cluster: Arc<ClusterConfig>` (`state.rs`) — `role` is a
  plain `Copy` field, immutable behind the `Arc`.
- gRPC holds `GrpcState.role: NodeRole` — a **value copy** taken at boot
  (`main.rs`), not a reference into `ClusterConfig`.
- pgwire holds `NebulaHandlers.role` / `NebulaQueryHandler.role: NodeRole` —
  also **value copies** at boot (`main.rs`).

So flipping `ClusterConfig.role` alone would leave gRPC and pgwire still
rejecting writes. The fix is a **single shared atomic role** all three read:

1. Introduce `AtomicNodeRole` (a `u8` atomic newtype in `nebula-core` next to
   `NodeRole`, with `load()`/`store()` and `is_read_only()`).
2. `AppState`, `GrpcState`, and the pgwire handlers each hold an
   `Arc<AtomicNodeRole>` — **the same Arc**, constructed once in `main.rs` and
   cloned into all three servers. `ClusterConfig.role` stays as the boot-time
   *initial* value used to seed it.
3. `guard_writes_on_follower`, the gRPC guards, and the pgwire guard all read
   `role.load().is_read_only()` instead of their cached copies.
4. `POST /admin/promote`: if currently `Follower`, `store(Leader)`, flip the
   server to stop tailing (cancel the follower task) and start accepting
   writes, return `{ "role": "leader", "promoted_at": ... }`. Idempotent: a
   promote on an already-leader is a 200 no-op. A `Standalone` node 409s
   (nothing to promote). Guarded by the existing `require_auth` layer (admin
   routes already sit behind it — `middleware.rs`); promotion additionally
   requires the bearer token in `NEBULA_OPERATOR_BEARER` (the operator secret
   the K8s operator already uses, `operator/.../nebulaclient`).

Demotion (leader→follower for the old leader rejoining) is the same mechanism
in reverse: set role `Follower`, set `NEBULA_FOLLOW_LEADER` to the new leader,
(re)start the follower tail task. Because a recreated container re-reads env,
the simplest demotion is **recreate with follower env** — no live-demote RPC
needed for the rollout (the old leader is being upgraded anyway).

## 6. Phase C — caught-up signal

The orchestrator needs a machine-readable "this node has caught up" gate. We
already compute lag in `admin_replication` (`router.rs` — `lag_bytes`,
`behind`, probing the leader's `/admin/replication`). Expose it as:

- `GET /healthz/caught-up` → `200 {"caught_up":true,"lag_bytes":0}` when this
  node is a follower whose applied cursor ≥ the probed leader head (within a
  small byte threshold for in-flight records), `503 {"caught_up":false,...}`
  otherwise. A leader/standalone returns `200 {"caught_up":true}` trivially.
- Prometheus metrics: `nebula_replication_lag_bytes` (gauge) and
  `nebula_replication_caught_up` (0/1) — so the rollout *and* dashboards/alerts
  share one source of truth.

"Caught up" = `behind == false` AND `lag_bytes` under a threshold (default a
few KiB to tolerate a record mid-flight). The orchestrator polls this between
steps; **a node does not count as serving until it returns caught-up.**

## 7. Phase D — nginx write-failover + drain-aware reads

`apps/showcase/nginx.conf` today routes writes straight to `${NEBULA_SERVER}`.
Changes:

- Add a `nebula_write_pool` upstream containing both nodes, with the **current
  leader preferred**. Writes route through it with
  `proxy_next_upstream … non_idempotent` and a short retry budget, so a write
  that hits a node returning the follower-409 (or a connection error mid-
  promotion) retries the other node. The 409 body is intercepted (like the 503
  intercept today) so the internal `read_only_follower` payload never reaches a
  client.
- **Drain-aware reads**: introduce a way to shed a node's read share *before*
  it is recreated. Two options (decided in implementation): (a) an nginx reload
  with the draining node removed from `nebula_read_pool`, or (b) a per-node
  `down` flag toggled via a tiny control endpoint. Reload (a) is simplest and
  the orchestrator already controls the compose project, so it can template the
  conf with the draining node omitted and `nginx -s reload` before the
  drain. The `weight=100` leader preference is dropped to even weighting during
  an active rollout so the standby can absorb full read traffic without the
  "snapshot stall on the follower" problem that motivated the weight — during a
  rollout the standby is the *serving* node, not snapshotting.

## 8. Phase E — the rolling orchestrator in deploy.yml

Replace the `recreate_db=1 → up -d --build --remove-orphans` branch with a
sequential loop (a script `scripts/rolling_upgrade.sh` called by `deploy.yml`,
so it's testable outside CI):

```
build images (no container touched)            # existing pre-step
if leader binary unchanged: converge sidecars only, exit   # existing fast path
else:
  for node in [follower, leader]:              # follower first, then leader
    assert exactly one node currently serving  # NO-SIMULTANEOUS-OFFLINE guard
    if node == leader:
      promote(other) ; repoint nginx writes ; wait other is leader
    drain(node)                                 # shed reads (Phase D)
    recreate(node) with correct role env        # one container only
    wait /healthz ready            (budget: T_ready)
    wait /healthz/caught-up        (budget: T_catchup)   # Phase C
    re-add node to read pool
  re-promote A to leader if desired ; final nginx converge
```

- **No-simultaneous-offline guard**: before recreating any node, assert the
  *other* node is `/healthz`-ready. If not, **abort** (don't recreate) and dump
  state — never violate the invariant to make progress.
- **Abort/rollback**: any budget breach (`T_ready`, `T_catchup`) stops the
  loop, leaves the not-yet-touched node serving, and exits non-zero. Because we
  only ever touched one node, the cluster is still healthy on abort. The
  partially-upgraded node either catches up on its own (transient) or is
  rolled back by re-pinning nginx to the untouched node (which never went
  down).
- **Observability**: each step echoes `::notice::` lines (`node`, phase:
  `draining|recreating|ready|caught-up`) into `$GITHUB_STEP_SUMMARY` and the
  metrics from Phase C carry the same signal for dashboards.

## 9. Phase F — the zero-failure test

`scripts/test_rolling_upgrade.sh` (style of `scripts/test_multiregion.sh`),
run in nightly + locally against a **scratch compose project** (never the live
node — pushing to `main` auto-deploys, §constraints):

1. Bring up a 2-node scratch project on a throwaway compose project name.
2. Start a background client doing continuous reads **and** writes (a loop of
   `PUT` a doc, `GET` it back, `SELECT` via pgwire), recording every non-2xx.
3. Run `rolling_upgrade.sh` to bump the image across both nodes.
4. Assert: **0 failed reads, 0 failed writes**, and (by polling
   `/admin/cluster/nodes` / `docker compose ps` throughout) **never two nodes
   down at once**, and each node reported caught-up before the next was
   touched.
5. Tear down the scratch project.

## 10. Correctness argument (summary)

- **No lost writes on promotion**: Phase A makes the follower's WAL complete,
  so the promoted node is a durable leader; a write accepted post-promotion is
  in *its* WAL.
- **No failed writes during the window**: writes route through `nebula_write_pool`
  with `non_idempotent` retry; the only node returning 409 is the one we are
  *not* writing to, and the retry lands on the leader. The promote happens
  *before* the old leader is drained, so there is always a write-accepting
  leader.
- **No failed reads**: at most one node is down (guard), the other is in the
  read pool and ready (asserted before recreate), drain sheds the outgoing
  node's share before it stops.
- **No simultaneous offline**: explicit pre-recreate readiness assert on the
  other node; abort rather than violate.
- **Bounded rejoin**: snapshots-on-standby (ADR 0007 Phase 1) keep the WAL tail
  small; `T_catchup` budget fails loud if not.

## 11. Phasing / PR plan

Each phase is a separate, independently-reviewable PR; later phases depend on
earlier ones:

- **A** — durable follower WAL (`append_replicated`). Self-contained; testable
  with a follower-restart unit/integration test. *No behavior change to leader.*
- **B** — `AtomicNodeRole` + `/admin/promote` + shared-role wiring across
  REST/gRPC/pgwire. Depends on A for safety.
- **C** — `/healthz/caught-up` + replication metrics. Independent of B; do
  alongside.
- **D** — nginx write pool + drain-aware reads. Depends on B (needs a
  promotable leader to fail over to).
- **E** — `rolling_upgrade.sh` + `deploy.yml` rewrite. Depends on B, C, D.
- **F** — `test_rolling_upgrade.sh`. Depends on E; gates the whole thing in CI.

## 11a. What real-stack testing changed (post-implementation)

Two things only surfaced when the rollout was run against an actual
2-node compose stack, not in unit tests — both worth recording because
they contradict the original plan above:

1. **Promote-without-fence causes split-brain.** The first cut promoted
   the standby and recreated the old leader, but the old leader's compose
   env is `NEBULA_NODE_ROLE=leader`, so it rejoined as a *second* leader.
   With both nodes leaders and neither following the other, they accepted
   independent writes and **diverged** (each had docs the other lacked).
   The availability checks (reads/writes succeed, never two offline) all
   passed — divergence is a *consistency* failure they don't catch. Fix:
   **fence-before-promote** — STOP the outgoing leader before promoting
   the incoming one, so there is never a two-leader moment; and end at
   the original topology via a transient follower-role override
   (`docker-compose.rollover.yml`) then a default-env recreate, so a
   later restart is coherent. The test now asserts exactly-one-leader and
   post-upgrade convergence to both nodes.

2. **Static nginx write weighting can't follow leadership.** A
   `weight=100` write pool keeps sending writes to `nebula-server` even
   after leadership moves to the follower — which then 409s them, and
   nginx has no `http_409` failover. Fix: the edge is **OpenResty + Lua**
   (`apps/showcase/lua/`), routing writes to whoever currently reports
   `role=leader` (cheap `GET /healthz/role`, cached briefly) and retrying
   the other node on a 409/503/connection-failure *within the same
   request*. No `nginx -s reload` — the edge follows leadership at request
   time. This also rides out the brief no-leader fence window so writes
   hold rather than fail.

**Residual tradeoff:** on a 2-node cluster, fence-before-promote means a
short (~2-3s) window per handoff where no node is leader. Writes are held
and retried (edge budget + client `Retry-After`); reads never stop. A
client that doesn't retry at all may see a 503 during a handoff — correct
backpressure, not data loss. Eliminating even that window needs ≥3 nodes
with quorum hand-off, which doesn't fit the host (§3.1).

## 12. Rejected / deferred

- **Auto-election / quorum (raft)** — needs ≥3 nodes, doesn't fit the host
  (ADR 0007 §1, §7). Orchestrator-triggered promotion gives the same upgrade
  safety without a consensus protocol or a third arena. Revisit only with
  ADR 0007 Phase 2 (quantization) or a cross-host topology.
- **Live leader→follower demotion RPC** — not needed for the rollout: the old
  leader is being recreated anyway, so it rejoins as a follower via env. A live
  demote endpoint is extra surface with no rollout consumer; deferred.
- **K8s-operator-driven rollout** (`operator/`) — the operator already models a
  `NebulaCluster` and could own this loop instead of `deploy.yml`. But prod is
  `docker compose` on one host today (ADR 0007 §1); the compose orchestrator is
  the load-bearing path. Porting the same drain→promote→wait logic into the
  operator's reconcile is a follow-up once prod moves to K8s.
- **Cross-host snapshot ship over gRPC** — only if prod stops being single-host
  (same as ADR 0007). The shared-volume snapshot exchange is assumed throughout.
