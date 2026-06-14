# Prompt: Zero-downtime rolling upgrade & rebalance (Couchbase-style)

> Reusable engineering prompt for NebulaDB. Paste into Claude Code (or hand to an
> engineer) to design and implement multi-replica rolling upgrades with no
> read/write downtime. Grounded in the current architecture — read the
> "Context" section before proposing changes; do **not** assume a generic
> Kubernetes/StatefulSet setup.

---

## Role

You are working on **NebulaDB**, a Rust vector+document database
(`crates/nebula-*`, `apps/*`) deployed via `docker compose` on a single
self-hosted host (`192.168.1.193`) by `.github/workflows/deploy.yml`. Today it
runs **two nodes** (`nebula-server` = leader, `nebula-follower` = warm standby)
behind an nginx read pool in `apps/showcase`.

## Objective

Make version upgrades of a single pod/instance **non-disruptive**, the way
Couchbase does a rolling rebalance: while one instance is being upgraded,
**reads and writes continue uninterrupted from the other replicas**, and the
upgraded instance rejoins and re-syncs (rebalances) before the next instance is
touched. The end state is N≥2 replicas where **at most one is offline at a
time**, the orchestrator never recreates all data nodes simultaneously, and
clients observe zero failed reads and zero failed writes across a full
cluster-wide version bump.

## Context (current behavior — verify against the repo, don't trust this blindly)

- **Topology**: leader + follower, same host, **shared snapshots volume**
  (ADR 0007 Phase 1). Raft (`crates/nebula-raft`) is compiled but **dead** —
  never `initialize()`d, needs ≥3 nodes, won't fit the 125 GiB host. Not the path.
- **Roles are boot-time static** via `NEBULA_NODE_ROLE`; the follower **409s
  writes** (`guard_writes_on_follower`). There is **no runtime promotion yet**
  (ADR 0007 Phase 3 is the planned `POST /admin/promote` + nginx write-failover).
- **Reads** already fail over node↔node on 503 in the nginx read pool
  (`max_fails=0` + `http_503`), but the pool **strongly prefers the leader**
  (`weight=100`, commit f378d5c) — so taking the leader down still hurts reads
  until failover settles.
- **Recovery is slow** when snapshots lapse: cold WAL replay is serial (ADR 0006)
  and has taken **hours** historically (memory: ~2h42m on 2026-06-03; a recent
  restart recovered in ~4 min because the WAL was small). Recovery time is a
  function of un-snapshotted WAL size — keep it small or upgrades stall.
- **The deploy recreates BOTH db nodes together** when the binary changes:
  `deploy.yml` runs `docker compose up -d --build --remove-orphans` (the
  `recreate_db=1` branch). **This is the downtime bug to fix** — it is the
  opposite of a rolling upgrade.
- `/healthz/live` = process liveness (200 even during recovery);
  `/healthz` = readiness (`503 {"status":"recovering"}` during WAL replay).
  Any rolling logic MUST gate on **readiness**, not liveness.

## Requirements

1. **Rolling, one-at-a-time recreate.** The deploy/orchestrator upgrades data
   nodes sequentially: drain → upgrade → wait-until-ready+caught-up → only then
   proceed to the next. Never recreate the last healthy serving node until a
   replacement is ready.
2. **Writes survive the leader upgrade.** Before recreating the leader, a warm
   replica must be promotable at runtime (ADR 0007 Phase 3) and nginx must
   route writes to it. No write returns 409/503/504 during the window.
3. **Reads survive any single node upgrade.** The read pool must not black-hole
   to a node that's draining/recovering; readiness-gated upstream removal, not
   just 503 retry. Reconsider the `weight=100` leader preference during drains.
4. **Rejoin = rebalance.** An upgraded node rejoins, restores from the shared
   snapshot, replays only its WAL tail, and reaches "caught up" (replication
   cursor ≈ leader head) **before** it counts toward quorum/serving and before
   the next node is touched. Define and expose a measurable "caught-up" signal.
5. **Bounded recovery.** Guarantee the un-snapshotted WAL is small at upgrade
   time (snapshots run off the standby per Phase 1) so each rejoin is minutes,
   not hours. Fail the rollout loud if a node can't catch up within a budget.
6. **No simultaneous-offline invariant.** Encode "at most one data node offline"
   as an explicit guard in the orchestrator; abort the rollout rather than
   violate it.
7. **Observable & reversible.** Emit progress (which node, draining/upgrading/
   caught-up), and support abort/rollback mid-rollout leaving the cluster
   healthy. Surface metrics for failover and catch-up lag.

## Deliverables

1. A design note `docs/design/0009-rolling-upgrade-rebalance.md` (follow the
   style of `0007-zero-downtime-reads-writes.md`): problem, target architecture,
   phased plan, correctness argument, rejected alternatives. Explicitly state
   how this builds on ADR 0007 Phases 1–3 and whether it needs N≥3 replicas.
2. Concrete changes to `.github/workflows/deploy.yml` (or a new orchestration
   script it calls) implementing the one-at-a-time drain→upgrade→wait loop with
   the no-simultaneous-offline guard.
3. The runtime promotion path if not yet present (ADR 0007 Phase 3):
   `POST /admin/promote`, runtime-mutable `cluster.role`, write-failover in the
   nginx config under `apps/showcase`.
4. A "caught-up" readiness signal (endpoint + metric) the orchestrator polls
   between steps.
5. Tests: a scripted rollout (extend `scripts/` like
   `test_multiregion.sh`) that bumps the version across the cluster while a
   background client does continuous reads+writes and asserts **zero failures**.

## Constraints & guardrails

- **Single host, 125 GiB RAM, two ~60 GiB arenas.** N≥3 full replicas don't fit
  today — if the design needs 3+, say so and pair it with ADR 0007 Phase 2 (int8
  quantization, ~4× smaller) or a cross-host plan; don't silently assume the RAM.
- Do **not** resurrect raft as the mechanism without justifying the node-count
  and footprint problem it was rejected for.
- Pushing to `main` **auto-deploys** (`deploy.yml` on push) and recreating data
  nodes triggers WAL recovery — so test the rollout logic on a scratch compose
  project / non-prod first; never validate destructively against the live node.
- Reads fail over today; **writes do not** — closing the write-continuity gap is
  the load-bearing part of this work, not the read path.
- Prefer extending the existing primitives (`snapshot_to(dir, mark_seq)`,
  `compact_wal()`, `min_subscriber_ack`, `FollowerCursor`) over new subsystems.

## Definition of done

A full cluster-wide version upgrade completes with **0 failed reads and 0 failed
writes** observed by a continuous client, at most one node offline at any moment,
each upgraded node verified caught-up before the next proceeds, and the whole
sequence driven automatically by the deploy with a working abort path.
