# Active-active + non-stop rollout prompt for nebuladb

Paste the section below into a fresh Claude Code session against this repo
when you want to actually ship "scales horizontally, runs non-stop,
active-active across regions."

The prompt is intentionally **decision-first**. Two RFCs in this repo
already cover the design (`docs/design/0001-multi-region-active-active.md`
and `docs/design/0003-write-availability.md`). Both end with
"Decision needed." Until those decisions are made, *no implementation
should start* — the warning in RFC 0003's closing paragraph applies:
multi-master without a chosen consistency model is a recipe for silent
data loss.

The phasing here mirrors the rollout plans in the two RFCs. Don't
reinvent them. Read both first, then come back to this file.

---

You are continuing work on NebulaDB — a vector-native Rust database
(REST + gRPC + Postgres wire), currently single-leader + one read-only
follower in a single region. The user wants the stack to scale
horizontally, accept writes during any single-node outage, and
ultimately accept writes from any region (active-active).

## 0. Read before doing anything

In this order:

1. `docs/design/0003-write-availability.md` — within-region write
   availability. Four options (A Raft, B multi-leader LWW, C etcd
   lease, D manual promotion). RFC recommends **B** as default with
   **D** as fallback. Ends with "Decision needed."
2. `docs/design/0001-multi-region-active-active.md` — cross-region
   active-active. Recommends Option A'/C' hybrid: per-bucket
   `home_region` + `home_epoch`, cross-region WAL tail, client-side
   routing, operator-managed failover via a new `NebulaRegionFailover`
   CRD. Has 5 open questions in §9.
3. `crates/nebula-grpc/src/cross_region.rs` (~335 lines) and
   `crates/nebula-server/src/cross_region_status.rs` (~269 lines) —
   skeleton of the cross-region replication service. Phase 2 of
   RFC 0001 is partly built. Read it before designing anything; you
   may be re-implementing what's already there.
4. `crates/nebula-server/src/cluster.rs` — `ClusterConfig`,
   `CrossRegionPeer`, env-var parsing. The shape of "peers and home
   regions" already exists at the config layer.
5. PR #25 (`fix/snapshot-streaming-oom`) — the streaming snapshot
   fix that unblocks production. Confirm it's merged on `main`
   before starting any HA work. If not merged, **stop and merge it
   first**; everything else is moot if the binary OOM-loops every
   15 minutes.

## 1. Two decisions that must land before code

Open a one-page issue for each. Do not start coding until both are
answered.

### Decision 1 — RFC 0003 option

Pick exactly one of A / B / C / D. Each option's implementation cost
diverges by an order of magnitude.

Default recommendation in the RFC: **B (multi-leader LWW)**. Reasoning
the RFC gives:
- Vector upserts are LWW-tolerant by data shape (stable
  `external_id`).
- No transactions to preserve, so Raft is overkill.
- Operational model stays flat: every node is the same.
- Estimate: 5–10 days + 1 week soak.

Counter-cases:
- If the target audience is enterprises that run Raft systems
  (etcd, CockroachDB), they expect **A**.
- If the team can't tolerate any consistency relaxation today,
  ship **D** as a tactical 2–3-day stopgap while B gets built.

### Decision 2 — RFC 0001 open questions (§9)

Five items, each blocks implementation:
1. Tiebreaker for concurrent failovers (operator lease vs
   majority-of-consumers vote).
2. Do followers tail remote regions directly, or always via local
   leader?
3. Schema for home-region map — one doc per bucket in its own seed,
   or a `__system/home_map` bucket.
4. pgwire SQLSTATE for "wrong home region" — reuse `25006` or pick
   a new code.
5. Auto-failback yes/no when the original home recovers.

The RFC leans toward: operator lease, leader-only, per-bucket seed
doc, `25006`, no auto-failback. Confirm or override.

## 2. Sequencing

Strictly in this order. Do not let a later phase start until the
previous one is verified in production.

| Phase | Work | Where | Estimate |
|---|---|---|---|
| 0 | Merge PR #25 (streaming snapshot). | `fix/snapshot-streaming-oom` | already done if merged; verify before continuing |
| 1 | Verify read failover actually works on `192.168.1.193`. Take `nebula-server` down, confirm `/api` reads on showcase still succeed via `nebula-follower`. If they don't, fix the nginx pool config first. | `apps/showcase/nginx/*.conf`, follower compose service | ~2 hours |
| 2 | RFC 0003 chosen option (default B). See §3 below. | `crates/nebula-wal`, `crates/nebula-index`, `crates/nebula-server/middleware.rs` | 5–10 days + 1 week soak (B); 2–3 days (D); 4–8 weeks (A) |
| 3 | RFC 0001 Phase 1 — `home_region` + `home_epoch` in bucket seed docs; `GET /admin/bucket/:b/home-region`; per-remote lag in `/admin/replication`. | `crates/nebula-server/router.rs`, operator's bucket controller | ~1 week |
| 4 | RFC 0001 Phase 2 — wire up `CrossRegionReplicationService` end-to-end. The skeleton exists; finish the gRPC service definition, peer config plumbing, lag metrics. | `crates/nebula-grpc/cross_region.rs`, `crates/nebula-server/cross_region_status.rs`, `crates/nebula-server/main.rs` | ~2 weeks |
| 5 | RFC 0001 Phase 3 — `nebula-client` SDK with bucket→region resolver + 30s cache; `require_home_region` middleware (parallel to `guard_writes_on_follower`); pgwire + gRPC parity. | new crate `nebula-client`, `crates/nebula-server/middleware.rs` | ~1 week |
| 6 | RFC 0001 Phase 4 — `NebulaRegionFailover` CRD + controller state machine (Preflight → Drain → EpochBump → Announce → Completed). | `operator/` | ~1 week |
| 7 | RFC 0001 Phase 5 — nice-to-haves: anti-affinity, per-bucket read preferences, conflict auditing. | mixed | as time permits |

Total: ~8–10 weeks of focused engineering for full RFC 0001 + the
chosen RFC 0003 option.

## 3. Phase 2 specifics if Decision 1 = B (multi-leader LWW)

The RFC's §"What changes if we pick (B)" lays it out. Concrete commits
in suggested order:

1. **WAL record ordering**. Extend `WalRecord` with `(wal_seq,
   node_id)` tuple. Replay's conflict rule: higher `wal_seq` wins;
   ties broken by `node_id`. Tests for both win conditions.
2. **Deterministic apply on conflict**. The HNSW insert path already
   tolerates a tombstone + re-insert. Verify that an upsert with an
   earlier-than-current `(wal_seq, node_id)` is no-op'd, not applied.
3. **Tombstone grace period**. Deletes leave a tombstone marker for
   24h (env-tunable). Compaction respects this. Without it, a late
   upsert from a partitioned peer can resurrect a deleted doc.
4. **Bi-directional replication**. Both nodes stream their WAL to
   each other. `apply_wal_record` on the receiving side is already
   idempotent — verify with a test that runs it twice on the same
   record.
5. **Retire `guard_writes_on_follower`**. The middleware in
   `crates/nebula-server/src/middleware.rs` should now allow writes
   on any node. Keep the symbol around with a deprecation note for
   one release; removal is a follow-up.
6. **Nginx pool**. `apps/showcase/nginx/*.conf` — rename
   `nebula_read_pool` to `nebula_pool` (or similar) and route ALL
   methods (not just `GET`) through it. Both nodes are equivalent.
7. **pgwire**. Stop returning `25006 read_only_sql_transaction` from
   either node on `INSERT`/`UPDATE`/`DELETE`. The follower guard's
   pgwire integration retires here too.

Soak test (week 2):
- Run a 10-minute concurrent-write workload against both nodes
  targeting overlapping keys. Confirm convergence within the
  cross-node tail's lag SLO (target ≤ 5s).
- Kill one node mid-workload. Other node accepts writes
  uninterrupted. Killed node rejoins, applies the missed records,
  converges.
- Net-split simulation: `iptables` drop between the two nodes for
  60s. Both nodes accept conflicting writes. Heal the partition.
  Confirm the LWW resolution matches what `(wal_seq, node_id)`
  ordering predicts.

## 4. Verification gates

You may not declare a phase "done" until all of these pass:

- `cargo test -p nebula-index -p nebula-vector -p nebula-server`
  green.
- `cargo clippy --workspace --tests -- -D warnings` green.
- The integration-test prompt in `docs/integration-test-prompt.md`
  passes end-to-end against the live stack (`192.168.1.193`).
- For phases 2 and 4–6: the soak workload above succeeds.
- For phases 3–6: a written runbook in `docs/runbooks/` covers
  failover, demotion, and reconciliation. No phase ships without
  the ops doc.

## 5. Anti-patterns — what NOT to do

- **Don't rewrite the design in the prompt.** The RFCs are
  authoritative. If you disagree with a recommendation, open a
  follow-up RFC; don't quietly implement a different one.
- **Don't change the on-disk WAL or snapshot wire format
  silently.** Existing snapshots and WAL segments on the
  `192.168.1.193` host must load on the new binary. The
  byte-identity guard in
  `crates/nebula-vector/src/hnsw.rs::tests::streaming_serialize_matches_owned_serialize`
  is the model — write the equivalent for any new format you
  introduce.
- **Don't bypass `wal.append` in the write path.** Every mutation
  must transit the WAL before any in-memory apply. The single
  invariant that makes recovery correct is "WAL is the source of
  truth." Multi-leader does not relax this.
- **Don't add CRDTs at the vector level.** RFC 0001's §"Option B"
  spells out why HNSW doesn't cope. If you need conflict
  resolution, do it at the `(bucket, external_id)` register
  layer, not inside the graph.
- **Don't introduce a new SPOF.** A router crate (Option A from
  RFC 0001) was explicitly rejected as a pure solution; only the
  client-side SDK router was accepted. Same goes for any
  "global coordinator" pattern — it defeats the point.
- **Don't ship a watchdog that auto-promotes on health-check
  flap.** If you implement Option D, the demotion side must be
  explicit. Auto-promotion + auto-demotion together is how you
  invent split-brain.

## 6. What to deliver

For each shipped phase:
1. A PR titled `<phase>: <one-line description>` following the
   repo's existing commit style (`<area>: <imperative>` —
   look at `git log --oneline` for examples).
2. Tests at the appropriate level (unit for module work,
   integration for cross-crate behavior).
3. A short entry in `docs/runbooks/` describing the operational
   change if any (failover, demotion, peer add/remove).
4. Updated metrics + a Grafana panel if the change adds a new
   failure mode worth alerting on (lag, conflict counts, demoted
   state).

## 7. When to escalate to the user

Stop and ask before:
- Picking between A / B / C / D in RFC 0003 if there's any doubt.
- Resolving any of the 5 open questions in RFC 0001 §9.
- Making any change that requires data migration on
  `192.168.1.193`.
- Force-pushing or rewriting history on any branch.
- Anything that could cause a write outage on the running stack
  beyond the recovery window already understood.

## 8. State of the world at time of writing

- The streaming snapshot fix landed in PR #25
  (`fix/snapshot-streaming-oom`). Verify it's merged before
  starting.
- `192.168.1.193` is running with `NEBULA_SNAPSHOT_INTERVAL_SECS=0`
  and `NEBULA_SNAPSHOT_WAL_BYTES=0` in `.env` as a temporary
  scheduler-disable workaround. Remove that block once PR #25 is
  deployed.
- `nebula-follower` and `nebula-showcase` have shown a pattern of
  getting stuck in `docker Created` state on this host. The 2-min
  `scripts/reconcile.sh` cron usually picks them back up. If they
  stay stuck, `docker compose up -d` clears it.
- WAL recovery currently takes ~140–200s on this host with a
  ~44 MB WAL — that's roughly 220–315 KB/s. Suspicion: the
  embedder is being re-called per chunk during replay. Worth a
  separate investigation; not blocking for this work.

Once you've read this file end-to-end and the two RFCs, your first
output should be: "I've read RFCs 0001 and 0003. My recommended
sequencing matches §2 of the prompt. Decisions 1 and 2 still need
the user's input — drafting issues for each now." Then draft them.
Do not start any code until both decisions land.
