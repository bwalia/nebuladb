# Active-active + non-stop rollout prompt for nebuladb

Paste the section below into a fresh Claude Code session against this repo
when you want to actually ship "scales horizontally, runs non-stop,
active-active across regions."

**Status: decisions landed.** The two RFCs that anchor this work
(`docs/design/0001-multi-region-active-active.md` and
`docs/design/0003-write-availability.md`) both shipped with
"Decision needed." flags. Those have been resolved in issues #32
(RFC 0003 = **A, Raft**) and #33 (RFC 0001 §9 = all five answered;
auto-failback uses hysteresis with manual-confirm opt-in). The
implementation specifics for Raft live in
`docs/design/0004-raft-implementation.md`, which is the gating doc
for Phase 2 work.

The phasing here mirrors the rollout plans in the two RFCs and the
implementation breakdown in ADR 0004. Don't reinvent them. Read
both RFCs and ADR 0004 first, then come back to this file.

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
   lease, D manual promotion). **Decision: A (Raft)**, tracked in
   closed issue #32.
2. `docs/design/0001-multi-region-active-active.md` — cross-region
   active-active. Recommends Option A'/C' hybrid: per-bucket
   `home_region` + `home_epoch`, cross-region WAL tail, client-side
   routing, operator-managed failover via `NebulaRegionFailover`.
   §9's five open questions are **all answered** in closed issue #33
   (1a, 2a, 3a, 4a, 5a-with-hysteresis).
3. **`docs/design/0004-raft-implementation.md`** — implementation
   ADR for the Raft choice. **Read this before scaffolding any
   code.** Covers `openraft` vs `raft-rs`, single group vs sharded,
   WAL/log relationship, snapshot integration, replica sizing.
4. `crates/nebula-grpc/src/cross_region.rs` (~335 lines) and
   `crates/nebula-server/src/cross_region_status.rs` (~269 lines) —
   skeleton of the cross-region replication service. Phase 2 of
   RFC 0001 is partly built. Read it before designing anything; you
   may be re-implementing what's already there.
5. `crates/nebula-server/src/cluster.rs` — `ClusterConfig`,
   `CrossRegionPeer`, env-var parsing. The shape of "peers and home
   regions" already exists at the config layer.
6. PR #25 (`fix/snapshot-streaming-oom`) — the streaming snapshot
   fix that unblocks production. Confirm it's merged on `main`
   before starting any HA work. If not merged, **stop and merge it
   first**; everything else is moot if the binary OOM-loops every
   15 minutes.

## 1. Decisions — landed

Both blocking decisions are now answered. Tracked in closed issues
#32 and #33.

### Decision 1 — RFC 0003 option: **A (Raft)**

Tracked: [#32](https://github.com/bwalia/nebuladb/issues/32), closed.

The owner chose **A (Raft consensus)** over the RFC default of B.
Implementation specifics live in `docs/design/0004-raft-implementation.md`
(this is now the gating doc for Phase 2 — read it before scaffolding
the `nebula-raft` crate).

Implications already absorbed below:
- Within-region phase scope expands from 5–10 days (B) to 4–8 weeks
  (A) + 2 weeks soak.
- Existing replication code (`crates/nebula-grpc/src/follower.rs`,
  `Wal::subscribe`, `state::FollowerCursor`) stays for standalone
  mode; deprecated for Raft clusters but not removed.
- HNSW index becomes a Raft state machine. Snapshot format is
  reused unchanged — `crates/nebula-index/src/durability.rs` plugs
  into `openraft`'s `RaftSnapshotBuilder`.
- K8s operator must enforce `replicas >= 3 && replicas % 2 == 1`
  via admission webhook for Raft-enabled clusters.

### Decision 2 — RFC 0001 §9: all five answered

Tracked: [#33](https://github.com/bwalia/nebuladb/issues/33), closed.

| Q | Topic | Answer |
|---|---|---|
| 1 | Failover tiebreaker | (a) operator namespace lease via `controller-runtime` |
| 2 | Follower cross-region tail | (a) leader-only; matches existing `crates/nebula-grpc/src/cross_region.rs` |
| 3 | Home-map schema | (a) per-bucket seed doc (already implemented in `crates/nebula-server/src/home_region.rs`) |
| 4 | pgwire SQLSTATE | (a) reuse `25006 read_only_sql_transaction` with distinct error message |
| 5 | Auto-failback | (a) **with hysteresis** — `NEBULA_FAILBACK_STABILITY_SECS` (default 300s); `NebulaBucket.spec.failback: auto\|manual` opt-out for regulated workloads |

Q5 implementation contract (excerpted from #33):
- Continuous health = N consecutive 30s gRPC pings; any failure
  resets the counter. Default 300s = 10 successes.
- New CRD `NebulaRegionFailback` carries the manual-confirm flow.
- Three new metrics: `nebula_failback_deferred_total{bucket,reason}`,
  `nebula_failback_completed_total{bucket}`,
  `nebula_failback_pending_seconds{bucket}`.
- Audit event `nebuladb-operator-failback` on every flip.

## 2. Sequencing

Strictly in this order. Do not let a later phase start until the
previous one is verified in production.

This phasing reflects the Raft decision (#32 = A). For the per-phase
implementation details inside Phase 2, read
`docs/design/0004-raft-implementation.md` §9 — that's the source of
truth and this table is its summary.

| Phase | Work | Where | Estimate |
|---|---|---|---|
| 0a | Merge PR #25 (streaming snapshot). Verify deployed on `192.168.1.193`. Remove the `NEBULA_SNAPSHOT_INTERVAL_SECS=0` workaround from `.env` once stable. | `fix/snapshot-streaming-oom` | 1 day |
| 0b | Verify read failover actually works on `192.168.1.193`. Take `nebula-server` down, confirm `/api` reads on showcase still succeed via `nebula-follower`. Use `scripts/integration_validate.sh` (PR #29) to check. If it fails, fix the nginx pool first. | `apps/showcase/nginx/*.conf`, follower compose service | 1 day |
| 2.1 | Scaffold new `nebula-raft` crate. `openraft` deps wired. `RaftLogStorage` over a new `.nrlog` segment format. Unit tests. | `crates/nebula-raft/` (new) | ~1 week |
| 2.2 | `RaftStateMachine` impl wrapping `TextIndex`. Determinism test: two peers apply the same log → byte-identical snapshot. | `crates/nebula-raft/`, `crates/nebula-index` | ~1 week |
| 2.3 | `RaftSnapshotBuilder` reusing the `nsnap` snapshot format. Streaming install on a fresh peer. | `crates/nebula-raft/`, `crates/nebula-index/src/durability.rs` | ~1 week |
| 2.4 | gRPC transport (`RaftService` in `nebula-grpc`). Membership change tests (add/remove peer). Dedicated bind port `:50052` (`NEBULA_RAFT_BIND`). | `crates/nebula-grpc/` | ~1 week |
| 2.5 | `nebula-server` boot integration. `NEBULA_RAFT_*` env vars. `guard_writes_on_follower` → `guard_writes_on_non_leader` for raft mode (response carries leader address). | `crates/nebula-server/{cluster,main,middleware,state}.rs` | ~1 week |
| 2.6 | **Soak: 2 weeks.** 3-node cluster on `192.168.1.193`-style hardware. Kill nodes, partition the network, fail back. Watch for committed-but-unapplied edge cases. | live cluster | 2 weeks |
| 3 | RFC 0001 Phase 1 — `home_region` + `home_epoch` in bucket seed docs (already started in `crates/nebula-server/src/home_region.rs`); `GET /admin/bucket/:b/home-region`; per-remote lag in `/admin/replication`. | `crates/nebula-server/router.rs`, operator's bucket controller | ~1 week |
| 4 | RFC 0001 Phase 2 — finish `CrossRegionReplicationService`. Skeleton at `crates/nebula-grpc/src/cross_region.rs` (335 lines) + `crates/nebula-server/src/cross_region_status.rs` (269 lines). | mixed | ~2 weeks |
| 5 | RFC 0001 Phase 3 — `nebula-client` SDK with bucket→region resolver + 30s cache; `guard_wrong_home_region` parity for gRPC + pgwire (REST already done). | new crate `nebula-client`, `crates/nebula-server/middleware.rs` | ~1 week |
| 6 | RFC 0001 Phase 4 — `NebulaRegionFailover` controller state machine (Preflight → Drain → EpochBump → Announce → Completed) + `NebulaRegionFailback` CRD with hysteresis + manual-confirm support per #33 Q5. | `operator/api/v1alpha1/`, `operator/internal/controllers/` | ~1.5 weeks |
| 7 | RFC 0001 Phase 5 — nice-to-haves: anti-affinity, per-bucket read preferences, conflict auditing. | mixed | as time permits |

**Total: ~12–16 weeks** of focused engineering for full RFC 0001 +
Raft (vs ~8–10 weeks the original prompt estimated under Option B).
Critical path is **2.1 → 2.6 (8 weeks)** before any RFC 0001 phase 3+
work can start — cross-region depends on a stable within-region HA
story.

## 3. Phase 2 specifics — Raft (Decision 1 = A)

Implementation details live in `docs/design/0004-raft-implementation.md`.
Read that document before scaffolding any code; the bullets here are
a pointer, not a substitute.

Decisions already locked in:

- **Library: `openraft 0.9.x`** (async-first; matches the `tokio` /
  `axum` codebase). See ADR §3.
- **Single Raft group v1.** No bucket-sharded groups. ADR §4.
- **Replace `nebula-wal` with `openraft`'s log storage** for raft
  clusters; `nebula-wal` stays for `NEBULA_NODE_ROLE=standalone`.
  ADR §5.
- **Reuse the `nsnap` snapshot format unchanged** — plug `openraft`'s
  `RaftSnapshotBuilder` into `crates/nebula-index/src/durability.rs`.
  ADR §6.
- **3-node minimum, odd replicas only.** Operator admission webhook
  enforces. ADR §7.
- **No standalone → raft online migration in v1.** Wipe + re-ingest.
  Document this clearly in the operator's CRD docs.

Soak test (Phase 2.6, two weeks on a 3-node cluster):

- Concurrent writes against the leader for 1 hour; kill the leader
  mid-workload; verify writes resume on the new leader within
  election timeout (~1s with `openraft` defaults).
- Network partition simulation: `iptables` drop between two of the
  three peers for 60s. The majority side stays writable; the
  minority side rejects writes. Heal the partition; verify the
  minority side catches up via log replay.
- Slow snapshot transfer: throttle the inter-peer link and bring up
  a fourth node as a learner. Snapshot install must complete; the
  PR #25 streaming snapshot fix is the prerequisite.
- Determinism check: dump the HNSW snapshot from each peer after the
  workload; bytes must be identical (the
  `streaming_serialize_matches_owned_serialize` test pattern, but
  cross-node).

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

Both blocking decisions are now landed (#32, #33), so the escalation
list is shorter:

- Anything not covered by `docs/design/0004-raft-implementation.md`
  — that ADR is the source of truth for Phase 2.
- The three open questions in ADR §12 (openraft pin, gRPC port,
  standalone deprecation) — flag before acting.
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

Once you've read this file end-to-end, RFCs 0001 + 0003, and ADR
0004, your first output should be: "I've read the rollout prompt,
RFCs 0001 + 0003, and ADR 0004. Decisions 1 and 2 are landed (#32 +
#33). Starting Phase 0a / 0b verification on `192.168.1.193`."
Don't start Phase 2.1 code until 0a and 0b are green.
