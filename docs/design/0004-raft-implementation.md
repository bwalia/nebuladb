# Design 0004: Raft implementation for within-region write availability

- **Status**: Draft (implementation ADR — needs sign-off, not a re-decision)
- **Author**: Claude + @bwalia
- **Created**: 2026-05-14
- **Tracks**: Issue #32 (RFC 0003 = A) implementation
- **Depends on**: RFC 0003 decision (Raft chosen)
- **Supersedes**: nothing — this is the *how* layer for the *what* RFC 0003 already chose

---

## 1. Why this doc exists

Issue #32 closed with **A — Raft**. RFC 0003 stops at "pick A/B/C/D"; it doesn't answer the implementation questions. This ADR closes that gap so engineers don't redesign the same thing twice.

This is **not** a re-decision of A vs B. RFC 0003 already covered that. If you disagree with A, reopen #32 — don't argue here.

---

## 2. Decisions to make

Five concrete questions, listed in order of how much downstream code each one affects.

| # | Question | Recommendation | Why now |
|---|---|---|---|
| 1 | `openraft` vs `raft-rs` | **`openraft`** | Sets the dependency for the whole feature |
| 2 | Single Raft group vs sharded-by-bucket | **Single group v1**, sharded as v2 | Sharded is months more work; single group ships in 4–8 weeks |
| 3 | WAL ↔ Raft log relationship | **Replace WAL with Raft's log; keep snapshot path** | The biggest engineering decision; affects on-disk format |
| 4 | Snapshot integration with `nebula-index` | **Reuse the existing snapshot format unchanged** | Touches `crates/nebula-index/src/durability.rs` |
| 5 | K8s replica count + quorum semantics | **3 nodes minimum, 5 supported** | Operator CRD validation needs to enforce this from day one |

Each question gets its own section with the alternatives and the chosen path.

---

## 3. Decision 1 — `openraft` vs `raft-rs`

### Choice: **`openraft`**

### Why

Both crates implement the same algorithm, but they target different ergonomics.

| | `raft-rs` (TiKV) | `openraft` |
|---|---|---|
| Async story | Sync core, callers wrap async | Async-first, `tokio` native |
| State-machine API | You drive the loop manually | Trait-based; you implement `RaftStateMachine` and the runtime drives it |
| Snapshot transfer | Manual chunking, no streaming | Streaming snapshots first-class |
| Membership changes | Joint consensus, manual | Built-in helpers |
| Production users | TiKV (largest), CockroachDB-style | Databend, sled-cluster, several open-source DBs |
| Maturity | More battle-hardened | Younger, but actively maintained, used in production |
| Doc quality | Sparse, code-as-docs | Worked examples, async-friendly |

NebulaDB is `axum` + `tokio` end to end. `openraft`'s async-first surface fits the existing crate boundaries; `raft-rs` would force a sync ↔ async bridge layer for every state-machine call. Streaming snapshot support also matters — we already pay to keep snapshots streaming-friendly (PR #25), and `openraft` lets us keep that property without re-inventing the chunked-transfer protocol.

### What we give up

`raft-rs`'s production track record at TiKV's scale. NebulaDB targets single-region clusters of 3–5 nodes serving vector workloads — `openraft`'s deployment envelope covers this comfortably. If we ever need to run thousands of Raft groups (the real `raft-rs` scale story), revisit.

### Concrete dependency

```toml
# crates/nebula-raft/Cargo.toml (new crate)
openraft = { version = "0.9", features = ["serde", "storage-v2"] }
```

Pin to a specific minor version; `openraft` has had API churn between minor releases.

---

## 4. Decision 2 — Single Raft group vs sharded

### Choice: **Single Raft group for v1. Sharding deferred to v2 (separate ADR).**

### Why

| Approach | v1 | Trade-off |
|---|---|---|
| **Single group** | One Raft group covers the whole cluster's writes. Every node is a peer in the same group. | All writes serialize through the leader. Throughput ceiling = leader's WAL write speed (~10–50 MB/s on commodity NVMe). Acceptable for the target workload. |
| **One group per bucket** | Each `bucket` is its own Raft group. Different leaders per bucket; horizontal write-scale. | Massive complexity: hundreds of Raft groups in one process, snapshot management × N, leader-balancing logic, hot-bucket leader contention. Multi-month additional effort. |
| **One group per shard (consistent hash)** | Buckets hashed into K Raft groups. Compromise. | Still a fan-out problem in the data plane; routing layer needs to know shard map. Worse than single-group for v1's scale. |

### Concrete impact

- **Cluster throughput in v1 = single-leader throughput.** This is the same ceiling as today's single-leader-with-followers, so we're not regressing — we're paying for HA + automatic failover, not extra write capacity. For workloads that need write-scale beyond one leader, the answer is **active-active across regions** (RFC 0001), not Raft-group sharding within a region.
- **One snapshot stream per failover/peer-join.** With sharded groups, a new peer would need to receive K snapshot streams; that gets ugly fast.
- **The HNSW index becomes one Raft state machine.** Apply order is total — every peer applies the same sequence of writes in the same order, so HNSW graphs converge byte-identically. Verifiable with the existing `streaming_serialize_matches_owned_serialize` test pattern.

### When to reconsider

- p99 write latency exceeds the SLA because the leader is saturated.
- A single bucket's writes dominate cluster traffic and we want to isolate it.
- Customer requirement for >50 MB/s aggregate write throughput in a single region.

---

## 5. Decision 3 — WAL ↔ Raft log relationship

This is the largest engineering decision in this ADR. Three options:

### Choice: **Replace `nebula-wal` with `openraft`'s log storage. Keep the snapshot path unchanged.**

### Why

| Option | Approach | Verdict |
|---|---|---|
| **(a) Coexist: WAL beneath Raft log** | Raft log writes get appended to the existing WAL; replay drives both Raft state recovery AND state-machine apply. | ❌ Two append-only logs in series doubles fsync cost. We tested this mentally: not worth it. |
| **(b) Wrap: WAL is the Raft log** | Implement `openraft::RaftLogStorage` over the existing `nebula-wal::Wal` crate. | ⚠️ Tempting, but the WAL's wire format (`crates/nebula-wal/src/lib.rs:1`) has no Raft term/index slots. Adding them = new wire format = same as (c). The "reuse" is illusory. |
| **(c) Replace: Raft log is authoritative** | `nebula-wal` is retired for raft-enabled clusters. `openraft` owns the log; the state machine applies decoded `LogEntry::Append` records into HNSW directly. | ✅ Cleanest. One write path, one fsync per commit. |

### Concrete migration

- **`nebula-wal` does NOT delete.** Keep the crate. `NEBULA_NODE_ROLE=standalone` continues to use it (RFC 0001 §2.1 hard constraint: backwards compat).
- **New crate `nebula-raft`** owns:
  - `openraft::RaftLogStorage` impl backed by a fresh `.nrlog` segment format
  - `openraft::RaftSnapshotBuilder` impl that calls into `crates/nebula-index/src/durability.rs`
  - The state-machine type (`RaftStateMachine` impl wrapping `TextIndex`)
- **`crates/nebula-server`**: write path branches at boot. `cluster.rs` config reads `NEBULA_RAFT_PEERS=...`; if present, write traffic goes through `nebula-raft`. If absent, today's WAL path runs unchanged (single-node / standalone).
- **No on-disk migration tool in v1.** Switching a cluster from non-raft to raft = wipe data dir, re-ingest. This is honest about the scope. A migrator can ship later if customers demand it.

### What this lets us reuse

- **Snapshot format is byte-identical** to today's. The Raft snapshot builder dumps the same `nsnap` file `crates/nebula-index/src/durability.rs` already produces. New peers transfer that file via `openraft`'s streaming snapshot API.
- **HNSW serialization, durability tests, the snapshot scheduler** all unchanged.
- **The integration test prompt + `scripts/integration_validate.sh`** keep working — the data plane shape is the same.

### What this discards

- `crates/nebula-grpc/src/follower.rs` — 9 lines of glue, but the whole `Wal::subscribe` path it serves becomes dead for raft clusters. Keep for standalone, deprecate for raft.
- `crates/nebula-server/state.rs::FollowerCursor` — same deal.
- The middleware `guard_writes_on_follower` — replaced by `openraft`'s "I am not the leader, redirect" response. Keep symbol with a deprecation note for one release.

---

## 6. Decision 4 — Snapshot integration

### Choice: **Reuse the `nsnap` snapshot format. Stream it via `openraft`'s `RaftSnapshotBuilder`.**

### Why

`openraft` expects two things from the state machine: (1) periodic snapshots so the log can be compacted, (2) streaming snapshot transfer to new peers.

We already produce snapshots. `crates/nebula-index/src/durability.rs` writes a `.part` file, fsyncs, renames, drops a `.ok` marker. PR #25 made this streaming-friendly to fix the OOM issue. Plugging `openraft` on top is mechanical:

- `RaftSnapshotBuilder::build_snapshot` → calls `TextIndex::snapshot()` returning the existing handle.
- `openraft` packages the snapshot into a `Vec<u8>` (small clusters) or streams it (large clusters). The streaming path is what PR #25 unblocked.
- Snapshot install on a follower → write the streamed bytes to `/var/lib/nebuladb/snapshots/raft-recv/<id>.nsnap`, atomic rename, then `TextIndex::open_persistent` from that path.

### What this preserves

- Backups (`crates/nebula-backup`) keep working unchanged. A backup still grabs a snapshot + bundles it.
- The byte-identity guard test (`crates/nebula-vector/src/hnsw.rs::tests::streaming_serialize_matches_owned_serialize`) continues to enforce snapshot fidelity.
- The 24h tombstone grace period (RFC 0003 §B item 3) **does not apply** with Raft — Raft total-ordering means there's no LWW conflict to resolve, so tombstones can compact at the next snapshot boundary like any other state.

### Snapshot trigger frequency

`openraft` snapshots when the log exceeds a configured size. Default to **64 MiB** of log to match today's WAL segment size. Tunable via `NEBULA_RAFT_SNAPSHOT_LOG_BYTES`.

The existing time-based snapshot scheduler (`crates/nebula-server/src/snapshot_scheduler.rs`) becomes redundant for raft clusters — `openraft` drives snapshot triggers from log growth. Keep the scheduler running for standalone.

---

## 7. Decision 5 — Cluster sizing

### Choice: **3 nodes minimum, 5 supported. Operator CRD enforces.**

### Why

Raft tolerates `f` failures with `2f + 1` peers:

| Peers | Tolerates | Comment |
|---|---|---|
| 1 | 0 | Standalone. Not Raft, just a single node — covered by `NEBULA_NODE_ROLE=standalone`. |
| 2 | 0 | Useless: any single-node failure loses quorum. CRD must reject. |
| **3** | **1** | **Default for production raft clusters.** Tolerates one node out (planned or unplanned). |
| 4 | 1 | Pays for an extra peer with no extra fault tolerance. Discourage. |
| 5 | 2 | Recommended for clusters spanning AZs where 2 simultaneous failures matter. |
| 7+ | 3+ | Diminishing returns; latency on log replication grows. Not blocked, just not recommended. |

### CRD validation

```yaml
# operator/api/v1alpha1/nebulacluster_types.go
spec:
  raft:
    enabled: true
    replicas: 3   # validated: >=3 AND odd. 4 rejected at admission.
```

Webhook validation: `replicas % 2 == 1 && replicas >= 3`. Even-replica clusters fail admission with a clear error: `"Raft cluster replicas must be odd and >= 3 (got 4)"`.

### Cross-region note

Raft is **within-region only** (RFC 0003 scope). A 3-node Raft group spread across 3 AZs in one region is the model. Cross-region active-active is RFC 0001's responsibility; Raft does not span regions in this design.

---

## 8. New code map

```
crates/
  nebula-raft/             NEW. RaftLogStorage + RaftSnapshotBuilder + state machine wrapper.
                           Depends on openraft, nebula-index, nebula-vector.
  nebula-wal/              UNCHANGED. Standalone-mode log; deprecated-but-supported for raft clusters.
  nebula-index/            UNCHANGED public API. Snapshot path called from nebula-raft instead of
                           directly from nebula-server when raft is enabled.
  nebula-server/           CHANGES:
    cluster.rs             Read NEBULA_RAFT_PEERS, NEBULA_RAFT_NODE_ID, NEBULA_RAFT_DATA_DIR.
    main.rs                Branch at boot: raft path or wal path.
    middleware.rs          guard_writes_on_follower → guard_writes_on_non_leader for raft mode;
                           response carries leader address so clients can re-route.
    state.rs               FollowerCursor unchanged (used by standalone path); add RaftHandle for
                           raft path.
  nebula-grpc/             CHANGES: ReplicationService stays for standalone; new RaftService for
                           openraft's network transport (gRPC-backed).
operator/
  api/v1alpha1/
    nebulacluster_types.go  Add `RaftSpec` field. Webhook validation for odd-replicas.
  internal/controllers/
    nebulacluster_controller.go  Reconcile raft StatefulSet replicas; surface leader in status.
docs/
  design/0004-...           THIS DOC.
  runbooks/raft-failover.md NEW (Phase 0 of rollout).
```

Estimated total new code: ~2500–3500 lines, ~1200 of test.

---

## 9. Phased delivery (revises rollout prompt §2)

The original rollout prompt assumed Option B (5–10 days). With Raft, the within-region phase expands. Total roadmap shifts from ~8–10 weeks to **~12–16 weeks**.

| Phase | Work | Duration |
|---|---|---|
| **0a** | Verify PR #25 (streaming snapshot) on `192.168.1.193`. Remove `NEBULA_SNAPSHOT_INTERVAL_SECS=0` workaround. | 1 day |
| **0b** | Verify read failover via showcase nginx pool (existing PR #17 path). | 1 day |
| **2.1** | Scaffold `nebula-raft` crate. `openraft` deps wired, `RaftLogStorage` over a new `.nrlog` segment format. Unit tests for log append + replay. | 1 week |
| **2.2** | `RaftStateMachine` impl wrapping `TextIndex`. State machine apply path produces identical HNSW state across peers (deterministic test). | 1 week |
| **2.3** | `RaftSnapshotBuilder` reusing the `nsnap` format. Streaming install on a fresh peer. | 1 week |
| **2.4** | gRPC transport (`RaftService` in `nebula-grpc`). Membership change tests (add/remove peer). | 1 week |
| **2.5** | `nebula-server` boot integration. `NEBULA_RAFT_*` env vars. Middleware swap. | 1 week |
| **2.6** | **Soak: 2 weeks.** 3-node cluster on `192.168.1.193`-style hardware. Kill nodes, partition the network, fail back. Watch for committed-but-unapplied edge cases. | 2 weeks |
| **3** | RFC 0001 Phase 1 (`home_region` + `home_epoch`) — already partly built; finish wiring. | 1 week |
| **4** | RFC 0001 Phase 2 (cross-region replication finish) | 2 weeks |
| **5** | RFC 0001 Phase 3 (`nebula-client` SDK + `require_home_region` everywhere) | 1 week |
| **6** | RFC 0001 Phase 4 (`NebulaRegionFailover` + `NebulaRegionFailback` controllers, hysteresis logic per #33) | 1 week |
| **7** | Phase 5 polish (anti-affinity, read prefs, conflict audit) | as time allows |

**Critical path** = 2.1 → 2.6 (8 weeks) before any RFC 0001 phase 3+ work can start, because cross-region depends on a stable within-region HA story.

---

## 10. Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| `openraft` API churn between 0.9.x → 0.10 mid-implementation | Medium | Hours of churn | Pin minor version. Plan an upgrade window after 2.6 soak completes, not during. |
| HNSW determinism violation under concurrent applies on a follower | Low | Silent state divergence | Single-threaded apply on the state machine; cover with a "two peers apply same log → byte-identical snapshot" test. |
| Snapshot transfer over slow cross-AZ link starves new peer (>5 min boot) | Medium | Failed peer joins | `openraft` streaming snapshots + the PR #25 streaming serialize fix together cap memory. Time budget is bounded by network speed × snapshot size. Document a sizing note: a 1 GiB index over a 100 Mbps link = ~80s — operators provision accordingly. |
| Operator CRD admission webhook miss-validates replica count | Low | User creates a 4-node cluster, suffers in production | Webhook enforced + a unit test for the admission policy. |
| Standalone path rots without test coverage | Medium | Customers on `NEBULA_NODE_ROLE=standalone` get broken in a future PR | Existing standalone tests stay; add a CI matrix dimension `cluster_mode={standalone, raft}` so both paths run on every PR. |

---

## 11. What we are NOT building (in v1)

- **Sharded Raft groups.** Single group only.
- **Cross-region Raft.** Within-region only; cross-region is RFC 0001's job.
- **Online migration from standalone → raft.** Wipe + re-ingest only.
- **Raft over the existing WAL format.** New `.nrlog` format.
- **Read scaling beyond the leader.** `openraft` supports learner nodes (read replicas without quorum vote) — v1 ships voting peers only. Learner support is a phase 7 nice-to-have if read-only scale becomes a customer ask.

---

## 12. Open questions (for sign-off)

1. **`openraft` version pin.** Lock to `0.9.x` now or wait for `0.10` (currently in beta)? Recommendation: ship on `0.9.x`, plan a migration after soak.
2. **gRPC transport between Raft peers — same `:50051` port, or a dedicated `:50052` for Raft traffic?** Separating ports lets ops apply different network policies (Raft ↔ Raft is highly trusted; client gRPC less so). Recommendation: dedicated port `:50052`, env-var `NEBULA_RAFT_BIND`.
3. **Standalone deprecation timeline?** Once Raft is the recommended HA path, do we deprecate `NEBULA_NODE_ROLE=standalone` (with the simple WAL) on a 12-month clock, or keep it indefinitely as the "small / dev" deployment? Recommendation: keep indefinitely; it's the right answer for the showcase + many self-hosted users.

---

## 13. Decision

**Approve `openraft` + single Raft group + Raft-owned log + reused snapshot format + 3-node-min topology.**

Once approved, work begins on Phase 2.1 (scaffold `nebula-raft` crate). The three open questions in §12 can be resolved during 2.1 — none blocks scaffolding.
