# Design 0007: Zero-downtime reads and writes

- **Status**: Accepted — Phase 1 in progress
- **Author**: Claude + @bwalia
- **Created**: 2026-06-03
- **Tracks**: production requirement — "no downtime; read and write without interruption"
- **Relates to**: 0003 (write-availability), 0004 (raft), 0006 (parallel WAL replay — recovery)

---

## 1. Why this exists

In production on `192.168.1.193` the stack suffers repeated read/write outages.
Root-caused this session (with `strace` + `docker events` + cgroup counters,
because `docker inspect` masks the real exit code of a restart-policy
container):

1. **Snapshots stall the serving node.** `TextIndex::snapshot()` /
   `snapshot_to()` holds `self.inner.read()` across the *entire* multi-minute
   bincode→zstd serialize of the ~60 GiB HNSW arena
   (`crates/nebula-index/src/lib.rs`, `crates/nebula-vector/src/hnsw.rs`
   `serialize_snapshot_into`). With `parking_lot` write-preference a writer
   queued behind it blocks subsequent readers too → the whole runtime stalls
   for minutes, every snapshot.
2. **Snapshots OOM the node.** The arena (~60 GiB, growing) plus the snapshot
   transient tips the cgroup; the kernel OOM-kills the process (SIGKILL/137,
   masked by docker as exit-0). The watchdog (SIGABRT/134) fires instead when
   memory is sufficient but the lock-hold exceeds its limit. Net: snapshots
   *never completed* on the prod leader for a long stretch (`completed` count
   was 0), so the WAL never compacted, so cold recovery replayed everything.
3. **Single-leader writes with no failover.** Role is a **boot-time static
   env var** (`NEBULA_NODE_ROLE`); the follower 409s writes
   (`middleware.rs::guard_writes_on_follower`); nginx pins writes to the
   leader hostname with no write-failover upstream. On leader loss, **writes
   are down for the full ~80 min–2 h cold recovery.**
4. **Recovery is serial.** `for rec in records { apply_wal_record }` — single
   tokio worker pinned at 100% CPU (ADR 0006). Only pathological when the WAL
   is large, which (2) guarantees.

openraft (`crates/nebula-raft`) is compiled but **dead**: never
`initialize()`d, off in prod, and needs ≥3 nodes (won't fit this 125 GiB host
running two ~60 GiB arenas). It is not the path.

## 2. Goal

Reads and writes continue **without interruption** through: periodic
snapshots, node OOM/crash, and node recovery. No multi-minute stalls; no
multi-hour write outages.

## 3. Target architecture

| Concern | Design |
|---|---|
| **Reads** | Served by any warm replica. The follower is already a warm standby (full index in RAM, continuously WAL-tailing). nginx read pool fails over node↔node on 503 (`max_fails=0` + `http_503`, PR #79). ✓ already done |
| **Snapshots** | **Taken on the standby (follower), never the write-serving leader.** The leader only *compacts* its WAL against the standby's committed snapshot seq. The leader never holds the index lock for a snapshot → no stall. |
| **Writes on node failure** | The follower is promotable to leader at **runtime** (no restart). Failover is **operator-triggered** for now (2-node clusters can't safely auto-elect without an arbiter — see §6); the endpoint + nginx write-failover make promotion a seconds-long operation instead of a multi-hour recovery. |
| **Footprint** | **int8 scalar quantization** of the vector arena → ~4× smaller (~60→~15 GiB) → OOM eliminated, snapshots/recovery cheaper, comfortably fits the host. |
| **Recovery** | Unchanged (ADR 0006): only slow when snapshots lapse; with snapshots always running (above) the WAL stays small → ~2 min, and it's off the serving path (failover moved serving to the warm node). |

## 4. Phase 1 — snapshot off the serving leader (this PR)

The single highest-leverage change. The hard primitives **already exist**:

- `TextIndex::snapshot_to(dir, mark_seq)` takes an **explicit** seq marker
  (`lib.rs`) — so a follower can stamp the snapshot with the leader-WAL seq it
  has *applied* (its replication cursor), not its own (empty) local WAL seq.
- `compact_wal()` takes **no index lock** — loads the latest snapshot header,
  prunes, and `wal.compact(header.wal_seq_at_snapshot)`. The leader can run it
  without stalling serving.
- `Wal::compact()` already clamps to `min_subscriber_ack`
  (`crates/nebula-wal/src/lib.rs`) so the leader won't drop WAL a follower
  still needs.
- The snapshot format is **node-portable** — proven by the raft
  build→install round-trip test (`crates/nebula-raft/src/snapshot.rs`).

### 4.1 Mechanism

Leader and follower run on the same host, so snapshots are exchanged through a
**shared snapshots directory** (a docker volume mounted on both nodes) rather
than a network transfer. (Cross-host deployments would swap this for a gRPC
snapshot-ship; out of scope here — single host in prod.)

- New env `NEBULA_SNAPSHOT_DIR` (default `$NEBULA_DATA_DIR/snapshots`,
  preserving current behavior). In prod both nodes point it at the shared
  volume. Recovery (`open_persistent`) and `compact_wal()` read snapshots from
  this dir.
- **Snapshot scheduler becomes role-aware**:
  - **Follower** (tailing a leader): runs the snapshot, calling
    `snapshot_to(shared_dir, applied_seq)` where `applied_seq` is the
    follower's current replication cursor position in the **leader's** WAL seq
    space (from `FollowerCursor`). It does NOT compact (it has no authoritative
    WAL). Its own ~2-min stall is invisible to clients: reads fail over to the
    leader, and it resumes WAL-tailing after.
  - **Leader**: **compact-only**. It never calls `snapshot()`/`snapshot_to`
    (never holds the lock). Each cycle it `compact_wal()`s against the latest
    snapshot the follower published to the shared dir.
  - **Standalone** (no follower): unchanged — snapshots locally as today (a
    single-node deployment accepts the stall; that's its trade for no replica).
- **Pruning**: only the snapshotter (follower/standalone) prunes; the leader
  never prunes (it doesn't own the snapshots). Keep the latest N so a
  recovering node always finds a complete one.

### 4.2 Correctness

- **Seq alignment**: the follower applies the leader's WAL in order and tracks
  its cursor; a snapshot stamped with that cursor's seq is exactly "covers
  leader-WAL through seq S." The leader compacting to S is safe — every record
  ≤ S is in the snapshot.
- **Compact safety**: `min_subscriber_ack` clamp already prevents the leader
  dropping WAL the follower hasn't acked; the follower's snapshot seq ≤ its ack
  by construction.
- **Recovery**: both nodes load the latest shared snapshot + replay their WAL
  tail from its seq (leader from its local WAL; follower resumes tailing from
  its cursor). Atomic `.part`→`.nsnap`+`.ok` publish means a reader only ever
  sees a complete snapshot.
- **Follower down**: no new snapshots are published; the leader's WAL grows
  (bounded by how long the follower is gone) and compaction no-ops — exactly
  the pre-existing `min_subscriber_ack` behavior. Self-heals when the follower
  returns. The `NebulaSnapshotAgeHigh` / `NebulaWalUnsnapshottedHigh` alerts
  (issue #69) already cover this.

### 4.3 Out of Phase 1 (kept for later phases)

- Phase 2 — int8 quantization (footprint → no OOM).
- Phase 3 — runtime `/admin/promote` + nginx write-failover (manual trigger;
  see 0003). Fast write failover on node loss.
- Cross-host snapshot ship over gRPC (only if prod stops being single-host).

## 5. Phase 2 — int8 quantization (footprint)

Store the flat `vectors: Vec<f32>` arena as int8 + scale, dequantize in
`vec_at`/distance, keep f32 re-rank for recall. ~4× RAM cut. Localized to the
arena + `vec_at` + distance fns + snapshot field. Removes the OOM class and
shrinks snapshot/recovery. Separate PR.

## 6. Phase 3 — fast write failover (manual promote)

Make `cluster.role` runtime-mutable; add `POST /admin/promote`
(Follower→Leader) that lifts the 409 write guard across REST/gRPC/pgwire;
persist replicated writes into the follower's own WAL (close the continuity
gap at `router.rs` `/admin/replication`); add an nginx write-capable upstream
with `proxy_next_upstream … non_idempotent`. **Operator-triggered**: a 2-node
cluster cannot safely auto-elect (split-brain) without an external arbiter, so
promotion is a deliberate human/runbook action for now. Result: leader loss →
seconds to restore writes (promote the warm follower) instead of hours.
Tracked against 0003.

## 7. Rejected / deferred

- **fork()/COW snapshot** — ideal data layout (plain owned Vecs) but hostile
  process (multi-thread tokio + allocator/lock fork-safety). High risk; A2
  (snapshot-on-standby) gets the same win without it.
- **Parallel WAL replay** — ADR 0006 rejected it; replay is only slow when
  snapshots lapse, which Phase 1 fixes.
- **mmap snapshot / on-disk HNSW** — new uncompressed format; revisit after
  quantization if restart RAM/time still hurts.
- **Raft** — dead in-tree and needs 3+ nodes; not viable on this host.
