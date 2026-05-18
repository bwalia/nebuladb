# Design 0006: Parallel WAL replay during recovery

- **Status**: Draft (engages with #66; needs decisions)
- **Author**: Claude + @bwalia
- **Created**: 2026-05-18
- **Tracks**: Issue #66 — Parallel WAL replay during recovery
- **Depends on**: PR #63 (compose patch + streaming snapshot read)

---

## 1. Why this exists

PR #63 fixed the *symptoms* that made cold recovery fail catastrophically:
- The kill-loop from healthcheck timeouts during recovery (compose `start_period` + `retries` bump)
- The 3x memory peak from `read_to_end` slurp on snapshot decompress (streaming-read fix)
- The follower OOM that blackholed half the showcase nginx pool

It did not fix the **throughput** of recovery itself. The replay loop is a serial `for rec in records { apply_wal_record(&rec) }` (`crates/nebula-index/src/lib.rs:277-279`). On `192.168.1.193` with a 3 GiB WAL (the unsnapshotted delta from the May 15 → May 18 scheduler-disabled window), recovery has been running >2.5h and is still going. One tokio worker pinned at 100% CPU; every other worker idle.

Even after the fixes in PR #63, **any operational hiccup that backs up the WAL turns a restart into a multi-hour outage**. That's not production-ready.

This ADR proposes how to fix it. The honest answer is "less parallel than I first thought" — see §3 for why.

---

## 2. Constraints discovered while reading the code

### 2.1 The apply path takes one global write lock

Every `apply_*_internal` function in `crates/nebula-index/src/lib.rs` does this:

```rust
let mut g = self.inner.write();   // global RwLock<Inner>
// ... mutate by_key, docs, parents, next_id, hnsw ...
```

`Inner` carries the entire bucket map (`by_key: AHashMap<(bucket, id), Id>`), the document store (`docs: AHashMap<Id, Document>`), parent-child links (`parents: AHashMap<(bucket, parent), AHashSet<id>>`), and the monotonic id counter. **There is no per-bucket sharding today.** Bucket parallelism on top of this lock buys nothing — every parallel worker would block on the same `inner.write()`.

The lock-order discipline at line 428 (`inner` → `hnsw`) compounds the constraint: HNSW mutations run *under* the `inner` write lock. Two parallel apply tasks would either deadlock or serialize on the same lock.

### 2.2 The id counter is a global sequence

`Inner.next_id: u64` is incremented monotonically every insert. Parallel inserts to two buckets still need to agree on what id to use next. Per-bucket id counters are doable but break the assumption (used by HNSW + the snapshot format) that internal ids are dense across the whole index.

### 2.3 Cross-bucket records exist but are rare

Looking at the `WalRecord` variants:

| Variant | Bucket-scoped? |
|---|---|
| `UpsertText { bucket, external_id, .. }` | yes |
| `UpsertDocument { bucket, doc_id, chunks, .. }` | yes |
| `Delete { bucket, external_id }` | yes |
| `DeleteDocument { bucket, doc_id }` | yes |
| `EmptyBucket { bucket }` | yes |

Every variant names a single bucket. There are no `MoveBetweenBuckets` or `RebuildAll` records. **In principle** records to different buckets are independent.

But: §2.1 says the lock structure doesn't reflect that independence today.

### 2.4 The bottleneck is CPU, not I/O

Live observation on `192.168.1.193`:
- `rchar` (read bytes) plateaued at 4.25 GB after ~1.5h — full WAL + snapshot consumed.
- Then 30+ min of *zero* I/O while one thread stayed at 100% CPU.
- `wchan = 0`, `syscall = running` — pure user-space CPU work.

That's HNSW link verification + bookkeeping, not disk read. Parallelism plans must target the CPU phase, not the I/O phase. **Pre-fetching from disk into a queue won't help.**

---

## 3. Three options, in order of cost

### Option A — Bucket-shard the lock (proposed)

**Change:** replace `RwLock<Inner>` with a `DashMap<bucket, Arc<Mutex<BucketInner>>>` plus a smaller global lock for cross-bucket fields (the next_id counter, the HNSW arena registry).

**Per-bucket inner:**
```rust
struct BucketInner {
    by_id: AHashMap<String, Id>,             // external_id → internal id
    docs: AHashMap<Id, Document>,
    parents: AHashMap<String, AHashSet<String>>,  // doc_id → chunk ids
}
```

**Replay:**
```rust
records.into_par_iter()
    .group_by(|r| r.bucket().to_owned())
    .for_each(|(bucket, recs)| {
        let inner = bucket_map.entry(bucket).or_default();
        let mut g = inner.lock();
        for rec in recs {
            apply_one(&mut g, rec)?;
        }
    });
```

**HNSW remains shared.** The HNSW arena is one big graph; sharding it per bucket is a separate ADR-sized change. Per-bucket `apply` still calls `self.hnsw.insert(id, vector)` which takes the HNSW's own lock.

**Throughput estimate:** an honest one. The lock contention is partly from `inner` and partly from `hnsw`. Sharding `inner` removes one of the two — likely 2-3x speedup if HNSW becomes the new bottleneck, more if HNSW lock-friction is mostly about its acquire/release path rather than the held duration. **Probably not 10x.** §6 acceptance criteria say 10x; this option may not hit it.

**Cost:** ~3-5 days of focused work + soak. Touches every site that holds `inner.write()` (about a dozen functions in `lib.rs`). Risk of subtle correctness bugs (duplicate ids, orphaned chunks).

### Option B — Shard HNSW per bucket as well

**Change:** Option A plus replace the single `Hnsw` with a `DashMap<bucket, Hnsw>`. Each bucket has its own graph.

**Pros:** Real parallelism. Workers don't share any lock except the (rare) `next_id` allocator.

**Cons:**
- **Snapshot format change.** The on-disk `nsnap` format serializes the HNSW as one structure. Per-bucket would either: (a) serialize each bucket's HNSW separately (manageable, format change with migration); (b) keep one logical graph but multiple physical arenas (harder).
- **Cross-bucket search.** Today `search_text(bucket=None, ...)` walks one HNSW. Per-bucket would have to merge top-K from N searches. Not free but tractable.
- **HNSW snapshot determinism.** PR #38's determinism test (`streaming_serialize_matches_owned_serialize`) assumes one graph. Adapting per-bucket is doable but the test doubles in size.

**Throughput estimate:** Genuinely 10x for this workload (writes spread across buckets). But the snapshot format break makes this a multi-week change that touches the operator's CRD, the backup engine, and every snapshot-test in the suite.

**Cost:** ~2-3 weeks. **Probably not worth it for the throughput problem alone.** Worth it if it composes with other goals (e.g. per-bucket TTL, per-bucket replication).

### Option C — Batch + commit, single-threaded but faster

**Change:** keep the serial loop, but batch the apply path:
- Take `inner.write()` once per *batch* (e.g. 1000 records) instead of per-record.
- Defer HNSW inserts to the end of the batch and do them in one pass.
- Memoize bucket lookups within a batch.

**Pros:** No lock-structure change. Backwards-compatible. Smallest diff.

**Cons:** Crash mid-batch loses up to N records of recovery progress (recovery restarts from the snapshot). Mitigatable by writing a "replay checkpoint" file every K batches. But increases code complexity for an incremental win.

**Throughput estimate:** 2-4x. Lock acquire/release is a real fraction of per-record cost; amortizing it helps. Doesn't address the actual single-thread CPU bottleneck.

**Cost:** ~1-2 days.

---

## 4. Open questions before code

These need answers (from the user, or via experimentation) before I commit to an option:

### 4.1 What's the actual replay throughput today?

I have circumstantial evidence (>2.5h on a 3 GiB WAL, ~one tokio worker pinned). I don't have a clean measurement. Before optimizing, profile a known-size WAL on quiet hardware:

```bash
# Synthetic: ingest N docs, count records, time recovery from a fresh boot.
NEBULA_DATA_DIR=/tmp/profile-data-$N ./scripts/seed.sh   # writes N docs
docker compose restart nebula-server
time curl --until-200 http://localhost:18080/healthz
```

The output gives **records/sec** and tells us where we are vs SLA. If it's 5k recs/sec, a 3 GiB WAL of small records is ~10 min — fine. If it's 50 recs/sec, we have a different problem. **We don't know which.**

### 4.2 What does an HNSW insert actually cost?

The single-thread profile suggests most of the wall time is in HNSW work, not bookkeeping. If HNSW insertion is the bottleneck, **Option A's lock-sharding doesn't help** (the HNSW lock is still serial). We'd need Option B.

A `perf record` or `tokio-console` capture during replay would name the actual hot function.

### 4.3 What's the production target?

"10x faster" was my acceptance criterion in #66. Is that right? If we only need "fast enough that a single-region restart is under 10 minutes," Option C might be enough. If we need "single-region restart under 60 seconds at any WAL size," only Option B reaches that.

The right SLA depends on the deployment. For a single-region-non-critical system, 10 min is fine. For an HA-claiming system, anything over a heartbeat is too long — but that's what raft mode is supposed to solve, by failing the leader over to a peer rather than rebooting.

### 4.4 Does this matter post-Raft?

Issues #32/#33 closed with Raft as the within-region HA story (PRs #35-#47, all merged). In raft mode, a node going down promotes a peer; the failing node's recovery happens in parallel with the cluster continuing to serve. **Slow recovery is a worse problem in standalone mode than in raft mode.**

Most production deployments will run raft. So:
- For raft mode, slow recovery is "annoying, not customer-facing."
- For standalone mode (the showcase, dev environments, single-tenant deployments), slow recovery is the wedge customers see.

If we expect production to converge on raft, **Option C might be enough for standalone — accept the 2-4x improvement, leave the deeper fix for if-and-when standalone customers ask.**

---

## 5. Recommended decision

**Default recommendation: do Option C now (1-2 days). Track Option A/B as a follow-up if and only if (4.1) measurement confirms Option C isn't enough.**

Reasoning:

1. **The actual throughput is unmeasured.** I'd rather take a small swing (C) and re-measure than commit to a 3-5 day refactor (A) or 2-3 week refactor (B) on speculation.

2. **The diminishing-returns curve** between options A and B is steep. A is "remove one lock contention source"; B is "actually parallelize" but at high integration cost. If C is enough for standalone, A is over-investment; if C is not enough, A might also not be enough and we end up at B anyway.

3. **Raft mode (already shipped) reduces the urgency.** Standalone-only deployments still hit the slow path, but they're a smaller and shrinking set.

4. **PR #63 already solved the operationally-acute problem.** Nobody is currently being killed by slow recovery — the patched leader is grinding through but not crash-looping. The fix can take its time to be designed right.

**If the user wants Option A or B regardless:** I'll write it. But the ADR's honest answer is "measure first, optimize second."

---

## 6. Acceptance criteria (revisable)

If we pursue Option A:
- 3 GiB WAL recovery in **<30 min** on 192.168.1.193-class hardware (vs. 2.5h+ today). 5x improvement.
- Periodic progress log line every 10s OR every 10k records, whichever first.
- All existing `crates/nebula-index/tests/durability.rs` tests pass unchanged.
- New test: parallel-bucket-replay produces byte-identical state to serial-replay across 1k records distributed across 100 buckets. (Determinism, not throughput, is what reviewers should care about most.)

If we pursue Option B:
- 3 GiB WAL recovery in **<5 min**. 30x.
- Plus everything in Option A's criteria.
- Plus: snapshot format migration tested both directions (V1 snapshot loads on V2 binary, V2 snapshot doesn't load on V1 — reject cleanly).

If we pursue Option C:
- 3 GiB WAL recovery in **<60 min** (vs 2.5h+). 2.5x.
- Plus first criterion in A's list.
- Plus: crash mid-batch is recoverable (restart resumes from a recent boundary, not always from snapshot).

---

## 7. What this ADR is NOT

- **Not a vote for Option B.** Per-bucket HNSW is a significant code change that affects features outside the recovery path; that needs its own ADR if the Option C measurement says we need it.
- **Not a Raft replacement.** Raft solves "we need to keep serving when a node dies." This solves "the dying node restarts faster." Different goals.
- **Not a snapshot-cadence change.** PR #63's compose defaults (15 min / 50 MiB) are already correct. The slow recovery on `192.168.1.193` was caused by the obsolete env-var workaround disabling the scheduler — **not** the defaults being wrong. This ADR assumes the scheduler is enabled.

---

## 8. Decision needed

1. **Approve the "measure first" approach (§4.1)?** A simple seeding + timing script gives us a real number before we commit to a refactor.
2. **If measurement says Option C is enough, ship that?** Or insist on A/B regardless because of growth projections?
3. **If measurement says Option B is required, accept the 2-3 week scope?** Snapshot format migration is the big-ticket item.

I recommend (1) — a 1-day measurement + recommendation update — before any code. The next concrete step is a reproducible benchmark. Want me to write that script as a separate small PR?

---

## 9. Measurement results (2026-05-18)

The benchmark from §4.1 was implemented as `crates/nebula-index/tests/recovery_bench.rs`. Run on the author's laptop (Apple Silicon), `--release` build, MockEmbedder dim=384, snapshot taken at the halfway mark to model production "snapshot + WAL tail" recovery:

| Records | Buckets | Recovery wall time | Throughput |
|---|---|---|---|
| 5,000 | 20 | 12.85s | **389 rec/sec** |
| 20,000 | 50 | 62.93s | **318 rec/sec** |

Throughput is stable in the 300-400 rec/sec range, mildly degrading as the HNSW graph grows. Extrapolating to the production sizes that matter:

| Scenario | Records | Predicted recovery |
|---|---|---|
| Steady state (50 MiB WAL cap) | ~50k | **~2 min** |
| One day without compaction | ~500k | ~25 min |
| The 192.168.1.193 pathological case (3 GiB WAL) | ~3M | ~2.5 hours ← **matches observed** |

### What this changes

The "Option B is required" hypothesis from §3 was **wrong for the typical production case**. With snapshots running on schedule, recovery touches ~50k records, and 2 minutes is already fine. The pathological hours-long recovery on `192.168.1.193` was caused by the snapshot scheduler being disabled (Bug C in PR #63) — **not** by the replay loop being structurally too slow.

### Updated recommendation

**Do nothing structural.** The right defense is keeping the snapshot scheduler healthy:

1. **Keep Bug C fixed** — `.env.deploy.example` already documents that `NEBULA_SNAPSHOT_INTERVAL_SECS=0` is obsolete. Reaffirmed by this measurement.
2. **Add monitoring** for "WAL bytes since last snapshot" — if it exceeds, say, 200 MiB, page someone. The endpoint `/admin/durability` already reports this; a Prometheus alert would close the loop. **This is the cheapest meaningful work.**
3. **Defer Options A/B/C indefinitely.** If a future operational incident produces another multi-GB unsnapshotted WAL, the watchdog + healthcheck-grace from PR #63 prevents the kill-loop, and recovery still finishes — just slowly. Slow-but-correct is acceptable for an incident-response edge case.

### Specifically rejected

- **Option B (per-bucket HNSW), 2-3 weeks:** rejected. The investment doesn't earn its keep against a problem that mostly doesn't exist when the scheduler runs. Reopening the case requires either a real customer SLA on cold-recovery time or a measurement showing the steady-state path itself isn't fast enough.
- **Option A (shard inner lock), 3-5 days:** rejected. Same reason — 2-3x of an already-fine 2 min is 40s. Not worth the regression risk of touching every apply site.
- **Option C (batch apply), 1-2 days:** rejected. Smallest swing, but still touches the apply path and doesn't materially change the steady-state experience.

### What the bench is for

The bench file (`crates/nebula-index/tests/recovery_bench.rs`) stays in the tree as **a regression guard** rather than active investigation. If a future change to the apply path tanks throughput from 300+/sec to 30/sec, running the bench manually surfaces it before it ships. Run with:

```bash
cargo test --release -p nebula-index --test recovery_bench -- --ignored --nocapture
```

### What this ADR is now

Closed-with-decision: **don't do this. Keep snapshots healthy. Watch WAL-since-snapshot in monitoring.** A Prometheus alert is the only follow-up worth tracking.
