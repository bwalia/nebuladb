# Technote: Prod memory footprint and deploy downtime

**Status:** living note, last updated 2026-10-30
**Audience:** platform / SRE / engineers touching `nebula-index` or the prod chart
**Related:** [design 0014](../design/0014-disk-backed-document-store.md) (disk-backed docs), [design 0007](../design/0007-zero-downtime-reads-writes.md) (zero-downtime), [active-active XDCR](active-active-xdcr.md), PRs #166, #167

---

## 1. Summary

- NebulaDB holds the whole corpus in RAM, so memory tracks **document count, not traffic**. Before the work below, idle region-b held 25.5 GB for 6.22M docs (~4.1 KB/doc).
- Three changes cut heap per doc by about **36%** (3,418 → ~2,190 B) on real prod documents. The last one is controlled by a new flag, `NEBULA_MEMORY_PROFILE`, which defaults to `small`.
- Every prod deploy currently causes **minutes of 503s** (~20–40 min on 2026-10-30). The cause is topology, not memory: nothing warm serves while the leader recovers. §5 has the plan to fix it.

## 2. Where the memory goes

Measured on a 1-in-1000 sample of prod `docs.jsonl` (8,317 docs: 62% `companies_house`, 38% `leads`; average text 370 B, metadata 341 B of JSON), loaded into a fresh index with a byte-counting allocator:

| Stage | Heap / doc | Change | What changed |
|---|---|---|---|
| Before | 3,418 B | | `Document` held as `serde_json::Value` tree + owned strings |
| #166 compact docs | 2,771 B | −647 B | metadata kept as JSON text, parsed on read; bucket name shared; `Box<str>` |
| #167 key index | 2,671 B | −100 B | `by_key` merged into a nested `by_bucket` map; no per-doc key copies |
| Memory trim (`small`) | ~2,190 B | −478 B | HNSW adjacency + BM25 posting lists shrunk to fit |

At 6.2M docs that is roughly **4.0 + 0.6 + 3.0 ≈ 7.6 GB** less. Caveats:

- The counter records requested bytes. jemalloc rounds allocations up to size classes, so the old layout, which made many more small allocations, probably cost more than shown.
- The trim saving depends on how a node was built:
  - **Built by replication or ingest** (region-b, or the leader after hours of writes): the full ~480 B/doc applies.
  - **Freshly restored from a snapshot:** HNSW lists come back exactly sized and only the BM25 share (~410 B/doc) applies. The HNSW slack builds back up as writes arrive, which the periodic trim handles.

What stays resident per doc (rough): int8 vector 772 B; HNSW links ~200–250 B; BM25 postings ~600–1,000 B; document text + metadata ~800 B; key maps ~100 B. Moving the document body to disk is design 0014 Phase 2.

## 3. The flag: `NEBULA_MEMORY_PROFILE`

| Profile | Allocator (jemalloc) | Index trim |
|---|---|---|
| **`small`** (default) | Freed pages returned to the OS after ~1 s (`dirty_decay_ms=1000`, `muzzy_decay_ms=0`). A background purge thread runs, so an idle node also shrinks. | Once after boot recovery (before serving), then every 15 min |
| `fast` | Freed pages kept ~10 s for reuse (fewer page faults under heavy churn) | Off |

Overrides (on top of either profile):

- `NEBULA_MEMORY_TRIM_SECS`: periodic trim interval; `0` disables it.
- `NEBULA_MEMORY_TRIM_BATCH`: nodes/terms per lock hold (default 20,000).

**Impact on serving:** the trim walks the HNSW graph and BM25 index in batches, releasing the lock between batches. A request waits at most one batch (milliseconds). Search results are unchanged; this is covered by tests and the probe compared results before and after. A full pass is ~5 ms per 8k docs, so a few seconds of CPU at 6.2M docs, spread out. The boot trim runs while the boot listener is still answering 503, so it adds nothing to serving downtime.

**Set it** in Helm via `server.env.NEBULA_MEMORY_PROFILE`. It's in the shared ConfigMap, so it applies to the leader, the follower and region-b. Use `fast` only for a bulk-ingest node where footprint doesn't matter.

**Check it:** the startup log has `memory profile`, and each pass logs `memory profile: index trimmed`. jemalloc's background thread only exists on Linux. On macOS the decay settings still apply, but only on the next allocator call.

## 4. Why a prod deploy means 503s today

Timeline of the `dbdece3` deploy on 2026-10-30:

| Time (UTC) | Event |
|---|---|
| 21:08 | `helm upgrade` restarts the leader **and** region-b together |
| 21:08 → | Leader boot listener answers **503** while it loads a 16 GB snapshot, rebuilds BM25 (single thread) and replays the WAL |
| 21:08 → | Region-b restarts **empty** (0 docs) |
| 21:27–21:47 | Leader serving again (6,479,474 docs); exact time unknown because the watcher lost the LAN |

Why nothing else served:

1. **The within-region follower is parked** (`ha.withinRegion.followers: 0`, Oct 29). It needed ~41 GiB, and its PVC is pinned to debian002 (30 GiB RAM), where it was OOM-killed 4× and destabilised neighbouring volumes. Design 0007's warm-standby read path therefore doesn't exist right now.
2. **Region-b can't stand in:**
   - It has a 50 Gi PVC, but replicated writes are applied in memory only (`apply_wal_record`, not appended to its own WAL).
   - Its snapshots are disabled (`NEBULA_SNAPSHOT_*=0`) because snapshot peaks OOM-killed it at its old footprint.
   - XDCR deliberately doesn't persist cursors (`cross_region.rs`), so after a restart it replays from the leader's `BEGIN`, which compaction has already removed.
   - It was filled by `scripts/bootstrap_region_mirror.sh`. **After a restart it probably needs that bootstrap again** (to verify on the next check).
   - It's also not a read fallback for `api.nebuladb.net` / `showcase`.
3. **One `helm upgrade` restarts both pods at once.**
4. **Leader recovery is long:** 16 GB snapshot + BM25 rebuild + WAL replay, mostly single-threaded (~1 core busy), 20–45 min.

## 5. Plan: deploys without 503s

In order. Each step is useful on its own.

1. **Make region-b durable.** Have the cross-region consumer append applied records to region-b's own WAL (`append_replicated` already exists) and persist its per-remote cursor. Then turn region-b's snapshots back on, which the smaller footprint now leaves room for: ~21 GB + one ~5–7 GB arena copy during a snapshot, against a 56 Gi limit. Region-b would then restart warm and resume from its cursor, instead of starting empty.
2. **Restart pods one at a time.** Split the deploy workflow: upgrade region-b, wait until it's healthy **and** caught up (doc count within x% of the leader, XDCR lag ≈ 0), then upgrade the leader.
3. **Read failover at the edge.** Route `api.nebuladb.net` and `showcase` reads to region-b when the leader returns 503 (same pattern as PR #79's nginx read pool: `proxy_next_upstream http_503`). Writes to buckets homed on the leader still fail during its recovery. Region-b only accepts writes for buckets it homes (see the active-active technote §1).
4. **Later:** shorten recovery itself (parallel BM25 rebuild, or persist BM25), and design 0014 Phase 2, which would shrink the follower enough to fit debian002 again and restore design 0007's full read+write failover.

**Until 1–3 land, treat every prod `helm upgrade` as a planned outage of 20–45 min.**

## 6. Runbook: checking memory

```sh
export KUBECONFIG=~/.kube/k3s1.yaml
kubectl -n prod top pods --containers | grep -E '^nebuladb(-region-b)?-'
# Heap vs reclaimable cache, and the peak since start:
kubectl -n prod exec <pod> -c server -- sh -c \
  'grep -E "^(anon|file) " /sys/fs/cgroup/memory.stat; grep VmHWM /proc/1/status'
# Doc count / version:
curl -s https://api.nebuladb.net/healthz
```

`anon` is the number that matters: it's heap, and it can't be reclaimed. `file` is page cache the kernel frees under pressure. Compare `anon` to the pod limit and keep it ≤ ~70%. Snapshot peaks (`VmHWM`) add about one HNSW arena copy on top.
