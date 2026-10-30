# Design 0014: Disk-backed document store

- **Status**: Proposed
- **Author**: Claude + @bwalia
- **Created**: 2026-10-30
- **Tracks**: idle RSS scales with corpus size; prod region-b holds 25 GiB with no traffic
- **Relates to**: `nebula-index` (`Inner.docs`, `durability.rs`), `nebula-bm25`, design 0006 (WAL replay), design 0008 §6 (BM25 rebuilt from `docs`)

---

## 1. Why this exists

Every document lives on the heap for the whole life of the process. Memory
tracks corpus size, not traffic:

| Pod (2026-10-30) | Docs | Heap (`anon`) | CPU | Note |
|---|---|---|---|---|
| `nebuladb-region-b` | 6.22M | 25.5 GB | 86m | idle, ~4.1 KB/doc |
| `nebuladb` (leader) | 6.39M | 52.6 GB (peak 65 GiB) | 2.3 cores | embed-migrate drain running |

The vectors are not the problem. HNSW already stores int8 codes
(`nebula-vector/src/hnsw.rs:160`), so 6.2M × 768 dims is ~4.8 GB. The bulk
is the `Document` itself (text, a `serde_json::Value` metadata tree, owned
`bucket` / `external_id` strings) held in `Inner.docs:
AHashMap<Id, Arc<Document>>` (`nebula-index/src/lib.rs:199`), plus key
maps that copy those strings again.

Most other vector databases (Qdrant payload storage, Weaviate's object
store, Milvus) keep the graph in RAM and the payload on disk. A search
only needs the payload for the final top-k hits.

## 2. Where the 4.1 KB/doc goes (estimate)

Inputs from a 1-in-1000 sample of prod `docs.jsonl` (8,317 docs): text
averages 370 B, metadata 341 B of compact JSON (~11.4 nodes), 51.8 tokens,
44.6 unique terms. `companies_house` is 62% of docs, `leads` 38%.

| Component | Lives in | Est. bytes/doc | Movable to disk? |
|---|---|---|---|
| int8 vector + scale | `Hnsw.codes/scales` | ~772 (exact) | No: every search hop reads it |
| HNSW adjacency (`Vec<Vec<Vec<u32>>>`) | `Hnsw.neighbors` | ~200–250 | No |
| HNSW id maps | `external`, `by_external` | ~40 | No |
| **`Document`**: text, metadata tree, strings, `Arc` | `Inner.docs` | **~1,800–2,400** | **Yes** |
| `by_key` `(String, String)` copies, `by_bucket`, map overhead | `Inner` | ~200 | Partly (§6 interning) |
| BM25 postings (16 B × ~45 terms + `Vec` slack) + `DocEntry` | `Bm25Index` | ~800–1,200 | Not in this design |
| **Total** | | **~3.8–4.9 KB** | measured: 4.1 KB |

The `Document` row is the single biggest item and the only large one the
search hot path doesn't need for every candidate. Phase 0 replaces these
estimates with measured numbers before any code moves.

## 3. Goal and non-goals

**Goal**: keep `Document` bodies on disk, with an in-RAM id → location
index and a bounded hot cache. Idle heap should drop roughly 40–55% at
the current corpus (target: region-b ≤ 14 GB at 6.2M docs) without a
measurable p99 regression on `/search`.

**Non-goals**:
- Moving HNSW or BM25 postings to disk (DiskANN / on-disk postings is a
  separate, larger design).
- Changing the WAL record format, the replication protocol or the public
  API. `Hit` still carries `text` and `metadata`.
- A new wire format for snapshots in Phase 1 (§7 keeps it compatible).

## 4. Constraints found in the code

1. **One choke point for writes.** `Inner::insert_doc` / `remove_doc`
   (`lib.rs:224–255`) are the only paths that add or remove docs, and both
   run under the `inner` write lock. The store hooks in here.
2. **The WAL is the durability authority.** Snapshot + WAL replay rebuild
   state, and replay of already-applied records is a no-op
   (`replaying_snapshotted_records_is_a_noop`, `is_unchanged` at
   `lib.rs:703`). So the doc store can use non-durable commits and stamp
   the WAL sequence it reflects. After a crash, replay from that sequence
   repairs it.
3. **Readers that touch `docs`:**
   - *Result assembly* (`search_run.rs:168, 219, 287, 330`): k point
     reads per query. Cheap.
   - *Exact-bucket scans* (`run_vector_exact_bucket`,
     `run_bm25_exact_bucket`): every doc in buckets ≤ 16,384 docs
     (`ALWAYS_BELOW`, `search_run.rs:122`), and BM25 re-tokenizes
     `doc.text` via `score_doc`. With docs on disk this would be up to
     16k reads per query. Vector exact scan only needs the vector (in
     RAM) plus k docs at the end, so it is fine once reordered.
   - *`is_unchanged`* on every upsert: one point read.
   - *Full scans*: `bucket_stats(top_keys)` (reads every metadata
     object), `export_bucket`, `for_each_doc_in_bucket` (embed-migrate),
     and the snapshot writer. Admin and background only.
   - *SQL residual filters* (`nebula-sql/src/executor.rs:696`) run on
     hits that were already fetched. No change.
4. **BM25 rebuilds from `docs` on load** (design 0008 §6). With docs on
   disk, startup streams the store instead of the snapshot body.
   Throughput is no worse than today.
5. **Region-b has no persistent volume** (`snapshots/` and `wal/` are
   empty). It needs a writable directory: an `emptyDir` on node disk is
   enough, since the store can be rebuilt.

## 5. Proposed design

### 5.1 Storage engine

**Recommend [`redb`](https://github.com/cberner/redb)**, a pure-Rust
embedded B-tree with ACID transactions and a single file. It keeps the
binary free of C++ dependencies, in line with design 0008 §3 ("keep the DB
binary lean").

- One table `docs: u64 (internal Id) → bytes`.
- Value is `bincode(Document)`, compressed with zstd per record. Text and
  metadata are mostly templated (`leads`, Companies House), so a trained
  zstd dictionary is a Phase 3 option.
- Metadata is stored as compact JSON bytes and parsed lazily into
  `serde_json::Value` only when a caller needs it.
- A `meta` table holds `applied_seq` (the last WAL sequence reflected in
  the store) and a format version.

Alternatives considered:

| Option | Why not (for now) |
|---|---|
| Custom append-only segment log + in-RAM offset index (16 B/doc) | Fewest dependencies and fastest reads, but we would own compaction, crash recovery and fsync ordering. Revisit if redb read latency fails the Phase 2 bar. |
| RocksDB | C++ dependency and build cost; violates the lean-binary principle. |
| `fjall` (pure-Rust LSM) | Viable; better for write-heavy loads. Our access pattern is point reads plus occasional bulk writes, which suits a B-tree. Keep as a fallback. |
| `sled` | Unmaintained. |
| mmap the snapshot file | The snapshot is one zstd stream, so it isn't randomly addressable. That would need a new format anyway. |

### 5.2 What stays in RAM

- HNSW (codes, scales, links, id maps). Unchanged.
- BM25 postings and `DocEntry`. Unchanged.
- `by_key`, `by_bucket`, `parents`, with interned bucket names (§6).
- **Hot doc cache**: a bounded LRU of `Arc<Document>` keyed by `Id`
  (default 256 MiB, `NEBULA_DOC_CACHE_MB`).
- **Small-bucket pin**: buckets ≤ `ALWAYS_BELOW` docs keep their
  documents resident, so exact-bucket BM25 scans never hit disk. In prod
  these buckets are tiny (demo, legal and probe buckets add up to a few
  thousand docs). The cost is bounded by the threshold times the number
  of small buckets, and is reported in `/metrics`.

### 5.3 Write path

`insert_doc` / `remove_doc` keep their signatures. Inside the existing
write lock they:

1. update BM25, `by_bucket` and `by_key` as today;
2. stage the doc write or delete into the open redb write transaction;
3. update the LRU and the small-bucket pin.

Commits are **group-committed**: every N ms or M records, and always
before a snapshot mark, with `applied_seq` written in the same
transaction. Commits are non-durable (no fsync). Durability comes from the
WAL, exactly as it does for in-memory state today. The redb commit runs on
a dedicated thread so the index write lock is never held across I/O. A
`pending: AHashMap<Id, Option<Arc<Document>>>` overlay serves reads for
uncommitted docs; a removed doc is a `None` entry.

### 5.4 Read path

`get_doc(id)` checks, in order: the `pending` overlay, the small-bucket
pin, the LRU, then a redb read (decompress, decode, insert into the LRU).
Result assembly fetches the k ids as one batch after releasing the
`inner` read lock, so disk reads never block writers. A hit whose doc has
vanished in the meantime is skipped, the same as a tombstone today.

### 5.5 Snapshots and recovery

- **Phase 1 (compatible)**: the snapshot writer streams documents from the
  store (a read transaction gives a consistent point-in-time view)
  instead of `freeze_for_snapshot`'s `Arc` clone. The `.nsnap` format is
  unchanged, so backup/restore (design 0002) and old binaries keep
  working.
- **Recovery**: open the store and read `applied_seq`.
  - If a valid snapshot exists with `seq ≤ applied_seq`, load only the
    HNSW body. Rebuild BM25 and the key maps by streaming the store, then
    replay the WAL from `applied_seq`.
  - Otherwise (store missing, corrupt or behind), rebuild the store from
    the snapshot's doc section, then replay as today. **The store is
    always rebuildable**, so deleting it is a valid fix.
- **Phase 3 (optional)**: snapshots reference a checkpointed copy of the
  store file instead of re-serializing every doc. That would cut the
  16 GB `.nsnap` writes, which currently happen every scheduler tick.

### 5.6 Full-scan callers

- `bucket_stats(0)` (counts only) uses `by_bucket` sizes and never reads
  docs.
- `bucket_stats(top_keys > 0)` and `export_bucket` iterate a redb read
  transaction in id order. This is sequential I/O and admin-only. Admin
  calls should pass a timeout.
- `for_each_doc_in_bucket` (embed-migrate) streams the same way.

## 6. Cheap wins that don't need disk (ship first)

Independent of the store, and these also shrink what goes on disk:

1. **Intern bucket names.** Use `Arc<str>` in `Document` and `by_key`.
   Today every doc owns a copy of `"companies_house"` or `"leads"` in
   `Document.bucket` and another in the `by_key` tuple.
2. **Compact metadata.** Store metadata as `Box<[u8]>` (compact JSON) or
   `Box<RawValue>` and parse on read. A `serde_json::Value` tree with ~11
   nodes is several times larger than its 341-byte JSON text.
3. **`Box<str>` instead of `String`** for immutable fields, dropping
   capacity slack.

Rough expected effect: ~0.8–1.4 KB/doc, so **5–8 GB at 6.2M docs**, with
no change to the I/O path. This is low risk, can ship as its own PR, and
makes the Phase 2 numbers easier to read.

## 7. Phased plan

| Phase | Scope | Exit criterion |
|---|---|---|
| **0. Measure** | Add jemalloc `stats.allocated` and per-structure size gauges to `/metrics`, and a `heap-profile` debug endpoint behind the admin token. Run on region-b. | §2 table replaced with measured numbers |
| **1. Compact in-RAM docs** | §6 items 1–3 | Region-b idle heap drops by ≥ 4 GB; all tests green |
| **2. Disk store behind a flag** | §5, gated by `NEBULA_DOC_STORE=disk` (default `memory`); `DocStore` trait with `MemoryDocStore` (today's behaviour) and `RedbDocStore` | Region-b ≤ 14 GB idle; `/search` p99 within +10% of baseline at k=10 with cold LRU; crash test (kill -9 mid-ingest, restart, compare `/healthz` docs and spot-check hits) passes |
| **3. Default on + snapshot reuse** | Flip the default in int → test → acc → prod; optional checkpoint-based snapshots; zstd dictionary | One week clean in prod; snapshot write time reduced |

`DocStore` is the seam that keeps `MemoryDocStore` around for tests and for
small deployments that would rather not have a file.

## 8. Risks

| Risk | Mitigation |
|---|---|
| Cold-cache p99 for searches whose top-k are all disk reads | Batch reads; NVMe-backed `local-path`; LRU sized from Phase 2 data; the page cache helps too (counted as reclaimable `file` memory, not `anon`) |
| Index lock held across I/O | Commits on their own thread; reads after dropping the lock (§5.3, §5.4) |
| Store and WAL diverge after a crash | `applied_seq` committed atomically with data; replay is idempotent; store is rebuildable |
| Pod memory limit vs page cache | cgroup limits count page cache, but the kernel reclaims it before OOM-killing. Keep `anon` ≤ ~70% of the limit |
| Region-b has no PVC | `emptyDir` sized for the store (~2–4 GB compressed at the current corpus) |
| Embed-migrate and re-embed bursts write heavily | Group commit; benchmark `upsert_text_bulk` in Phase 2 |

## 9. Open questions

1. Is NVMe guaranteed on every node that runs a nebuladb pod (debworker00,
   debian004)? This decides the cold-read budget.
2. Should the small-bucket pin threshold be its own knob, or reuse
   `ALWAYS_BELOW`?
3. Is it acceptable for `bucket_stats(top_keys)` to become an O(bucket)
   disk scan, or should top metadata keys be maintained incrementally?
4. Phase 3 checkpoint snapshots change what `nebula-backup` ships. Is that
   worth the format change, given the current 16 GB snapshot cadence?

## 10. Decision needed

Approve Phase 0 + Phase 1 now (low risk, no format change). Decide on
Phase 2 once Phase 0 numbers are in.
