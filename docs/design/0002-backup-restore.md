# Design 0002: Backup & Restore

- **Status**: Draft
- **Author**: Claude + @bwalia
- **Created**: 2026-05-05
- **Builds on**: design 0001 (multi-region), gap #4 (bucket export/import)
- **Supersedes**: the empty `NebulaCluster.spec.backup` field shipped in `cc042a0`

---

## 1. Problem

NebulaDB has durable state today — WAL segments and snapshots in
`NEBULA_DATA_DIR` — but nothing ships that data off-box. A node loss
takes the data with it. The operator's `NebulaBackupSpec` is a typed
no-op. There is no way to:

- Capture a known-good cluster image and ship it to object storage.
- Roll a cluster back to a specific moment.
- Move a cluster between environments via a backup → restore loop.
- Survive a region outage without `NebulaRegionFailover`'s prerequisites.

We want: **a backup + restore subsystem competitive with single-node
managed databases (Postgres-style PITR, S3-compatible target,
operator-driven schedule), built honestly on top of what NebulaDB
actually has, not what a distributed-DB whitepaper assumes.**

---

## 2. Constraints and non-goals

### Hard constraints

1. **No new storage engine.** NebulaDB is single-process per region;
   the WAL is already the source of truth for replay. Backup builds on
   the existing snapshot + WAL primitives — no new on-disk formats.

2. **Backups are an admin endpoint, not a hot-path concern.** The
   write path stays untouched: snapshotting already takes only the
   index read lock briefly, and WAL segments are immutable once
   rolled. No new locks or fsync stalls in steady state.

3. **One storage protocol that works for the most users.** S3-
   compatible APIs cover AWS S3, MinIO, GCS via the S3 interop layer,
   Backblaze B2, Cloudflare R2, Wasabi. Native Azure Blob and native
   GCS clients each cost ~3000 lines of crate dependency for users who
   could already use the S3-compat path. Defer them.

4. **Operator-driven, not hand-cranked.** The operator owns scheduling
   and retention via a CRD. The CLI is for operators reaching into a
   single pod for ad-hoc work, not the primary surface.

### Non-goals for v1

- **Distributed shard backup.** NebulaDB has no shards. A "shard
  backup" today means "this node's full state."
- **Encryption at rest in NebulaDB itself.** S3 server-side encryption
  + KMS keys are configured on the bucket. We document how, not
  implement crypto.
- **Continuous backup ("PITR with sub-second granularity").**
  v1 ships scheduled backups + WAL replay restore that resolves at
  WAL-record granularity (~milliseconds in practice).
- **Backup deduplication.** v1 chains incremental backups via WAL
  segments — that's already deduplicated. Cross-backup chunked dedup
  needs a content-defined chunker; defer.

---

## 3. Options considered

### Option A — Tarball-of-data-dir, scheduled

Periodically: take a snapshot, then `tar zstd` the snapshot file +
every WAL segment after the previous snapshot's WAL seq, upload to S3.
Restore: download the latest manifest's components, point a fresh
`NEBULA_DATA_DIR` at them, boot.

**Pros**
- Cheapest possible build. Reuses snapshot + WAL replay code paths.
- Tarballs are debuggable — you can untar one to inspect.
- Restore is "boot a new pod with this dir" — same recovery path as
  a normal restart, exhaustively tested.

**Cons**
- Naive PITR: restore lands at "snapshot + every WAL after it,"
  not at a chosen timestamp. Need a WAL replay step that stops at
  a target.
- Each backup ships every byte since the last snapshot, even if
  only one record changed. Snapshot frequency dictates churn.

### Option B — Per-record CDC stream

Subscribe to the WAL via the existing `Wal::subscribe` API and
upload every record to S3 individually. Restore: apply records in
order.

**Pros**
- True continuous backup; PITR is exact.
- Network-efficient when writes are sparse.

**Cons**
- 30-50× the S3 PUT operations of option A — you're paying for
  every write twice.
- Cold restore takes O(history) to replay; full snapshots are
  needed anyway for fast recovery.
- Effectively reinventing the WAL on top of S3.

### Option C — Hybrid: scheduled snapshot + WAL trickle

Scheduled "base" snapshots every N hours. Between snapshots, every
new WAL segment as it rolls is uploaded to a separate S3 prefix.
Manifest links them. Restore: pick a target time, download the
latest base before that time + every WAL segment up to it, replay
to the target time.

**Pros**
- Real PITR (segment-rotation granularity, ~minutes by default).
- Bounded restore time: you replay at most N hours of WAL.
- Clear cost model: PUTs scale with WAL rotation rate, not write rate.
- Fits NebulaDB's existing WAL segmentation perfectly.

**Cons**
- Two upload paths (scheduled + on-rotate). More moving parts than
  option A.
- Manifest complexity grows linearly with WAL segments.

### Recommendation: **Option C**, with v1 shipping the scheduled half
and v1.1 adding the WAL trickle.

This is exactly the path major databases took: pg_basebackup +
WAL archive, MySQL mysqldump + binlog, Redis RDB + AOF. It works
because it composes existing primitives, and the upgrade from "v1
scheduled-only" to "v1.1 with PITR" is purely additive.

---

## 4. Architecture (v1)

```
┌─────────────────────────────────────────────────────────────┐
│  nebula-server (leader pod, region-local)                   │
│                                                              │
│  ┌─────────────┐                                            │
│  │ /admin/     │ ── trigger ──┐                             │
│  │ backup      │              │                             │
│  └─────────────┘              ▼                             │
│                       ┌──────────────────┐                  │
│                       │ nebula-backup    │                  │
│                       │ ─ snapshot()     │                  │
│                       │ ─ tar+zstd       │                  │
│                       │ ─ sha256         │                  │
│                       │ ─ S3 multipart   │                  │
│                       │ ─ manifest.json  │                  │
│                       └──────────────────┘                  │
└─────────────────────────────────────────────────────────────┘
                                │
                                ▼
                         ┌──────────────┐
                         │ S3-compat    │
                         │ bucket/      │
                         │  cluster/    │
                         │   <id>/      │
                         │    manifest  │
                         │    base.tzst │
                         │    wal-NN... │
                         └──────────────┘
```

### S3 layout

```
s3://<bucket>/
  <cluster_name>/
    backups/
      <backup_id>/                     # ULID, sortable by time
        manifest.json                   # canonical inventory
        base.tar.zst                    # snapshot + base WAL bundle
        SHA256SUMS                      # one line per artifact
    wal/                                # v1.1 — WAL archive (trickle)
      000000NN.nwal.zst
```

### `manifest.json`

```json
{
  "backup_id": "01HV6...",
  "cluster_name": "nebula-prod",
  "region": "us-east-1",
  "version": "0.1.1",
  "git_commit": "abc1234",
  "kind": "full",
  "started_at": "2026-05-05T10:00:00Z",
  "completed_at": "2026-05-05T10:01:23Z",
  "embedder": {
    "model": "text-embedding-3-small",
    "dim": 1536
  },
  "wal_range": {
    "start_seq": 0,
    "end_seq": 12,
    "end_byte_offset": 4096123
  },
  "artifacts": [
    {
      "name": "base.tar.zst",
      "size_bytes": 145234567,
      "sha256": "..."
    }
  ],
  "doc_count": 4_521_337,
  "bucket_count": 18
}
```

The manifest is the **commit point**. Until `manifest.json` is
uploaded last, the backup is invalid (incomplete). Restore reads the
manifest first and refuses if any artifact's sha256 doesn't match.

### Why tar + zstd

- `tar` lets us bundle the snapshot file + every WAL segment in a
  single object — fewer S3 PUTs, easier deletion.
- `zstd` is fast (compression ~500 MB/s, decompression ~1.5 GB/s on a
  modern laptop) and gets ~3-4× on vector+text data.
- Streaming-friendly: we can pipe `tar -> zstd -> multipart upload`
  without materializing on local disk.

### Multipart upload

Backups can be tens of GB. We use S3 multipart uploads with 16 MiB
parts so:
- Failure mid-upload doesn't waste the whole transfer.
- Throughput parallelizes across parts.
- The whole upload aborts cleanly via `AbortMultipartUpload` on
  cancel, rather than leaving a half-finished object.

---

## 5. Restore architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Operator (or operator-installed CronJob, or `nebulactl`)   │
│                                                              │
│  POST /admin/restore { backup_id, target_time? }            │
│        │                                                     │
│        ▼                                                     │
│  pre-flight:                                                 │
│    ─ refuse if NEBULA_DATA_DIR has data                      │
│    ─ refuse if embedder model/dim differ from manifest       │
│    ─ refuse if version is more than one major behind         │
│        │                                                     │
│        ▼                                                     │
│  download:  base.tar.zst → /var/lib/nebuladb                 │
│  verify:    sha256 of every artifact vs SHA256SUMS           │
│  unpack:    tar -xJf into NEBULA_DATA_DIR                    │
│        │                                                     │
│        ▼                                                     │
│  pgbench-style "first boot" replay:                          │
│    open WAL → roll forward to (a) end_seq for full restore,  │
│                                  or (b) target_time for PITR │
└─────────────────────────────────────────────────────────────┘
```

Restore intentionally happens **on a fresh node**, not in-place. A
pod with existing state refuses to restore — silent overwrite is
exactly the kind of data-loss surprise we never want to inflict on
operators. The operator's restore CR creates a new cluster scoped
for restore, then renames it.

### PITR

WAL replay already supports stopping at a cursor. We add an optional
`target_time: ISO-8601` parameter; the WAL records carry no timestamp
today (a real gap), so v1 PITR resolves at WAL-segment-rotation
granularity — same as MySQL binlog without `--stop-position`. v1.1
adds per-record timestamps to the WAL format.

---

## 6. CRDs + operator behavior

### `NebulaBackupSchedule` (new, namespaced)

```yaml
apiVersion: nebula.nebuladb.io/v1alpha1
kind: NebulaBackupSchedule
metadata:
  name: nightly-prod
  namespace: nebuladb
spec:
  clusterRef: nebula-prod
  schedule: "0 2 * * *"        # cron, region-local time
  retention:
    keepLast: 14                # keep N most recent
    keepDailyDays: 30
    keepWeeklyWeeks: 12
    keepMonthlyMonths: 12
  storage:
    s3:
      bucket: my-nebuladb-backups
      region: us-east-1
      prefix: nebula-prod
      endpoint: ""              # blank = AWS; set for MinIO/R2/etc.
      credentialsSecretRef:
        name: nebuladb-s3
        accessKeyKey: access_key
        secretKeyKey: secret_key
  throttle:
    maxBandwidthMbps: 100
status:
  lastBackupId: "01HV6..."
  lastBackupAt: "2026-05-05T02:01:23Z"
  nextScheduledAt: "2026-05-06T02:00:00Z"
  conditions: []
```

The operator's controller:
1. Watches `NebulaBackupSchedule` CRs.
2. Creates a `CronJob` per schedule that POSTs to the cluster's
   leader pod's `/admin/backup` endpoint.
3. Watches resulting `NebulaBackup` CRs (next).
4. Applies retention policy by deleting old `NebulaBackup` CRs +
   their S3 objects.

### `NebulaBackup` (new, namespaced; created by the schedule)

One CR per backup attempt. Survives the CronJob's pod for status
visibility:

```yaml
apiVersion: nebula.nebuladb.io/v1alpha1
kind: NebulaBackup
metadata:
  name: nebula-prod-20260505-020000
  namespace: nebuladb
  ownerReferences:
    - kind: NebulaBackupSchedule
      name: nightly-prod
spec:
  clusterRef: nebula-prod
  scheduleRef: nightly-prod
status:
  phase: Running|Completed|Failed
  backupId: "01HV6..."
  startedAt: ...
  completedAt: ...
  manifestURL: "s3://.../manifest.json"
  sizeBytes: 145234567
  durationSeconds: 83
  conditions: []
```

### `NebulaRestore` (new, namespaced)

```yaml
apiVersion: nebula.nebuladb.io/v1alpha1
kind: NebulaRestore
metadata:
  name: restore-prod-to-staging
spec:
  # Source backup (any cluster's backup is restorable to any cluster
  # if version + embedder match — we validate at apply time).
  sourceBucket: my-nebuladb-backups
  sourceBackupId: "01HV6..."
  # Target cluster — must exist and be empty.
  targetClusterRef: nebula-staging
  # Optional: stop replay at this WAL cursor or time.
  pitr:
    targetTime: "2026-05-04T15:30:00Z"
status:
  phase: Pending|Downloading|Verifying|Unpacking|Replaying|Completed|Failed
  bytesDownloaded: 0
  conditions: []
```

---

## 7. REST API

All endpoints are admin-protected (existing auth middleware).

| Endpoint                          | Method | Body / Response                                     |
|-----------------------------------|--------|------------------------------------------------------|
| `/admin/backup`                   | POST   | `{ kind: "full", storage: { ... } }` → `{ backup_id }` |
| `/admin/backup/:id`               | GET    | manifest + status                                   |
| `/admin/backup/:id/abort`         | POST   | aborts in-progress upload                           |
| `/admin/backups`                  | GET    | list of recent local backups (ring buffer of last 50) |
| `/admin/restore`                  | POST   | `{ backup_id, source: { ... }, pitr?: ... }` → `{ restore_id }` |
| `/admin/restore/:id`              | GET    | progress: phase, bytes_downloaded, eta              |

The leader does the work. Followers reject `POST /admin/backup` with
`409 read_only_follower` — same guard as other writes.

---

## 8. CLI (`nebulactl`)

A thin Rust binary wrapping the REST API. Lives in a new
`crates/nebulactl` (similar shape to `nebula-client`):

```bash
nebulactl backup start --cluster nebula-prod
nebulactl backup list
nebulactl backup show 01HV6...
nebulactl restore start --backup-id 01HV6... --target nebula-staging
nebulactl restore show <restore_id> --watch
```

Used by the operator's CronJob (which runs the CLI inside a tiny
container image), and by humans reaching into a single pod.

---

## 9. Observability

Prometheus metrics added in `nebula-backup`:

| Metric                                    | Type    |
|-------------------------------------------|---------|
| `nebula_backup_attempts_total`            | counter |
| `nebula_backup_failures_total`            | counter |
| `nebula_backup_duration_seconds`          | histogram |
| `nebula_backup_size_bytes`                | gauge   |
| `nebula_backup_throughput_bytes_per_sec`  | gauge   |
| `nebula_restore_attempts_total`           | counter |
| `nebula_restore_failures_total`           | counter |
| `nebula_restore_duration_seconds`         | histogram |
| `nebula_wal_archived_segments_total`      | counter (v1.1) |

Logs use the existing `LogBus`, so the SSE log endpoint shows backup
progress in the admin UI without any extra wiring.

---

## 10. Security

- **Credentials**: never on the CR. The operator reads
  `credentialsSecretRef` and injects S3 creds as env vars on the
  CronJob's pod. Server-to-S3 traffic is via the AWS SDK which uses
  TLS by default.
- **At-rest encryption**: documented as the responsibility of the S3
  bucket (`s3:x-amz-server-side-encryption: aws:kms`). The backup
  manifest carries the bucket's encryption settings as metadata so a
  later audit can verify.
- **Restore validation**: refuse to restore if the manifest's
  `embedder.model` or `embedder.dim` differs from the target's. Cross-
  embedder restore would silently corrupt search results; better to
  error than to debug "why are my similarity scores nonsense" three
  weeks later.

---

## 11. Failure modes

| Failure                              | Behavior                                                      |
|--------------------------------------|---------------------------------------------------------------|
| S3 unreachable mid-upload            | Multipart abort; retry with backoff; mark backup `Failed`     |
| Pod crashes mid-backup               | Multipart upload garbage-collected by S3 lifecycle rule       |
| Manifest written before all parts    | Atomic: manifest is the LAST PUT. Until then the backup is invisible |
| Disk full during snapshot            | Snapshot already returns `IndexError::Invalid` cleanly        |
| Corrupted artifact at restore        | sha256 mismatch → refuse, leave target untouched              |
| Version skew (older binary, newer manifest) | Refuse with `version_too_old`; operator must use the matching image |
| Embedder model mismatch              | Refuse with `embedder_mismatch`                               |

---

## 12. Rollout plan

### Phase 1 — engine (~1 week)
- [ ] New crate `nebula-backup`
  - Trait `BackupTarget` with one impl: `S3Target` using `aws-sdk-s3`
  - `Backup::run()` orchestrates snapshot → tar → zstd → multipart upload
  - `Restore::run()` orchestrates download → verify → unpack → replay
- [ ] `WalCursor`-aware tar packaging in `nebula-wal`: helper to
  enumerate "every segment from cursor X to current"
- [ ] Adapter in `nebula-index` so backup can drive a snapshot
  without going through the HTTP layer

### Phase 2 — REST + CLI (~3 days)
- [ ] `/admin/backup`, `/admin/restore`, list endpoints in
  `nebula-server`
- [ ] New `crates/nebulactl` Rust binary
- [ ] Tests: golden-path backup → restore on a tmpdir-backed S3
  emulator (`s3-server` or `minio`)

### Phase 3 — operator CRDs (~3 days)
- [ ] `NebulaBackupSchedule`, `NebulaBackup`, `NebulaRestore` types
- [ ] Schedule controller: maintains a CronJob per schedule
- [ ] Backup controller: parses CronJob output → CR status
- [ ] Restore controller: drives a NebulaCluster lifecycle into
  restore mode

### Phase 4 — retention + observability (~2 days)
- [ ] Retention policy (`keepLast`, `keepDaily/Weekly/Monthly`) —
  computes the keep set from CR list, deletes both the CR and the
  S3 objects
- [ ] Prometheus metrics + Grafana panel in
  `deploy/grafana/dashboards/nebula-backup.json`
- [ ] Gatus tile for "last successful backup < 25h ago"

### Phase 5 (v1.1) — WAL trickle + true PITR
- [ ] On WAL segment rotate, async-upload the closed segment
- [ ] Per-record timestamps in WAL records (additive — old replays
  ignore the field)
- [ ] PITR resolves at record granularity instead of segment

---

## 13. Risks

| Risk                                        | Likelihood | Mitigation |
|---------------------------------------------|-----------|------------|
| Backup bandwidth saturates the leader pod   | Medium    | Throttle on the upload side via `tokio::io` rate-limiter; CR exposes `maxBandwidthMbps` |
| S3 costs balloon for chatty clusters        | Low       | `keepLast` retention; v1.1 WAL trickle dedupes by segment |
| Embedder upgrade silently breaks PITR       | Medium    | Manifest carries embedder model+dim; restore refuses on mismatch |
| Ops accidentally restore over live data     | Medium    | Restore only into an empty `NEBULA_DATA_DIR`; CR refuses targeting a non-empty cluster |
| Manifest in S3 but artifacts missing        | Low       | Manifest is the last write; sha256-verify all artifacts on restore |
| Cross-region restore with stale home_epoch  | Medium    | Manifest carries every bucket's home_epoch; restore writes them back into seed docs |

---

## 14. Open questions

1. **Resumable backups across pod restarts.** v1 has no resume — a
   crashed backup restarts from scratch. Adding resume requires
   persisting the multipart upload ID locally, which is ~50 lines and
   probably worth it. Defer or include?

2. **Per-bucket backup.** Gap #4 already gives us bucket export. We
   could expose `NebulaBackup.spec.buckets[]` as a filter. Easy to
   add; question is whether anyone wants it (typical use is "back up
   the whole cluster"). Lean toward not adding until asked.

3. **Cross-cluster restore validation.** When restoring a backup taken
   on cluster A into cluster B with the same embedder + version, do we
   require the cluster names to match? Strict (refuse) is safer;
   permissive (warn) is more flexible. **Lean strict**, with a
   `--force-cross-cluster` escape hatch.

4. **Backup of follower-mode pods.** Followers have a partial WAL
   (only what they've received from the leader). v1 says: only leader
   pods take backups. Operator targets the leader service.

5. **`nebulactl` distribution.** Static binary release alongside the
   server image, or `kubectl exec` into a server pod? **Lean toward**
   shipping the static binary — operators expect a CLI on their
   workstation, not in the cluster.

---

## 15. Decision

**Recommendation: ship Option C, phase 1 first.**

Strengths:
- Composes existing primitives (snapshot, WAL, single S3 client).
- Delivers real value at every phase boundary — phase 1 alone is a
  usable backup product, phase 5 turns it into a PITR product.
- No new on-disk formats; restore goes through the same recovery code
  paths as a normal restart.

Alternatives rejected:
- **Per-record CDC**: cost + complexity not justified for v1.
- **Native Azure/GCS clients**: covered by S3-compat; defer.
- **Snapshot-only (no WAL bundling)**: loses every write since the
  last snapshot, which is a poor RPO story.

Next step: agree on the open questions in §14, then start phase 1.

---

## Appendix: relationship to design 0001 (multi-region)

- Backup manifests record each bucket's `home_region` + `home_epoch`.
  A cross-region restore re-hydrates the seed docs with these values
  so the new cluster knows who owns what.
- A backup taken in region A can be restored in region B; the restore
  is independent of the cross-region tail.
- Disaster recovery flow: every region runs its own
  `NebulaBackupSchedule`; a regional outage failovers `NebulaCluster`
  topology (existing operator behavior), and an irrecoverable region
  loss is solved by `NebulaRestore` from the most recent backup of the
  affected region.
