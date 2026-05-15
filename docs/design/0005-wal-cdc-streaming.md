# Design 0005: WAL + CDC streaming as the nervous system

- **Status**: Draft (vision document — needs decisions before implementation)
- **Author**: Claude + @bwalia
- **Created**: 2026-05-15
- **Inspired by**: PostgreSQL WAL, CockroachDB Raft logs, Couchbase DCP/XDCR, Kafka streams

---

## 0. Why this exists

If WAL is just "the durability log," it will be reinvented six times — once for replication, once for backup, once for the AI indexing pipeline, once for time-travel, once for external integrations, once for observability. We've already got two of those: `nebula-wal` for durability and the new Raft log in `nebula-raft` for consensus.

This RFC proposes treating the WAL as a **change stream** that subscribers consume — replication, backup, CDC integrations, AI pipelines, and external apps all read from the same authoritative log. The motivation is the same as PostgreSQL's logical decoding and Couchbase's DCP: one stream, many consumers, deterministic replay.

The key claim — "WAL is the nervous system, not just durability" — is the only thing this doc advocates for that the codebase doesn't already do.

---

## 0.1 Status of the "next steps" from the original brief

The original brief closed with a 5-item "build next" list. **Three of those items already exist on `main`.** Calling that out up front so a reviewer doesn't kick off work that's been done:

| Original brief item | Status | Where it lives |
|---|---|---|
| WAL writer implementation | ✅ **Done** | `crates/nebula-wal/src/lib.rs` — segmented append-only log, `NEBWAL01` magic, CRC32, fsync-on-append, 64 MiB rotation, torn-tail recovery (~813 lines). Raft mode adds a parallel writer at `crates/nebula-raft/src/log.rs` with `NEBRAF01` magic + term/index slots (~1000 lines). |
| CDC gRPC streaming server | 🟨 **Half done** | `nebula-grpc::ReplicationService::TailWal` ships records over a gRPC server-streaming RPC today. **Missing**: typed `CdcEvent` shape and a subscriber-filter language (`bucket=X`, `op_type IN (...)`). Covered by §2.2 + §5.4 of this RFC. |
| Replication apply engine | ✅ **Done** | Two of them, depending on mode. Standalone follower replication uses `TextIndex::apply_wal_record` (`crates/nebula-index/src/lib.rs`). Raft mode uses `NebulaStateMachine::apply_entries` (`crates/nebula-raft/src/state_machine.rs`) — same entrypoint as standalone WAL replay, which is the determinism property the cross-peer test locks in. |
| Snapshot + WAL recovery | ✅ **Done** | `crates/nebula-index/src/durability.rs` (`write_snapshot_streaming`, `load_latest_snapshot`) + `TextIndex::open_persistent` driving the load-snapshot-then-replay-WAL boot cycle. Streaming-friendly per PR #25. |
| Vector-aware CDC consumer | 🟥 **Missing** | No actual consumer today. The infrastructure exists — `WalRecord::UpsertDocument` carries chunks + resolved vectors, `SubscriberHub` fans out, `ReplicationService::TailWal` ships them — but there is no concrete consumer that mirrors writes into an external vector store, fires a webhook on embedding events, or pumps to Kafka. §5.5 (webhook plugin) is the first one this RFC proposes building. |

**Net: 3 of 5 already done, 1 half done, 1 missing.** The 1 missing + the half-done piece are exactly what §5.2–§5.5 propose.

The honest gap *none* of those 5 items name: **no 3-node soak**. Eleven raft PRs are stacked unmerged with zero runtime against a real cluster. Phase 2.6 in `docs/active-active-rollout-prompt.md` is the work that gates production confidence in any of this.

---

## 1. What already exists (and shouldn't be re-built)

Honest inventory before proposing anything:

| Concern | Today's implementation | RFC interaction |
|---|---|---|
| Append-only segmented log | `nebula-wal` (`crates/nebula-wal/src/lib.rs`) — `NEBWAL01` magic, 64 MiB segments, CRC32, torn-tail recovery | **Reuse**, don't reinvent |
| Raft consensus log | `nebula-raft` (`crates/nebula-raft/src/log.rs`) — `NEBRAF01` magic, term/index slots | **Reuse for raft mode** |
| Snapshot + WAL replay recovery | `crates/nebula-index/src/durability.rs` | **Reuse** |
| Per-record schema | `WalRecord { UpsertText, UpsertDocument, Delete, DeleteDocument, EmptyBucket }` | **Extend** with `lsn` + `tx_id` slots when needed; do NOT redefine in protobuf |
| Replication subscriber hub | `crates/nebula-wal/src/subscriber.rs` (`SubscriberHub`, ack tracking) | **Reuse** as the CDC fan-out primitive |
| gRPC tail | `nebula-grpc::ReplicationService::TailWal` | **Generalize** into a CDC stream |
| Cross-region tail | `nebula-grpc::CrossRegionReplicationService` | **Already shaped like CDC** |
| Snapshot streaming | PR #25 — bounded-memory serialize | **Reuse** |
| Backup engine | `nebula-backup` | **Already a CDC consumer pattern**, just not branded that way |

The proposed system is **80% rebranding + small extensions to existing code**, not a greenfield rewrite. If this RFC ever produces a "delete and rewrite nebula-wal" PR, that's a sign of scope creep.

---

## 2. The four delta proposals

What this RFC actually adds on top of what's there:

### 2.1 Add `lsn` + `tx_id` to `WalRecord`

Today `WalRecord` is purely "what mutation happened." It has no monotone log sequence number visible to consumers; the WAL's framing carries an implicit byte offset, but nothing inside the record names "I am the 7th record in this log."

LSN (log sequence number, monotone u64 across the log lifetime) lets a consumer track "I've read up through LSN N" with a single integer instead of `(segment_seq, byte_offset)`. The Raft log already has `(term, index)` which serves this role; standalone WAL doesn't.

`tx_id` is groundwork for the cross-document transactions the codebase doesn't have yet (`USE_CASES.md` lists this as not-supported). Add the field now and leave it `0` for the single-record-is-a-commit world we live in.

**Wire-format impact:** appending two `u64` fields to `WalRecord`'s body changes the bincode encoding. We have a discipline for this — see `crates/nebula-wal/src/lib.rs:91` ("Bincode's default is compact and fast; it does *not* tolerate enum-variant reordering, so new variants MUST be appended"). New **fields** are different from new variants and would break old readers. The clean path is a new `WalRecord::V2 { lsn, tx_id, inner: WalRecordV1 }` wrapper variant or a header/body split. This is a real wire-format change and gets its own implementation ADR.

### 2.2 Generalize `ReplicationService::TailWal` into a typed CDC stream

The existing replication tail emits `WalRecord` blobs to a single follower. CDC needs:

- **Subscriber filtering**: subscribe to `bucket=X`, `op_type IN (Insert,Delete)`, etc.
- **Multiple independent consumers** with separate cursors (already supported by `SubscriberHub::min_ack`).
- **Resumable from a checkpoint LSN** (already supported via `WalCursor`).

The data structure exists. The missing piece is a higher-level `CdcEvent` shape that names the operation explicitly (vs. clients pattern-matching `WalRecord` variants). Today's `WalRecord` is *almost* a CDC event already — the proposed `CdcEvent { op, key, old_value, new_value }` shape is mostly a rename + a subscriber-filter language.

**Don't add a parallel protobuf schema for CDC.** The bincode-in-bytes pattern from `nebula-raft/proto/raft.proto` (ADR 0004) applies here: the CDC envelope is a bincode-encoded `WalRecord` plus the LSN; subscribers run the same nebuladb binary.

### 2.3 Logical decoding plugins (Kafka / webhooks)

This is genuinely new. Today there's no integration layer for "ship every write to Kafka" or "fire a webhook on delete." Per the RFC text, useful for:

- External analytics pipelines (mirror to Kafka, ETL elsewhere)
- App webhooks ("notify Slack when a doc is deleted from `incidents`")
- Search-side AI indexing that lives outside nebuladb

**Honest scope:** this is a new crate (`nebula-cdc-plugins`) plus per-plugin integrations. None of it is on the critical path for any existing user. **Defer until a real customer asks.**

### 2.4 Time-travel queries via WAL replay

"Query past state at time T" by replaying the WAL up to a target LSN against a base snapshot. Powerful, but:

- Requires retention beyond what compaction allows today (compaction trims segments older than the newest snapshot).
- Requires either a **read-only replica per query** (expensive) or a **pure-functional replay** that doesn't mutate the live state machine (significant new code).
- The same outcome — "what was the state at T?" — is achievable today by restoring a backup from time T to a side cluster.

**Recommendation: don't build this.** The use case is real but the cost-benefit lands closer to "use the backup system" than "ship time travel."

---

## 3. What the original document gets right

Things from the RFC text that are correct as stated and worth preserving in the implementation:

- **Per-shard WAL** to avoid a global write bottleneck. NebulaDB doesn't shard today (`USE_CASES.md` lists automatic sharding as not supported), but if/when it does, per-shard WAL is the right shape.
- **fsync-before-ack** for durability. Already enforced via `WalConfig::fsync_on_append: true`.
- **Subscriber checkpoints** as `(consumer_id, last_lsn)` pairs. Today's `SubscriberHub` tracks ack cursors but in-process only; a persistent checkpoint store would let a Kafka consumer reboot without missing events.
- **gRPC streaming over a custom binary protocol.** Already chosen for `ReplicationService`. CDC should match.
- **Vector-aware CDC** — chunks + embeddings appear in `WalRecord::UpsertDocument` already. Consumers see the resolved vectors. Good as is.

---

## 4. What the original document gets wrong (or duplicates)

Honest list of where the RFC text would lead someone to redo work:

- **"WAL Record Design"** with a fresh `pub struct WalRecord` is **incompatible** with the existing `WalRecord` enum in `crates/nebula-wal/src/lib.rs`. A reviewer reading the RFC verbatim would write a parallel type. Implementation must extend the existing enum, not redefine.
- **"Operation Types: Insert / Update / Delete / VectorInsert / VectorDelete / EmbeddingCreate / ChunkCreate / SchemaChange"** — this is already covered by the existing variants (`UpsertText`, `UpsertDocument`, `Delete`, `DeleteDocument`, `EmptyBucket`). `SchemaChange` doesn't apply (NebulaDB has no schema). The new categorization isn't load-bearing.
- **"Streaming Protocol: gRPC streaming"** — done. `ReplicationService::TailWal` exists.
- **Recovery flow ("Read WAL → Replay committed ops → Rebuild memtables → Recover indexes")** — done. `TextIndex::open_persistent`.
- **Backup integration ("Base Snapshot + WAL replay")** — done. `nebula-backup` + RFC 0002.
- **Per-shard WAL directories** — irrelevant until sharding exists. Premature.
- **Transaction commit records (`BeginTx / WriteOps / CommitTx`)** — there's no transaction concept to commit. Adding the framing without a transaction model adds wire format weight for a future feature that may never need it.

---

## 5. Concrete delivery plan (if approved)

Phasing, with explicit dependencies on what's already merged:

| Phase | Work | Builds on | Estimate |
|---|---|---|---|
| **5.1** | Add LSN to existing `WalRecord` via a `V2` wrapper variant (per §2.1). Migrator reads V1 segments and rewrites as V2 on first compact. | nebula-wal | 1 week |
| **5.2** | Define `CdcEvent` as a thin typed view over `WalRecord` + LSN. Add a subscriber-filter language (`bucket=X`, `op_type IN (...)`). | §5.1, `SubscriberHub` | 1 week |
| **5.3** | Persistent subscriber checkpoint store. Today's hub is in-process only; persist `(consumer_id, last_lsn)` so a restarted consumer doesn't double-replay. | §5.2 | 3 days |
| **5.4** | gRPC `CdcService::Subscribe(filter) -> stream<CdcEvent>`. Generalizes the existing `ReplicationService::TailWal` shape. | §5.2 | 1 week |
| **5.5** | First plugin: webhook fan-out. Validates the plugin trait shape against a real consumer before we add Kafka/etc. | §5.4 | 1 week |
| **5.6** | (Optional) Kafka sink plugin. | §5.5 | 1 week |
| **5.7** | (Deferred) Time-travel queries. See §2.4 — recommend not doing this. | §5.1 | not estimated |

**Total realistic delivery: ~5 weeks of focused work for §5.1–§5.5.** Phases 5.6 and 5.7 are gated on actual customer demand.

---

## 6. What changes vs. what's already merged

Re-stated for the reviewer who's going to ask "do we already do this?":

```
Existing                      | Delta this RFC adds
------------------------------|------------------------------
nebula-wal segments           | + LSN field on records (§5.1)
SubscriberHub fan-out         | + persistent checkpoint store (§5.3)
ReplicationService::TailWal   | + filter language + CdcEvent rename (§5.2, §5.4)
WalRecord enum (5 variants)   | unchanged shape; LSN added via V2 wrapper
Snapshot + WAL replay         | unchanged
nebula-backup                 | unchanged
Raft log + state machine      | unchanged
gRPC bincode-in-bytes pattern | reused for CdcService
```

Net new code: ~2000–3000 lines across §5.1–§5.5. Net new wire format: one new enum variant on `WalRecord`, one new gRPC service, one persistent file format for checkpoints.

---

## 7. Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| Wire-format change for LSN breaks existing on-disk WAL segments at `192.168.1.193` | Medium | V2 wrapper variant + migrator on first compact (§5.1). The byte-identity guard test in `crates/nebula-vector/src/hnsw.rs::tests::streaming_serialize_matches_owned_serialize` is the model — write the equivalent for V1↔V2 round-trip. |
| Persistent subscriber checkpoints leak Raft-vs-standalone abstraction | Low | Checkpoints are LSN-keyed; the LSN is the same number whether the log is `nebula-wal` or `nebula-raft`. Both modes already monotone-track their log position. |
| CDC consumer back-pressure starves WAL compaction | Medium | Already mitigated by `Wal::compact` clamping to `min(all_follower_acks)` (`PROBLEMS_SOLVED.md` problem #5). The same discipline applies to CDC consumer acks. |
| Plugin author writes a slow webhook that wedges the leader | High (if not designed for) | Plugins consume from the **subscriber hub**, not inline with `wal.append`. A slow webhook fans out to a per-consumer queue; if the queue fills, that consumer stalls, not the leader. |

---

## 8. What this RFC is NOT

- **Not a vote for transactions.** `tx_id` is a forward-compatible field, not a commitment to build cross-document transactions. RFC 0001 / `USE_CASES.md` list this as not-supported.
- **Not a Raft replacement.** `nebula-raft` and `nebula-wal` continue to coexist. Raft mode uses the Raft log; standalone uses `nebula-wal`. CDC streams from whichever is active.
- **Not Kafka.** We are not building a general-purpose streaming platform. CDC here is "tail your own database's writes," same scope as PostgreSQL logical decoding.
- **Not a sharding proposal.** Per-shard WAL is mentioned as future-compatible; sharding itself is out of scope.

---

## 9. Decision needed

Before any code:

1. **Approve the §5.1 wire-format change** — V2 wrapper for LSN. This is the only structurally invasive piece. Everything else builds on top.
2. **Decide §2.3 timing** — webhook plugin (§5.5) on the critical path, or wait for a customer ask?
3. **Confirm §2.4 punted** — time-travel queries deferred indefinitely; backup-to-side-cluster is the substitute.

Without (1), nothing else lands. (2) and (3) are scheduling.

---

## Appendix: relationship to existing RFCs and ADRs

- **RFC 0001 (multi-region active-active):** the cross-region tail this RFC generalizes is already RFC 0001's design. CDC is a strict superset — same wire shape, broader audience.
- **RFC 0002 (backup/restore):** backup engine is already a CDC consumer in spirit. Formalizing this lets backup share the checkpoint store from §5.3.
- **RFC 0003 (write availability) / ADR 0004 (Raft):** CDC streams from the Raft log when raft mode is on, from `nebula-wal` otherwise. The LSN field is the shared abstraction.
- **PROBLEMS_SOLVED.md problems #5 ("slow follower breaks WAL compaction") and #6 ("don't know if replica is keeping up"):** both are CDC concerns rebranded. Already partially solved.
