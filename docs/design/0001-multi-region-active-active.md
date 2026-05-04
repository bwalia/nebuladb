# Design 0001: Multi-Region Active-Active Writes

- **Status**: Draft
- **Author**: Claude + @bwalia
- **Created**: 2026-05-04
- **Tracks**: Gap #3 from the Kubernetes operator follow-up
- **Depends on**: gap #4 (bucket export/import, landed in 0.1.1)
- **Supersedes**: the "cross-region active-active" row in `USE_CASES.md`

---

## 1. Problem

NebulaDB today is **single-writer**: one leader per cluster, zero or more
read-only followers tailing the leader's WAL over gRPC. This is fine for
single-region deployments but forces every writer in the world to pay a
transatlantic round trip against one leader.

The Kubernetes operator's `NebulaCluster` CRD accepts a `regions[]` field
that provisions an independent cluster per region — but those clusters do
not talk to each other. A write in `us-east-1` is invisible in `us-west-2`.
That's a degraded product.

We want: **writes are accepted in any region, converge everywhere, and
survive a full region outage.**

---

## 2. Constraints and non-goals

### Hard constraints

1. **HNSW is graph-mutating.** Two concurrent inserts of the same vector
   in two regions are not trivially mergeable — the graph structure
   depends on insertion order. We cannot "merge two HNSWs" in a CRDT
   sense; the best we can do is treat the document layer as the source
   of truth and rebuild the graph from reconciled documents.

2. **Embedder determinism is not guaranteed.** `MockEmbedder` is
   deterministic; OpenAI's `text-embedding-3-small` is *mostly*
   deterministic but versioning is a moving target. Protocols must
   transport raw vectors (already the case in `ExportedDoc`).

3. **Operator must not become a router.** The operator reconciles desired
   state. A per-request data plane sitting inside the operator loop would
   be a misuse of the pattern. Routing belongs in a sidecar or in the
   client SDK.

4. **We keep backwards compatibility.** `NEBULA_NODE_ROLE=standalone`
   must keep working unchanged. Active-active is opt-in via CRD spec.

### Non-goals for v1

- **Global serializability.** We accept eventual consistency with
  bounded lag. Most AI/RAG workloads are LWW-tolerant.
- **Vector-level merge.** If two regions insert the same doc id with
  different text at the same millisecond, last-writer-wins (by Lamport
  timestamp) — not a three-way text merge.
- **Sync writes.** Every write is async-replicated; the write response
  returns as soon as the local region has durably accepted it.

---

## 3. Options considered

### Option A — Gateway router with per-region leaders

Add a new `nebula-router` crate: a stateless L7 proxy that inspects the
bucket name (or a header) and forwards the write to its "home" region.
Reads hit the nearest region. Each region is its own independent cluster
running leader+followers as today.

**Pros**
- Minimal NebulaDB changes. The router is a new component, not a change
  to the storage engine.
- Conflict resolution is trivial: one home region per bucket, no
  concurrent writes on the same key across regions.
- Drops in front of existing clients without API changes.

**Cons**
- **Not actually active-active from the user's POV.** Writes to bucket
  `catalog` always go to its home region; a regional outage means every
  client worldwide can't write to `catalog` until failover.
- Router becomes a new SPOF unless we deploy multiple.
- Home-region assignment becomes a schema decision — hard to migrate
  buckets between regions after the fact (mitigated by gap #4 swap).
- Doesn't solve "writes are accepted in any region" — it just hides the
  home-region constraint behind a proxy.

### Option B — Vector-level LWW CRDT

Treat each `(bucket, external_id)` as a register with a Lamport clock.
Each region writes locally. A background task exchanges write logs across
regions; receiving regions apply records whose Lamport clock exceeds
their local record for the same key.

**Pros**
- True active-active: any region accepts any write.
- Well-understood consistency model.

**Cons**
- **HNSW doesn't cope.** An LWW apply of vector `v2` over `v1` requires
  deleting the old graph node and inserting a new one. Cross-region
  concurrent replaces on the same key produce wildly different graphs
  depending on arrival order. The *final* state is consistent (LWW on
  vector bytes), but intermediate search results are noisy.
- Lamport clocks need a reliable broadcast channel; gossip is extra
  infrastructure we don't have.
- Breaks the WAL's linear log guarantee — applies are no longer strictly
  ordered, so follower-tailing semantics get weaker.

### Option C — Sharded buckets with home-region pinning

Build on the gap #4 export/import primitive. Each bucket has a `home_region`.
Writes to a bucket are only accepted in its home region. Reads can happen
anywhere via async replication. A region outage failovers the home to
another region by running a swap rebalance.

**Pros**
- Composes cleanly with today's leader/follower design.
- Clear failover story: "bucket X's home was us-east-1, now it's us-west-2."
- Already has the move-data primitive (gap #4 swap).

**Cons**
- Same foot-gun as Option A: one home region per bucket means no
  concurrent writes from two regions.
- Doesn't solve "writes are accepted in any region" without per-bucket
  granularity (the client has to know which bucket lives where).

### Option A'/C' — Hybrid (recommended)

Option A's routing layer + Option C's home-region pinning + the gap #4
swap primitive for bucket migration:

1. Each bucket has a `home_region` stored in a metadata document in the
   bucket itself (seed doc already exists — see `NebulaBucket` controller).
2. A lightweight **client-side** router (Rust crate `nebula-client`)
   reads the home-region map and steers writes to the right region.
   Reads prefer the local region.
3. Cross-region WAL replication (new gRPC `CrossRegionReplicationService`)
   tails the home region's WAL into non-home regions so local reads are
   fresh.
4. A `NebulaRegionFailover` CRD rebinds a bucket to a new home region
   by driving a swap rebalance (gap #4) and publishing the new home in
   the seed doc.

This is active-active **at the cluster level**: every region serves all
reads, every region accepts writes for its home buckets, and failover
is a bucket-level operation instead of a DNS-flip.

---

## 4. Proposed architecture (Option A'/C' hybrid)

```
                        ┌───────────────────────────────┐
                        │      nebula-client SDK        │
                        │  reads bucket home-region map │
                        │  from any region's /admin/…   │
                        └──────────┬──────────┬─────────┘
                           writes  │    reads │ (any region)
                      (home region)│          │
        ┌───────────────────────────┴───┐   ┌──┴────────────────────────┐
        │  us-east-1  (home: catalog)   │   │  us-west-2 (home: logs)   │
        │ ┌─────────┐   ┌─────────┐     │   │  ┌─────────┐ ┌─────────┐  │
        │ │ Leader  │──▶│Follower │...  │   │  │ Leader  │▶│Follower │  │
        │ └────┬────┘   └─────────┘     │   │  └────┬────┘ └─────────┘  │
        │      │                        │   │       │                   │
        │   gRPC WAL tail (catalog)     │   │    gRPC WAL tail (logs)   │
        │      ├──────────────────────────────────▶ │                   │
        │      │                        │   │       │                   │
        │      ◀───────────────────────────────────┤ gRPC WAL tail (logs)
        └──────────────────────────────┘    └────────────────────────────┘
```

### New components

1. **`nebula-client`** (new crate): Rust client SDK that resolves a
   bucket to its home region via a cached call to
   `GET /admin/bucket/:b/home-region`. Falls back to a configured default
   region when a bucket has no home set.

2. **`CrossRegionReplicationService`** (new gRPC service): runs in
   every region. Similar to today's `ReplicationService` but filtered —
   a non-home region subscribes only to WAL records for buckets whose
   home is elsewhere. Each region's leader streams its "owned" writes to
   every other region's leader, which applies them with `apply_wal_record`
   into the local index.

3. **`NebulaRegionFailover`** (new CRD, namespaced): declares the intent
   "bucket X, currently homed in region A, should be homed in region B."
   The controller:
   - Verifies region A is actually down (or user-forced).
   - Uses gap #4 swap to ensure region B has the latest bucket state.
   - Updates the bucket's seed doc with `home_region: B`.
   - Stops the inbound cross-region tail for this bucket on region B.
   - Starts an outbound tail from region B to every other region.

### New storage concept: home region

Stored in the bucket seed doc already written by the `NebulaBucket`
controller:

```json
{
  "kind": "nebuladb-operator-seed",
  "bucket": "catalog",
  "home_region": "us-east-1",
  "replicated_to": ["us-west-2", "eu-west-1"],
  "home_epoch": 3
}
```

`home_epoch` is a monotonic counter bumped on every failover. Cross-region
consumers tag incoming records with the expected epoch and reject records
from an earlier epoch — this is the safety net against "old home region
came back and tried to write."

### Write path (region A, bucket owned by A)

1. Client calls `POST /bucket/catalog/doc` on any region A pod.
2. Leader accepts, WAL-appends, responds to client.
3. `ReplicationService` fans the record out to A's local followers
   (existing behavior).
4. `CrossRegionReplicationService` fans the record out to region B and
   region C's leaders, tagged with `home_epoch=3`.
5. Each non-home region's leader `apply_wal_record`s the record into
   its local index. No local WAL append — the record is replayable from
   region A's WAL if we lose region B's state.

### Write path (region A, bucket owned by B)

1. Client SDK sees `home_region: B` in cache, redirects to region B.
2. If the SDK can't reach region B, write fails with
   `429 wrong_home_region`. The SDK may fall back to a buffering write
   that only succeeds after region B is reachable again — or the
   operator can trigger a `NebulaRegionFailover` to promote region A.

### Read path (any region)

Reads always hit the local region. Local index has:
- Every doc in buckets it owns (authoritative).
- Every doc in buckets it doesn't own (eventually consistent, via
  cross-region tail).

`/admin/replication` reports the max cross-region lag per remote home
as part of its status payload. Clients that need strong reads can set a
`If-Match-Home-Epoch` header.

### Failover path

Normal case (region A dies, bucket `catalog` is homed there):
1. Operator detects every pod in region A is unreachable for > 60s.
2. Operator creates a `NebulaRegionFailover` CR for `catalog → us-west-2`.
3. Controller:
   - Reads the last-received `home_epoch` from region B's local state.
   - Bumps `home_epoch` to N+1.
   - Patches region B's bucket seed doc with `home_region: us-west-2,
     home_epoch: N+1`.
   - Starts outbound cross-region tail from B to C.
   - Clients pick up the new home via their next
     `GET /admin/bucket/:b/home-region` refresh (TTL ~30s).

Reconciliation when A comes back:
- A's leader starts up, sees its own seed doc says `home_epoch: N`.
- A polls B and sees `home_epoch: N+1`.
- A enters **demoted** mode: refuses writes on that bucket, subscribes
  as a consumer to B's outbound tail. It catches up via WAL apply.
- Operator may run another `NebulaRegionFailover` to move the home back
  once A is caught up.

---

## 5. Correctness arguments

### Safety: at most one home per bucket at a time

Enforced by the `home_epoch` monotonic counter. Only the region listed in
the seed doc for epoch N accepts writes. A deposed region discovering a
higher epoch will self-demote on the next poll (or be force-demoted by
the operator). Concurrent failover CRs targeting the same bucket race on
the epoch bump in the seed doc — whichever region's CAS wins is the new
home.

**Open question A.** The seed doc is itself a replicated record. If two
regions both try to bump `home_epoch` during a network partition, we
need a tiebreaker. Candidates:
- Operator holds a namespace-scoped lock (easy, relies on etcd).
- Each region writes to its own home-region doc in a
  `__system/home_epoch_votes` bucket, and the one with the majority of
  consumer acks wins.

### Liveness: a bucket's writes eventually converge

Assumes the cross-region tail is live. If region B's tail from region A
stalls, B will show staleness in `/admin/replication`. Operator alerts on
> N MB lag. No automatic demotion on lag alone — demotion requires the
home region to be *unreachable*, not just slow.

### Partition tolerance

During a split-brain (region A up, region B up, cross-region link down):
- Each region continues serving reads on all buckets (from local state).
- Each region accepts writes only for buckets it owns.
- When the link heals, outbound tails resume from the saved cursor.
- No data loss because WALs are durable on both ends.

---

## 6. Operator CRD changes

### `NebulaCluster` additions

```yaml
spec:
  regions:
    - name: us-east-1
      crossRegion:
        enabled: true
        # For each other region, the cross-region tail endpoint.
        peers:
          - name: us-west-2
            grpcURL: "http://nebula-prod-us-west-2-leader.nebuladb.svc:50051"
          - name: eu-west-1
            grpcURL: "http://nebula-prod-eu-west-1-leader.nebuladb.svc:50051"
```

This is descriptive — the cluster knows about its peers but peers also
advertise themselves via `/admin/cluster/nodes`.

### `NebulaBucket` additions

```yaml
spec:
  clusterRef: nebula-prod
  homeRegion: us-east-1       # NEW: where writes must land
  replicatedTo:               # NEW: which regions get the WAL tail
    - us-west-2
    - eu-west-1
```

Defaulting: if unset, `homeRegion` = first region listed in the parent
cluster's `regions[]`, `replicatedTo` = all other regions.

### `NebulaRegionFailover` (new)

```yaml
apiVersion: nebula.nebuladb.io/v1alpha1
kind: NebulaRegionFailover
metadata:
  name: catalog-to-us-west-2
  namespace: nebuladb
spec:
  clusterRef: nebula-prod
  bucket: catalog
  newHomeRegion: us-west-2
  force: false                  # require current home to be dead
  timeoutSeconds: 600
status:
  phase: Running|Completed|Failed
  fromEpoch: 3
  toEpoch: 4
  startedAt: …
  completedAt: …
```

---

## 7. Rollout plan

### Phase 1 — foundation (~1 week)

- [ ] Add `home_region` + `home_epoch` fields to the seed doc. Populate
      from `NebulaBucket.spec.homeRegion` on reconcile.
- [ ] New REST: `GET /admin/bucket/:b/home-region` → reads the seed doc.
- [ ] Update the operator's bucket controller to own seeding.
- [ ] Update `/admin/replication` to include per-remote-home lag fields.

### Phase 2 — cross-region replication (~2 weeks)

- [ ] New `nebula-grpc` service `CrossRegionReplicationService`.
- [ ] Background `cross_region::spawn_with_store()` task in
      `nebula-server/main.rs`, gated on cluster config.
- [ ] `NebulaCluster.spec.regions[].crossRegion.peers` plumbed into the
      task startup.
- [ ] Lag metrics + `/metrics` counters.

### Phase 3 — write routing (~1 week)

- [ ] New crate `nebula-client` with bucket→region resolver + 30s cache.
- [ ] REST middleware `require_home_region`: rejects writes with
      `429 wrong_home_region` if the bucket's home isn't the current node's
      region. Parallel to `guard_writes_on_follower`.
- [ ] gRPC + pgwire parity guards.

### Phase 4 — failover (~1 week)

- [ ] `NebulaRegionFailover` CRD + controller.
- [ ] Failover state machine: `Preflight → Drain → EpochBump → Announce →
      Completed`.
- [ ] Operator observability: events, status conditions.

### Phase 5 — nice-to-haves

- [ ] Anti-affinity in `NebulaCluster` regions so same-AZ accidental
      collisions are rejected at admission.
- [ ] Per-bucket read-preference: `readPreference: local|home|any`.
- [ ] Conflict auditing: if a record arrives from an unexpected region
      for a given epoch, log + expose on `/admin/audit`.

---

## 8. Risks

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Home-epoch tiebreaker races during partition | Medium | Operator-held lock via namespace-scoped lease resource (leader election primitive already in controller-runtime) |
| Cross-region WAL lag grows unbounded | Low | Alert at > 100 MB, auto-drain by slowing local writes (like InfluxDB's hint-handoff backpressure) |
| Two regions disagree about bucket home after a long partition | Medium | `home_epoch` + forced-demotion on catch-up ensures convergence; worst case, operator reconciliation-loops until one side wins |
| Client SDK cache serves stale home-region after failover | Low | 30s TTL; writes that hit the wrong home return `429` with the new home in the body, SDK updates immediately |
| gRPC follower tail with multiple remote feeds causes duplicate applies | Low | `apply_wal_record` is idempotent (by design for single-region); extend dedupe to tag records with `(region, wal_seq)` |

---

## 9. Open questions

1. **Tiebreaker for concurrent failovers.** See §5 "Open question A." —
   leaning toward controller-runtime leases, since we already use them
   for leader election of the operator itself.

2. **Do followers participate in cross-region?** Current draft has only
   leaders consume cross-region tails. Follower tail is a separate gRPC
   stream. Consider if followers should directly tail remote regions
   (avoids leader fan-out) or always go through the local leader.

3. **Schema for the home-region map.** Is it one doc per bucket in the
   seed, or a separate `__system/home_map` bucket with one doc per
   region? Separate bucket is cleaner for global queries but means the
   home map itself needs a home region — chicken and egg.

4. **pgwire in a multi-region world.** Does `INSERT` to a non-home
   region return `25006` (the follower guard code from gap #2) or a
   new SQLSTATE? Leaning `25006` with a distinct error message, since
   clients already handle the read-only-txn code.

5. **Failback automation.** Should we auto-failback when the original
   home region recovers, or require a human to create a new
   `NebulaRegionFailover`? Auto-failback saves ops toil but introduces
   oscillation risk on flaky links.

---

## 10. What we are NOT building

- **Global transactions.** No 2PC, no Paxos, no Raft across regions.
- **Automatic conflict resolution across home regions.** A bucket has
  exactly one home at any epoch.
- **A new storage engine.** This is purely a coordination layer on top
  of today's WAL + HNSW + replication.

---

## 11. Decision

**Recommendation: proceed with Option A'/C' hybrid.**

Strengths:
- Composes with existing primitives (gap #4 swap, follower tail).
- No changes to HNSW or the WAL format.
- Failover is operator-driven and observable.
- Client SDK caching keeps the hot path proxy-free.

Alternatives rejected:
- Pure Option A (router-only): doesn't actually deliver active-active.
- Pure Option B (CRDT): doesn't fit HNSW, weakens WAL guarantees.

Next step: agree on the open questions in §9, then start Phase 1.

---

## Appendix: relationship to the K8s operator gaps

- **Gap #1** (version introspection) — needed by the failover controller
  to verify both regions are running compatible builds before it bumps
  the epoch. Already landed.
- **Gap #2** (follower write guard on gRPC/pgwire) — the `require_home_region`
  middleware in Phase 3 reuses exactly this pattern. Already landed.
- **Gap #4** (bucket export/import) — directly reused by
  `NebulaRegionFailover` to catch the target region up before the
  epoch bump. Already landed.

Three of the four gaps were prerequisites for this design, not accidents.
Gap #3 on its own was never a single feature — it's this whole doc.
