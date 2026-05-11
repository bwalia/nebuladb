# RFC 0003: Write availability during leader outage

Status: **draft — needs decision**
Author: triggered by repeated showcase 502s during cold WAL recovery on 192.168.1.193 (May 2026)

## Problem

Cold-start WAL recovery on a node with non-trivial ingest currently
takes 7–11 minutes on the dev stack. PR #17 keeps **reads** alive
during that window via failover to a follower. But **writes** during
leader outage still fail with 409 (the follower's
`guard_writes_on_follower` middleware correctly refuses them).

For "production use of nebuladb" the user has flagged this as
unacceptable. We need a story for write availability that doesn't
ship split-brain in the box.

## Non-goals

- Global consistency across regions. Cross-region replication
  already exists with eventual semantics and is out of scope here.
- Strict serializability. NebulaDB has no transactions.
- Sub-second failover. Recovery in seconds (not minutes) is the
  bar — anything below that needs separate work on recovery speed
  itself (snapshot frequency, parallel HNSW insert).

## Options

### A. Raft consensus

Cluster of N nodes (typically 3 or 5). Writes go to the elected
leader, replicated to a majority before ack. Automatic leader
election on failure. Strong consistency.

- **Pros**: well-understood; correct under partition; no split-brain.
- **Cons**: significant engineering effort. We'd pull in `openraft`
  or `raft-rs`, rewrite the write path to drive the log through the
  consensus module, refactor the existing follower replication into
  raft log shipping. The HNSW index becomes the raft state machine.
  Estimate: 4–8 focused weeks. The replication code we already
  have would mostly be discarded.
- **Operational complexity**: needs at least 3 nodes for fault
  tolerance; tooling for adding/removing peers; snapshot transfer.
- **When to pick**: target audience is enterprises that already run
  raft systems (etcd, TiDB, CockroachDB). They expect this and
  will pay for it.

### B. Multi-leader with conflict resolution

Every node accepts writes; replication is asynchronous in both
directions; conflicts resolved by deterministic rule
(timestamp + node id, vector clocks, or "highest WAL seq wins").

- **Pros**: cheaper to implement (~1–2 weeks). All nodes are always
  available for writes. Failover is just "send your traffic somewhere
  else"; no election needed.
- **Cons**: weak consistency. A write to node A and a write to the
  same id on node B during a network partition will conflict; one
  wins, the other is silently overwritten on reconciliation.
  For vector upserts (idempotent, last-write-wins is fine) this is
  often acceptable. For deletes vs upserts on the same id during a
  partition, you can get a deleted doc resurrecting itself or
  vice versa. Workarounds (tombstones with grace period) add complexity.
- **When to pick**: target audience is teams that want
  always-available writes and accept "eventual consistency". Most
  vector workloads — semantic search, RAG corpus — tolerate this.

### C. External coordinator (etcd / Consul)

External system holds the "current leader" lease. NebulaDB nodes
check the coordinator before accepting writes; if leader's lease
expires, a follower can claim it.

- **Pros**: avoids implementing consensus inside nebuladb. etcd is
  battle-tested. Implementation: ~1 week.
- **Cons**: external dependency operators must run. Failover time
  is bounded by lease TTL (typically 5–10s). Doesn't solve the
  fundamental "write during transition" problem — there's still a
  window where neither node is leader.
- **When to pick**: you already run etcd/Consul for other reasons,
  or you want HA without owning the consensus layer.

### D. Manual promotion + monitoring

Operator runs `nebulactl promote nebula-follower` during outage.
Followers can be promoted; the old leader rejoins as a follower
when it comes back up.

- **Pros**: minimal implementation (~2 days). Operations team's
  on-call procedure handles failover.
- **Cons**: not zero-downtime. Requires human in the loop or a
  scripted watchdog. Write outage during the promotion window
  (seconds to a few minutes depending on automation).
- **When to pick**: small ops team that already has paging /
  on-call. Honest answer for many self-hosted deployments.

## Recommendation

For NebulaDB's positioning (vector-native, RAG-friendly,
self-hostable), I'd recommend **B (multi-leader with LWW)** as the
default with **D (manual promotion)** available as a fallback
for users who can't tolerate any consistency relaxation.

Reasoning:

- Vector workloads are dominated by upserts with stable
  `external_id`. LWW conflict resolution matches the data model.
- Raft is overkill if we're not promising serializable transactions
  (which we aren't).
- Multi-leader keeps the operational model simple: every node is
  the same; no special "leader" role to manage.
- The user's actual pain — showcase 502s during leader recovery —
  is fully resolved by phase 1 (read failover, PR #17) plus B
  for writes.

That said, **the decision is yours**. The implementation effort
diverges by an order of magnitude between options.

## What changes if we pick (B)

1. Drop the `Leader` / `Follower` distinction on the write path.
   Both nodes accept writes; both replicate bi-directionally.
2. Add a deterministic ordering to `WalRecord`: `(wal_seq, node_id)`
   tuple. On replay, conflicts resolve to whichever has the higher
   `wal_seq`; ties broken by `node_id`.
3. Tombstones for deletes survive for a grace period (24h?) before
   compaction so a partition + late upsert doesn't resurrect.
4. The `guard_writes_on_follower` middleware goes away.
5. Showcase nginx routes ALL methods to `nebula_read_pool` (rename
   to `nebula_pool`) — both nodes equivalent.

Estimate: ~5–10 days of focused work + a week of soak testing.

## What changes if we pick (D)

1. New `POST /admin/promote` endpoint on the follower. Promotes
   itself to leader: stops gRPC client to the (presumed-dead)
   leader, flips `guard_writes_on_follower` off, starts accepting
   writes.
2. New `POST /admin/demote` on a node that's no longer leader,
   reversing the above.
3. `nebulactl promote <node>` / `nebulactl demote <node>` CLI.
4. Optional watchdog (separate sidecar): polls both nodes,
   promotes follower if leader's been unhealthy for > T seconds.
5. Showcase nginx already does the right thing; reads route to
   the pool, writes go to whichever node is currently leader.
   When the watchdog promotes the follower, an operator updates
   `${NEBULA_SERVER}` to point at the new leader.

Estimate: ~2–3 days. The watchdog could be a tiny bash script.

## Decision needed

Pick one of A / B / C / D (or explicitly defer). I'll start
implementation once chosen. Don't ship anything beyond phase 1
in the meantime — multi-master without a chosen consistency
model is a recipe for silent data loss.
