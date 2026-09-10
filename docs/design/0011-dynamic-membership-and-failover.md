# Design 0011: Dynamic Raft membership, graceful node replacement, and automatic multi-region failover

- **Status**: Implemented — this PR. (Implementing it surfaced two
  pre-existing bugs that made multi-node Raft non-functional; see §6.1.)
- **Author**: Claude + @bwalia
- **Created**: 2026-07-10
- **Tracks**: mission requirement — "take a cluster node down and bring a new one up, and the cluster reconfigures itself with no data loss and no loss of service, including multi-region HA"
- **Relates to**: 0001 (multi-region active-active), 0004 (Raft implementation), 0007 (zero-downtime reads/writes), 0009 (rolling upgrade & rebalance), 0010 (reliability & graceful degradation)

---

## 1. Why this exists

Phase 2.5b (commit history through `nebula-raft`) landed a complete openraft
storage/network/state-machine stack: on-disk log, durable vote, snapshot
build/install, gRPC transport, and a `RaftHandle` wired into the write path.
When `NEBULA_RAFT_PEERS` is set, writes go through quorum consensus and leader
failure triggers automatic re-election — that part of within-region HA already
works.

Two capabilities were designed-around but never built, and together they are
exactly what "swap a node with no downtime, across regions" requires:

1. **The Raft membership set is frozen at boot.** `RaftConfig::from_env`
   parses `NEBULA_RAFT_PEERS` into a static `BTreeMap<NodeId, NebulaNode>`,
   requires it to be odd and ≥3, and never changes it again. openraft ships
   `add_learner` / `change_membership` / `initialize`, but nothing in NebulaDB
   calls them (see the comment at `crates/nebula-raft/src/boot.rs:229` —
   "those go through `Raft::change_membership` directly" — with no caller). So
   there is no way to add a replacement node to the quorum or evict a dead one
   at runtime. Replacing a node means editing env and restarting every peer.
   The `NodeId` doc comment (`types.rs:20`) papers over this by asserting "a
   new pod replacing a dead one keeps the same id, so the cluster sees a
   restart, not a membership change" — that only works for in-place restarts
   with the same durable data dir, not for a genuinely new node.

2. **Multi-region failover is entirely operator-driven.** ADR 0001 designs a
   `NebulaRegionFailover` CRD that a human creates *after* detecting a dead
   region; the cross-region consumer (`crates/nebula-grpc/src/cross_region.rs`)
   only guards against a *deposed* region leaking stale writes
   (`epoch_is_fresh`). There is no health detection, no automatic epoch bump,
   and ADR 0001 §9 explicitly lists auto-failback as an open question. A region
   outage is therefore a write outage for every bucket homed there until a
   human intervenes.

ADR 0010 Phase 4 ("adaptive replication") is about replication *efficiency*
(catch-up batching, WAL compression) and is orthogonal to both gaps. This ADR
fills the topology gaps directly.

## 2. Goals / non-goals

**Goals**

- Add a node to a live Raft cluster and remove one, quorum-safe, with no data
  loss and no write outage (minority-at-a-time reconfiguration).
- A repeatable graceful node-replacement flow (drain → add replacement →
  promote → remove old) exposed over the admin API and scripted for operators.
- One-time cluster formation (`initialize`) for a fresh Raft cluster, guarded
  so it is idempotent and cannot split-brain an already-formed cluster.
- Automatic multi-region failure detection and failover: a healthy region
  detects a dead home region and promotes itself (epoch bump + home rebind)
  without a human, with anti-oscillation guards.

**Non-goals**

- Automatic *within-region* leader promotion by a human — Raft already
  auto-elects; the `/admin/promote` path (ADR 0009) stays for legacy
  WAL-mode 2-node clusters only.
- Cross-region Raft / global consensus. Multi-region stays active-active WAL
  streaming with epoch-fenced home ownership (ADR 0001 §10 non-goal upheld).
- Automatic *failback* to an original region once it recovers. Recovery
  re-joins as a consumer (existing `epoch_is_fresh` demotion); re-homing back
  is still an operator decision (oscillation risk, ADR 0001 §9). We make this
  a deliberate non-goal, not an oversight.
- Sharding / partial replication. Every voter still holds the full dataset.

## 3. Within-region: dynamic Raft membership

### 3.1 openraft primitives (verified against openraft 0.9.24)

`Raft<NebulaTypeConfig>` exposes, gated on the default `OneshotResponder` that
`declare_raft_types!` gives us (so all three are callable on our handle):

- `initialize(members)` — one-time cluster formation; commits the initial
  membership entry, then the node campaigns. Safe to call on multiple nodes
  with the *same* member set; *different* sets split-brain (openraft docs).
- `add_learner(id, node, blocking)` — adds a non-voting replica and starts
  replication to it. With `blocking = true`, returns only once the leader
  believes the learner's log is caught up — this is the catch-up barrier that
  makes promotion safe.
- `change_membership(members: impl Into<ChangeMembers>, retain)` — commits a
  joint-consensus reconfiguration. `ChangeMembers::AddVoterIds(set)` promotes
  existing learners to voters; `RemoveVoters(set)` demotes; with
  `retain = false` a removed voter is dropped entirely rather than kept as a
  learner.

Error surface we map: `ChangeMembershipError::{InProgress, LearnerNotFound,
EmptyMembership, NotAllowed}` and, for formation,
`InitializeError::{NotAllowed, NotInMembers}`.

### 3.2 RaftHandle API (crate `nebula-raft`)

New methods on `RaftHandle`, each a thin wrapper that maps openraft's error
enums onto a new `MembershipError` (kept distinct from `SubmitError` because
the failure modes and HTTP mappings differ):

```rust
pub async fn initialize_cluster(&self) -> Result<(), MembershipError>;
pub async fn add_learner(&self, id: NodeId, addr: String) -> Result<(), MembershipError>;
pub async fn promote_to_voter(&self, id: NodeId) -> Result<(), MembershipError>;
pub async fn remove_node(&self, id: NodeId) -> Result<(), MembershipError>;
pub fn membership(&self) -> MembershipView; // voters, learners, leader, this id
```

- `initialize_cluster` calls `is_initialized()` first and returns
  `MembershipError::AlreadyInitialized` (a no-op success for idempotency at the
  handler layer) rather than letting openraft error — so re-running the
  bootstrap is safe. Members are `self.config.peers` (the boot map).
- `add_learner` always uses `blocking = true`: an operator/automation calling
  this wants "add and catch up," and the caller gets a clean error if catch-up
  can't complete instead of a silently-lagging learner that a later
  `promote_to_voter` would reject with `LearnerNotFound`/lagging.
- `remove_node` uses `RemoveVoters({id})` with `retain = false`. We do **not**
  additionally `RemoveNodes` — keeping the node metadata is harmless and lets a
  bounced node rejoin as a learner without re-supplying its address.
- `membership()` reads `raft.metrics().borrow().membership_config` — no await,
  no consensus round-trip; it is a local view for the status endpoint.

`RaftConfig::from_env` keeps the ≥3-and-odd guard **for initial formation
only**. That guard is about the *bootstrap* set; runtime membership legitimately
passes through even counts during a joint-consensus step, which openraft
handles internally, so no runtime guard is added there.

### 3.3 Admin endpoints (crate `nebula-server`)

Gated on `AppState.raft.is_some()` (else `400 raft_not_enabled`). Mirrors the
`/admin/promote` handler shape:

| Method & path | Action | Body |
|---|---|---|
| `GET  /admin/raft/membership` | current voters / learners / leader / this id | — |
| `POST /admin/raft/initialize` | one-time cluster formation (idempotent) | — |
| `POST /admin/raft/learner` | add learner + block until caught up | `{ "node_id": u64, "addr": "host:50052" }` |
| `POST /admin/raft/voter/:id` | promote a caught-up learner to voter | — |
| `DELETE /admin/raft/node/:id` | remove a voter from the quorum | — |

Error mapping (`MembershipError` → `ApiError`):

- `InProgress` → `409 membership_in_progress` (another reconfig is committing;
  retry).
- `LearnerNotFound` → `400 learner_not_found` (promote before add, or catch-up
  failed).
- `NotLeader { addr }` → `421 not_leader` (reuses the existing NotLeader body
  so the same smart-client retry logic re-targets the leader).
- `AlreadyInitialized` → `200` no-op (idempotent formation).
- everything else → `500 internal`.

Membership changes must run on the leader; openraft returns `ForwardToLeader`
off-leader, which we surface as `421` with the leader address — the operator
script (and any smart client) retries against it.

### 3.4 Graceful node-replacement flow

Replacing node `old_id`@`old_addr` with `new_id`@`new_addr` (new id, because a
genuinely fresh pod has empty durable state and must catch up from a snapshot,
not impersonate the dead node's id):

1. Start the replacement process with `NEBULA_RAFT_NODE_ID=new_id` and its own
   empty data dir. It comes up idle (not in any membership set yet).
2. `POST /admin/raft/learner {new_id, new_addr}` on the leader → openraft
   streams a snapshot + log tail to it and blocks until caught up. **Quorum is
   unchanged during this phase** (learners don't vote), so writes keep flowing.
3. `POST /admin/raft/voter/new_id` → joint-consensus step adds it to the voter
   set. Now an N→N+1 voter cluster.
4. `DELETE /admin/raft/node/old_id` → joint-consensus step removes the dead
   voter. Back to N voters, one of them brand new.

At every step a quorum of the *current* voter set is available, so committed
writes are never lost and the leader never stalls (unless the operator removes
a voter that would break quorum — the script refuses that; see §5). This is the
Raft analogue of ADR 0009's WAL-mode rolling replace, and it supersedes the
"keeps the same id" shortcut in `types.rs:20` for the new-node case.

## 4. Cross-region: automatic failover

### 4.1 What exists

`cross_region.rs` tails remote regions and applies records, guarded by
`epoch_is_fresh` (skip records whose `home_epoch` is below the local seed doc's
epoch). Failover today = a human bumps `home_epoch` on the new region via the
operator CRD. There is no health signal driving that bump.

### 4.2 Design: a region health monitor + autonomous promotion

A new `RegionFailoverMonitor` (in `nebula-grpc`, symmetric with
`cross_region::spawn_all`) runs on every region-participating leader. For each
bucket whose `home_region` is a *remote* region, it watches the liveness of
that remote (reusing the cross-region tail connection state — a region whose
tail has been in backoff/error continuously for `failover_grace` counts as
down; it does not open a second probe channel).

When a remote home region has been unreachable for `failover_grace`
(default 60s, `NEBULA_REGION_FAILOVER_GRACE_SECS`) **and** this region is a
declared failover candidate for that bucket
(`NEBULA_REGION_FAILOVER_CANDIDATE=true`, default false — opt-in so only one
region self-promotes), the monitor performs the promotion locally:

1. Read the bucket's seed doc; compute `new_epoch = local_epoch + 1`.
2. Write the seed doc with `home_region = my_region`, `home_epoch = new_epoch`
   through the normal write path (so it is WAL-durable and replicates).
3. From now on this region accepts writes for the bucket; the epoch fence
   (`epoch_is_fresh`) means the old region's late writes are rejected
   everywhere, and when the old region recovers it sees the higher epoch on its
   own inbound stream and self-demotes to consumer — exactly the existing ADR
   0001 recovery path, now reached without a human.

### 4.3 Anti-oscillation & split-brain guards

Automatic failover across a flaky WAN is where this gets dangerous. Guards:

- **Opt-in single candidate.** Only a region with
  `NEBULA_REGION_FAILOVER_CANDIDATE=true` self-promotes. Operators set exactly
  one candidate per home region; two candidates promoting concurrently is the
  one split-brain we must prevent, and the epoch fence resolves even that —
  the higher-epoch writer wins, the other self-demotes on its next inbound
  record — but we keep the single-candidate rule as the primary defence and
  the epoch fence as backstop.
- **Grace + hysteresis.** Promotion requires `failover_grace` of *continuous*
  unreachability. A flap that reconnects inside the window resets the counter.
  This mirrors ADR 0010's mode-machine hysteresis.
- **Monotonic epochs, no auto-failback.** Recovery never auto-re-homes. The
  original region rejoins as a consumer; re-homing back is an operator action.
  This makes oscillation impossible by construction — epochs only ever
  increase and home only moves on explicit human failback or a *new* detected
  outage of the *current* home.
- **Cooldown.** After a self-promotion, the monitor will not promote the same
  bucket again for `failover_cooldown` (default 5×grace), bounding churn if a
  region is genuinely flapping.

### 4.4 Why not Raft across regions

Cross-region RTT (tens–hundreds of ms) makes synchronous quorum writes
unacceptable for the active-active latency budget, and ADR 0001 already chose
epoch-fenced home ownership over cross-region consensus. We keep that; §4 adds
*detection and actuation* to the design that was previously human-triggered,
nothing more.

## 5. Operator surface

`scripts/replace_node.sh` drives §3.4 against a leader URL:

```
replace_node.sh --leader http://node1:8080 \
  --new-id 4 --new-addr node4:50052 --old-id 2
```

It: (1) verifies the target is the leader (follows `421` `leader_addr`),
(2) refuses to run if removing `old_id` would drop the surviving voter count
below a quorum for the *post-add* set, (3) adds the learner and waits for
`/admin/raft/membership` to list it, (4) promotes, (5) removes the old node,
(6) prints the final membership. It logs every skipped/aborted safety check
loudly (ADR 0010's "no silent caps" principle).

## 6. Testing

- **Multi-node Raft integration test** (`nebula-raft`): three in-process nodes
  over the real gRPC transport on ephemeral ports; `initialize`; wait for a
  leader; replicate a mutation and assert all three converge; `add_learner` a
  4th; `promote_to_voter`; `remove_node` one original; assert (a) no committed
  data lost, (b) a write submitted mid-reconfiguration still commits. This is
  the harness that did not previously exist (only single-node bootstrap tests).
- **Handler tests** (`nebula-server`): `/admin/raft/*` return `400
  raft_not_enabled` when raft is off; error-code mapping unit tests for
  `MembershipError → ApiError`.
- **Failover monitor tests** (`nebula-grpc`): grace/hysteresis counter resets
  on reconnect; single-candidate gate; epoch is bumped monotonically;
  cooldown suppresses re-promotion.

### 6.1 Two pre-existing bugs the multi-node harness caught

The single-node bootstrap tests that existed before this work could not
exercise log *replication* (a 1-node cluster is its own quorum and
applies straight from its own log). The new harness immediately exposed
two defects that made multi-node Raft non-functional:

1. **Fabricated last-log term** (`raft_storage.rs get_log_state`). The
   method reconstructed the last `LogId` with a hardcoded term `0`
   instead of the entry's real term. openraft's replication-matching
   compares full `LogId`s, so followers never matched and quorum-commit
   never advanced — the first client write blocked forever. Fixed by
   tracking `last_term` in the log's writer state alongside `last_index`
   and reporting the real value.

2. **Membership entries discarded from the log** (`storage.rs
   from_openraft_entry`). Non-`Normal` entries were collapsed into an
   empty-bucket marker, so an `EntryPayload::Membership` lost its config
   on the way to disk. When openraft read the log back to answer
   `change_membership`, `get_membership()` returned `None` and openraft
   panicked (`res.membership.unwrap()`). Fixed by adding `Membership`
   and `Blank` variants to `LogPayload` (additive, bincode-compatible)
   and round-tripping all three entry kinds faithfully.

A related config fix: the workspace now enables openraft's
`single-term-leader` feature. NebulaDB runs vanilla elected-leader Raft,
and the `.nrlog` frame only persists `term + index` — which is exactly
the term-only `CommittedLeaderId` that mode uses. The default
`leader_id_adv` mode expects a per-entry leader `node_id` the log can't
supply, producing phantom conflicts. See the `Cargo.toml` comment.

## 7. Rollout & compatibility

All additive. Raft membership endpoints are dead code unless
`NEBULA_RAFT_PEERS` is set. The failover monitor is dead unless
`NEBULA_REGION_FAILOVER_CANDIDATE=true`. Default deployments are byte-for-byte
unchanged. No migration.

## 8. Open questions

- Should `add_learner`'s blocking catch-up have a caller-supplied timeout so a
  never-catching-up learner (e.g. a huge snapshot over a slow link) returns a
  `408`-style error instead of holding the HTTP request? Deferred: current
  behavior is openraft's internal wait; revisit under ADR 0010 Phase 4's
  adaptive catch-up work.
- Region liveness reuse of the tail connection vs. a dedicated health RPC.
  Started with reuse (no new channel); a dedicated `/healthz`-style region
  probe may be cleaner if we later want faster detection than the tail backoff
  cadence gives.
