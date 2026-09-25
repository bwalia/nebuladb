# NebulaDB HA runbook — promote, follower replace, cross-region

Applies to helm-managed rings with `ha.withinRegion` / `ha.crossRegion`
enabled (`int`, `prod` as of this rollout). Raft quorum is **not** used.

## Topology

| Role | Workload | Notes |
|------|----------|-------|
| Primary (region-a) | `deploy/nebuladb` | `NEBULA_NODE_ROLE=leader`, `NEBULA_REGION=us-east-1` |
| Follower | `deploy/nebuladb-follower` | WAL tail + probe-proxy sidecar for catch-up under API keys |
| Region-b | `deploy/nebuladb-region-b` | Standalone peer, `NEBULA_REGION=us-west-2` |

Check:

```bash
export KUBECONFIG=~/.kube/k3s1.yaml
kubectl -n <ns> get pods,svc -l app.kubernetes.io/instance=nebuladb
curl -sS -H "Authorization: Bearer $API_KEY" \
  https://<host>/api/v1/admin/replication | jq .
curl -sS -H "Authorization: Bearer $API_KEY" \
  https://<host>/api/v1/admin/cluster/nodes | jq .
```

Healthy signals:

- Follower: `GET /healthz/caught-up` → **200**, `caught_up: true`
- Cross-region remotes: `healthy: true` after at least one applied record
- Cluster peers: `healthy: true` on follower and region-b

## Promote follower → leader (manual failover)

Use only when the primary is down or you are practicing failover on **int**.

1. Confirm follower is caught up: `curl -sS http://<follower>:8080/healthz/caught-up`
2. `curl -sS -X POST -H "Authorization: Bearer $API_KEY" http://<follower>:8080/api/v1/admin/promote`
3. Point clients / ingress at the promoted pod (or swap Service selector).
4. Bring a new follower online against the new leader (`NEBULA_FOLLOW_LEADER`).
5. Do **not** run two writers for the same region.

## Replace a failed follower

1. Delete the follower pod (Deployment recreates) or `helm upgrade` after fixing node/PVC.
2. Wait for `healthz/caught-up` 200. Fresh followers **cannot** rebuild a multi-million-doc corpus from WAL alone once older segments are compacted — the leader keeps only the live segment (~tens of MB) plus a full `.nsnap`. Prefer snapshot bootstrap (below).
3. If the PVC is corrupt: scale follower to 0, delete PVC `nebuladb-follower-data`, scale to 1, then **still** copy the leader snapshot (WAL BEGIN is empty history).

### Bootstrap follower from leader snapshot (prod-sized)

When `follower.docs << leader.docs` but `follower.cursor` already matches `/healthz/wal-tip`, TailWal has nothing left to replay — the index was never loaded from a real snapshot (partial `.nsnap` is a common failure mode).

```bash
export KUBECONFIG=~/.kube/k3s1.yaml
NS=prod
SNAP=snapshot-00000000000000000117.nsnap   # match leader ls snapshots/
TIP=$(kubectl -n "$NS" exec deploy/nebuladb -- wget -qO- http://127.0.0.1:8080/healthz/wal-tip)

kubectl -n "$NS" scale deploy/nebuladb-follower --replicas=0
# Attach follower PVC (or hostPath on the follower node) and:
#   rm -f snapshots/* wal/*
#   copy leader's $SNAP + $SNAP.ok (must be full size — kubectl exec|pipe truncates ~100Mi;
#   use chunked dd, node-local copy, or object storage)
#   write follower.cursor = $TIP
kubectl -n "$NS" scale deploy/nebuladb-follower --replicas=1
# Wait for recovery (~minutes for multi-GB nsnap) then:
kubectl -n "$NS" exec deploy/nebuladb-follower -c server -- wget -qO- http://127.0.0.1:8080/healthz
kubectl -n "$NS" exec deploy/nebuladb-follower -c server -- wget -qO- http://127.0.0.1:8080/healthz/caught-up
```

Docs on follower should match the leader (± live writes). Same procedure applies to `nebuladb-region-b` when it is a cold mirror of the same corpus.

## Cross-region remote unhealthy

Symptoms: `/admin/replication` remotes show `healthy: false` and/or `last_error`.

1. Confirm both leaders are Ready and gRPC 50051 is reachable across Services.
2. Check logs: `kubectl -n <ns> logs deploy/nebuladb -c server | rg cross-region`
3. Transient errors during primary restart are expected (backoff). Wait for reconnect.
4. `healthy` stays false until the first record is applied from that peer — write a small doc in the peer’s home region to confirm.
5. Dimension / embedder mismatch between regions will fail applies — keep `NEBULA_EMBED_DIM` identical.

## Prod notes

- Primary host: `https://showcase.nebuladb.net`
- Initial region-b sync of the full `leads` corpus is async; prefer export/import for a cold mirror rather than relying solely on WAL replay from BEGIN on multi-million doc buckets.
- Raft (`NEBULA_RAFT_*`) stays **off** until soak evidence exists.
- **Memory:** cold start / snapshot recovery for ~3M docs needs ≥24Gi limit (8Gi OOMs).
- **Probes:** liveness = `/healthz/live`, readiness = `/healthz/live` (must stay in Service during WAL recovery so nginx sees boot-stub 503 and fails over — readiness on `/healthz` caused 502/504 on 2026-09-24).
- Follower catch-up of the full corpus is gradual; watch `cluster/nodes` peer `docs` and `/healthz/caught-up`.

## 504 / 502 during queries (RCA 2026-09-24)

**Symptom:** `showcase.nebuladb.net` returned nginx **504** / **502** while the UI polled `/healthz` and admin APIs (and any `/api` call in the same window).

**Evidence (prod showcase nginx, UTC):**
- `18:10:42` `GET /api/v1/admin/durability` → **504** (`upstream timed out while connecting`)
- Same minute: multiple **502** `connect() failed (111: Connection refused)` to leader `10.43.83.140:8080` and follower `10.43.216.55:8080`
- Correlated with leader cold recovery after redeploy (~3.5 min WAL/snapshot reload; boot log `serving 503 until recovery completes`)

**Root cause:**
1. Readiness probe used `/healthz`, which returns **503 during recovery** → kube removes the pod from Service Endpoints.
2. Showcase nginx upstreams are the leader/follower **Services**. Empty Endpoints → **connection refused** / connect timeout → nginx **502/504**, not the intended boot-stub **503** failover.
3. Follower was also restarting / not caught up (`cursor seq 0`, ~29k docs vs ~3.1M on leader), so the backup peer could not absorb traffic.

**Permanent fix:**
- Readiness → `/healthz/live` so the boot listener stays in the Service and returns 503 for nginx `proxy_next_upstream`.
- Showcase nginx maps **502/504 → clean 503 + Retry-After**; longer `proxy_read_timeout` for search/SQL paths.
- Keep follower caught up (`/healthz/caught-up` 200) so failover has a real target — see “Replace a failed follower” if docs diverge badly.
