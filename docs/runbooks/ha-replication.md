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
2. Wait for `healthz/caught-up` 200. Fresh followers replay from WAL BEGIN / snapshot catch-up — large corpora (prod ~3M docs) can take a long time and need RAM similar to the leader.
3. If the PVC is corrupt: scale follower to 0, delete PVC `nebuladb-follower-data`, scale to 1 (full re-sync).

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
- **Probes:** liveness = `/healthz/live`, readiness = `/healthz`. Using `/healthz` for liveness kill-loops recovery.
- Follower catch-up of the full corpus is gradual; watch `cluster/nodes` peer `docs` and `/healthz/caught-up`.
