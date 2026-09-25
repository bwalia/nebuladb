# nebuladb (Helm chart)

Single chart that deploys NebulaDB + the showcase admin UI on
Kubernetes. Optional Redis subchart for the second-tier embedding
cache.

![NebulaDB architecture](../../../docs/architecture.png)

For the full system view (clients, edge middleware, query planes,
core engines, durability, replication, HA), see the
[architecture diagram](../../../docs/architecture.svg) in the repo
root `docs/` directory.

## Install

```bash
# From a local checkout:
helm dependency update deploy/helm/nebuladb
helm install nebula deploy/helm/nebuladb

# With Redis bundled:
helm install nebula deploy/helm/nebuladb --set redis.enabled=true

# Pointing at an external Redis:
helm install nebula deploy/helm/nebuladb \
  --set externalRedisUrl=redis://prod-redis:6379

# Production-shaped deploy:
helm install nebula deploy/helm/nebuladb -f my-values.yaml
```

## Upgrade

```bash
helm upgrade nebula deploy/helm/nebuladb -f my-values.yaml
```

Note the Deployment uses `Recreate` strategy — one Pod at a time.
NebulaDB is currently in-memory single-node; rolling alongside a
second writer would split state. Once snapshot-and-restore ships,
the strategy flips to `RollingUpdate`.

**Liveness and readiness must use `/healthz/live`**, not `/healthz`.
During WAL / snapshot recovery `/healthz` returns 503. Probing it as
**liveness** kill-loops a healthy recovering primary (multi-million-doc
corpus). Probing it as **readiness** removes the pod from Service
Endpoints so showcase nginx sees connection refused / timeouts → **502/504**
instead of the boot stub's 503 failover (prod 2026-09-24).

## Key values

| Path | Default | Purpose |
|---|---|---|
| `server.image.repository` | `bwalia/nebula-server` | Docker Hub repo |
| `server.image.tag` | `latest` | Image tag — pin in production |
| `server.replicaCount` | `1` | Must stay 1 per role (RWO PVC); use `ha.*` for mirrors |
| `server.env.*` | various | Mirrors every `NEBULA_*` env var |
| `server.secretEnv` | `{}` | Map env-var → Secret name for JWT / API keys |
| `server.ingress.enabled` | `false` | Set true to expose REST |
| `server.persistence.enabled` | `false` | WAL + snapshots on a PVC when true |
| `ha.withinRegion.enabled` | `false` | Deploy a follower that tails the primary over gRPC |
| `ha.crossRegion.enabled` | `false` | Set `NEBULA_REGION` + peer WAL; optional region-b leader |
| `redis.enabled` | `false` | Bundle Bitnami Redis |
| `externalRedisUrl` | `""` | Point at external Redis |
| `showcase.enabled` | `true` | Deploy the React admin UI |
| `serviceMonitor.enabled` | `false` | kube-prometheus-stack scrape target |

### HA topology

HA is **multi-Deployment**, not replica scale-out:

- Primary: existing `<release>` Deployment (role `leader` when a follower is enabled)
- Follower: `<release>-follower` + own PVC (`NEBULA_FOLLOW_LEADER`, `NEBULA_LEADER_REST_URL`)
- Region-b: `<release>-region-b` + own PVC (`NEBULA_CROSS_REGION_PEERS` both ways)

Enable on int via `values-int.yaml` (`ha.withinRegion` + `ha.crossRegion`). Verify with `/api/v1/admin/replication` and `scripts/test_multiregion.sh` pointed at the two REST Services.

See `values.yaml` for the full list with inline comments.

## Secrets

Don't put API keys in `values.yaml`. Create a Kubernetes Secret and
reference it:

```yaml
# my-values.yaml
server:
  secretEnv:
    NEBULA_API_KEYS: nebula-api-keys
    NEBULA_JWT_SECRET: nebula-jwt
```

```bash
kubectl create secret generic nebula-api-keys \
  --from-literal=NEBULA_API_KEYS=my-first-key,my-second-key

kubectl create secret generic nebula-jwt \
  --from-literal=NEBULA_JWT_SECRET="$(openssl rand -hex 32)"
```

The chart wires these via `envFrom: secretRef`, so every key in the
Secret becomes an env var on the Pod.

## Uninstall

```bash
helm uninstall nebula
# If persistence is enabled, PVCs stay behind. Remove explicitly:
kubectl delete pvc -l app.kubernetes.io/instance=nebula
```
