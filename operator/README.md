# NebulaDB Kubernetes Operator

Go / Kubebuilder operator that manages the full lifecycle of [NebulaDB](../README.md)
clusters on Kubernetes.

- **CRDs**: `NebulaCluster`, `NebulaBucket`, `NebulaRebalance`
- **Controllers**: cluster reconciliation, bucket seeding, rebalance/failover state machine
- **Admission**: validating webhook for `NebulaCluster`
- **Observability**: operator exports Prometheus metrics on `:8080/metrics`;
  managed pods expose NebulaDB's own metrics on `:8080/metrics` inside the pod

## What the operator manages

| Kind               | Scope     | Purpose                                                                           |
|--------------------|-----------|-----------------------------------------------------------------------------------|
| `NebulaCluster`    | Namespace | Desired topology: leader + followers per region, storage, AI config, auth, upgrade |
| `NebulaBucket`     | Namespace | Declarative bucket seeding against a parent cluster                               |
| `NebulaRebalance`  | Namespace | One-shot rebalance / failover / upgrade coordination                              |

## Architecture

```
+-------------------------+         +-------------------------+
|  NebulaCluster CR       |         |  NebulaBucket CR        |
+-----------+-------------+         +-----------+-------------+
            |                                     |
            v                                     v
+-------------------------+         +-------------------------+
|  Cluster Reconciler     |         |  Bucket Reconciler      |
|  - leader StatefulSet   |         |  - seeds bucket via     |
|  - follower StatefulSet |         |    /api/v1/bucket/*/doc |
|  - services, PVCs       |         |  - tracks doc counts    |
|  - snapshot+Recreate    |         |  - empties on delete    |
+-----------+-------------+         +-------------------------+
            |
            v
+-------------------------+
|  Rebalance Reconciler   |
|  (Standard / Failover / Swap)
+-------------------------+
```

## Known backend gaps

NebulaDB is a young project. The operator's CRD surface is intentionally broader
than what the backend can do today — this keeps the contract stable while the
database catches up.

| Feature                             | Status in NebulaDB | Operator behavior                                    |
|------------------------------------|---------------------|-------------------------------------------------------|
| Leader/Follower via gRPC WAL tail  | Supported           | Fully automated                                       |
| Snapshot + WAL compaction          | Supported           | Called before upgrades and scale changes              |
| Implicit bucket creation           | Supported           | Seed doc upserted by bucket controller                |
| Follower write rejection (REST)    | Supported           | Reconciler routes writes to leader service only       |
| Swap rebalance engine              | **Not implemented** | `NebulaRebalance` Type=Swap moves to `NotImplemented` |
| Cross-region active-active         | **Not supported**   | Regions are app-side routed; CR provisions per-region |
| Version introspection API          | Supported (0.1.1+)  | Operator probes `/healthz.version` + `/admin/version` |
| gRPC / pgwire follower write guard | Supported (0.1.1+)  | gRPC returns FAILED_PRECONDITION, pgwire SQLSTATE 25006 |

## Quick start

```bash
# 1. Install CRDs + operator via kustomize
kubectl apply -k config/default

# 2. Or via Helm
helm install nebuladb-operator deploy/helm/nebuladb-operator \
    --namespace nebuladb-system --create-namespace

# 3. Create a cluster
kubectl create ns nebuladb
kubectl apply -f examples/cluster-basic.yaml

# 4. Watch
kubectl -n nebuladb get nebulaclusters,nebulabuckets,nebularebalances -w
```

## Upgrade runbook

Because NebulaDB is single-writer-per-node, the operator uses a
**snapshot + Recreate** flow rather than a rolling update. Sequence:

1. User bumps `spec.version` on the `NebulaCluster`.
2. Validating webhook warns if `upgradeStrategy=SwapRebalance` (not implemented).
3. Reconciler updates the leader StatefulSet template (OnDelete strategy).
4. Before deleting the leader pod, reconciler calls `POST /api/v1/admin/snapshot`.
5. Leader pod is deleted. STS recreates it with the new image tag.
6. Follower pods upgrade one at a time after the leader is healthy.
7. Status phase moves `Upgrading -> Running`; `lastSnapshotAt` is populated.

Manual failover (until the Failover controller automates role flipping):

```bash
kubectl apply -f examples/rebalance-failover.yaml
# then edit the follower STS env: NEBULA_NODE_ROLE=leader
```

## Development

```bash
make generate manifests      # regenerate CRDs + deepcopy (requires controller-gen)
make build                   # compile bin/manager
make docker-build IMG=...    # build container image
make test                    # run unit tests
```

Local run against a kind cluster:

```bash
kind create cluster
kubectl apply -k config/crd
go run ./cmd/manager --enable-webhooks=false
```

## License

Apache-2.0 (matches the parent NebulaDB project).
