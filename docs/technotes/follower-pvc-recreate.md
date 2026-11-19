# Technote: Recreate the within-region follower PVC on a larger node

**Status:** living note, last updated 2026-11-19  
**Audience:** platform / SRE bringing `nebuladb-follower` back in prod  
**Related:** [memory footprint](memory-footprint-and-deploy-downtime.md), [active-active XDCR](active-active-xdcr.md), `scripts/bootstrap_region_mirror.sh`, `deploy/helm/nebuladb/values-prod.yaml` (`ha.withinRegion`)

---

## 1. Summary

- The within-region follower needs roughly the **same working-set RAM as the leader** (tens of GiB today; leader limit is **96 Gi**).
- Its data volume uses **local-path**, so the PVC is **pinned to one node**. In prod that node is **debian002** (~32 Gi allocatable).
- Bootstrapping a fresh ~20 GB snapshot onto that PVC, then scaling to 1, ends in **CrashLoopBackOff / exit 137 (OOM)** while the server loads the snapshot.
- **Fix:** park the follower, delete the old PVC, recreate it on a node with enough free RAM, raise the memory limit, bootstrap again from the leader snapshot.

Public write-up: [website/blog/follower-pvc-recreate.html](../../website/blog/follower-pvc-recreate.html).

---

## 2. What happened (18–19 Nov 2026)

1. `bootstrap_region_mirror.sh` copied leader snapshot `snapshot-…0478.nsnap` (~20 GB) onto the follower PVC in 32 MiB chunks, verified SHA-256, seeded the WAL resume cursor at segment 478, and scaled `nebuladb-follower` to 1.
2. The pod started, bound the boot listener (503 until recovery), then the **server** container was **OOM-killed (exit 137)** repeatedly.
3. Limits at the time: follower **32 Gi** on **debian002**; leader **96 Gi** on **debworker00**.
4. PVC annotation: `nebuladb-follower-data` → selected-node **debian002**.
5. Follower was parked at **0 replicas** to stop thrashing.

This matches the existing chart comment in `values-prod.yaml`: the mirror was already parked once for the same reason (Oct 2026).

---

## 3. Why deleting the PVC is required

`local-path` volumes do not float between nodes. Patching `nodeSelector` or raising the memory limit **without** deleting the PVC leaves the pod on debian002 (or Pending if the selector disagrees with the volume).

| Step | Without PVC recreate | With PVC recreate |
|---|---|---|
| Raise memory to 64 Gi+ | Still scheduled where the volume lives (32 Gi node) → OOM again | Can land on a 64 Gi+ node |
| `nodeSelector` to bigger node | Pending (volume affinity vs selector) | New volume binds on the selected node |

---

## 4. Procedure (prod)

Use `KUBECONFIG` for the prod cluster. Prefer a maintenance window: the leader stays up; only the within-region warm standby is offline (already at 0).

### 4.1 Pick a target node

Need **≥ ~64 Gi free allocatable** after other workloads (snapshot peaks can spike above steady RSS). Measured Nov 2026:

| Node | Approx. allocatable | Notes |
|---|---|---|
| `debworker00` | ~128 Gi | **chosen** — only reachable node with headroom; co-locates with leader (keep follower limit at 64 Gi) |
| `debian003` / `debian004` | ~64 Gi | too small once other tenants / region-b are counted |
| `cloud003` / `vps005` | ~192 Gi | edge taint + **no CNI path** to `nebuladb.prod.svc` (wget times out) |

Confirm free capacity:

```bash
kubectl describe node <NODE> | sed -n '/Allocated resources/,/Events/p'
```

### 4.2 Park the follower

```bash
kubectl -n prod scale deploy/nebuladb-follower --replicas=0
kubectl -n prod wait --for=delete pod -l app.kubernetes.io/component=server-follower --timeout=180s || true
```

### 4.3 Delete the old PVC

```bash
kubectl -n prod delete pvc nebuladb-follower-data
```

Wait until it is gone. If it sticks in `Terminating`, check for a leftover pod or finalizer.

### 4.4 Chart / values changes

In `deploy/helm/nebuladb/values-prod.yaml` under `ha.withinRegion`:

1. Set `followers: 1` (only after the new PVC can bind on a large enough node).
2. Raise `resources.limits.memory` (e.g. **64 Gi** or higher; do not leave **32 Gi**).
3. Add a `nodeSelector` (or affinity) for the chosen node, matching however the chart exposes it for the follower Deployment.

Then upgrade (or apply an equivalent patch). Example memory patch if not using Helm for the one-off:

```bash
kubectl -n prod patch deploy nebuladb-follower --type strategic -p '{
  "spec": {
    "template": {
      "spec": {
        "nodeSelector": { "kubernetes.io/hostname": "<NODE>" },
        "containers": [{
          "name": "server",
          "resources": { "limits": { "memory": "64Gi" }, "requests": { "memory": "8Gi" } }
        }]
      }
    }
  }
}'
```

Ensure the follower Deployment still mounts a PVC named `nebuladb-follower-data` (Helm will recreate it on first schedule if the chart owns the PVC template). If the PVC is chart-managed, `helm upgrade` after deleting the PVC is the cleanest path.

### 4.5 Recreate empty volume on the new node

Scale briefly to 1 so the PVC binds, confirm node and size, then scale back to 0 for bootstrap:

```bash
kubectl -n prod scale deploy/nebuladb-follower --replicas=1
kubectl -n prod get pvc nebuladb-follower-data -o jsonpath='node={.metadata.annotations.volume\.kubernetes\.io/selected-node} size={.status.capacity.storage}{"\n"}'
kubectl -n prod scale deploy/nebuladb-follower --replicas=0
kubectl -n prod wait --for=delete pod -l app.kubernetes.io/component=server-follower --timeout=180s || true
```

If the PVC bound on the wrong node, delete it again and fix the selector before retrying.

### 4.6 Bootstrap from the leader snapshot

```bash
KUBECONFIG=~/.kube/k3s1.yaml \
  NS=prod SOURCE=nebuladb TARGET=nebuladb-follower \
  TARGET_NODE=debworker00 \
  ./scripts/bootstrap_region_mirror.sh
```

`TARGET_NODE` pins the mirror helper (and therefore the WaitForFirstConsumer local-path PVC) onto the same host as the follower `nodeSelector`.

Expect ~hours for a ~20 GB transfer (32 MiB chunks). The script freezes a private copy on the leader, hashes it, copies to the helper/target PVC, seeds the resume cursor, and scales the target to 1.

Do **not** skip the `.ok` marker / finalize steps: without them recovery ignores the snapshot and the follower boots empty.

### 4.7 Verify

```bash
kubectl -n prod get pod -l app.kubernetes.io/component=server-follower -o wide
# While recovering: HTTP 503 on :8080 is expected until the snapshot is loaded.
kubectl -n prod exec deploy/nebuladb-follower -c server -- \
  wget -qS -O- http://127.0.0.1:8080/healthz 2>&1 | head -20
# Cockpit / showcase peer path (from inside the showcase pod):
kubectl -n prod exec deploy/nebuladb-showcase -c showcase -- \
  wget -qS -O- http://127.0.0.1/peer/follower/healthz 2>&1 | head -20
```

Success: follower Ready, `/healthz` 200, peer probe no longer 503, RSS not climbing into the limit under idle load.

---

## 5. Failure modes to watch

| Symptom | Likely cause |
|---|---|
| Exit 137 / CrashLoop after bootstrap | Memory limit or node still too small; snapshot peak > limit |
| PVC Pending | Selector vs local-path binding; old PVC not deleted |
| Bootstrap `kubectl cp` websocket EOF | Network flake mid-chunk — re-run script (or resume path if supported); clean stale `nebula-mirror-helper-*` pods |
| Follower Ready but 0 docs | Snapshot missing `.ok` / finalize skipped |
| Leader 503 during bootstrap | Should not happen; abort and check leader separately |

---

## 6. Longer-term

- Prefer scheduling HA peers on nodes whose allocatable RAM exceeds **leader limit + snapshot headroom**, not “same as current RSS”.
- Document PVC node pins in the runbook whenever `local-path` is used.
- Disk-backed documents ([design 0014](../design/0014-disk-backed-document-store.md)) reduce heap per doc but do **not** remove the need for a large HNSW working set.
