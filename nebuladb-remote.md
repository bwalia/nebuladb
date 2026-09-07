# NebulaDB — remote resume handoff

Exported 2026-09-05. Covers a session that ran 2026-09-03 → 09-05 and touched
the live k3s1 cluster, the wslproxy edge, GitHub, and the showcase app.

---

## 1. Resuming this session

The conversation is a **local** transcript on the originating Mac:

```
~/.claude/projects/-Users-balinderwalia-projects-nebuladb/202a64cd-1e84-4a79-b70b-dd97ac1b8c84.jsonl
```

Local CLI sessions are not synced anywhere, so full continuity requires a shell
on that machine (SSH / Tailscale / VS Code Remote), then:

```bash
cd ~/projects/nebuladb
claude --resume 202a64cd-1e84-4a79-b70b-dd97ac1b8c84
#   --fork-session   branch off instead of continuing in place
#   --continue       most recent session in this directory
```

`--cloud` and `--teleport` create *new* remote sessions; they will not carry
this context. CLI at time of export: 2.1.261.

### Prerequisites on the resuming machine

Without these, most of the work below is not reproducible:

| Need | Used for |
|---|---|
| `~/.kube/k3s1.yaml` | every cluster operation (context is not set globally — export `KUBECONFIG`) |
| `gh` authenticated as the repo owner | PRs, workflow dispatch, secret management |
| Docker Hub / registry creds | only if rebuilding images locally |

Cluster access pattern used throughout:

```bash
export KUBECONFIG=~/.kube/k3s1.yaml
```

---

## 2. State at export

### Cluster — healthy

```
ring    server   showcase
int     1/1      1/1
test    1/1      1/1
acc     1/1      1/1
prod    1/1      1/1
```

### Edge — fixed and serving

All four public hosts return `200` with real content
(`<title>NebulaDB — Knowledge Ops</title>`):

```
int-nebuladb.nebuladb.net    200
test-showcase.nebuladb.net   200
acc-showcase.nebuladb.net    200
showcase.nebuladb.net        200
```

Response headers now show `x-debug-origin-port: 8888` — the corrected backend.

### Repo

`main` @ `79dc13e`. Two PRs merged this session: **#110** (edge rule fix) and
**#111** (Cloudflare + wslproxy registration as a Ring Promoter job).

Branch `feat/enterprise-showcase` @ `1122c78` is **pushed, no PR opened**.

---

## 3. What was done

### 3.1 Dead node `debian010` — three stranded volumes recovered

`debian010` was removed from the cluster while three `local-path` PVs were
pinned to it by node affinity. Those PVs can never bind, so the pods were
unschedulable indefinitely.

| Claim | Was | Now |
|---|---|---|
| `int/nebuladb-data` | Pending 7d22h | 1/1 on `ubuntu001` |
| `acc/nebuladb-data` | Pending 12d | 1/1 on `ubuntu001` |
| `prod/spectoncr-grafana` | 2 pods Pending 9–12d | 3/3 on `debian002` |

**The recovery procedure** (reusable — this will recur on the next node removal):

```bash
export KUBECONFIG=~/.kube/k3s1.yaml
kubectl scale deploy/<name> -n <ns> --replicas=0
# Retain FIRST: reclaimPolicy=Delete makes the local-path provisioner try to
# run a cleanup pod ON THE DEAD NODE, wedging the PV in Terminating.
kubectl patch pv <pv> -p '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}'
kubectl delete pvc <claim> -n <ns>
kubectl delete pv <pv>
# recreate the PVC WITH its Helm labels/annotations, else the next
# `helm upgrade` fails with "invalid ownership metadata"
kubectl scale deploy/<name> -n <ns> --replicas=1
```

Manifest backups were written to the session scratchpad (ephemeral — gone).

**Data loss, accepted at the time:** int and acc NebulaDB datastores are
**empty**. Reseed with `NEBULA_TOKEN=... python3 scripts/seed_leads.py`.
Grafana lost only `grafana.db` (users/prefs/alert state); dashboards and
datasources are sidecar-provisioned from ConfigMaps and self-restored.

### 3.2 Edge redirect loop — root-caused and fixed

All four hosts served an infinite `301 → https://<host>:443/`.

**Cause:** the shared wslproxy rule `nebuladb-prod-default`
(`04152bcb-b3ae-e831-2a07-9d38cca77688`) routed to `13.42.188.136:80` — a
**retired AWS ELB** (`Server: awselb/2.0`) left over from before k3s1 moved off
AWS. It answers everything with a blanket HTTP→HTTPS redirect, which the edge
relayed to the client.

k3s was never at fault: the pod returned 200 directly, and Traefik returned 200
for every nebuladb Host header.

**Fix:** repointed to `193.237.176.232:8888`, the k3s1 ingress entry the working
`*.workstation.co.uk` hosts already use. Verified 200 for all four hosts with
byte-identical content to the pod.

**How it reached production:** the `*/30` drift guard added in #110 fired at
**09:17 on 2026-09-05** and pushed the corrected rule to the edge. That is what
turned the loop into a 200 — no manual edge edit was ever made.

### 3.3 Ring Promoter edge job (#111)

Ported jobshout's `register-edge-vhost` pattern:

- `deploy/edge/wslproxy-server-{int,test,acc,prod}.json` — per-ring specs
- `.github/workflows/register-edge-vhost.yml` — Cloudflare CNAME → wslproxy
  registration → public health verify, on `*/30` cron plus dispatch
- `deploy/edge/ring-promoter-app.yaml` — the `nebuladb-edge` app block

Two deliberate departures from jobshout, both documented in the workflow header:
**upsert** via `/api/projects/import` rather than jobshout's create-if-absent
(which cannot repair an existing-but-wrong rule), and **Cloudflare direct** with
a fallback to wslproxy `/api/dns/provision`.

Health verify uses `--max-redirs 0` on purpose: a redirect loop answers 301
forever and following it would score as a pass.

⚠️ `register-edge-vhost.yml` **has never run.** It replaced the workflow that
actually fixed production, so drift protection is currently unproven.

### 3.4 Showcase — enterprise demo, phase 1 (branch, no PR)

Roughly **8 of the build prompt's 45 sections**. Extends the existing Vite app
rather than rewriting in Next.js — the showcase ships through Dockerfile →
nginx bearer injection → Helm → four rings, and SSR buys nothing for a
client-side control plane. **This is an assumption you can overrule.**

Built:

| Module | Purpose |
|---|---|
| `demo/provenance.ts`, `components/Provenance.tsx` | Every value is `Sourced<T>` with a LIVE / DERIVED / SIMULATED badge; no path renders a number without its origin |
| `demo/tracer.ts`, `components/UnderTheHood.tsx` | `api.ts` records real calls; §37 shows genuine traffic |
| `demo/simulation.ts` | Seeded PRNG → 6-node/2-region topology, failover + rebalance state machines; pure `(seed, tick)` so demos reproduce |
| `demo/mcp.ts` | 14 MCP tools, 13 backed by endpoints that genuinely exist |

Sections: Executive demo, MCP playground, AI agents + supervisor, Cluster /
multi-region / failover, Swap rebalance.

**Two bugs caught by testing against the live cluster** (port-forward), which
would otherwise have shipped broken:

1. `/vector/search` takes `vector: Vec<f32>`, **not** a query string → 422.
2. **NebulaDB SQL requires a semantic predicate.** Plain `SELECT … LIMIT 1` is
   rejected: `"WHERE must include semantic_match(...) or vector_distance(...)"`.
   Valid form:
   `SELECT id, text FROM docs WHERE semantic_match(text, 'disaster recovery') LIMIT 5`

That second point is a genuine product differentiator — the engine plans
retrieval and filtering together — and the SQL section should lead with it.

---

## 4. Open items

### Blocking

1. **ring-promoter repo has drifted from live.**
   `deploy/k8s/configmap.yaml` lists `[jobshout]`; the live
   `workstation-ring-promoter` ConfigMap carries `[jobshout, jobshout-com,
   wslvault]`. **Applying the repo file would delete two apps.** Reconcile
   repo→live before adding `nebuladb-edge`.
   Note: that repo is on branch `bw/orbital-stage-layout` with an uncommitted
   change to `internal/diagnose/diagnose.go` — left untouched.

2. **`nebuladb` is registered with no Ring Promoter at all** (checked all three
   instances). `nebuladb-edge` would be its first entry.

### Important

3. **PR #95 `feat/mcp-native-platform` invalidates a showcase assumption.** It
   adds a real `nebula-mcp` crate (rmcp 2.2, Streamable HTTP at `/mcp`). The
   showcase MCP tab currently states "NebulaDB does not expose an MCP server
   endpoint yet" and simulates the transport. **If #95 lands, rewrite that
   page against the real server.** Related open PRs: #91–#94 (AI decoupling,
   workload classes, WAL gap, dynamic membership/failover — the last also
   overlaps the simulated failover page).

4. **Duplicate rule ownership.** `04152bcb…` exists in both
   `nebuladb/.github/wslproxy/` (correct backend) and
   `wslproxy/data/rules/prod/` (**still the stale ELB**). The nebuladb drift
   guard wins by frequency, but a wslproxy-side sync could revert production.
   One repo should own that rule.

5. **`CLOUDFLARE_API_TOKEN` is not set** on nebuladb, so the DNS step takes the
   wslproxy fallback (which has 502'd). DNS is currently correct, so this is
   latent. `gh secret set CLOUDFLARE_API_TOKEN`

6. **`npm run lint` is a silent no-op** in `apps/showcase` — the script calls
   `eslint`, but eslint is not in `devDependencies`, so it exits 0 having
   checked nothing. Close before the §42 test work.

7. **`unit + clippy` is red on `main`** and has been since at least Sep 2 at an
   unchanged SHA. Cause: `clippy::result_large_err` on tonic-generated
   `nebula-raft` code (`tonic::Status` Err-variant ≥176 bytes) under clippy
   1.98. Both #110 and #111 were merged over it deliberately. It masks real
   signal in nightly.

### Structural

8. **`local-path` + RWO means any ring is one node-removal from repeating §3.1.**
   int and acc both landed on `ubuntu001`, so that node is now a single point
   of failure for two rings. Consider a replicated storage class or a
   "drain PVs before decommission" runbook.

---

## 5. Next actions

```bash
export KUBECONFIG=~/.kube/k3s1.yaml
cd ~/projects/nebuladb

# 1. Prove the replacement drift guard works (it has never run)
gh workflow run register-edge-vhost.yml -f ENV=int
gh run watch

# 2. Reseed the empty int/acc datastores
NEBULA_TOKEN=... python3 scripts/seed_leads.py

# 3. Showcase phase 1 — open a PR, or preview in int without merging
gh pr create --base main --head feat/enterprise-showcase
gh workflow run deploy-k3s.yml --ref feat/enterprise-showcase \
  -f TARGET_ENV=int -f DEPLOYMENT_TYPE=build-and-deploy
```

Showcase phase 2, in dependency order — the enterprise seed dataset (§6/§32)
comes first because ingestion, chunking, vector playground, hybrid, RAG,
evaluation and benchmarks are all hollow without real content, and int/acc are
currently empty.
