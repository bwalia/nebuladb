# Technote: NebulaDB multi-region active-active (home-region + XDCR)

**Status:** production topology (showcase + helm `prod`)  
**Audience:** platform / SRE / product engineers  
**Related:** [RFC 0001](../design/0001-multi-region-active-active.md), [HA runbook](../runbooks/ha-replication.md), [HA vision prompt](../ha-xdcr-vision-prompt.md)  
**Live demo:** https://showcase.nebuladb.net (Cluster, Rebalance, RAG, Talent Match, …)

---

## 1. One-sentence model

Each region accepts **writes only for buckets it homes**; **XDCR replicates both ways**; **reads work locally everywhere**. The same bucket is **not** writable in two regions at once.

That is active-active **at the cluster level** (every region serves all reads; every region accepts writes for its home buckets), not concurrent dual-writer for a single bucket.

---

## 2. Why this shape (not “write anywhere”)

Vector + document corpora are large. Once WAL segments are compacted, a cold peer **cannot** catch up from the live WAL tip alone — it needs a full `.nsnap` bootstrap, then incremental XDCR.

Pinning each bucket to one **home region** gives:

| Property | Benefit |
|----------|---------|
| Single writer per bucket | No last-writer-wins merge across oceans |
| Local reads everywhere | RAG / search stay in-region after XDCR apply |
| Failover = re-home | Promote or bump `home_epoch`, not a global DNS flip for every write |
| Observable lag | `/admin/replication` exposes `lag_bytes`, `behind`, per-remote health |

Couchbase XDCR is the useful mental model: filtered cross-datacenter replication with an explicit topology, not multi-master CRDTs for every key.

---

## 3. Production topology (current)

| Role | Workload | Region env | Notes |
|------|----------|------------|-------|
| Primary (east) | `deploy/nebuladb` | `NEBULA_REGION=us-east-1`, `NEBULA_NODE_ROLE=leader` | Accepts writes for east-home buckets |
| Follower (east) | `deploy/nebuladb-follower` | Same region, WAL tail | Within-region HA; promote via `/admin/promote` |
| Region-b (west) | `deploy/nebuladb-region-b` | `NEBULA_REGION=us-west-2` | Peer region; bidirectional XDCR |

Showcase nginx proxies:

- `/peer/follower/…` → east follower  
- `/peer/region-b/…` → west peer  

Public UI tabs **Cluster** and **Rebalance** bind to live APIs — not a fake six-node map:

- `GET /api/v1/admin/cluster/nodes`
- `GET /api/v1/admin/replication` (`lag_bytes`, `behind`, `remotes[]`)
- export / import / promote paths as wired in the showcase

---

## 4. Home region (write affinity)

Home is stored on the operator seed document (`__nebuladb_operator_seed__`) metadata:

```json
{
  "kind": "nebuladb-operator-seed",
  "bucket": "catalog",
  "home_region": "us-east-1",
  "replicated_to": ["us-west-2"],
  "home_epoch": 3
}
```

- **`home_region`** — which region’s leader may accept client writes for that bucket.  
- **`home_epoch`** — monotonic counter bumped on failover; stale writers from a deposed home are rejected.  
- Empty / missing home — writes may land wherever they arrive (dev convenience; avoid in prod for dual-region buckets).

**Write guard:** middleware `guard_wrong_home_region` returns `wrong_home_region` when the node’s `NEBULA_REGION` ≠ bucket home.

**Read path:** any healthy region serves documents and vector search from the local index (after XDCR has applied the WAL records).

**Admin probe:**

```bash
curl -sS -H "Authorization: Bearer $API_KEY" \
  https://showcase.nebuladb.net/api/v1/admin/bucket/<bucket>/home-region | jq .
```

---

## 5. XDCR (cross-region WAL tail)

Each region’s leader streams **owned** writes to peer leaders. Peers apply with `apply_wal_record` into the local index.

Important operational truths:

1. **XDCR does not backfill compacted history.** If region-b docs ≪ east docs, snapshot-bootstrap first (`scripts/bootstrap_region_mirror.sh`).
2. **Steady-state lag** is visible on `/admin/replication` (`lag_bytes`, `behind`, remote `healthy` / `last_error`).
3. **`healthy: false` until first apply** is normal for a quiet remote — write a small doc in that remote’s home to prove the path.
4. Keep **`NEBULA_EMBED_DIM`** (and embedder config) identical across regions or applies fail.

Within a region, the existing follower WAL tail remains the HA path; XDCR is the cross-region path.

---

## 6. Prod checklist (prove it)

### 6.1 Snapshot-bootstrap west

```bash
export KUBECONFIG=~/.kube/k3s1.yaml
NS=prod SOURCE=nebuladb TARGET=nebuladb-region-b \
  ./scripts/bootstrap_region_mirror.sh
```

Gate: west doc count ≈ east (± live writes); `/healthz` and catch-up/replication healthy.

Do **not** rely on `kubectl exec | pipe` for multi-GiB `.nsnap` — use chunked `dd`, node-local copy, or object storage (see runbook).

### 6.2 Seed home regions

Example demo buckets (names illustrative — use whatever you seed in prod):

| Bucket | Home | Writes where |
|--------|------|----------------|
| `aa-east-demo` | `us-east-1` | East leader only |
| `aa-west-demo` | `us-west-2` | West leader only |

Upsert / patch the seed doc metadata so `home_region` is set; verify with `/admin/bucket/.../home-region`.

### 6.3 Prove east → west and west → east

1. Write a doc to `aa-east-demo` on east → wait for XDCR → `GET` the same id on west.  
2. Write a doc to `aa-west-demo` on west → wait → `GET` on east.  
3. Attempt a write to `aa-east-demo` on west → expect `wrong_home_region`.  
4. Watch Cluster tab: remotes flip `healthy`, lag drops after traffic.

### 6.4 Show live peers in Cluster (not the fake map)

Open https://showcase.nebuladb.net → **Cluster**:

- Peer list from `/admin/cluster/nodes` (leader, follower, region-b).  
- Replication panel from `/admin/replication`.  
- Promote only when practising failover or east leader is down.

**Rebalance** tab: live export/import / rebalance controls against the same topology — no simulated node graph.

---

## 7. Scenario catalogue

### S1 — Local RAG after remote write

1. Home `support_kb` in east; ingest tickets in east.  
2. West serves `POST /api/v1/vector/search` and showcase **RAG** against the replicated corpus.  
3. Observe: search latency is local; answer quality matches east once lag ≈ 0.

### S2 — Split write affinity by domain

1. `shop_catalog` homes west (catalogue team region).  
2. `companies_house` homes east (Companies House ingest).  
3. Showcase **Shop Catalog** / **Companies House** tabs keep working; each write path stays on its home.

### S3 — Within-region follower loss

1. Kill east follower pod.  
2. Leader keeps serving; Deployment recreates follower.  
3. If corpus is multi-million docs and WAL tip already matches, **snapshot-bootstrap** the follower (runbook) — TailWal alone will not rebuild compacted history.

### S4 — Region partition

1. Break east↔west gRPC briefly.  
2. Each region continues local reads + home writes.  
3. On heal, lag spikes then converges; no dual-writer merge required.

### S5 — Wrong-home write (client bug)

1. Client posts to west for an east-home bucket.  
2. API returns `wrong_home_region` with `home_region`, `home_epoch`, `node_region`.  
3. SDK / gateway should refresh home map from `/admin/bucket/:b/home-region`.

### S6 — Failover / re-home

1. Declare east home bucket should move west (ops / future operator CRD).  
2. Ensure west has latest state (XDCR caught up or snapshot).  
3. Bump `home_epoch`, set `home_region=us-west-2`.  
4. East rejects new writes; west accepts; peers continue XDCR from the new home.

### S7 — Streaming RAG under replication

1. Showcase **RAG** tab: SSE stream with context chunks then tokens.  
2. Run the same query against east and west after convergence.  
3. Chunk provenance / Under the Hood panels show local API origin.

### S8 — Observability

1. Grafana (`grafana.workstation.co.uk`) Nebula dashboards: query latency, durability, backup, reliability.  
2. Prometheus scrape of showcase / nebula-server metrics.  
3. Alert rules in `deploy/prometheus/alerts.yml` for lag / health.

---

## 8. API quick reference

| Endpoint | Purpose |
|----------|---------|
| `GET /api/v1/admin/cluster/nodes` | Live peers + health |
| `GET /api/v1/admin/replication` | Local tip, lag, remotes |
| `GET /api/v1/admin/bucket/:b/home-region` | Write affinity |
| `POST /api/v1/admin/promote` | Follower → leader (within region) |
| `GET /healthz`, `/healthz/caught-up`, `/healthz/wal-tip` | Liveness / catch-up / tip |

Vector / RAG (any region, local index):

- `POST /api/v1/vector/search`  
- Showcase RAG SSE pipeline (ingest → chunk → embed → search → generate)

---

## 9. Anti-patterns

- Expecting XDCR to rebuild a cold region without a snapshot.  
- Leaving production buckets without `home_region` when two writers exist.  
- Running two leaders in one region.  
- Showing a decorative multi-region map that is not backed by `/admin/cluster/nodes`.  
- Different embedding dimensions between regions.

---

## 10. Screenshot checklist (for docs / blog)

Capture from https://showcase.nebuladb.net (auth as used in prod showcase):

1. **Cluster** — peer table + remotes healthy, lag visible.  
2. **Rebalance** — live export/import UI (no fake map).  
3. **RAG** — query + streamed answer with context chunks.  
4. **Talent Match** or **Enterprise Search** — semantic retrieval scenario.  
5. **Metrics** / Grafana overview — lag or QPS panels.  
6. Optional: `curl` / jq of `/admin/replication` and home-region JSON in a terminal still.

Store under `docs/articles/assets/` as:

- `cluster-live-peers.png`  
- `rebalance-live.png`  
- `rag-streaming.png`  
- `talent-match.png`  
- `grafana-nebula-overview.png`  
- `replication-api-json.png`

---

## 11. Further reading

- `docs/design/0001-multi-region-active-active.md`  
- `docs/runbooks/ha-replication.md`  
- `docs/ha-xdcr-vision-prompt.md`  
- `docs/active-active-rollout-prompt.md`  
- `scripts/bootstrap_region_mirror.sh`  
- Showcase: `apps/showcase/src/tabs/ClusterTab.tsx`, `RebalanceTab.tsx`, `RagTab.tsx`
