# NebulaDB prod deploy runbook

How to ship to `prod` (`https://showcase.nebuladb.net`) safely, and what a
normal rollout looks like. For failover, follower bootstrap and the 502/504
RCA, see [ha-replication.md](ha-replication.md).

## How deploys are triggered

`.github/workflows/deploy-k3s.yml`:

| Trigger | Target |
|---|---|
| Push / merge to `main` | builds images and deploys to **int** automatically |
| `workflow_dispatch` | any env (`int`, `test`, `acc`, `prod`), any ref |

```bash
# Deploy main to prod (build images from this commit, then helm upgrade)
gh workflow run deploy-k3s.yml --ref main \
  -f TARGET_ENV=prod -f DEPLOYMENT_TYPE=build-and-deploy

# Find the run, then follow it
gh run list -w deploy-k3s.yml -L 3
gh run watch <run-id> --exit-status
```

- Image tags are the commit short SHA (`IMAGE_TAG` blank → short SHA).
- One deploy per environment at a time: `concurrency: deploy-k3s-<env>` with
  `cancel-in-progress: false`, so a second dispatch **queues** behind the
  running one instead of cancelling it.
- The `/healthz` `git_commit` field reports `unknown`, so to find what prod runs, use
  `gh run list -w deploy-k3s.yml --json event,headSha,displayTitle` and look for the
  latest successful `workflow_dispatch` with `TARGET_ENV=prod`.

## Before you deploy

1. **Is there anything to ship?** Compare against the SHA prod runs:
   `git diff --stat <prod-sha> origin/main`. A docs-only diff gains nothing and still
   costs the rollout window below, so skip it.
2. **CI is green on the commit.** CI installs the latest *stable* toolchain, so a new
   clippy lint can fail CI even when your local clippy passes. Reproduce with
   `rustup update stable`, then
   `cargo clippy --workspace --all-targets --locked -- -D warnings` and
   `RUSTFLAGS="-D warnings" cargo test --workspace --locked --no-fail-fast`.
   (On 2026-10-18, clippy 1.99's `double_must_use` fired inside `#[async_trait]`;
   fixed by bumping `async-trait`.)
3. **Verified on int first.** Merges auto-deploy there. For an unmerged branch,
   dispatch with `--ref <branch> -f TARGET_ENV=int`. Check `/healthz`, that the
   showcase bundle contains the new UI, and an API smoke test of the change.
4. **Pause long-running jobs that read prod** (dumps, backfills, reingests). During
   the partial-data window a missing doc returns **404, not 5xx**, so a dumper can
   silently record real docs as absent. Pause with `kill -STOP <pid>` and resume with
   `kill -CONT <pid>` only after the doc count is back to full (see below).
5. **Never change `NEBULA_EMBED_DIM` / the embedder in a normal deploy.** The server
   refuses to boot when the snapshot dimension differs from the embedder's
   (`crates/nebula-index/src/lib.rs`). Changing it is a wipe + reingest migration.

## What a normal rollout looks like

Measured with a 5s `/healthz` probe during real prod rollouts (~8.3M docs):

| Phase | Duration | What users see |
|---|---|---|
| Image build + helm upgrade | ~2–3 min | normal service |
| Leader restart and WAL / snapshot recovery | **~2–2.5 min** | `503` (boot stub, with `Retry-After`) |
| Leader back but reads served from a lagging follower | **~11 min** | `200`, but **partial data**: `/healthz` shows ~3.15M instead of ~8.3M docs; searches and doc GETs miss the rest |
| Fully recovered | — | `/healthz` shows the full doc count |

| Date | Commit | 503 window | Partial-data window |
|---|---|---|---|
| 2026-10-18 | `657b31c` (RAG apps) | 13:34:42 → 13:36:34 (~2 min) | 13:36:34 → 13:47:36 (~11 min, 3,150,137 docs) |
| 2026-10-18 | `4b14085` (README) | 14:50:46 → 14:53:04 (~2.3 min) | 14:53:04 → 15:03:48 (~10.7 min, 3,155,683 docs) |

The partial-data phase is the known follower-replication defect (first seen
2026-09-23): reads route to a follower that is up but has not caught up, and
nothing marks those responses as partial. Until it's fixed, **treat the rollout as
finished only when the doc count is back to full**, not when the workflow goes green.

### Probe while you deploy

```bash
LOG=prod-probe.log
while true; do
  code=$(curl -s -m 8 -o /tmp/hz.json -w '%{http_code}' https://showcase.nebuladb.net/healthz)
  docs=$(python3 -c "import json;print(json.load(open('/tmp/hz.json')).get('docs','-'))" 2>/dev/null || echo -)
  echo "$(date +%H:%M:%S) $code $docs" >> $LOG; sleep 5
done
# Summarise the transitions:
awk 'BEGIN{p=""} {k=$2" "$3; if(k!=p){print; p=k}}' $LOG
```

## After the deploy

1. Wait until `/healthz` shows the **pre-deploy doc count** for three probes in a row.
2. Verify the change itself, e.g.:
   ```bash
   H=https://showcase.nebuladb.net
   JS=$(curl -s $H/ | grep -oE 'assets/index-[A-Za-z0-9_-]+\.js' | head -1)
   curl -s $H/$JS | grep -c "Talent Match"        # new UI is in the bundle
   ```
3. Resume any jobs you paused (`kill -CONT <pid>`).
4. Add the measured windows to the table above.

## Rollback

Redeploy a previous image without rebuilding:

```bash
gh workflow run deploy-k3s.yml --ref main \
  -f TARGET_ENV=prod -f DEPLOYMENT_TYPE=deploy -f IMAGE_TAG=<previous-short-sha>
```

A rollback is a rollout too: expect the same ~2 min of 503s plus the partial-data window.

## Things that can bite

- **Shared rate limit.** The showcase nginx injects one token for every `/api` request,
  and the server limits it to `NEBULA_RATE_LIMIT_RPS` (20/s) with a 120 burst. A heavy
  script through the showcase host competes with real users for that budget. Give
  batch jobs their own API key.
- **`/api/v1/admin/buckets` is slow on prod** (~14s for 8.3M docs), close to the
  ingress timeout. Use `?top_keys=0` and a long client timeout.
- **Don't use `POST /api/v1/admin/backup` on prod.** It builds a fresh in-memory
  snapshot (~+30Gi) on a leader whose periodic snapshot already peaks around 30Gi
  (limit 96Gi). Copy the on-disk snapshot + WAL off the PVC instead.
