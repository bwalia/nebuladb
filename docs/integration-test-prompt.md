# Integration test prompt for nebuladb

Paste the section below into a fresh Claude Code session against this repo
(or hand it to any agent with shell + curl access to `192.168.1.193`) when
you want a full reliability sweep of the live stack.

The prompt is intentionally read-only by default. Phase 2 ("cold-restart
behaviour") only runs when you explicitly say **"go heavy"** — that phase
restarts `nebula-server`, which causes a brief outage for the showcase URL
while WAL recovery completes.

---

You are validating that the nebuladb stack on `192.168.1.193` is reliable
end-to-end. Run every check below, report a punch list of pass/fail with
one-line reasons. Don't stop at the first failure — collect all results
then summarize. Don't restart the stack unless explicitly told. Read-only
probes only; no ingest unless a check requires it.

## 0. Preconditions
- `docker compose ps` shows all 7 services running and healthy (allow
  `nebula-gatus` either way — it's been chronically unhealthy).
- `cat .env` shows host-port overrides, no commented-out rate-limit knobs
  that would override defaults.

## 1. Liveness & boot timing
1. `/healthz` on `:18080` → 200 in <2s.
2. Showcase root on `:15173/` → 200 in <2s.
3. Grafana on `:13000/api/health` → 200.
4. Prometheus on `:19090/-/ready` → 200.
5. From `docker inspect nebula-server`: `State.StartedAt` to
   `State.Health.Status=healthy` transition. Grep `State.Health.Log` for
   the first probe with `ExitCode: 0` and compute the gap. Report it;
   flag if >120s — that's evidence the stack is brittle to restart.
6. Look at `State.Health.FailingStreak` — should be 0. If non-zero,
   root-cause why before continuing.

## 2. Cold-restart behaviour (RUN ONLY IF USER SAYS "go heavy")
1. Note current WAL size: `sudo du -sh /var/lib/docker/volumes/nebuladb_nebula-data/_data`.
2. `docker compose restart nebula-server` and start a stopwatch.
3. Poll `/healthz` every 5s until 200; record total seconds.
4. If >300s, the `start_period: 300s` window is insufficient — file as a finding.
5. Verify `nebula-showcase` did NOT enter `Created` state (i.e. its
   `depends_on` waited successfully).

## 3. Document API
1. POST a doc to `bucket=integration-test`:
   `{"id":"smoke-1","text":"…","metadata":{"source":"integration-test"}}` —
   expect 200 with `{bucket, id, dim}`.
2. GET it back at `/api/v1/bucket/integration-test/doc/smoke-1` — expect
   echo with intact metadata.
3. Bulk POST 5 docs to `/docs/bulk` — expect `inserted: 5`.
4. Bulk POST 1001 docs — expect 400 (cap is 1000).
5. POST with missing `text` → expect 400 (not 500).
6. POST with missing `id` → expect 422.
7. DELETE the doc → expect 204; then GET → 404.

## 4. SQL surface (against `leads` bucket if it exists, else `integration-test`)
1. `SELECT id FROM <bucket> WHERE semantic_match(text, 'something') LIMIT 5`
   — expect rows.
2. `SELECT id FROM <bucket> WHERE city = 'X'` (no `semantic_match`) —
   expect 400 with a clear "missing semantic clause" error.
3. `EXPLAIN` the first query via `/query/explain` — expect a JSON plan
   tree.
4. Time 10 sequential
   `SELECT * FROM <bucket> WHERE semantic_match(text, '<varied>')`
   queries; flag if any takes >2s on a warm cache.

## 5. Rate limiter (depends on PR #9 trusted-proxies behaviour)
1. From the host, hammer `/api/v1/query` with 200 quick requests using
   bursts of `?_=$RANDOM` to defeat caching. Expect **none** to 429
   (because `NEBULA_TRUSTED_PROXIES=*` should be set, so the showcase
   IP isn't sharing a bucket).
2. Send 200 requests with the SAME `Authorization: Bearer abc` header.
   Expect 429 to start firing once burst (default 120) is exhausted.
3. After a 429, verify `Retry-After` header is set and is a positive
   integer.

## 6. Streaming endpoints (must NOT be killed by the optional request timeout)
1. Open SSE on `/api/v1/admin/logs/stream` and hold for 60s — expect at
   least one keepalive comment, no premature close.
2. POST to `/api/v1/ai/rag` with `{"query":"hello","top_k":1}` — expect
   SSE stream, hold for 30s, expect graceful end.

## 7. Persistence
1. Note doc count: GET `/api/v1/admin/buckets` → record per-bucket sizes.
2. `docker compose restart nebula-server` (only if user said "go heavy"
   earlier).
3. After health, re-fetch `/admin/buckets` — counts must match exactly.

## 8. Logging & observability
1. `docker logs --tail 50 nebula-server` should be **non-empty** (PR #10
   enabled stderr logs). If empty, that's a finding.
2. `/metrics` on `:18080` → 200 with `nebula_queries_total`,
   `nebula_inserts_total`, `nebula_rate_limited` lines present.
3. Prometheus has scraped nebula-server in the last 60s:
   `curl :19090/api/v1/query?query=up{job=~".*nebula.*"}` returns 1.

## 9. Resource & limit sanity
1. `docker inspect nebula-server` — confirm `HostConfig.NanoCpus` is set
   (current: 2 CPUs). Flag if 0 (no cap → could starve other tenants
   on this shared host).
2. Container memory usage <80% of `Memory` limit via
   `docker stats --no-stream`.
3. No orphan containers (e.g. `<hash>_nebula-redis`) holding ports —
   `docker ps -a | grep -E '^[a-f0-9]{12}_nebula'` should be empty.

## 10. Report
Output a markdown table:

| # | check | result | note |
|---|---|---|---|

Then:
- **Reliability summary**: green / yellow / red.
- **Top 3 risks** the checks exposed.
- **Recommended fixes**, each with file:line if known.

Don't propose code changes — just identify the issues. The user will
decide what to fix next.
