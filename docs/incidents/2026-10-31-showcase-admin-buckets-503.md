# RCA: showcase.nebuladb.net dashboard errors on load (31 Oct 2026)

- **Severity:** partial outage of the showcase dashboard (Executive, Admin and Rebalance tabs). The database, `api.nebuladb.net` and search all stayed up.
- **Detected:** reported by the owner as "500 http error" around 19:28 UTC. Pod logs show `GET /api/v1/admin/buckets` → **503**, `upstream timed out (110)`.
- **Status:** mitigated in prod by the showcase layers in PR #174. The permanent server fix is in PR #173 and ships with the next database deploy.

## What happened

1. Opening the showcase loads the Executive tab, which polls `/healthz`, `/admin/stats` and `/admin/buckets` every 5 s.
2. `/admin/buckets` answered from a 15 s cache. On a cache miss it scanned **every document** (`TextIndex::bucket_stats`), counting docs and metadata keys.
3. At **8.3M docs**, the size after the embed-migrate drain finished at 14:28 UTC, a cold scan took **45 s** (measured on prod). The showcase nginx gives `/api/` calls **15 s**, so it answered 503.
4. The Executive tab fetched its three endpoints with `Promise.all`, so the one slow call blanked the whole page, including health and stats, which had answered in under 0.3 s.

## Root causes

| # | Cause | Why it mattered |
|---|---|---|
| 1 | **Request-path full scan.** `/admin/buckets` ran an O(corpus) scan while the request waited, and held its cache mutex across it, so every concurrent poller waited too. | Latency grew with data. It was fine at 84k docs and broke at 8.3M. |
| 2 | **A recent change made the scan slower.** PR #166 stored metadata as JSON text to save RAM, so counting keys now parses 8.3M JSON strings. The endpoint wasn't measured at prod scale. | It pushed a borderline scan well past the timeout. |
| 3 | **Corpus growth.** The drain grew the corpus from 6.5M to 8.3M docs (+28%). | Same as above. |
| 4 | **No degradation in the UI.** `Promise.all` turned one failed panel into a failed page. | The blast radius was the whole dashboard instead of one panel. |
| 5 | **Two copies of the nginx config.** The chart mounts `deploy/helm/nebuladb/files/showcase/default.conf.template` over the image's `apps/showcase/nginx.conf`. The first fix (PR #172, a longer timeout) only edited the image copy, so it **could not have taken effect**. PR #170's `/examples` location had the same problem. | Fixes looked shipped but weren't. |

Contributing: with a 15 s TTL and a 45 s scan, a scan ran almost continuously while a dashboard was open, keeping about one leader core busy.

## Fixes

| Layer | Change | PR | In prod? |
|---|---|---|---|
| nginx | Stale-while-revalidate cache on `/admin/buckets`: serve the last good copy instantly, one background refresh, keep serving through upstream errors, timeouts and slow responses. Leader-pinned, 120 s timeout, no retry. | #174 | via `showcase-only` |
| UI | Executive and Rebalance tabs load each panel independently (`Promise.allSettled`), keep the last good data and name what failed. | #174 | via `showcase-only` |
| Config | Chart copy synced with the image copy. **CI fails if they differ.** | #174 | yes |
| Deploy | `showcase-only` now also patches the nginx ConfigMap, rolls once with a config hash, and verifies the live config matches the repo. | #174 | yes |
| Server | `/admin/buckets` never scans on the request path: counts come from the bucket index (O(buckets)), and keys come from a background-refreshed cache (TTL 300 s, one refresh at a time). | #173 | next database deploy (zero-503 work) |

## Verification

- **nginx cache**, tested in containers: fill → `MISS`; then `HIT` in about 2 ms; upstream returning 503 → `STALE` 200 in about 5 ms; upstream hanging 30 s → `STALE`/`UPDATING` 200 in about 2 ms.
- **Server:** `admin_buckets_never_blocks_on_a_scan`, where every keyed call answers in under 2 s, and `bucket_counts_match_the_full_scan`.
- **Prod after rollout:** the live ConfigMap hash matches the repo, `/admin/buckets` returns `X-Cache-Status`, and the leader pod is unchanged.

## Lessons

1. **Measure admin endpoints at prod scale,** not just the hot path. The memory change was tested for search, not for the scan that reads every document.
2. **No O(corpus) work while a request waits.** Precompute, maintain incrementally, or serve stale.
3. **One source of truth for config,** with CI enforcing it. "Keep in sync" comments don't work.
4. **Verify that a fix is live,** not just that the deploy succeeded. Check the running config and the response headers.
5. **Dashboards degrade per panel.** Never let `Promise.all` turn one failure into an outage.

## Follow-ups

- Ship #173 with the zero-503 database deploy work.
- Synthetic check: time `/api/v1/admin/buckets` and the showcase landing load every 15 min and alert on > 5 s or non-200. Today Prometheus scrapes the showcase `/metrics` and gets a 302 to the login page, so that monitoring is blind.
