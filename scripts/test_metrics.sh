#!/usr/bin/env bash
# test_metrics.sh — /metrics is Prometheus text format, scraped by the
# prometheus container. We validate two layers:
#
#   1. The server's /metrics endpoint surfaces the counters we expect
#      and they move when we send traffic.
#   2. Prometheus is scraping nebula-server (job `up` == 1 in its own
#      HTTP API).
#
# The prometheus check is skipped when PROM_URL is unset, so the
# script is still usable standalone against a bare server.

# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
trap finish EXIT

PROM_URL="${PROM_URL:-http://localhost:9090}"

log "metrics endpoint"
metrics=$(curl -sSf "${BASE_URL}/metrics")
for m in nebula_requests_total nebula_searches_semantic nebula_docs_inserted; do
  echo "$metrics" | grep -q "^${m} " \
    && pass "/metrics surfaces ${m}" \
    || fail "/metrics missing ${m}"
done

# Compare before/after counter delta to confirm traffic actually moves
# the needle. If this fails the counter was present but static, which
# is often a wiring regression.
log "counters move under traffic"
before=$(echo "$metrics" | awk '/^nebula_searches_semantic /{print $2}')
# A couple of requests to shift the counter.
for _ in 1 2 3; do
  http_post /api/v1/ai/search '{"query":"ping","top_k":1}' >/dev/null || true
done
after=$(curl -sSf "${BASE_URL}/metrics" | awk '/^nebula_searches_semantic /{print $2}')
if [[ -z "$before" || -z "$after" ]]; then
  fail "nebula_searches_semantic not numeric (before='$before' after='$after')"
elif (( after > before )); then
  pass "nebula_searches_semantic moved from $before → $after"
else
  fail "counter didn't move ($before → $after)"
fi

# Prometheus scrape health, if Prometheus is wired in.
if curl -sfm 5 "${PROM_URL}/-/ready" >/dev/null 2>&1; then
  log "prometheus scrape target is up"
  # /api/v1/query returns `{"status":"success","data":{"result":[{"value":[<ts>,"1"]}]}}`
  val=$(curl -sS "${PROM_URL}/api/v1/query" --data-urlencode 'query=up{job="nebula-server"}' \
        | jq -r '.data.result[0].value[1] // "0"')
  assert_eq "1" "$val" "prometheus says nebula-server is up"
else
  log "prometheus unavailable (skipping scrape check)"
fi
