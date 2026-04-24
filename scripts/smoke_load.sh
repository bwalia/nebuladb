#!/usr/bin/env bash
# smoke_load.sh — basic concurrent-traffic smoke test.
#
# Goal: catch the obvious regressions — deadlocks, crash-on-parallel,
# dropped responses — not benchmarking. Real benchmarking lives in
# `cargo bench`. We fire N concurrent requests, count successes, and
# measure p95 wall-clock latency.
#
# "Acceptable" thresholds are deliberately loose for a CI runner with
# no GPU. Adjust the env knobs if your hardware is faster/slower.

# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
trap finish EXIT

CONCURRENCY="${CONCURRENCY:-20}"
TOTAL="${TOTAL:-60}"
P95_BUDGET_MS="${P95_BUDGET_MS:-750}"

# Seed a handful of varied docs so search has something to rank.
log "seeding load-test corpus"
for i in $(seq 1 10); do
  http_post /api/v1/bucket/docs/doc \
    "{\"id\":\"load-$i\",\"text\":\"zero trust widget number $i\",\"metadata\":{\"i\":$i}}" \
    >/dev/null
done

# Fire TOTAL requests, CONCURRENCY at a time. xargs -P keeps the
# concurrency constant without needing GNU parallel (not always
# installed on CI images).
log "running $TOTAL requests with concurrency=$CONCURRENCY"
run_one() {
  # %{time_total} is seconds-as-float; %{http_code} is HTTP status.
  # The TAB separator is deliberate — makes awk parsing trivial.
  curl -sS -o /dev/null \
    -X POST "${BASE_URL}/api/v1/ai/search" \
    -H 'content-type: application/json' \
    -d '{"query":"zero trust","top_k":3}' \
    -w '%{http_code}\t%{time_total}\n'
}
export -f run_one
export BASE_URL

tmp="$_NB_STATE_DIR/load.tsv"
seq 1 "$TOTAL" | xargs -n1 -P "$CONCURRENCY" -I{} bash -c 'run_one' >"$tmp"

# How many succeeded?
ok=$(awk -F'\t' '$1==200' "$tmp" | wc -l | tr -d ' ')
assert_eq "$TOTAL" "$ok" "all $TOTAL requests succeeded"

# p95 latency. Sort times, pick the 95th percentile index.
p95_secs=$(awk -F'\t' '{print $2}' "$tmp" | sort -n \
  | awk -v n="$TOTAL" 'BEGIN{i=int(n*0.95)} NR==i{print; exit}')
if [[ -z "$p95_secs" ]]; then
  fail "could not compute p95 latency"
else
  p95_ms=$(awk -v s="$p95_secs" 'BEGIN{printf "%d", s*1000}')
  if (( p95_ms <= P95_BUDGET_MS )); then
    pass "p95 latency ${p95_ms}ms (budget ${P95_BUDGET_MS}ms)"
  else
    fail "p95 latency ${p95_ms}ms exceeds ${P95_BUDGET_MS}ms"
  fi
fi

# Cleanup.
for i in $(seq 1 10); do
  curl -sS -X DELETE "${BASE_URL}/api/v1/bucket/docs/doc/load-$i" >/dev/null || true
done
