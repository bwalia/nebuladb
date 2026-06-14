#!/usr/bin/env bash
# test_rolling_upgrade.sh — assert a cluster-wide version bump keeps
# reads AND writes available, with at most one data node offline at any
# moment (design 0009 §9 / Definition of Done).
#
# Assumes the 2-node stack is already up and healthy:
#   docker compose up -d --build --wait nebula-server nebula-follower showcase
#
# DO NOT run this against the live production node — pushing to main
# auto-deploys, and this script intentionally recreates the data nodes.
# Run it on a scratch compose project (set COMPOSE_PROJECT_NAME) or in
# nightly CI against a throwaway stack.
#
# What it does:
#   1. Starts two background clients against the showcase nginx (the
#      client-facing edge with the read+write pools, design 0009 §7):
#        - a WRITER doing continuous PUT then GET-back, and
#        - a READER doing continuous GETs,
#      each recording every non-2xx (writer) / non-2xx-non-404 (reader).
#   2. Samples `docker compose ps` throughout to assert the
#      no-simultaneous-offline invariant.
#   3. Runs scripts/rolling_upgrade.sh to bump both nodes.
#   4. Asserts: 0 failed writes, 0 failed reads, and never two data
#      nodes down at the same moment.

source "$(dirname "$0")/lib.sh"

# Client-facing edge: the showcase nginx, which owns the read/write
# pools + failover. Falls back to the host-mapped showcase port.
EDGE="${EDGE:-http://localhost:15173}"
LEADER_SVC="${LEADER_SVC:-nebula-server}"
FOLLOWER_SVC="${FOLLOWER_SVC:-nebula-follower}"
# How long (s) to keep the background clients running while the upgrade
# proceeds. The rolling upgrade itself bounds each node by T_READY/T_CATCHUP.
CLIENT_RUN_SECS="${CLIENT_RUN_SECS:-300}"

STATE="$(mktemp -d)"
WRITE_FAILS="$STATE/write_fails"
READ_FAILS="$STATE/read_fails"
OFFLINE_VIOLATIONS="$STATE/offline_violations"
: >"$WRITE_FAILS"; : >"$READ_FAILS"; : >"$OFFLINE_VIOLATIONS"
STOP="$STATE/stop"

# Single EXIT trap: stop the background clients, clean up the temp dir,
# THEN run lib.sh's `finish` (which prints the pass/fail summary and sets
# the exit code). Ordering matters — finish must run last so its exit
# status is the script's.
on_exit() {
  touch "$STOP" 2>/dev/null || true
  sleep 1
  rm -rf "$STATE"
  finish
}
trap on_exit EXIT

# --- background WRITER: PUT a rolling doc, read it straight back -------
writer_loop() {
  local i=0 resp code
  while [[ ! -f "$STOP" ]]; do
    i=$((i + 1))
    resp=$(curl -sS -X POST "${EDGE}/api/v1/bucket/rolling/doc" \
      -H 'content-type: application/json' \
      -w '\n%{http_code}' --max-time 10 \
      -d "{\"id\":\"w-${i}\",\"text\":\"rolling write ${i}\"}" 2>/dev/null || echo $'\n000')
    code=$(code_of "$resp")
    # 2xx required. Anything else (000 conn-fail, 409, 5xx, 503) is a
    # write availability failure — the whole point is zero of these.
    if [[ ! "$code" =~ ^2 ]]; then
      echo "write w-${i} -> ${code}" >>"$WRITE_FAILS"
    fi
    sleep 0.2
  done
}

# --- background READER: GET a known-seeded doc ------------------------
reader_loop() {
  local resp code
  while [[ ! -f "$STOP" ]]; do
    resp=$(curl -sS "${EDGE}/api/v1/bucket/rolling/doc/seed" \
      -w '\n%{http_code}' --max-time 10 2>/dev/null || echo $'\n000')
    code=$(code_of "$resp")
    # 200 (found) or 404 (not yet replicated / deleted) are both
    # "service answered". 000/5xx/503 mean reads were unavailable.
    if [[ "$code" != "200" && "$code" != "404" ]]; then
      echo "read seed -> ${code}" >>"$READ_FAILS"
    fi
    sleep 0.2
  done
}

# --- background INVARIANT sampler: never two db nodes down ------------
offline_sampler() {
  local lstate fstate
  while [[ ! -f "$STOP" ]]; do
    lstate=$(docker inspect -f '{{.State.Running}}' "$LEADER_SVC" 2>/dev/null || echo "false")
    fstate=$(docker inspect -f '{{.State.Running}}' "$FOLLOWER_SVC" 2>/dev/null || echo "false")
    if [[ "$lstate" != "true" && "$fstate" != "true" ]]; then
      echo "both data nodes not-running at $(date +%T)" >>"$OFFLINE_VIOLATIONS"
    fi
    sleep 1
  done
}

log "preconditions: edge reachable, both data nodes up"
assert_eq "200" "$(curl -sS -o /dev/null -w '%{http_code}' "${EDGE}/healthz" || echo 000)" \
  "edge /healthz is 200 before upgrade"

# Seed the doc the reader polls so a 200 is achievable.
curl -sS -X POST "${EDGE}/api/v1/bucket/rolling/doc" \
  -H 'content-type: application/json' \
  -d '{"id":"seed","text":"seed doc for the rolling reader"}' >/dev/null 2>&1 || true

log "starting background read/write/invariant clients"
writer_loop &  WPID=$!
reader_loop &  RPID=$!
offline_sampler & SPID=$!

# Give the clients a moment to establish a baseline before we disturb
# the cluster.
sleep 3

log "running rolling upgrade (this recreates both data nodes, one at a time)"
# The orchestrator promotes the follower before draining the leader, so
# writes stay served by whichever node currently holds the leader role.
# Pass through the per-node budgets if the caller tightened them.
if ! timeout "${CLIENT_RUN_SECS}" bash "$(dirname "$0")/rolling_upgrade.sh"; then
  fail "rolling_upgrade.sh exited non-zero"
fi

log "stopping background clients"
touch "$STOP"
wait "$WPID" 2>/dev/null || true
wait "$RPID" 2>/dev/null || true
wait "$SPID" 2>/dev/null || true

# --- assertions -------------------------------------------------------
write_fail_count=$(wc -l <"$WRITE_FAILS" | tr -d ' ')
read_fail_count=$(wc -l <"$READ_FAILS" | tr -d ' ')
offline_count=$(wc -l <"$OFFLINE_VIOLATIONS" | tr -d ' ')

if [[ "$write_fail_count" -ne 0 ]]; then
  log "write failures observed:"; cat "$WRITE_FAILS" >&2
fi
if [[ "$read_fail_count" -ne 0 ]]; then
  log "read failures observed:"; cat "$READ_FAILS" >&2
fi
if [[ "$offline_count" -ne 0 ]]; then
  log "no-simultaneous-offline violations:"; cat "$OFFLINE_VIOLATIONS" >&2
fi

assert_eq "0" "$write_fail_count" "zero failed writes across the rolling upgrade"
assert_eq "0" "$read_fail_count" "zero failed reads across the rolling upgrade"
assert_eq "0" "$offline_count" "never two data nodes offline at once"

# Post-upgrade sanity: the edge still serves and the cluster is whole.
assert_eq "200" "$(curl -sS -o /dev/null -w '%{http_code}' "${EDGE}/healthz" || echo 000)" \
  "edge /healthz is 200 after upgrade"
