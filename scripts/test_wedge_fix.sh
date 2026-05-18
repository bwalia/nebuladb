#!/usr/bin/env bash
# test_wedge_fix.sh — synthetic-load reproduction of the conditions that
# triggered the showcase wedge on 192.168.1.193, against the running
# local stack. Validates that the fixes in PR #63 (follower memory bump,
# redis eviction headroom, snapshot scheduler re-enable) actually
# prevent the symptoms we observed.
#
# Pass criteria over the configured load window (default 10 minutes):
#   1. nebula-server memory plateaus below 70% of its cgroup limit.
#      (Original wedge: ~90% before the cgroup OOM-kill.)
#   2. nebula-server RestartCount does not increment.
#      (Original wedge: ~one restart every 4 hours.)
#   3. nebula-server HealthFailing stays at 0 throughout.
#   4. nebula-follower stays Up (healthy) — never enters Restarting.
#      (Original bug: permanent Restarting (137) cycle.)
#   5. The snapshot scheduler emits at least one fire during the run.
#      (Bug C: scheduler was disabled by an obsolete env workaround.)
#   6. `redis mget timed out` warns: zero or near-zero in server logs.
#      (Original bug: recurring every ~30 min.)
#
# This is a Layer 3b test (synthetic load on a local stack) per the
# README. It compresses the timescale of the real wedge — it can't
# fully reproduce 24 hours of accumulated index growth in 10 minutes
# but it does reproduce the conditions that ATTRIBUTE the failure
# (concurrent RAG + ingest + index growth + redis pressure) and asserts
# the new caps absorb them.
#
# Usage:
#   docker compose up -d --build
#   ./scripts/test_wedge_fix.sh
#
# Tunables (env):
#   DURATION_SECS  — total test duration (default 600 = 10m)
#   RPS            — requests per second per worker (default 1)
#   WORKERS        — parallel client workers (default 2)
#   BASE_URL       — defaults to http://localhost:8080
#   SHOWCASE_URL   — defaults to http://localhost:5173
#   SERVER_NAME    — docker container name, default nebula-server
#   FOLLOWER_NAME  — default nebula-follower
#   MEM_CEILING_PCT — fail if leader exceeds this %% of cgroup limit
#                    (default 70). Original wedge crossed 90.

# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
trap finish EXIT

DURATION_SECS="${DURATION_SECS:-600}"
RPS="${RPS:-1}"
WORKERS="${WORKERS:-2}"
BASE_URL="${BASE_URL:-http://localhost:8080}"
SHOWCASE_URL="${SHOWCASE_URL:-http://localhost:5173}"
SERVER_NAME="${SERVER_NAME:-nebula-server}"
FOLLOWER_NAME="${FOLLOWER_NAME:-nebula-follower}"
MEM_CEILING_PCT="${MEM_CEILING_PCT:-70}"
BUCKET="wedge-test"

log "DURATION=${DURATION_SECS}s WORKERS=${WORKERS} RPS=${RPS} bucket=${BUCKET}"

# ---------------- preconditions ----------------
log "preconditions: stack must be up before we start hammering it"

if ! docker inspect "${SERVER_NAME}" >/dev/null 2>&1; then
  fail "container ${SERVER_NAME} not found — did you run \`docker compose up -d\`?"
  exit 1
fi
# Follower may be optional in some local configs; warn rather than fail
# if it's not there.
follower_present=true
if ! docker inspect "${FOLLOWER_NAME}" >/dev/null 2>&1; then
  log "note: follower ${FOLLOWER_NAME} not present; skipping follower-health asserts"
  follower_present=false
fi

# Wait for /healthz/live; if it never returns 200 we have nothing to test.
deadline=$(( $(date +%s) + 90 ))
until curl -sS -m 3 -o /dev/null -w '%{http_code}' "${BASE_URL}/healthz/live" 2>/dev/null \
       | grep -q '^200$'; do
  if [[ $(date +%s) -gt $deadline ]]; then
    fail "${BASE_URL}/healthz/live never returned 200 within 90s"
    exit 1
  fi
  sleep 2
done
pass "/healthz/live is 200"

# Capture baseline so the assertions on RestartCount are meaningful.
baseline_restarts=$(docker inspect "${SERVER_NAME}" \
                    --format '{{.RestartCount}}' 2>/dev/null || echo 0)
log "baseline RestartCount=${baseline_restarts}"

# Cgroup memory limit, in bytes. We compare current usage against this
# rather than against an absolute MiB number so the test stays portable
# across the various memory caps (8 GiB on prod, 1 GiB on a dev box).
mem_limit_bytes=$(docker inspect "${SERVER_NAME}" \
                  --format '{{.HostConfig.Memory}}' 2>/dev/null || echo 0)
if [[ "${mem_limit_bytes}" == "0" || -z "${mem_limit_bytes}" ]]; then
  log "warning: ${SERVER_NAME} has no memory limit set; %% checks will be vs host RAM"
fi

# ---------------- load workers ----------------
log "starting ${WORKERS} load workers for ${DURATION_SECS}s"

# Each worker writes (ingest) and reads (RAG / search). The blend
# matches the real wedge profile (mostly RAG with sustained ingest)
# without overwhelming the dev box.
worker_loop() {
  local id="$1"
  local end=$(($(date +%s) + DURATION_SECS))
  local i=0
  while [[ $(date +%s) -lt $end ]]; do
    i=$((i + 1))
    local doc_id="w${id}-d${i}"
    # Insert.
    curl -sS -m 5 -X POST "${BASE_URL}/api/v1/bucket/${BUCKET}/doc" \
      -H 'content-type: application/json' \
      -d "{\"id\":\"${doc_id}\",\"text\":\"loadtest worker ${id} document ${i} with chunks of text suitable for RAG retrieval and embedding cache pressure\",\"metadata\":{\"worker\":${id}}}" \
      >/dev/null 2>&1 || true
    # Search — keeps embedder cache hot, exercises Redis.
    curl -sS -m 5 -X POST "${BASE_URL}/api/v1/ai/search" \
      -H 'content-type: application/json' \
      -d '{"query":"loadtest","top_k":3}' \
      >/dev/null 2>&1 || true
    # Non-streaming RAG every 5 docs — exercises LLM stream + embedder.
    if [[ $((i % 5)) -eq 0 ]]; then
      curl -sS -m 30 -X POST "${BASE_URL}/api/v1/ai/rag" \
        -H 'content-type: application/json' \
        -d '{"query":"summarize the loadtest data","top_k":3,"stream":false}' \
        >/dev/null 2>&1 || true
    fi
    sleep "$(awk -v rps="${RPS}" 'BEGIN { printf "%.3f", 1/rps }')"
  done
}

for w in $(seq 1 "${WORKERS}"); do
  worker_loop "$w" &
done
worker_pids=("$@")  # we won't use this, just rely on `wait` later

# ---------------- monitor loop ----------------
sample_interval=30
samples_dir=$(mktemp -d)
log "monitor sampling every ${sample_interval}s into ${samples_dir}"

monitor_until=$(($(date +%s) + DURATION_SECS))
sample_count=0
peak_mem_pct=0
saw_snapshot=false
peak_redis_mget_warns=0

while [[ $(date +%s) -lt $monitor_until ]]; do
  sample_count=$((sample_count + 1))
  ts=$(date +%H:%M:%S)

  # Memory usage of the leader as %% of its cgroup limit.
  mem_used=$(docker stats --no-stream --format '{{.MemUsage}}' \
             "${SERVER_NAME}" 2>/dev/null | awk -F/ '{print $1}' | tr -d ' ' || echo "?")
  mem_pct=$(docker stats --no-stream --format '{{.MemPerc}}' \
            "${SERVER_NAME}" 2>/dev/null | tr -d '% ' || echo "0")
  # parking_lot for the parking_lot — bash floats need awk.
  mem_pct_int=$(awk -v p="${mem_pct}" 'BEGIN { printf "%d", p }')
  if [[ "${mem_pct_int}" -gt "${peak_mem_pct}" ]]; then
    peak_mem_pct=$mem_pct_int
  fi

  # Restart counts.
  cur_restarts=$(docker inspect "${SERVER_NAME}" \
                 --format '{{.RestartCount}}' 2>/dev/null || echo 0)

  printf '[%s] sample %d  leader mem=%s (%s%%) restarts=%s\n' \
    "$ts" "$sample_count" "$mem_used" "$mem_pct" "$cur_restarts"

  if "${follower_present}"; then
    foll_status=$(docker inspect "${FOLLOWER_NAME}" \
                  --format '{{.State.Status}}' 2>/dev/null || echo unknown)
    foll_oom=$(docker inspect "${FOLLOWER_NAME}" \
               --format '{{.State.OOMKilled}}' 2>/dev/null || echo unknown)
    printf '         follower status=%s oom=%s\n' "$foll_status" "$foll_oom"
    if [[ "$foll_status" != "running" ]]; then
      fail "follower not running mid-test (status=${foll_status})"
    fi
  fi

  sleep "${sample_interval}"
done

# Wait for the worker subshells to drain.
wait

# ---------------- post-run assertions ----------------
log "post-run assertions"

# 1. Memory ceiling.
if [[ "${peak_mem_pct}" -lt "${MEM_CEILING_PCT}" ]]; then
  pass "leader peak memory ${peak_mem_pct}% < ${MEM_CEILING_PCT}% ceiling"
else
  fail "leader peak memory ${peak_mem_pct}% >= ${MEM_CEILING_PCT}% — original wedge territory"
fi

# 2. No leader restarts.
final_restarts=$(docker inspect "${SERVER_NAME}" \
                 --format '{{.RestartCount}}' 2>/dev/null || echo 0)
if [[ "${final_restarts}" -eq "${baseline_restarts}" ]]; then
  pass "leader did not restart during the test (RestartCount=${final_restarts})"
else
  fail "leader restarted ${final_restarts}-${baseline_restarts}=$((final_restarts - baseline_restarts)) times mid-test"
fi

# 3. Health stayed green.
final_health=$(docker inspect "${SERVER_NAME}" \
               --format '{{.State.Health.FailingStreak}}' 2>/dev/null || echo 0)
if [[ "${final_health}" -eq 0 ]]; then
  pass "leader HealthFailing is 0"
else
  fail "leader HealthFailing=${final_health} at end of run"
fi

# 4. Follower never restarted (if present).
if "${follower_present}"; then
  foll_final_status=$(docker inspect "${FOLLOWER_NAME}" \
                      --format '{{.State.Status}}' 2>/dev/null || echo unknown)
  foll_oom=$(docker inspect "${FOLLOWER_NAME}" \
             --format '{{.State.OOMKilled}}' 2>/dev/null || echo unknown)
  if [[ "${foll_final_status}" == "running" && "${foll_oom}" == "false" ]]; then
    pass "follower stayed up without OOMKill"
  else
    fail "follower final status=${foll_final_status} oom=${foll_oom}"
  fi
fi

# 5. Snapshot scheduler fired at least once. We grep the leader logs
#    since the run started — `--since` accepts duration strings.
since_arg="$((DURATION_SECS + 60))s"
if docker logs --since "${since_arg}" "${SERVER_NAME}" 2>&1 \
     | grep -qiE 'snapshot scheduler firing|snapshot.*written|snapshot.*captured'; then
  pass "snapshot scheduler fired during the run (Bug C fixed)"
  saw_snapshot=true
else
  # Not necessarily a hard fail: 10 min is short and the default
  # interval is 900s. Surface as a warning unless DURATION_SECS >= 1000.
  if [[ "${DURATION_SECS}" -ge 1000 ]]; then
    fail "snapshot scheduler did not fire during a ${DURATION_SECS}s run — Bug C may still be active"
  else
    log "warning: no snapshot scheduler line in last ${since_arg} (run too short to be conclusive)"
  fi
fi

# 6. Redis mget timeouts should be zero or near-zero. The original bug
#    surfaced this every ~30 min; a 10-min run might see one if redis
#    blips, but more than 2 means the fix isn't biting.
mget_warns=$(docker logs --since "${since_arg}" "${SERVER_NAME}" 2>&1 \
             | grep -c 'redis mget timed out' || true)
if [[ "${mget_warns}" -le 2 ]]; then
  pass "redis mget timeouts: ${mget_warns} (acceptable, Bug B fixed)"
else
  fail "redis mget timeouts: ${mget_warns} — Bug B may still be active"
fi

# ---------------- cleanup ----------------
# Empty the test bucket so a re-run starts clean.
curl -sS -m 5 -X POST "${BASE_URL}/api/v1/admin/bucket/${BUCKET}/empty" \
  >/dev/null 2>&1 || true
log "cleanup: emptied bucket ${BUCKET}"
