#!/usr/bin/env bash
# Rolling, one-at-a-time upgrade of the NebulaDB data nodes (design 0009
# §8). Replaces the old `docker compose up -d --build --remove-orphans`
# that recreated BOTH db nodes at once — the downtime bug.
#
# Invariant: at most ONE data node is offline at any moment. Before
# recreating a node we assert the OTHER node is serving; we abort the
# rollout rather than ever violate this.
#
# Sequence for the fixed-role 2-node compose topology (nebula-server =
# leader, nebula-follower = warm standby):
#
#   1. Build images (touches no running container).
#   2. If the server binary is unchanged: converge sidecars only, done
#      (the existing fast path — a docs/test-only merge must not recycle
#      the db nodes and restart WAL recovery).
#   3. Upgrade the FOLLOWER first: it serves ~1% of reads and no writes,
#      so recreating it is the cheapest. Wait until it is healthy AND
#      caught-up before proceeding.
#   4. Promote the follower to leader (POST /admin/promote) so writes
#      have a home, THEN upgrade the old leader. Writes in flight fail
#      over to the promoted node via nebula_write_pool (design 0009 §7);
#      reads were already failing over. Wait until the old leader is
#      healthy and caught-up as a follower of the new leader.
#
# Env:
#   COMPOSE_PROJECT       compose project name (default: nebuladb)
#   LEADER_HEALTH_URL     default http://localhost:18080  (host-mapped REST)
#   FOLLOWER_HEALTH_URL   default http://localhost:8081
#   OPERATOR_BEARER       bearer token for POST /admin/promote (optional)
#   T_READY_SECS          per-node readiness budget (default 180)
#   T_CATCHUP_SECS        per-node catch-up budget (default 180)
#   FORCE_ROLL=1          skip the binary-unchanged fast path and always
#                         perform the full rolling sequence (used by the
#                         zero-failure test, where images are pre-built)
#   DRY_RUN=1             print the plan, touch nothing
#
# Exit non-zero on any budget breach or invariant violation, leaving the
# not-yet-upgraded node serving (the cluster is still healthy on abort
# because we only ever touched one node).

set -Eeuo pipefail

LEADER_SVC="nebula-server"
FOLLOWER_SVC="nebula-follower"
LEADER_HEALTH_URL="${LEADER_HEALTH_URL:-http://localhost:18080}"
FOLLOWER_HEALTH_URL="${FOLLOWER_HEALTH_URL:-http://localhost:8081}"
T_READY_SECS="${T_READY_SECS:-180}"
T_CATCHUP_SECS="${T_CATCHUP_SECS:-180}"
DRY_RUN="${DRY_RUN:-0}"

log()  { printf '[rolling] %s\n' "$*"; }
note() { printf '::notice::%s\n' "$*"; }
die()  { printf '::error::%s\n' "$*" >&2; exit 1; }

run() {
  if [[ "$DRY_RUN" == "1" ]]; then
    log "DRY_RUN: $*"
  else
    "$@"
  fi
}

# HTTP status of a URL, or 000 on connection failure (never errors).
http_code() {
  curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$1" 2>/dev/null || echo "000"
}

# True when a node's /healthz is reachable and not 5xx — i.e. it is
# serving. /healthz returns 503 during WAL recovery, which we treat as
# "not serving yet".
is_serving() {
  local code
  code="$(http_code "$1/healthz")"
  [[ "$code" == "200" ]]
}

# Block until a node's /healthz is 200 or the budget elapses.
wait_ready() {
  local name="$1" url="$2" deadline=$(( $(date +%s) + T_READY_SECS ))
  log "waiting for $name readiness (budget ${T_READY_SECS}s) at $url/healthz"
  while :; do
    if is_serving "$url"; then
      note "$name: ready"
      return 0
    fi
    [[ $(date +%s) -lt $deadline ]] || die "$name not ready within ${T_READY_SECS}s — aborting rollout"
    sleep 3
  done
}

# Block until a node reports caught-up (design 0009 §6) or the budget
# elapses. /healthz/caught-up is 200 when caught up, 503 while trailing.
wait_caught_up() {
  local name="$1" url="$2" deadline=$(( $(date +%s) + T_CATCHUP_SECS ))
  log "waiting for $name catch-up (budget ${T_CATCHUP_SECS}s) at $url/healthz/caught-up"
  while :; do
    if [[ "$(http_code "$url/healthz/caught-up")" == "200" ]]; then
      note "$name: caught up"
      return 0
    fi
    [[ $(date +%s) -lt $deadline ]] || die "$name did not catch up within ${T_CATCHUP_SECS}s — aborting rollout"
    sleep 3
  done
}

# NO-SIMULTANEOUS-OFFLINE guard: assert `other` is serving before we
# recreate `node`. Abort (don't recreate) if not — never make progress
# by violating the invariant.
assert_other_serving() {
  local about_to_recreate="$1" other_name="$2" other_url="$3"
  if ! is_serving "$other_url"; then
    die "refusing to recreate $about_to_recreate: $other_name is not serving (would leave zero serving nodes)"
  fi
  note "guard ok: $other_name serving while $about_to_recreate is recreated"
}

# Recreate exactly ONE compose service on the freshly built image,
# without touching the other db node.
recreate_one() {
  local svc="$1"
  note "recreating $svc (one node only)"
  run docker compose up -d --build --no-deps "$svc"
}

promote() {
  local url="$1"
  local args=(-s -o /dev/null -w '%{http_code}' --max-time 10 -X POST "$url/api/v1/admin/promote")
  [[ -n "${OPERATOR_BEARER:-}" ]] && args+=(-H "authorization: Bearer ${OPERATOR_BEARER}")
  local code
  if [[ "$DRY_RUN" == "1" ]]; then
    log "DRY_RUN: POST $url/api/v1/admin/promote"
    return 0
  fi
  code="$(curl "${args[@]}" 2>/dev/null || echo 000)"
  [[ "$code" == "200" ]] || die "promote failed at $url (HTTP $code)"
  note "promoted node at $url to leader"
}

# ---- main ----------------------------------------------------------------

log "building images (no running container touched)"
run docker compose build

# Fast path: if the server binary is byte-identical, skip the db dance
# entirely and just converge sidecars. Mirrors the pre-existing
# binary-hash gate in deploy.yml so docs/test-only merges don't recycle
# the data nodes.
cur_img="$(docker inspect "$LEADER_SVC" --format '{{.Image}}' 2>/dev/null || echo "")"
new_img="$(docker image inspect nebuladb/server:dev --format '{{.Id}}' 2>/dev/null || echo "")"
bin_hash() {
  [ -n "$1" ] || { echo ""; return; }
  docker run --rm --entrypoint sha256sum "$1" /usr/local/bin/nebula-server 2>/dev/null | awk '{print $1}'
}
if [[ "${FORCE_ROLL:-0}" != "1" && -n "$cur_img" && -n "$new_img" ]]; then
  if [[ "$(bin_hash "$cur_img")" == "$(bin_hash "$new_img")" && -n "$(bin_hash "$new_img")" ]]; then
    note "server binary unchanged — converging sidecars only, no rolling upgrade"
    sidecars="$(docker compose config --services | grep -vxE "${LEADER_SVC}|${FOLLOWER_SVC}" || true)"
    # shellcheck disable=SC2086
    run docker compose up -d --build $sidecars
    run docker compose up -d --no-recreate "$LEADER_SVC" "$FOLLOWER_SVC"
    note "rolling upgrade: nothing to roll"
    exit 0
  fi
fi

note "server binary changed — performing rolling upgrade"

# Step 1: upgrade the FOLLOWER. The leader must be serving throughout.
assert_other_serving "$FOLLOWER_SVC" "$LEADER_SVC" "$LEADER_HEALTH_URL"
recreate_one "$FOLLOWER_SVC"
wait_ready "$FOLLOWER_SVC" "$FOLLOWER_HEALTH_URL"
wait_caught_up "$FOLLOWER_SVC" "$FOLLOWER_HEALTH_URL"

# Step 2: promote the upgraded follower so writes have a home, THEN
# upgrade the old leader. The promoted node must be serving while the
# old leader is recreated.
promote "$FOLLOWER_HEALTH_URL"
assert_other_serving "$LEADER_SVC" "$FOLLOWER_SVC" "$FOLLOWER_HEALTH_URL"
recreate_one "$LEADER_SVC"
wait_ready "$LEADER_SVC" "$LEADER_HEALTH_URL"
wait_caught_up "$LEADER_SVC" "$LEADER_HEALTH_URL"

note "rolling upgrade complete: both nodes on the new image, never two offline"
docker compose ps
