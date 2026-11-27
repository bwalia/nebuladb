#!/usr/bin/env bash
# Staged server rollout on k8s (design 0009, k8s edition).
#
# What it guarantees: there is never a moment with zero serving nodes,
# so READS never 503 — nginx's read pool fails them over to the
# follower while the leader restarts. WRITES during the leader's
# recovery (minutes, on a ~21 GB snapshot) still get nginx's clean
# "retry shortly" 503: the follower is read-only until promoted, and
# this script does not promote (the compose orchestrator,
# scripts/rolling_upgrade.sh, does). Promotion-based k8s rollout is the
# remaining step for write availability.
#
# All three server Deployments (leader, follower, region-b) share one
# ConfigMap checksum and use `Recreate`, so a plain `helm upgrade`
# restarts them together — a window with no serving node, i.e. 503s.
# This script orders the rollout around `helm upgrade`:
#
#   preflight  Refuse to start (prod) unless the follower is Ready and
#              caught up — it is what serves while the leader restarts.
#              ALLOW_DOWNTIME=true overrides, loudly.
#   pause      Pause the leader Deployment so `helm upgrade` updates its
#              spec without restarting it.
#   finish     Wait for follower + region-b to roll and the follower to
#              catch up, then resume the leader and wait for it. If the
#              follower never comes back, the leader stays paused on the
#              old version, still serving — a failed deploy, not an outage.
#
# Usage: NS=prod scripts/k8s_staged_rollout.sh preflight|pause|finish
# Env:   NS (namespace), RELEASE (default nebuladb), ALLOW_DOWNTIME,
#        CATCHUP_TIMEOUT_SECS (default 2700: a 21 GB snapshot takes
#        ~23 min to load), LEADER_RECOVERY_TIMEOUT_SECS (default 3600),
#        HOLD (space-separated Deployments paused with the leader and
#        released the moment it goes down, e.g. the showcase when it is
#        switching API keys the old leader would reject),
#        DRY_RUN=1 (print only).
set -euo pipefail

NS="${NS:?NS (namespace) is required}"
RELEASE="${RELEASE:-nebuladb}"
LEADER="$RELEASE"
FOLLOWER="$RELEASE-follower"
REGION_B="$RELEASE-region-b"
ALLOW_DOWNTIME="${ALLOW_DOWNTIME:-false}"
CATCHUP_TIMEOUT_SECS="${CATCHUP_TIMEOUT_SECS:-2700}"
LEADER_RECOVERY_TIMEOUT_SECS="${LEADER_RECOVERY_TIMEOUT_SECS:-3600}"
HOLD="${HOLD:-}"

log() { printf '[staged-rollout] %s\n' "$*" >&2; }
run() { if [ "${DRY_RUN:-}" = 1 ]; then log "DRY_RUN: $*"; else "$@"; fi; }
k() { kubectl -n "$NS" "$@"; }

exists() { k get deploy "$1" >/dev/null 2>&1; }
replicas() { k get deploy "$1" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo 0; }
ready() { local r; r=$(k get deploy "$1" -o jsonpath='{.status.readyReplicas}' 2>/dev/null); echo "${r:-0}"; }

# 0 when the follower reports caught up with the leader's WAL.
follower_caught_up() {
  kubectl get --raw "/api/v1/namespaces/$NS/services/$FOLLOWER:rest/proxy/healthz/caught-up" 2>/dev/null \
    | grep -q '"caught_up":true'
}

has_follower() { exists "$FOLLOWER" && [ "$(replicas "$FOLLOWER")" -ge 1 ]; }

preflight() {
  if has_follower && [ "$(ready "$FOLLOWER")" -ge 1 ] && follower_caught_up; then
    log "follower Ready and caught up: reads fail over during the leader restart"
    return 0
  fi
  local why="follower not running"
  has_follower && why="follower not Ready or not caught up (ready=$(ready "$FOLLOWER"))"
  if [ "$ALLOW_DOWNTIME" = true ]; then
    log "WARNING: $why. ALLOW_DOWNTIME=true: proceeding; the leader restart WILL serve 503s."
    return 0
  fi
  log "REFUSING: $why. A leader restart now would 503 reads and writes."
  log "Fix the follower first (values-prod.yaml ha.withinRegion notes), or re-run"
  log "with ALLOW_DOWNTIME=true to accept a downtime window."
  return 1
}

pause() {
  if ! exists "$LEADER"; then log "no leader Deployment yet (first install): nothing to pause"; return 0; fi
  if ! has_follower; then log "no follower: leader not paused (ALLOW_DOWNTIME path)"; return 0; fi
  run k rollout pause "deploy/$LEADER"
  local d
  for d in $HOLD; do
    exists "$d" && run k rollout pause "deploy/$d"
  done
  log "leader${HOLD:+ and $HOLD} paused; helm upgrade will update their specs without restarting them"
}

# 0 once the leader answers /healthz with 200 (the boot listener serves
# 503 until recovery completes, while the pod already reports Ready).
leader_serving() {
  kubectl get --raw "/api/v1/namespaces/$NS/services/$LEADER:rest/proxy/healthz" 2>/dev/null \
    | grep -q '"status":"\(ok\|degraded\)"'
}

wait_rollout() {
  exists "$1" && [ "$(replicas "$1")" -ge 1 ] || return 0
  log "waiting for $1 to roll out"
  run k rollout status "deploy/$1" --timeout=15m
}

finish() {
  local paused
  paused=$(k get deploy "$LEADER" -o jsonpath='{.spec.paused}' 2>/dev/null || true)
  if [ "$paused" != true ]; then
    # No staging happened (no follower / first install): plain wait.
    wait_rollout "$LEADER"
    return 0
  fi
  wait_rollout "$FOLLOWER"
  wait_rollout "$REGION_B"
  if [ "${DRY_RUN:-}" != 1 ]; then
    local deadline=$(( $(date +%s) + CATCHUP_TIMEOUT_SECS ))
    until follower_caught_up; do
      if [ "$(date +%s)" -ge "$deadline" ]; then
        log "FAILED: follower not caught up after ${CATCHUP_TIMEOUT_SECS}s."
        log "Leader left PAUSED on its old version, still serving. Investigate the"
        log "follower, then: kubectl -n $NS rollout resume deploy/$LEADER"
        return 1
      fi
      sleep 10
    done
  fi
  log "follower caught up on the new version; releasing the leader"
  run k rollout resume "deploy/$LEADER"
  if [ -n "$HOLD" ] && [ "${DRY_RUN:-}" != 1 ]; then
    # Release the held Deployments once the old leader pod is gone, so
    # they never talk to it with config it doesn't know.
    k wait --for=delete pod -l "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=server" \
      --timeout=10m 2>/dev/null || true
  fi
  local d
  for d in $HOLD; do
    exists "$d" && run k rollout resume "deploy/$d"
  done
  run k rollout status "deploy/$LEADER" --timeout=15m
  if [ "${DRY_RUN:-}" != 1 ]; then
    local t0 deadline
    t0=$(date +%s); deadline=$(( t0 + LEADER_RECOVERY_TIMEOUT_SECS ))
    log "leader pod up; waiting for it to finish recovery (writes 503 until then)"
    until leader_serving; do
      if [ "$(date +%s)" -ge "$deadline" ]; then
        log "FAILED: leader still recovering after ${LEADER_RECOVERY_TIMEOUT_SECS}s (reads are on the follower)"
        return 1
      fi
      sleep 15
    done
    log "leader serving again after $(( $(date +%s) - t0 ))s of recovery"
  fi
  for d in $HOLD; do
    exists "$d" && run k rollout status "deploy/$d" --timeout=10m
  done
  log "leader rolled; staged rollout complete"
}

case "${1:-}" in
  preflight) preflight ;;
  pause) pause ;;
  finish) finish ;;
  *) echo "usage: NS=<ns> $0 preflight|pause|finish" >&2; exit 2 ;;
esac
