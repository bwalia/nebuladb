#!/usr/bin/env bash
# Prod watchdog: checks that are true problems, not noise.
#
#   health  Every server Deployment the chart asks for is Ready; the
#           leader is not left paused by a failed staged rollout; the
#           follower is caught up; the public /healthz answers "ok" (not
#           "degraded" — a failing embedding endpoint).
#   drift   What prod runs differs from what the repo's chart renders
#           for it (values changed but never deployed, or hand edits
#           with kubectl). Prod ran a stale config for a week because
#           nothing checked this.
#
# Prints one line per problem and exits 1 if there are any, so the
# workflow can post them to Slack. Usage:
#   NS=prod scripts/prod_watch.sh health|drift
set -uo pipefail

NS="${NS:-prod}"
RELEASE="${RELEASE:-nebuladb}"
PUBLIC_URL="${PUBLIC_URL:-https://showcase.nebuladb.net}"
CHART="${CHART:-./deploy/helm/nebuladb}"
problems=()
problem() { problems+=("$*"); }
k() { kubectl -n "$NS" "$@"; }

health() {
  local d spec ready paused
  for d in "$RELEASE" "$RELEASE-follower" "$RELEASE-region-b" "$RELEASE-showcase"; do
    spec=$(k get deploy "$d" -o jsonpath='{.spec.replicas}' 2>/dev/null) || continue
    ready=$(k get deploy "$d" -o jsonpath='{.status.readyReplicas}' 2>/dev/null)
    if [ "${spec:-0}" -ge 1 ] && [ "${ready:-0}" -lt "$spec" ]; then
      problem "$d: ${ready:-0}/$spec ready"
    fi
  done
  # The chart asks for a follower but none is scheduled — prod has no
  # failover target, and any leader restart serves 503s.
  if [ "$(k get deploy "$RELEASE-follower" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo 0)" = 0 ]; then
    problem "$RELEASE-follower is scaled to 0: no failover target, a leader restart will serve 503s"
  fi
  paused=$(k get deploy "$RELEASE" -o jsonpath='{.spec.paused}' 2>/dev/null)
  if [ "$paused" = true ]; then
    problem "$RELEASE (leader) is PAUSED — a staged rollout stopped half way; it serves the old version"
  fi
  if k get deploy "$RELEASE-follower" >/dev/null 2>&1; then
    if ! kubectl get --raw "/api/v1/namespaces/$NS/services/$RELEASE-follower:rest/proxy/healthz/caught-up" 2>/dev/null \
        | grep -q '"caught_up":true'; then
      problem "$RELEASE-follower is not caught up with the leader"
    fi
  fi
  local body status
  body=$(curl -fsS -m 10 "$PUBLIC_URL/healthz" 2>&1) || { problem "$PUBLIC_URL/healthz failed: ${body:0:200}"; body=""; }
  if [ -n "$body" ]; then
    status=$(printf '%s' "$body" | sed -n 's/.*"status":"\([a-z]*\)".*/\1/p')
    if [ "$status" != ok ]; then
      local down
      down=$(printf '%s' "$body" | grep -o '"name":"[^"]*","fallback":false,"healthy":false' | sed 's/"name":"\([^"]*\)".*/\1/' | paste -sd, -)
      problem "$PUBLIC_URL/healthz status=$status${down:+ (embedding endpoints down: $down)}"
    fi
  fi
}

drift() {
  local tag out
  # Render with the image tags prod actually runs, so a pending image
  # bump is not reported as drift — config is what we're after.
  tag=$(k get deploy "$RELEASE" -o jsonpath='{.spec.template.spec.containers[0].image}' | sed 's/.*://')
  local stag
  stag=$(k get deploy "$RELEASE-showcase" -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null | sed 's/.*://')
  helm dependency build "$CHART" >/dev/null 2>&1 || true
  # NEBULA_DEPLOYED_AT and similar per-render stamps change every
  # render; diff ignores them via the filter below.
  out=$(helm template "$RELEASE" "$CHART" -n "$NS" -f "$CHART/values-$NS.yaml" \
        --set server.image.tag="$tag" --set showcase.image.tag="${stag:-$tag}" 2>/dev/null \
      | kubectl diff -n "$NS" -f - 2>/dev/null \
      | grep -E '^[-+] ' \
      | grep -v -E 'NEBULA_DEPLOYED_AT|deployed-at|generation:|checksum/|helm.sh/chart|app.kubernetes.io/version|deployment.kubernetes.io/revision|meta.helm.sh/release-|kubectl.kubernetes.io/restartedAt|nebuladb.net/nginx-config|^[-+] +annotations:$' )
  if [ -n "$out" ]; then
    problem "prod differs from the repo chart (values not deployed, or hand edits):"$'\n'"$(printf '%s\n' "$out" | head -40)"
  fi
}

case "${1:-}" in
  health) health ;;
  drift) drift ;;
  *) echo "usage: NS=<ns> $0 health|drift" >&2; exit 2 ;;
esac

if [ "${#problems[@]}" -gt 0 ]; then
  printf '%s\n' "${problems[@]}"
  exit 1
fi
echo "ok"
