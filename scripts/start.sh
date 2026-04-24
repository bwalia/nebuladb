#!/usr/bin/env bash
# start.sh — one-command dev bootstrap for the NebulaDB showcase.
#
# What it does:
#   1. Picks free host ports for REST / gRPC / pgwire / showcase /
#      Grafana / Prometheus (so devs with common ports already taken
#      don't have to untangle "port already in use" errors).
#   2. Builds + runs the compose stack with `--wait` so we don't
#      move on until every container's healthcheck is green.
#   3. Runs scripts/seed.sh to populate the knowledge corpus.
#   4. Prints clickable URLs and an at-a-glance status table.
#
# Usage:
#   ./scripts/start.sh                # build, up, seed, open-ready
#   ./scripts/start.sh --no-seed      # skip seeding
#   ./scripts/start.sh --no-showcase  # skip building the React app
#   ./scripts/start.sh --model phi3   # use a smaller LLM model
#   ./scripts/start.sh --down         # stop + remove volumes
#   ./scripts/start.sh --logs         # tail combined logs after boot
#   ./scripts/start.sh --fresh        # down + up (no volume wipe)
#   ./scripts/start.sh --wipe         # down -v + up (wipes models, data)
#
# Env overrides (rarely needed — the script picks free ports):
#   NEBULA_REST_HOST_PORT   NEBULA_GRPC_HOST_PORT   NEBULA_PG_HOST_PORT
#   NEBULA_SHOWCASE_PORT    NEBULA_GRAFANA_PORT     NEBULA_PROM_PORT
#
# Exit codes:
#   0 on success; non-zero on boot failure or seed failure.

set -Eeuo pipefail

# shellcheck source=scripts/lib.sh
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# ---------- colors ----------
if [[ -t 1 ]]; then
  C_GREEN=$'\033[32m'; C_YELLOW=$'\033[33m'; C_BLUE=$'\033[34m'
  C_RED=$'\033[31m'; C_BOLD=$'\033[1m'; C_RESET=$'\033[0m'
else
  C_GREEN=""; C_YELLOW=""; C_BLUE=""; C_RED=""; C_BOLD=""; C_RESET=""
fi
say()  { printf '%s[*]%s %s\n' "$C_BLUE" "$C_RESET" "$*"; }
ok()   { printf '%s[✓]%s %s\n' "$C_GREEN" "$C_RESET" "$*"; }
warn() { printf '%s[!]%s %s\n' "$C_YELLOW" "$C_RESET" "$*"; }
die()  { printf '%s[✗]%s %s\n' "$C_RED" "$C_RESET" "$*" >&2; exit 1; }

# ---------- arg parsing ----------
DO_SEED=1
DO_SHOWCASE=1
DO_LOGS=0
ACTION="up"
MODEL="${OLLAMA_CHAT_MODEL:-tinyllama}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-seed)      DO_SEED=0 ;;
    --no-showcase)  DO_SHOWCASE=0 ;;
    --logs)         DO_LOGS=1 ;;
    --down)         ACTION="down" ;;
    --fresh)        ACTION="fresh" ;;
    --wipe)         ACTION="wipe" ;;
    --model)        shift; MODEL="$1" ;;
    -h|--help)
      sed -n '1,40p' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) die "unknown arg: $1 (try --help)" ;;
  esac
  shift
done

# ---------- sanity checks ----------
command -v docker >/dev/null || die "docker is required"
docker compose version >/dev/null 2>&1 || die "docker compose plugin is required (v2)"

# ---------- teardown paths, handled up front ----------
if [[ "$ACTION" == "down" ]]; then
  say "stopping stack"
  docker compose down -v --remove-orphans
  ok "stack stopped; volumes removed"
  exit 0
fi
if [[ "$ACTION" == "fresh" ]]; then
  say "restarting stack (keeping volumes so model weights survive)"
  docker compose down --remove-orphans >/dev/null || true
  ACTION="up"
fi
if [[ "$ACTION" == "wipe" ]]; then
  say "wiping stack (models, Redis, Prometheus, Grafana data all gone)"
  docker compose down -v --remove-orphans >/dev/null || true
  ACTION="up"
fi

# ---------- port picking ----------
# Ask the kernel for a free port by briefly binding 127.0.0.1:0. If
# the user supplied a port via env we respect it; otherwise we test
# the compose default and, if taken, fall back to the kernel pick.
free_port() {
  python3 - <<'PY'
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

port_in_use() {
  local p="$1"
  # `lsof` isn't universal; try `nc -z` first, fall back to python.
  if command -v nc >/dev/null 2>&1; then
    nc -z 127.0.0.1 "$p" >/dev/null 2>&1
  else
    python3 -c "import socket,sys; s=socket.socket(); sys.exit(0 if s.connect_ex(('127.0.0.1',$p))==0 else 1)"
  fi
}

pick_port() {
  # pick_port VAR_NAME DEFAULT_PORT
  local var="$1" default="$2" current
  current="${!var:-}"
  if [[ -n "$current" ]]; then
    printf '%s' "$current"
    return
  fi
  if port_in_use "$default"; then
    warn "host port $default is in use; picking a free one for $var" >&2
    free_port
  else
    printf '%s' "$default"
  fi
}

export NEBULA_REST_HOST_PORT="$(pick_port NEBULA_REST_HOST_PORT 8080)"
export NEBULA_GRPC_HOST_PORT="$(pick_port NEBULA_GRPC_HOST_PORT 50051)"
export NEBULA_PG_HOST_PORT="$(pick_port NEBULA_PG_HOST_PORT 5433)"
export NEBULA_SHOWCASE_PORT="$(pick_port NEBULA_SHOWCASE_PORT 5173)"
# Grafana / Prometheus host ports aren't wired to env vars in compose
# yet (they're static). We print them as-is but note the conflict.
export NEBULA_GRAFANA_PORT="${NEBULA_GRAFANA_PORT:-3000}"
export NEBULA_PROM_PORT="${NEBULA_PROM_PORT:-9090}"

export OLLAMA_CHAT_MODEL="$MODEL"
export OLLAMA_EMBED_MODEL="${OLLAMA_EMBED_MODEL:-nomic-embed-text}"

# ---------- build + up ----------
services=(ollama ollama-init nebula-server redis prometheus grafana)
if [[ $DO_SHOWCASE -eq 1 ]]; then
  services+=(showcase)
fi

say "starting: ${services[*]}"
say "model: chat=$OLLAMA_CHAT_MODEL  embed=$OLLAMA_EMBED_MODEL"
say "ports: rest=$NEBULA_REST_HOST_PORT grpc=$NEBULA_GRPC_HOST_PORT pg=$NEBULA_PG_HOST_PORT ui=$NEBULA_SHOWCASE_PORT"

# `--wait` blocks until every container's healthcheck is green or
# the timeout fires. 15 minutes covers a cold Ollama model pull on
# a home connection; adjust if your pipe is fast.
if ! docker compose up -d --build --wait --wait-timeout 900 "${services[@]}"; then
  warn "compose up did not reach 'healthy' within the timeout"
  docker compose ps
  die "boot failed — check 'docker compose logs <service>'"
fi
ok "compose stack is healthy"

# ---------- extra readiness probe ----------
# compose healthchecks fire from *inside* containers, but what the
# user cares about is "can my browser reach this?". Probe the
# host-forwarded port too.
"$ROOT/scripts/wait_for.sh" "http://localhost:$NEBULA_REST_HOST_PORT/healthz" 60 '"status":"ok"' \
  || die "nebula-server /healthz unreachable on host"

# ---------- seed ----------
if [[ $DO_SEED -eq 1 ]]; then
  say "seeding knowledge corpus"
  BASE_URL="http://localhost:$NEBULA_REST_HOST_PORT" "$ROOT/scripts/seed.sh" >/dev/null \
    && ok "corpus seeded" \
    || warn "seed script reported failures (see output above)"
fi

# ---------- env file ----------
# Drop a sourceable file alongside the repo so follow-up scripts
# (seed.sh, test_*.sh, ad-hoc curl) don't need to re-derive the
# picked ports. `export` so a caller can simply `source .nebula.env`.
cat >"$ROOT/.nebula.env" <<EOF_ENV
# Generated by scripts/start.sh at $(date -u +"%Y-%m-%dT%H:%M:%SZ")
export BASE_URL="http://localhost:$NEBULA_REST_HOST_PORT"
export PG_HOST="localhost"
export PG_PORT="$NEBULA_PG_HOST_PORT"
export PROM_URL="http://localhost:$NEBULA_PROM_PORT"
export OLLAMA_URL="http://localhost:11434"
export OLLAMA_CHAT_MODEL="$OLLAMA_CHAT_MODEL"
export OLLAMA_EMBED_MODEL="$OLLAMA_EMBED_MODEL"
export NEBULA_REST_HOST_PORT="$NEBULA_REST_HOST_PORT"
export NEBULA_GRPC_HOST_PORT="$NEBULA_GRPC_HOST_PORT"
export NEBULA_PG_HOST_PORT="$NEBULA_PG_HOST_PORT"
export NEBULA_SHOWCASE_PORT="$NEBULA_SHOWCASE_PORT"
EOF_ENV

# ---------- summary ----------
echo
printf '%s=== NebulaDB showcase is up ===%s\n' "$C_BOLD" "$C_RESET"
printf '  %sREST%s       http://localhost:%s\n'        "$C_BOLD" "$C_RESET" "$NEBULA_REST_HOST_PORT"
printf '  %sgRPC%s       localhost:%s\n'               "$C_BOLD" "$C_RESET" "$NEBULA_GRPC_HOST_PORT"
printf '  %spgwire%s     psql -h localhost -p %s -U any -d any\n' "$C_BOLD" "$C_RESET" "$NEBULA_PG_HOST_PORT"
if [[ $DO_SHOWCASE -eq 1 ]]; then
  printf '  %sShowcase%s   http://localhost:%s\n'      "$C_BOLD" "$C_RESET" "$NEBULA_SHOWCASE_PORT"
fi
printf '  %sPrometheus%s http://localhost:%s\n'        "$C_BOLD" "$C_RESET" "$NEBULA_PROM_PORT"
printf '  %sGrafana%s    http://localhost:%s (admin / admin)\n' "$C_BOLD" "$C_RESET" "$NEBULA_GRAFANA_PORT"
echo
printf '  %sStop:%s      ./scripts/start.sh --down\n'  "$C_BOLD" "$C_RESET"
printf '  %sLogs:%s      docker compose logs -f nebula-server\n' "$C_BOLD" "$C_RESET"
printf '  %sRe-seed:%s   source .nebula.env && ./scripts/seed.sh\n' "$C_BOLD" "$C_RESET"
printf '  %sSmokes:%s    source .nebula.env && ./scripts/test_rest.sh\n' "$C_BOLD" "$C_RESET"
echo

if [[ $DO_LOGS -eq 1 ]]; then
  # `--tail=50` means we don't dump ancient logs, but the user still
  # sees a decent amount of recent context immediately. `-f` keeps
  # following until Ctrl+C.
  docker compose logs --tail=50 -f
fi
