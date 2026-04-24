#!/usr/bin/env bash
# wait_for.sh — poll an HTTP endpoint until it returns success or a
# timeout elapses. Used by CI to make sure the stack has actually
# finished booting (model pull, index warmup, etc.) before the tests
# start issuing requests.
#
# Usage:
#   wait_for.sh <url> [<timeout_secs>] [<probe_body_contains>]
#
# Example:
#   ./scripts/wait_for.sh http://localhost:8080/healthz 120 '"ok"'
#
# We deliberately don't use `docker compose wait` here — that only
# checks the container's own healthcheck, which tells us *the service
# is up* but not *it's responding to the URL we care about from the
# host network*. This script does the latter end-to-end.

set -Eeuo pipefail

URL="${1:?usage: wait_for.sh <url> [timeout_secs] [body_contains]}"
TIMEOUT="${2:-120}"
NEEDLE="${3:-}"

start_ts=$(date +%s)
attempt=0
while :; do
  attempt=$((attempt + 1))
  # `-f` makes curl exit non-zero on HTTP 4xx/5xx. `-m 3` caps each
  # individual attempt so a hung server doesn't eat the full budget.
  if body=$(curl -sfm 3 "$URL" 2>/dev/null); then
    if [[ -z "$NEEDLE" || "$body" == *"$NEEDLE"* ]]; then
      echo "wait_for: $URL is ready (attempt $attempt)"
      exit 0
    fi
  fi
  now=$(date +%s)
  if (( now - start_ts >= TIMEOUT )); then
    echo "wait_for: timed out after ${TIMEOUT}s ($URL)" >&2
    # Emit a last-ditch probe so CI logs include the actual response
    # the server is returning when we give up — saves a debugging hop.
    echo "---- last probe body ----" >&2
    curl -sm 3 "$URL" 2>&1 | head -c 2000 >&2 || true
    echo >&2
    exit 1
  fi
  sleep 2
done
