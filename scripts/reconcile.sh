#!/usr/bin/env bash
# Idempotent reconcile for the NebulaDB compose stack.
# Safe to run repeatedly: --no-recreate makes it a no-op when every
# service is already Up. Picks up the gap that `restart: unless-stopped`
# misses — containers stuck in `Created` state (e.g. after an interrupted
# `compose up` or a partial deploy) are never auto-restarted by Docker.
set -euo pipefail
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
cd /home/bwalia/nebuladb

LOG=/home/bwalia/nebuladb/.reconcile.log

# Backstop: cron appends our stdout to $LOG with no external rotation, so
# cap it here. Keep the tail rather than truncating to zero so the last
# few real events survive.
if [ -f "$LOG" ] && [ "$(stat -c%s "$LOG" 2>/dev/null || echo 0)" -gt 1048576 ]; then
  tail -n 200 "$LOG" > "$LOG.tmp" 2>/dev/null && mv "$LOG.tmp" "$LOG"
fi

# `compose up` prints a per-container state line every run; on a no-op
# cycle that is pure noise. Capture it and only surface output when a
# container actually changed state (or compose failed) — keeps the cron
# log small and makes any real event easy to spot.
out=$(docker compose up -d --no-recreate --no-build 2>&1) && rc=0 || rc=$?

# nebula-ollama-init is a one-shot (restart: "no"); compose re-runs it
# Created->Started->Exited every cycle, so ignore its lines when deciding
# whether a *real* state change happened.
if [ "$rc" -ne 0 ] || printf '%s\n' "$out" | grep -v 'ollama-init' | grep -qiE 'Created|Started|Recreated|Restarted|Stopped|Removed|Error'; then
  printf '%s  rc=%s\n%s\n' "$(date -Is)" "$rc" "$out"
fi

exit "$rc"
