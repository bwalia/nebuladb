#!/usr/bin/env bash
# Idempotent reconcile for the NebulaDB compose stack.
# Safe to run repeatedly: --no-recreate makes it a no-op when every
# service is already Up. Picks up the gap that `restart: unless-stopped`
# misses — containers stuck in `Created` state (e.g. after an interrupted
# `compose up` or a partial deploy) are never auto-restarted by Docker.
set -euo pipefail
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
cd /home/bwalia/nebuladb
exec docker compose up -d --no-recreate --no-build 2>&1
