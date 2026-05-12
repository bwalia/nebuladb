#!/usr/bin/env bash
# Idempotent reconcile for the NebulaDB compose stack.
#
# Plain `docker compose up -d` is what we want here. Compose only
# recreates a container when its config diff'd against the current
# .env/compose file — so a healthy stack with no config changes is
# a no-op (compose just prints "Container X Running"). The reconcile
# only does work when something actually drifted from desired state:
#
#   * container in `Created` (interrupted compose up / cancelled
#     deploy) → started
#   * container Exited → restarted (covers the gap left by
#     `restart: unless-stopped`, which doesn't fire for never-started
#     containers)
#   * .env / compose changed since the last `up` → recreated against
#     current config (avoids stale port mappings et al)
#   * service entirely missing → created and started
#
# `--no-recreate` was tempting as a "be even more conservative" guard
# but it actively breaks case 3: containers stuck in Created with
# stale port bindings get re-started with the broken config instead
# of recreated against the live .env.
#
# `--no-build` keeps the cron tick fast — image rebuilds belong in
# the deploy workflow, not on a 2-min timer.
set -euo pipefail
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
cd /home/bwalia/nebuladb
exec docker compose up -d --no-build 2>&1
