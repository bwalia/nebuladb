# syntax=docker/dockerfile:1.7
#
# Two-stage build for the showcase app.
#
# Stage 1: build the static bundle with Node.
# Stage 2: serve it with nginx, which also proxies `/api`, `/healthz`,
#          and `/metrics` to the nebula-server container so the
#          browser never talks to the backend directly (no CORS).

FROM node:20-slim AS build
WORKDIR /src
# Copy manifests first so a source-only change skips install.
COPY package.json package-lock.json ./
# node-slim is debian-based; more reliable than alpine on arm64
# builds where some npm/bsdtar combinations hit "Exit handler never
# called" under the npm cache layer. `--include=dev` keeps the build
# tooling (tsc, vite) in the install even if NODE_ENV=production.
#
# Failure-mode trap: npm 10.8.2's "Exit handler never called!" path
# (seen on registry TLS errors and occasional flakes) prints to
# stderr and then either exits 0 or hangs. An exit 0 gets cached as
# a successful layer and the next stage explodes with "tsc: not
# found" half a build later; a hang stalls the whole job until the
# CI ceiling kills it.
#
# npm's *internal* --fetch-retries can't save us here: the bug
# aborts the npm process before that retry logic engages, so a high
# --fetch-retry-maxtimeout just lets a hung fetch stack up and burn
# the build clock (that's what tipped the last attempt into a
# timeout). Instead we retry at the shell level: cap each attempt
# with `timeout` so a hung run is killed and retried rather than
# stalling, wipe node_modules between attempts so partial/corrupt
# state can't poison the next try, and gate success on the dev-dep
# bins actually being present — an exit-0-but-corrupt install fails
# the check and retries instead of caching a broken layer. After 3
# attempts we exit 1 loudly so the bad layer stays uncached and the
# operator sees the real npm error.
#
# TLS-inspecting proxies: the root cause behind the original
# UNABLE_TO_GET_ISSUER_CERT_LOCALLY flake is a corporate proxy
# presenting a CA that node's bundled trust store doesn't know. Pass
# that CA as a BuildKit secret (id=proxy_ca) and we point node at it
# via NODE_EXTRA_CA_CERTS for the install. The secret is optional
# (required=false): on clean networks — GitHub hosted runners, local
# dev — no secret is supplied, the file is empty, and we skip it.
# Secret mounts never land in an image layer, and it's scoped to
# this RUN because only the registry fetch needs it; `npm run build`
# is offline.
RUN --mount=type=secret,id=proxy_ca,target=/run/secrets/proxy_ca.crt,required=false \
    set -e; \
    if [ -s /run/secrets/proxy_ca.crt ]; then \
      export NODE_EXTRA_CA_CERTS=/run/secrets/proxy_ca.crt; \
      echo "Using proxy CA from build secret for npm fetches"; \
    fi; \
    for attempt in 1 2 3; do \
      echo "npm ci attempt ${attempt}/3"; \
      rm -rf node_modules; \
      if timeout 300 npm ci --no-audit --no-fund --include=dev \
             --fetch-retries=2 --fetch-retry-mintimeout=5000 --fetch-retry-maxtimeout=20000 \
         && test -x node_modules/.bin/tsc \
         && test -x node_modules/.bin/vite; then \
        echo "npm ci succeeded on attempt ${attempt}"; \
        break; \
      fi; \
      if [ "${attempt}" = 3 ]; then \
        echo "npm ci failed after 3 attempts (likely npm 'Exit handler never called!')" >&2; \
        exit 1; \
      fi; \
      echo "npm ci attempt ${attempt} failed; clearing node_modules and retrying" >&2; \
      sleep 5; \
    done
COPY tsconfig.json vite.config.ts tailwind.config.js postcss.config.js index.html ./
COPY src ./src
RUN npm run build

FROM nginx:1.27-alpine AS runtime
# `envsubst` is part of gettext; used by the start script to
# substitute `$NEBULA_SERVER` into the nginx config at boot. That's
# how we keep the image environment-agnostic.
RUN apk add --no-cache gettext wget
COPY --from=build /src/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/templates/default.conf.template
# Defaults target the compose topology (leader `nebula-server` +
# follower `nebula-follower`, Docker's embedded DNS at 127.0.0.11).
# In standalone mode, set NEBULA_SERVER=http://host.docker.internal:8080.
# The Helm chart overrides all four for Kubernetes (Service DNS +
# cluster DNS); see apps/showcase/nginx.conf for how each is used.
ENV NEBULA_SERVER=http://nebula-server:8080
ENV NEBULA_LEADER_HOST=nebula-server
ENV NEBULA_FOLLOWER_HOST=nebula-follower
ENV NGINX_RESOLVER=127.0.0.11
EXPOSE 80
HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
  CMD wget -q -O /dev/null http://localhost/ || exit 1
