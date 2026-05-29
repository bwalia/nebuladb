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
# stderr but exits 0. That gets cached as a successful layer, and
# the next stage explodes with "tsc: not found" half a build later.
# We harden the layer two ways: (1) raise fetch retries so a single
# DNS / TLS blip doesn't tip us into the bad codepath, and (2)
# verify the key dev-dep bins landed before declaring success — if
# they didn't, fail the layer loudly so the cache stays clean and
# the operator sees the real cause in the next attempt.
RUN npm ci --no-audit --no-fund --include=dev \
    --fetch-retries=5 --fetch-retry-mintimeout=10000 --fetch-retry-maxtimeout=60000 \
 && test -x node_modules/.bin/tsc \
 && test -x node_modules/.bin/vite
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
# Default: talk to `nebula-server` (the compose service name). In
# standalone mode, set NEBULA_SERVER=http://host.docker.internal:8080.
ENV NEBULA_SERVER=http://nebula-server:8080
EXPOSE 80
HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
  CMD wget -q -O /dev/null http://localhost/ || exit 1
