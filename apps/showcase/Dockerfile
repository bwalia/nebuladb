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
RUN npm ci --no-audit --no-fund --include=dev
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
