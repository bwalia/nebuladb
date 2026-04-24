# syntax=docker/dockerfile:1.7
#
# Two-stage build for `nebula-server`. Stage 1 is a fat builder image
# with the Rust toolchain; stage 2 is a slim runtime with only the
# compiled binary and its runtime SSL deps.
#
# We mount cargo's registry + target dirs as BuildKit caches so
# successive builds are measured in seconds, not minutes. Requires
# BuildKit (`DOCKER_BUILDKIT=1` or Docker ≥ 23).

ARG RUST_VERSION=1.83
ARG DEBIAN_VARIANT=bookworm-slim

# -----------------------------------------------------------------------------
# Stage 1: build
# -----------------------------------------------------------------------------
FROM rust:${RUST_VERSION} AS builder

# `tonic-build` shells out to `protoc` at build time for the gRPC
# crate's codegen. The official rust image doesn't ship it, so we
# install it here. Keep this layer at the top so edits to Cargo.toml
# / sources don't invalidate it.
RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Copy only the workspace manifests first so dependency resolution
# caches independently of source changes. Any source edit that doesn't
# touch Cargo.toml / Cargo.lock will skip this whole layer.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates/nebula-core/Cargo.toml     crates/nebula-core/Cargo.toml
COPY crates/nebula-vector/Cargo.toml   crates/nebula-vector/Cargo.toml
COPY crates/nebula-embed/Cargo.toml    crates/nebula-embed/Cargo.toml
COPY crates/nebula-chunk/Cargo.toml    crates/nebula-chunk/Cargo.toml
COPY crates/nebula-llm/Cargo.toml      crates/nebula-llm/Cargo.toml
COPY crates/nebula-cache/Cargo.toml    crates/nebula-cache/Cargo.toml
COPY crates/nebula-index/Cargo.toml    crates/nebula-index/Cargo.toml
COPY crates/nebula-server/Cargo.toml   crates/nebula-server/Cargo.toml

# Copy actual sources. We do this after the manifests so Cargo can see
# every crate's source tree when we build.
COPY crates crates

# Release build. `--locked` forces use of the committed Cargo.lock and
# fails CI if a manifest edit changes resolution without updating the
# lock — a common source of "works on my machine" drift.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p nebula-server && \
    cp /src/target/release/nebula-server /usr/local/bin/nebula-server

# -----------------------------------------------------------------------------
# Stage 2: runtime
# -----------------------------------------------------------------------------
FROM debian:${DEBIAN_VARIANT} AS runtime

# Runtime needs CA certs for TLS to OpenAI / Ollama / the compat
# endpoints. `ca-certificates` is the only transitive runtime dep since
# reqwest is built with rustls and statically links OpenSSL is not used.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates wget \
 && rm -rf /var/lib/apt/lists/*

# Non-root user with a stable UID so bind-mounted data volumes keep
# predictable ownership across hosts.
RUN groupadd --system --gid 10001 nebula \
 && useradd  --system --uid 10001 --gid nebula --create-home --shell /sbin/nologin nebula

COPY --from=builder /usr/local/bin/nebula-server /usr/local/bin/nebula-server

USER nebula
WORKDIR /home/nebula

# Default: bind to all interfaces so the container is reachable; dev
# config overrides to 127.0.0.1. Env vars handle the rest (LLM backend,
# API keys, cache size) so the image is workload-agnostic.
ENV NEBULA_BIND=0.0.0.0:8080

EXPOSE 8080

# `exec` form so Ctrl-C / SIGTERM reach the process (not a shell wrapper).
ENTRYPOINT ["/usr/local/bin/nebula-server"]
