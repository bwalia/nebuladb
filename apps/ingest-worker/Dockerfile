# syntax=docker/dockerfile:1.7
#
# Two-stage build for the NebulaDB ingestion worker sidecar (design
# 0008 §8). Build from the repo root:
#
#   docker build -f apps/ingest-worker/Dockerfile -t nebula-ingest-worker .
#
# PDF/DOCX support is OFF by default to keep the image lean and the
# native-parser CVE surface out of the common path. Enable with:
#
#   docker build -f apps/ingest-worker/Dockerfile \
#     --build-arg FEATURES=office -t nebula-ingest-worker:office .

ARG RUST_VERSION=1.83
ARG DEBIAN_VARIANT=bookworm-slim
ARG FEATURES=""

# -----------------------------------------------------------------------------
# Stage 1: build
# -----------------------------------------------------------------------------
FROM rust:${RUST_VERSION} AS builder
ARG FEATURES

WORKDIR /src

# Copy the whole workspace — the worker depends on nebula-chunk and the
# build needs the workspace manifest + lockfile for --locked.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates crates
COPY apps apps

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    if [ -n "$FEATURES" ]; then FEAT="--features $FEATURES"; else FEAT=""; fi && \
    cargo build --release --locked -p nebula-ingest-worker $FEAT && \
    cp /src/target/release/nebula-ingest-worker /usr/local/bin/nebula-ingest-worker

# -----------------------------------------------------------------------------
# Stage 2: runtime
# -----------------------------------------------------------------------------
FROM debian:${DEBIAN_VARIANT} AS runtime

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

RUN groupadd --system --gid 10002 ingest \
 && useradd  --system --uid 10002 --gid ingest --create-home --shell /sbin/nologin ingest

COPY --from=builder /usr/local/bin/nebula-ingest-worker /usr/local/bin/nebula-ingest-worker

USER ingest
WORKDIR /home/ingest

# Configure via env (NEBULA_URL, NEBULA_BUCKET, NEBULA_TOKEN) or flags.
# Mount the corpus and pass paths as args, e.g.:
#   docker run --rm -e NEBULA_URL=http://db:8080 -e NEBULA_BUCKET=docs \
#     -v $PWD/corpus:/data nebula-ingest-worker /data
ENTRYPOINT ["/usr/local/bin/nebula-ingest-worker"]
