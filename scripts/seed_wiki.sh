#!/usr/bin/env bash
# seed_wiki.sh — bulk-ingest N synthetic docs derived from real
# Wikipedia summaries, for local performance testing.
#
# Defaults load 100,000 documents via /api/v1/bucket/wiki/docs/bulk
# in 100-item batches. A fresh MockEmbedder stack (the local default)
# handles this on a laptop in a few minutes; a remote embedder will
# be rate-/quota-bound by the provider.
#
# Usage:
#   BASE_URL=http://localhost:18080 ./scripts/seed_wiki.sh
#   BASE_URL=... TARGET=10000 BATCH=200 ./scripts/seed_wiki.sh
#   BASE_URL=... BUCKET=wiki   CATEGORIES="science,history" ...
#
# Design notes:
#   * We fetch ~$SEED_SIZE real summaries (default 200) from the
#     Wikipedia random-article API, then cycle through them with
#     a monotone suffix to reach $TARGET. This avoids tens of
#     thousands of network round-trips to en.wikipedia.org, which
#     would take hours and hit their polite-use rate limit.
#   * Rate limiting on the NebulaDB side is expected to be off (the
#     stack's token-bucket defaults reject this traffic by design).
#     The script checks and yells loudly if it sees a 429.
#   * Bulk endpoint preserves per-item error handling, so a poisoned
#     text in one batch won't kill the whole run.

source "$(dirname "$0")/lib.sh"
trap finish EXIT

TARGET="${TARGET:-100000}"
BATCH="${BATCH:-100}"
BUCKET="${BUCKET:-wiki}"
SEED_SIZE="${SEED_SIZE:-200}"
WIKI_URL="${WIKI_URL:-https://en.wikipedia.org/api/rest_v1/page/random/summary}"

# ---------- preflight ----------
log "target=$TARGET batch=$BATCH bucket=$BUCKET seed_size=$SEED_SIZE"

preflight=$(curl -sS -m 3 "$BASE_URL/healthz" 2>/dev/null || true)
if [[ "$preflight" != *'"model"'* ]]; then
  fail "BASE_URL=$BASE_URL is not NebulaDB (/healthz: ${preflight:0:160})"
  exit 1
fi
log "server: $(echo "$preflight" | jq -c '{model, dim, docs}')"

# Warn if the rate limiter is on and will throttle us. We don't know
# the configured limit from outside, but we can check for the common
# sign of overload: a single warm-up insert that comes back 429.
probe=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$BASE_URL/api/v1/bucket/$BUCKET/doc" \
  -H 'content-type: application/json' \
  -d '{"id":"__probe__","text":"warmup","metadata":{}}')
if [[ "$probe" == "429" ]]; then
  fail "server returned 429 on warmup — set NEBULA_RATE_LIMIT=off or raise NEBULA_RATE_LIMIT_RPS for this load"
  exit 1
fi
curl -sS -X DELETE "$BASE_URL/api/v1/bucket/$BUCKET/doc/__probe__" >/dev/null 2>&1 || true

# ---------- fetch real summaries ----------
log "fetching $SEED_SIZE Wikipedia random summaries"
tmp_summaries=$(mktemp)
# User-Agent per Wikipedia policy; a generic UA gets throttled hard.
UA="NebulaDB-seed/0.1 (https://github.com/bwalia/nebuladb)"

fetched=0
attempts=0
while (( fetched < SEED_SIZE && attempts < SEED_SIZE * 3 )); do
  attempts=$((attempts + 1))
  # Pull one summary. We don't parallelise: Wikipedia's random
  # endpoint is polite-rate-limited and parallel requests just get
  # rate-shaped instead of running faster.
  # -L because `/page/random/summary` returns a 303 redirect to
  # the actual /page/summary/<title> endpoint; without -L curl
  # just prints the redirect body and we get no JSON.
  resp=$(curl -sSL -m 8 -H "User-Agent: $UA" "$WIKI_URL" 2>/dev/null || true)
  title=$(jq -r '.title // empty' <<<"$resp" 2>/dev/null)
  extract=$(jq -r '.extract // empty' <<<"$resp" 2>/dev/null)
  if [[ -n "$title" && -n "$extract" && ${#extract} -ge 40 ]]; then
    # Emit as a single-line NDJSON record: {title, extract}. Text
    # is squashed to one line because NebulaDB's chunker handles
    # the real document boundaries; we just need embeddable text.
    jq -cn --arg t "$title" --arg x "$extract" '{title: $t, extract: $x}' >>"$tmp_summaries"
    fetched=$((fetched + 1))
    # Progress every 20 to keep the terminal alive.
    (( fetched % 20 == 0 )) && log "fetched $fetched/$SEED_SIZE"
  fi
done

if (( fetched < 10 )); then
  fail "Wikipedia fetch produced only $fetched summaries; aborting"
  exit 1
fi
pass "collected $fetched unique summaries"

# ---------- bulk insert ----------
# We'll write batches to a temp file and POST them one by one. Doing
# this in bash means we never hold the full 100k batch in memory.
log "generating + inserting $TARGET documents in batches of $BATCH"

inserted=0
start_ts=$(now_ms)
batch_no=0
batch_file=$(mktemp)

flush_batch() {
  local n
  n=$(wc -l <"$batch_file" | tr -d ' ')
  if (( n == 0 )); then
    return 0
  fi
  batch_no=$((batch_no + 1))
  # `jq -s` slurps lines into an array, which is what the server
  # expects under {"items": [...]}.
  local payload
  payload=$(jq -s '{items: .}' "$batch_file")
  local status body resp
  resp=$(curl -sS -o /tmp/seed_wiki_resp -w '%{http_code}' -m 120 \
    -X POST "$BASE_URL/api/v1/bucket/$BUCKET/docs/bulk" \
    -H 'content-type: application/json' \
    -d @- <<<"$payload" || echo 000)
  status="$resp"
  body=$(cat /tmp/seed_wiki_resp 2>/dev/null)
  if [[ "$status" != "200" ]]; then
    fail "batch $batch_no failed: status=$status body=${body:0:200}"
    return 1
  fi
  local added
  added=$(jq -r '.inserted' <<<"$body")
  inserted=$((inserted + added))
  : >"$batch_file"

  # Progress line every batch: rate + ETA. now_ms from lib.sh.
  local now elapsed rate eta
  now=$(now_ms)
  elapsed=$(( (now - start_ts) / 1000 + 1 ))
  rate=$(( inserted / elapsed ))
  if (( rate > 0 )); then
    eta=$(( (TARGET - inserted) / rate ))
    printf '  batch %d: %d/%d inserted (%d/s, ETA %ds)\n' \
      "$batch_no" "$inserted" "$TARGET" "$rate" "$eta"
  else
    printf '  batch %d: %d/%d inserted\n' "$batch_no" "$inserted" "$TARGET"
  fi
}

# Cycle through the fetched summaries, varying the id + text so each
# inserted doc is unique. MockEmbedder hashes the full text so two
# docs with the same base text but different variant suffixes end up
# at different HNSW positions.
i=0
while (( i < TARGET )); do
  # 1-based index into the summaries file, cycling.
  sidx=$(( (i % fetched) + 1 ))
  variant=$(( i / fetched ))
  summary=$(sed -n "${sidx}p" "$tmp_summaries")
  title=$(jq -r '.title' <<<"$summary")
  extract=$(jq -r '.extract' <<<"$summary")

  id="wiki-$i"
  # Variant suffix nudges MockEmbedder into a distinct hash slot
  # without lying about content; a real embedder would see nearly-
  # identical vectors for low variant counts (which is fine for
  # load-testing the index, not a correctness concern).
  if (( variant == 0 )); then
    text="$extract"
  else
    text="$extract (variant $variant)"
  fi

  # Append to the batch file as an NDJSON row. Use jq to escape.
  jq -cn --arg id "$id" --arg text "$text" --arg title "$title" \
      --argjson variant "$variant" \
      '{id: $id, text: $text, metadata: {title: $title, variant: $variant}}' \
    >>"$batch_file"

  i=$((i + 1))
  if (( i % BATCH == 0 )); then
    flush_batch || exit 1
  fi
done
flush_batch || exit 1

rm -f "$batch_file" "$tmp_summaries" /tmp/seed_wiki_resp

elapsed=$(( ( $(now_ms) - start_ts ) / 1000 + 1 ))
rate=$(( inserted / elapsed ))
log "done: $inserted inserted in ${elapsed}s (${rate}/s)"
log "try:"
log "  curl -sS -X POST '$BASE_URL/api/v1/ai/search' -H 'content-type: application/json' -d '{\"query\":\"history of science\",\"top_k\":5,\"bucket\":\"$BUCKET\"}' | jq"
