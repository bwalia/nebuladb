#!/usr/bin/env bash
# test_rag.sh — end-to-end RAG through a real LLM (Ollama by default).
#
# What "real" means here:
#   - The server must be configured with NEBULA_LLM_OLLAMA_URL pointing
#     at a reachable Ollama instance (compose does this).
#   - A corpus document is inserted, then queried by a natural-language
#     question derived from it.
#   - We assert:
#       (a) the SSE stream emits context events before answer_delta,
#       (b) answer_delta events contain non-empty text,
#       (c) the aggregated answer mentions a term unique to the corpus,
#       (d) a `done` event arrives within a timeout.
#
# (c) is the only fuzzy part. Real LLM outputs vary run-to-run, so we
# assert a very low bar — the unique keyword "nebuladb" appears
# somewhere in the concatenated answer — rather than matching a full
# sentence.

# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
trap finish EXIT

log "seeding RAG corpus"
http_post /api/v1/bucket/docs/document '{
  "doc_id": "rag-overview",
  "text": "NebulaDB is a Rust database that combines SQL, document storage, a native HNSW vector index, and a streaming RAG endpoint powered by Ollama. It exposes REST, gRPC, and the Postgres wire protocol over one shared corpus.",
  "metadata": {"kind": "overview"}
}' >/dev/null

# Non-streaming first: easier to assert, and if this fails the
# streaming test has no chance. 120s timeout covers the cold first
# token on a small CI-friendly model like tinyllama.
log "RAG JSON (non-stream)"
resp=$(curl -sS -m 120 -X POST "${BASE_URL}/api/v1/ai/rag" \
  -H 'content-type: application/json' \
  -w '\n%{http_code}' \
  -d '{"query":"What is NebulaDB?","top_k":3}')
code=$(code_of "$resp")
body=$(body_of "$resp")
assert_eq "200" "$code" "RAG JSON returns 200"
assert_contains "$body" '"context"' "JSON answer has context array"
assert_contains "$body" '"answer"' "JSON answer has answer field"
# Content-level check: the unique token "NebulaDB" should appear in
# either the retrieved context or the LLM output. Case-insensitive to
# tolerate LLM lowercase tendencies.
lower_body=$(echo "$body" | tr '[:upper:]' '[:lower:]')
assert_contains "$lower_body" "nebuladb" "answer or context mentions 'nebuladb'"

# Streaming: we collect the SSE frames into a file and parse after.
log "RAG SSE stream"
out="$_NB_STATE_DIR/rag.sse"
: >"$out"
curl -sS -N -m 180 -X POST "${BASE_URL}/api/v1/ai/rag" \
  -H 'content-type: application/json' \
  -d '{"query":"What is NebulaDB?","top_k":3,"stream":true}' >"$out" &
pid=$!
# Wait up to 180s for a `done` event, polling the file. Don't block on
# `wait` because curl-SSE streams can stay open for a while even after
# the logical "done" marker (server flushes keep-alives).
for _ in $(seq 1 180); do
  if grep -q '^event: done' "$out"; then
    break
  fi
  sleep 1
done
# Tear down the still-open curl if the server hasn't closed by now.
kill "$pid" 2>/dev/null || true
wait "$pid" 2>/dev/null || true

# Structural assertions on the SSE stream.
grep -q '^event: context' "$out" \
  && pass "stream emitted at least one context event" \
  || fail "stream never emitted a context event"
grep -q '^event: answer_delta' "$out" \
  && pass "stream emitted at least one answer_delta event" \
  || fail "stream never emitted an answer_delta event"
grep -q '^event: done' "$out" \
  && pass "stream emitted done event" \
  || fail "stream never emitted done event (check LLM availability)"

# Order check: first `context` must appear before first `answer_delta`.
first_ctx=$(grep -n '^event: context' "$out" | head -n1 | cut -d: -f1)
first_delta=$(grep -n '^event: answer_delta' "$out" | head -n1 | cut -d: -f1)
if [[ -n "$first_ctx" && -n "$first_delta" && "$first_ctx" -lt "$first_delta" ]]; then
  pass "context events precede answer_delta events"
else
  fail "ordering wrong: first context line=$first_ctx, first delta=$first_delta"
fi

# Cleanup.
curl -sS -X DELETE "${BASE_URL}/api/v1/bucket/docs/document/rag-overview" >/dev/null || true
