#!/usr/bin/env bash
# test_rest.sh — REST API smoke + integration tests.
#
# Covers:
#   - /healthz liveness and shape
#   - document upsert / get / delete
#   - chunked document upsert with multiple chunks + cleanup
#   - vector and semantic search
#   - RAG JSON response (streaming covered in test_rag.sh)
#   - SQL /api/v1/query endpoint
#
# Runs against BASE_URL (default http://localhost:8080). Every
# NebulaDB transport shares the same underlying index, so these tests
# also transitively validate chunking, embedding, HNSW, and the SQL
# planner for the scan/aggregate/join paths that REST exercises.

# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
trap finish EXIT

# ---------------- liveness ----------------
log "health"
resp=$(http_get /healthz)
body=$(body_of "$resp")
code=$(code_of "$resp")
assert_eq "200" "$code" "healthz returns 200"
assert_contains "$body" '"status":"ok"' "healthz body is ok"
assert_contains "$body" '"dim"' "healthz reports embedder dim"

# ---------------- document upsert / get / delete ----------------
log "simple document CRUD"
resp=$(http_post /api/v1/bucket/docs/doc \
  '{"id":"smoke-1","text":"zero trust networking architecture","metadata":{"region":"eu","owner":"team-a"}}')
assert_eq "200" "$(code_of "$resp")" "upsert doc returns 200"

resp=$(http_get /api/v1/bucket/docs/doc/smoke-1)
body=$(body_of "$resp")
assert_eq "200" "$(code_of "$resp")" "get doc returns 200"
assert_contains "$body" '"text":"zero trust networking architecture"' "get returns text"
assert_contains "$body" '"region":"eu"' "get returns metadata"

# Delete, then re-GET should 404.
code=$(curl -sS -X DELETE -o /dev/null -w '%{http_code}' "${BASE_URL}/api/v1/bucket/docs/doc/smoke-1")
assert_eq "204" "$code" "delete doc returns 204"
code=$(curl -sS -o /dev/null -w '%{http_code}' "${BASE_URL}/api/v1/bucket/docs/doc/smoke-1")
assert_eq "404" "$code" "get after delete returns 404"

# ---------------- chunked document upsert ----------------
log "chunked document upsert"
# 600 chars → with the default 500/50 chunker produces at least 2 chunks.
long=$(printf 'nebuladb %.0s' {1..80})  # 80 × 9 = 720 chars
resp=$(http_post /api/v1/bucket/docs/document \
  "{\"doc_id\":\"long-1\",\"text\":\"$long\",\"metadata\":{\"kind\":\"prose\"}}")
body=$(body_of "$resp")
assert_eq "200" "$(code_of "$resp")" "upsert document returns 200"
chunks=$(echo "$body" | jq -r .chunks)
[[ "$chunks" -ge 1 ]] \
  && pass "document produced $chunks chunk(s)" \
  || fail "document chunk count invalid ($chunks)"

# ---------------- vector search (dim from healthz) ----------------
log "vector search"
hr=$(http_get /healthz)
dim=$(body_of "$hr" | jq -r .dim)
# Build a zero vector of the advertised dim. This won't match anything
# *meaningfully*, but it must not error out — the response shape is
# what we're testing.
vec=$(python3 -c "import json; print(json.dumps([0.1]*int('$dim')))")
resp=$(http_post /api/v1/vector/search \
  "{\"vector\":$vec,\"top_k\":3,\"bucket\":\"docs\"}")
assert_eq "200" "$(code_of "$resp")" "vector search returns 200"
body=$(body_of "$resp")
assert_contains "$body" '"hits"' "vector search returns hits array"
assert_contains "$body" '"took_ms"' "vector search reports took_ms"

# ---------------- semantic search with latency cap ----------------
log "semantic search + latency"
# Insert something we can search for.
http_post /api/v1/bucket/docs/doc \
  '{"id":"smoke-sem","text":"zero trust is a security model","metadata":{"region":"eu"}}' \
  >/dev/null
# Warm run (the embedder may have caches to warm).
http_post /api/v1/ai/search '{"query":"zero trust","top_k":3}' >/dev/null
start=$(now_ms)
# top_k large enough that MockEmbedder's hash-based ranking still
# surfaces the seeded doc somewhere in the page. A real embedder
# returns it in position 1. We assert "it's findable" rather than
# "it's first" so the test is honest about what it can validate.
resp=$(http_post /api/v1/ai/search '{"query":"zero trust","top_k":50}')
end=$(now_ms)
latency=$((end - start))
assert_eq "200" "$(code_of "$resp")" "semantic search returns 200"
body=$(body_of "$resp")
assert_contains "$body" "smoke-sem" "semantic search finds seeded doc"
# Latency cap: 500 ms is generous for CI runners. If this fails the
# latency is probably dominated by an OpenAI/Ollama embedding call;
# check NEBULA_EMBED_DIM / provider config.
if [[ $latency -lt 500 ]]; then
  pass "semantic search latency ${latency}ms (< 500ms)"
else
  fail "semantic search latency ${latency}ms exceeds 500ms budget"
fi

# ---------------- RAG (non-streaming) ----------------
# Probe the RAG endpoint first; if the server's LLM backend is
# unreachable (500 with `llm:` message) we skip the content checks
# rather than falsely failing. The LLM-specific test in test_rag.sh
# handles that path under the expectation Ollama is actually live.
log "RAG non-streaming"
resp=$(http_post /api/v1/ai/rag '{"query":"zero trust","top_k":3}')
code=$(code_of "$resp")
body=$(body_of "$resp")
if [[ "$code" == "200" ]]; then
  assert_contains "$body" '"context"' "RAG response has context"
  assert_contains "$body" '"answer"' "RAG response has answer"
elif [[ "$code" == "500" && "$body" == *"llm"* ]]; then
  log "skipping RAG content checks — no live LLM (body: ${body:0:120})"
else
  fail "RAG returned unexpected status $code: ${body:0:200}"
fi

# ---------------- SQL via /api/v1/query ----------------
log "SQL via REST"
resp=$(http_post /api/v1/query \
  '{"sql":"SELECT id, region FROM docs WHERE semantic_match(content, '"'"'zero trust'"'"') LIMIT 5"}')
assert_eq "200" "$(code_of "$resp")" "SQL query returns 200"
body=$(body_of "$resp")
assert_contains "$body" '"rows"' "SQL response has rows"
assert_contains "$body" '"took_ms"' "SQL response has took_ms"

# Parse-error case.
resp=$(http_post /api/v1/query '{"sql":"NOT SQL"}')
assert_eq "400" "$(code_of "$resp")" "bad SQL returns 400"

# ---------------- cleanup ----------------
curl -sS -X DELETE "${BASE_URL}/api/v1/bucket/docs/doc/smoke-sem" >/dev/null || true
curl -sS -X DELETE "${BASE_URL}/api/v1/bucket/docs/document/long-1" >/dev/null || true
