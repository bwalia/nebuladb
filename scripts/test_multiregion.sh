#!/usr/bin/env bash
# test_multiregion.sh — cross-region CRUD + RAG convergence against the
# two-region docker-compose topology (docker-compose.multiregion.yml).
#
# Assumes both regions are already up and healthy:
#   docker compose -f docker-compose.multiregion.yml up -d --build --wait
#
# What it asserts:
#   1. A write accepted in region-a becomes readable in region-b
#      (cross-region replication, A -> B).
#   2. A write accepted in region-b becomes readable in region-a (B -> A).
#   3. An update in region-a propagates the new revision to region-b.
#   4. A delete in region-a removes the doc in region-b.
#   5. A RAG/semantic search in region-b over an A-homed bucket returns
#      the authoritative document — reads served locally, correctly.
#   6. Both regions report each other under /admin/replication remotes.
#
# Convergence is asserted with a bounded poll, not a fixed sleep: the
# script fails loudly (with how far it got) if a region falls behind the
# CONVERGE_TIMEOUT budget, turning replication lag into a CI signal.

source "$(dirname "$0")/lib.sh"
trap finish EXIT

# Region REST endpoints. Override to point at a remote stack.
REGION_A="${REGION_A:-http://localhost:18080}"
REGION_B="${REGION_B:-http://localhost:18081}"

# Max seconds to wait for a single record to converge cross-region.
# The cross-region tail resumes from BEGIN per connect and runs an
# epoch check per record, so this is more generous than in-region lag.
CONVERGE_TIMEOUT="${CONVERGE_TIMEOUT:-30}"

# post <base> <path> <json> — POST and echo "body\ncode".
post() {
  curl -sS -X POST "${1}${2}" -H 'content-type: application/json' -w '\n%{http_code}' -d "$3"
}

# get_code <base> <path> — echo just the HTTP status of a GET.
get_code() {
  curl -sS -o /dev/null -w '%{http_code}' "${1}${2}"
}

# wait_for_doc <base> <bucket> <id> <description> — poll until the doc
# is fetchable (200) or the budget elapses. Reports elapsed seconds.
wait_for_doc() {
  local base="$1" bucket="$2" id="$3" desc="$4"
  local start now code
  start=$(date +%s)
  while :; do
    code=$(get_code "$base" "/api/v1/bucket/${bucket}/doc/${id}")
    if [[ "$code" == "200" ]]; then
      now=$(date +%s)
      pass "$desc (converged in $((now - start))s)"
      return 0
    fi
    now=$(date +%s)
    if (( now - start > CONVERGE_TIMEOUT )); then
      fail "$desc (not converged within ${CONVERGE_TIMEOUT}s; last code $code)"
      return 1
    fi
    sleep 1
  done
}

# wait_for_absent <base> <bucket> <id> <description> — poll until the
# doc returns 404 (delete converged) or budget elapses.
wait_for_absent() {
  local base="$1" bucket="$2" id="$3" desc="$4"
  local start now code
  start=$(date +%s)
  while :; do
    code=$(get_code "$base" "/api/v1/bucket/${bucket}/doc/${id}")
    if [[ "$code" == "404" ]]; then
      now=$(date +%s)
      pass "$desc (converged in $((now - start))s)"
      return 0
    fi
    now=$(date +%s)
    if (( now - start > CONVERGE_TIMEOUT )); then
      fail "$desc (delete not converged within ${CONVERGE_TIMEOUT}s; last code $code)"
      return 1
    fi
    sleep 1
  done
}

log "preconditions: both regions healthy"
assert_eq "200" "$(get_code "$REGION_A" /healthz)" "region-a /healthz is 200"
assert_eq "200" "$(get_code "$REGION_B" /healthz)" "region-b /healthz is 200"

# --- 1. A -> B convergence (create) ---
log "write catalog/sku-1 in region-a, expect it in region-b"
resp=$(post "$REGION_A" /api/v1/bucket/catalog/doc \
  '{"id":"sku-1","text":"authoritative catalog item from region a","metadata":{"price":10}}')
assert_eq "200" "$(code_of "$resp")" "region-a accepted the write"
wait_for_doc "$REGION_B" catalog sku-1 "region-b sees A-homed catalog/sku-1"

# --- 2. B -> A convergence (create, other direction) ---
log "write logs/evt-1 in region-b, expect it in region-a"
resp=$(post "$REGION_B" /api/v1/bucket/logs/doc \
  '{"id":"evt-1","text":"authoritative log event from region b","metadata":{"level":"warn"}}')
assert_eq "200" "$(code_of "$resp")" "region-b accepted the write"
wait_for_doc "$REGION_A" logs evt-1 "region-a sees B-homed logs/evt-1"

# --- 3. Update propagation (A -> B) ---
log "update catalog/sku-1 in region-a, expect new revision in region-b"
post "$REGION_A" /api/v1/bucket/catalog/doc \
  '{"id":"sku-1","text":"updated catalog item revision two","metadata":{"price":20}}' >/dev/null
# Poll until region-b's copy reflects the new text.
start=$(date +%s)
while :; do
  body=$(curl -sS "${REGION_B}/api/v1/bucket/catalog/doc/sku-1")
  if [[ "$body" == *"revision two"* ]]; then
    pass "region-b reflects the updated revision"
    break
  fi
  now=$(date +%s)
  if (( now - start > CONVERGE_TIMEOUT )); then
    fail "region-b did not reflect the update within ${CONVERGE_TIMEOUT}s"
    break
  fi
  sleep 1
done

# --- 5. RAG / semantic search in region-b over the A-homed bucket ---
# Seed a tiny corpus in region-a; query by a doc's exact text in
# region-b and assert it ranks first.
log "seed RAG corpus in region-a, retrieve in region-b"
for pair in \
  'doc-zero-trust|zero trust network access with continuous verification' \
  'doc-dns-failover|dns failover strategies across multiple regions' \
  'doc-gitops|kubernetes gitops continuous deployment pipeline'; do
  id="${pair%%|*}"
  text="${pair#*|}"
  post "$REGION_A" /api/v1/bucket/knowledge/doc \
    "{\"id\":\"${id}\",\"text\":\"${text}\",\"metadata\":{\"source\":\"nightly\"}}" >/dev/null
done
wait_for_doc "$REGION_B" knowledge doc-gitops "region-b received the RAG corpus"

# ai/search embeds the query and returns ranked hits. Query by the
# dns-failover doc's exact text; the deterministic embedder makes it the
# nearest neighbour, so it must be the first hit.
search=$(post "$REGION_B" /api/v1/ai/search \
  '{"query":"dns failover strategies across multiple regions","bucket":"knowledge","top_k":3}')
search_body=$(body_of "$search")
assert_eq "200" "$(code_of "$search")" "region-b ai/search returned 200"
# The first hit's id should be doc-dns-failover. We check the top hit by
# extracting the first "id" after the hits array opens.
top_id=$(echo "$search_body" | jq -r '.hits[0].id')
assert_eq "doc-dns-failover" "$top_id" "top RAG hit in region-b is the dns-failover doc"

# --- 4. Delete propagation (A -> B) ---
log "delete catalog/sku-1 in region-a, expect removal in region-b"
# NOTE: cross-region delete relies on the Delete WAL record streaming to
# region-b. region-a's own copy is removed immediately.
curl -sS -X DELETE "${REGION_A}/api/v1/bucket/catalog/doc/sku-1" >/dev/null
wait_for_absent "$REGION_B" catalog sku-1 "region-b drops the deleted catalog/sku-1"

# --- 6. Replication topology visibility ---
log "each region lists the other under /admin/replication remotes"
rep_a=$(curl -sS "${REGION_A}/api/v1/admin/replication")
rep_b=$(curl -sS "${REGION_B}/api/v1/admin/replication")
assert_contains "$rep_a" "us-west-2" "region-a /admin/replication references us-west-2"
assert_contains "$rep_b" "us-east-1" "region-b /admin/replication references us-east-1"

# Cleanup the docs we created (best-effort).
for d in "catalog/doc/sku-1" "logs/doc/evt-1" \
         "knowledge/doc/doc-zero-trust" "knowledge/doc/doc-dns-failover" \
         "knowledge/doc/doc-gitops"; do
  curl -sS -X DELETE "${REGION_A}/api/v1/bucket/${d}" >/dev/null 2>&1 || true
  curl -sS -X DELETE "${REGION_B}/api/v1/bucket/${d}" >/dev/null 2>&1 || true
done
