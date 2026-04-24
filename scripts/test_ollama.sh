#!/usr/bin/env bash
# test_ollama.sh — direct Ollama API checks.
#
# Verifies the model-pull init container did its job and the daemon
# actually serves both generation and embedding. Confirmation that
# the LLM side of the stack is alive is important because a broken
# Ollama still returns plausible-looking HTTP from the NebulaDB
# server — the RAG test might fail in an unhelpful way, so this
# script runs *first* in CI.
#
# OLLAMA_URL defaults to the host-exposed port from compose. In
# production we never talk to Ollama from outside the network.

set -Eeuo pipefail
source "$(dirname "$0")/lib.sh"
trap finish EXIT

OLLAMA_URL="${OLLAMA_URL:-http://localhost:11434}"
CHAT_MODEL="${OLLAMA_CHAT_MODEL:-tinyllama}"
EMBED_MODEL="${OLLAMA_EMBED_MODEL:-nomic-embed-text}"

log "tags endpoint"
tags=$(curl -sSf "${OLLAMA_URL}/api/tags")
assert_contains "$tags" "$CHAT_MODEL" "chat model '$CHAT_MODEL' is pulled"
assert_contains "$tags" "$EMBED_MODEL" "embed model '$EMBED_MODEL' is pulled"

log "generation API"
# Tiny prompt, hard cap on output tokens. The goal is "does it
# respond at all", not "is the answer good".
resp=$(curl -sSf -m 60 -X POST "${OLLAMA_URL}/api/generate" \
  -d "{\"model\":\"$CHAT_MODEL\",\"prompt\":\"Reply with: ok\",\"stream\":false,\"options\":{\"num_predict\":16}}")
text=$(echo "$resp" | jq -r .response)
[[ -n "$text" && "$text" != "null" ]] \
  && pass "generation returned text (${#text} chars)" \
  || fail "generation returned empty response"

log "embedding API"
# Don't use `-f` here; we want to see the body for the `fail` message
# rather than have curl exit non-zero silently when the model's missing.
resp=$(curl -sS -m 30 -X POST "${OLLAMA_URL}/api/embeddings" \
  -d "{\"model\":\"$EMBED_MODEL\",\"prompt\":\"hello world\"}" || true)
dim=$(echo "$resp" | jq '.embedding | length' 2>/dev/null || echo 0)
# nomic-embed-text is 768-dim; other models differ. We just assert
# "more than a handful of floats came back", which is a real
# regression signal (zero = broken, non-zero = working).
if [[ "${dim:-0}" -gt 32 ]]; then
  pass "embedding returned ${dim}-dim vector"
else
  fail "embedding returned suspiciously short vector (dim=${dim:-0}; body=${resp:0:200})"
fi
