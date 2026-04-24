#!/usr/bin/env bash
# seed.sh — populate NebulaDB with a small realistic corpus.
#
# Designed for the showcase app: gives Semantic Search / RAG / Hybrid
# tabs something meaningful to work with on a fresh boot. The docs
# are inline so the script stays self-contained (no external fixture
# files to keep in sync).
#
# Usage:
#   BASE_URL=http://localhost:8080 ./scripts/seed.sh
#
# The script is idempotent — each doc uses a stable id, so re-running
# just refreshes the content.

source "$(dirname "$0")/lib.sh"
trap finish EXIT

upsert_doc() {
  local id="$1" text="$2" region="$3" owner="$4"
  local body
  body=$(jq -cn \
    --arg id "$id" \
    --arg text "$text" \
    --arg region "$region" \
    --arg owner "$owner" \
    '{id: $id, text: $text, metadata: {region: $region, owner: $owner}}')
  local resp
  resp=$(http_post /api/v1/bucket/docs/doc "$body")
  assert_eq "200" "$(code_of "$resp")" "seed $id"
}

log "seeding 'docs' bucket"

# Security / zero-trust family.
upsert_doc "zt-overview" \
  "NebulaDB enforces zero trust networking by verifying every request with per-caller bearer tokens or JWTs, not trust in the network perimeter. The rate limiter is keyed on the authenticated principal so a leaked key is throttled even across rotating IPs." \
  "eu" "platform"

upsert_doc "zt-jwt" \
  "JWT verification is optional and coexists with the static bearer allowlist. Tokens are validated HS256 with a clock-skew allowance of 30 seconds. Expected issuer and audience are enforced when configured." \
  "us" "platform"

upsert_doc "zt-ratelimit" \
  "Rate limits use a token bucket per authenticated principal. The bucket refills at a configurable rate-per-second up to a configurable burst cap. A 429 response carries an honest Retry-After header so polite clients back off automatically." \
  "eu" "platform"

# DNS + failover family.
upsert_doc "dns-failover" \
  "DNS failover in NebulaDB-backed services relies on short TTLs (usually 30 seconds) and active health checks. On an unhealthy region the DNS authority withdraws that region's A records within one TTL, and clients retry against the remaining regions." \
  "eu" "networking"

upsert_doc "dns-geosteering" \
  "Geo-steering routes users to their nearest healthy region, falling back to the next-closest on failure. Combined with client-side retry this masks regional outages of up to two minutes with no user-visible errors." \
  "us" "networking"

# Incident narratives.
upsert_doc "incident-2024-01" \
  "A DNS failover misconfiguration in EU-west caused 3 minutes of elevated error rates. Root cause: TTL was set to 300s, quadruple the intended value. Fix: enforced TTL ceiling in the DNS pipeline config." \
  "eu" "platform"

upsert_doc "incident-2024-02" \
  "Embedding cache was disabled during a hot-patch window; semantic search p95 spiked to 1.2s. Fix: redeployed with cache re-enabled and added a Grafana alert on cache hit ratio < 50%." \
  "us" "platform"

# Developer / subscription family (used by the JOIN demo).
upsert_doc "dev-onboarding" \
  "Developer onboarding spans repository access, staging credentials, and the RAG-powered internal docs portal. New hires can query NebulaDB directly via psql or the REST API to retrieve runbooks." \
  "eu" "developer-experience"

upsert_doc "subscription-tiers" \
  "NebulaDB subscription tiers: Free (rate-limited, single-node), Pro (multi-node, Redis embedding cache, priority support), and Enterprise (SLA, SSO, pgwire on a dedicated port). Billing is per active embedding model." \
  "eu" "developer-experience"

upsert_doc "subscription-limits" \
  "Free-tier subscriptions are capped at 10 requests per second burst and 1 QPS sustained. Pro subscribers get 500 RPS burst, 100 QPS sustained. Enterprise limits are negotiated per contract." \
  "us" "developer-experience"

log "seeded 10 docs"
log "try:"
log "  curl -sS -X POST $BASE_URL/api/v1/ai/search -H 'content-type: application/json' -d '{\"query\":\"dns failover\",\"top_k\":3}' | jq"
log "  or open the showcase UI and visit the Hybrid tab."
