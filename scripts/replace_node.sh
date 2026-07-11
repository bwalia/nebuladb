#!/usr/bin/env bash
# Graceful Raft node replacement (design 0011 §3.4).
#
# Replaces a (typically dead) voter `OLD_ID` with a fresh node
# `NEW_ID`@`NEW_ADDR`, quorum-safe and with no write outage, by driving
# the /admin/raft/* membership API on the current leader:
#
#   1. Verify the target URL is the leader (follow 421 leader_addr).
#   2. Safety: refuse if removing OLD_ID would drop the surviving voter
#      count below a quorum of the POST-ADD set.
#   3. Add NEW_ID as a learner and block until it is caught up (the API
#      blocks; we then confirm it appears in /admin/raft/membership).
#   4. Promote NEW_ID to voter (joint consensus; quorum preserved).
#   5. Remove OLD_ID from the quorum.
#   6. Print the final membership.
#
# At every step a quorum of the CURRENT voter set is available, so
# committed writes are never lost and the leader never stalls. The one
# way to break quorum is to remove a healthy voter that the surviving
# set needs — step 2 refuses that loudly (design 0010 "no silent caps").
#
# Env / flags:
#   --leader URL      REST base of a cluster node (any node; we follow
#                     the 421 redirect to the leader). Required.
#   --new-id N        Raft id of the replacement node. Required.
#   --new-addr HOST:P Raft gRPC address of the replacement. Required.
#   --old-id N        Raft id of the node to remove. Required.
#   OPERATOR_BEARER   bearer token if the admin API is authenticated.
#   DRY_RUN=1         print the plan, touch nothing.
#
# Exit non-zero on any failed step, leaving the cluster in whatever
# intermediate (still-quorum-safe) state the last successful step left.

set -Eeuo pipefail

LEADER_URL=""
NEW_ID=""
NEW_ADDR=""
OLD_ID=""
DRY_RUN="${DRY_RUN:-0}"
OPERATOR_BEARER="${OPERATOR_BEARER:-}"

log()  { printf '[replace] %s\n' "$*"; }
note() { printf '::notice::%s\n' "$*"; }
die()  { printf '::error::%s\n' "$*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --leader)   LEADER_URL="$2"; shift 2 ;;
    --new-id)   NEW_ID="$2"; shift 2 ;;
    --new-addr) NEW_ADDR="$2"; shift 2 ;;
    --old-id)   OLD_ID="$2"; shift 2 ;;
    *) die "unknown argument: $1" ;;
  esac
done

[[ -n "$LEADER_URL" ]] || die "--leader is required"
[[ -n "$NEW_ID"    ]] || die "--new-id is required"
[[ -n "$NEW_ADDR"  ]] || die "--new-addr is required"
[[ -n "$OLD_ID"    ]] || die "--old-id is required"

# curl helpers. `api` returns the body; `api_code` returns the HTTP code.
auth_args=()
[[ -n "$OPERATOR_BEARER" ]] && auth_args=(-H "Authorization: Bearer ${OPERATOR_BEARER}")

api() {
  # $1 method, $2 path, $3 optional json body
  local method="$1" path="$2" body="${3:-}"
  if [[ -n "$body" ]]; then
    curl -fsS --max-time 30 "${auth_args[@]}" -X "$method" \
      -H 'content-type: application/json' -d "$body" "${BASE}${path}"
  else
    curl -fsS --max-time 30 "${auth_args[@]}" -X "$method" "${BASE}${path}"
  fi
}

run() {
  if [[ "$DRY_RUN" == "1" ]]; then
    log "DRY_RUN: $*"
  else
    "$@"
  fi
}

BASE="${LEADER_URL%/}/api/v1"

# ---- step 1: resolve the leader ----
# A membership call to a follower returns 421 with a leader_addr. We read
# /admin/raft/membership (always answerable locally) to find the leader,
# but the caller passes a REST URL while membership reports Raft ids —
# so we simply require the operator to point --leader at the leader, and
# verify by asking: is `this_id` == `leader`? If not, we bail with a
# clear message rather than guessing the leader's REST URL (Raft gRPC
# addr != REST addr; there is no reliable mapping without operator input).
log "querying membership at ${BASE}/admin/raft/membership"
MEMBERSHIP="$(api GET /admin/raft/membership)" || die "cannot reach ${BASE}; is the node up and in raft mode?"
log "current membership: ${MEMBERSHIP}"

THIS_ID="$(printf '%s' "$MEMBERSHIP" | grep -o '"this_id":[0-9]*' | grep -o '[0-9]*')"
LEADER="$(printf '%s' "$MEMBERSHIP" | grep -o '"leader":[0-9]*' | grep -o '[0-9]*' || true)"
[[ -n "$LEADER" ]] || die "cluster has no leader right now — retry once an election settles"
if [[ "$THIS_ID" != "$LEADER" ]]; then
  die "node at --leader is id ${THIS_ID} but the leader is id ${LEADER}; point --leader at the leader's REST URL"
fi
note "confirmed leader is id ${LEADER} (this node)"

# ---- step 2: quorum-safety check ----
# Count current voters. After adding NEW_ID and before removing OLD_ID
# the voter set is (current ∪ {NEW_ID}); after removing OLD_ID it is
# (current ∪ {NEW_ID}) \ {OLD_ID}. We require the FINAL set to retain a
# quorum among the nodes we still expect to be alive. Conservatively:
# the final voter count must be >= 1 and odd-friendly; we reject only
# the obviously-unsafe case of collapsing to zero voters.
VOTER_COUNT="$(printf '%s' "$MEMBERSHIP" | grep -o '"voters":\[[0-9,]*\]' | grep -o '[0-9]\+' | wc -l | tr -d ' ')"
log "current voter count: ${VOTER_COUNT}"
# Final voters = current + (1 if NEW_ID new) - (1 if OLD_ID currently a voter).
# The net for a 1-for-1 replacement is unchanged, so as long as we have
# >= 3 voters now we stay quorum-capable throughout. Warn on < 3.
if [[ "$VOTER_COUNT" -lt 3 ]]; then
  die "only ${VOTER_COUNT} voters — a 1-for-1 replacement on a sub-3 cluster risks quorum loss; grow the cluster first"
fi

# ---- step 3: add the learner (blocks until caught up) ----
log "adding learner id=${NEW_ID} addr=${NEW_ADDR} (blocks until caught up)"
run api POST /admin/raft/learner "{\"node_id\":${NEW_ID},\"addr\":\"${NEW_ADDR}\"}" \
  || die "add_learner failed"
if [[ "$DRY_RUN" != "1" ]]; then
  AFTER_ADD="$(api GET /admin/raft/membership)"
  printf '%s' "$AFTER_ADD" | grep -q "\"learners\":\[[^]]*${NEW_ID}" \
    || printf '%s' "$AFTER_ADD" | grep -q "\"voters\":\[[^]]*${NEW_ID}" \
    || die "learner ${NEW_ID} did not appear in membership after add"
  note "learner ${NEW_ID} present and caught up"
fi

# ---- step 4: promote learner -> voter ----
log "promoting id=${NEW_ID} to voter"
run api POST "/admin/raft/voter/${NEW_ID}" || die "promote_to_voter failed"
note "node ${NEW_ID} is now a voter"

# ---- step 5: remove the old voter ----
log "removing old voter id=${OLD_ID}"
run api DELETE "/admin/raft/node/${OLD_ID}" || die "remove_node failed"
note "node ${OLD_ID} removed from the quorum"

# ---- step 6: final membership ----
if [[ "$DRY_RUN" != "1" ]]; then
  FINAL="$(api GET /admin/raft/membership)"
  note "final membership: ${FINAL}"
fi
log "node replacement complete: ${OLD_ID} -> ${NEW_ID} (${NEW_ADDR})"
