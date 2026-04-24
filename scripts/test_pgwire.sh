#!/usr/bin/env bash
# test_pgwire.sh — drive the Postgres wire front end with psql.
#
# NebulaDB's pgwire implements the **simple** query protocol only.
# psql's default `\d`/command mode speaks simple-query; *parameterized*
# queries use extended and will fail.
#
# We don't try to run DDL here because NebulaDB's SQL dialect is
# SELECT-only; ingestion happens over REST (see test_rest.sh). This
# script verifies that an already-ingested corpus is queryable through
# the pgwire front end with identical semantics to REST.

# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
trap finish EXIT

# Seed a doc via REST so there's something for psql to retrieve.
log "seeding corpus via REST"
http_post /api/v1/bucket/docs/doc \
  '{"id":"pg-1","text":"nebuladb pgwire smoke test","metadata":{"region":"eu"}}' \
  >/dev/null

export PGPASSWORD=ignored # noop startup, but psql complains if absent

run_psql() {
  # `-v ON_ERROR_STOP=1` makes psql exit non-zero on SQL errors so
  # our `assert` helpers catch them. `-X` skips .psqlrc to keep
  # behavior reproducible across developer machines.
  PGCONNECT_TIMEOUT=5 psql \
    -X \
    -v ON_ERROR_STOP=1 \
    -h "$PG_HOST" -p "$PG_PORT" \
    -U "$PG_USER" -d "$PG_DB" \
    "$@"
}

log "psql can connect"
assert "psql connects and runs trivial statement" \
  run_psql -c 'SET client_encoding TO UTF8'

log "select returns seeded row"
# psql -A -t -F $'\t' renders tab-separated, unaligned, no header — easy to grep.
rows=$(run_psql -A -t -F $'\t' \
  -c "SELECT id, region FROM docs WHERE semantic_match(content, 'nebuladb pgwire') LIMIT 5")
assert_contains "$rows" "pg-1" "SELECT over pgwire returns seeded doc"

log "aggregate works over pgwire"
rows=$(run_psql -A -t -F $'\t' \
  -c "SELECT region, COUNT(*) AS n FROM docs WHERE semantic_match(content, 'nebuladb') GROUP BY region")
# Every row we inserted had region=eu; expect at least one group.
assert_contains "$rows" "eu" "GROUP BY returns eu bucket"

log "parse error surfaces as SQLSTATE"
# Expect non-zero exit because the statement is garbage. We *want*
# psql to fail here.
if run_psql -c 'NOT SQL' 2>/dev/null; then
  fail "garbage query should have failed"
else
  pass "garbage query rejected by server"
fi

# Cleanup.
curl -sS -X DELETE "${BASE_URL}/api/v1/bucket/docs/doc/pg-1" >/dev/null || true
