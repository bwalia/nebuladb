#!/usr/bin/env bash
# Shared helpers for NebulaDB CI test scripts.
#
# Source this from every test script. Provides:
#   - strict bash defaults
#   - colorized `pass` / `fail` logging
#   - `assert` and `assert_eq` for human-readable test output
#   - `BASE_URL` / `PG_HOST` / `PG_PORT` env-driven config
#
# Every `fail` also increments a counter; the calling script's `finish`
# handler prints a summary and exits non-zero on any failure.
#
# This is deliberately plain bash + curl + jq — no Python runtime, no
# test framework — so the scripts run identically on a dev laptop and
# on a GitHub-hosted runner.

set -Eeuo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
# Portable wall-clock milliseconds. `date +%s%3N` is a GNU extension
# that BSD / macOS `date` doesn't support, so we fall back to a short
# Python invocation (present on every CI image and most dev boxes).
now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }
PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5433}"
PG_USER="${PG_USER:-any}"
PG_DB="${PG_DB:-any}"
# Give each script its own state file. This lets `finish` print a
# summary without needing the script to track failures manually.
_NB_STATE_DIR="${_NB_STATE_DIR:-/tmp/nebuladb-test-$$}"
mkdir -p "$_NB_STATE_DIR"
: >"$_NB_STATE_DIR/failures"

# Terminal colors only when we're actually talking to a tty. CI logs
# keep ANSI codes fine but piping through `tee` into a file without
# colors is nicer.
if [[ -t 1 ]]; then
  _C_RED=$'\033[31m'
  _C_GREEN=$'\033[32m'
  _C_YELLOW=$'\033[33m'
  _C_RESET=$'\033[0m'
else
  _C_RED=""
  _C_GREEN=""
  _C_YELLOW=""
  _C_RESET=""
fi

log()  { printf '%s[*]%s %s\n' "$_C_YELLOW" "$_C_RESET" "$*"; }
pass() { printf '%s[✓]%s %s\n' "$_C_GREEN" "$_C_RESET" "$*"; }
fail() {
  printf '%s[✗]%s %s\n' "$_C_RED" "$_C_RESET" "$*" >&2
  echo "$*" >>"$_NB_STATE_DIR/failures"
}

# assert <bool-command> <description>
#   runs the given command, passes on success, fails on non-zero.
assert() {
  local desc="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    pass "$desc"
  else
    fail "$desc  (cmd failed: $*)"
  fi
}

# assert_eq <expected> <actual> <description>
assert_eq() {
  local expected="$1" actual="$2" desc="$3"
  if [[ "$expected" == "$actual" ]]; then
    pass "$desc"
  else
    fail "$desc  (expected '$expected', got '$actual')"
  fi
}

# assert_contains <haystack> <needle> <description>
assert_contains() {
  local haystack="$1" needle="$2" desc="$3"
  if [[ "$haystack" == *"$needle"* ]]; then
    pass "$desc"
  else
    fail "$desc  (needle '$needle' not found)"
  fi
}

# finish: print a summary and exit. Source this into every script via
# `trap finish EXIT` so a bail-out mid-script still reports cleanly.
finish() {
  local rc=$?
  local n
  n=$(wc -l <"$_NB_STATE_DIR/failures" | tr -d ' ')
  if [[ "$n" -eq 0 && "$rc" -eq 0 ]]; then
    printf '\n%s===> ALL PASSED%s\n' "$_C_GREEN" "$_C_RESET"
    exit 0
  fi
  printf '\n%s===> %d failure(s)%s\n' "$_C_RED" "$n" "$_C_RESET" >&2
  if [[ "$n" -gt 0 ]]; then
    cat "$_NB_STATE_DIR/failures" >&2
  fi
  exit 1
}

# http_post <path> <json-body> -> prints body, errors on non-2xx.
# The `-w` format writes the HTTP status on the final line so we can
# assert on it without parsing the body.
http_post() {
  local path="$1" body="$2"
  curl -sS -X POST "${BASE_URL}${path}" \
    -H 'content-type: application/json' \
    -w '\n%{http_code}' \
    -d "$body"
}

http_get() {
  local path="$1"
  curl -sS "${BASE_URL}${path}" -w '\n%{http_code}'
}

# body_of <response> — strip the last line (http code) added by the helpers.
# The default `""` lets `set -u` coexist with curl responses that
# come back empty (timeouts, network errors) without tripping on an
# unbound arg.
body_of() { echo "${1:-}" | sed '$d'; }
code_of() { echo "${1:-}" | tail -n1; }
