#!/usr/bin/env bash
# Deploy the NebulaDB dashboard suite to Grafana.
#
# Idempotent: ensures the Prometheus datasource and the "NebulaDB" folder
# exist, then upserts every dashboards/*.json (overwrite=true, stable
# uids). Run locally or from CI:
#
#   GRAFANA_URL=https://grafana.workstation.co.uk \
#   GRAFANA_USER=admin GRAFANA_PASSWORD=... ./deploy/grafana/deploy.sh
#
# If GRAFANA_PASSWORD is unset, falls back to /tmp/grafanaapikey (admin
# password file used for local deploys).
set -euo pipefail

GRAFANA_URL="${GRAFANA_URL:-https://grafana.workstation.co.uk}"
GRAFANA_USER="${GRAFANA_USER:-admin}"
if [ -z "${GRAFANA_PASSWORD:-}" ]; then
  if [ -f /tmp/grafanaapikey ]; then
    GRAFANA_PASSWORD="$(tr -d '[:space:]' </tmp/grafanaapikey)"
  else
    echo "set GRAFANA_PASSWORD or write the admin password to /tmp/grafanaapikey" >&2
    exit 1
  fi
fi
# In-cluster Prometheus the datasource should point at if it has to be
# created. On the central obs stack the DS already exists (uid
# "prometheus") and this is never used.
PROM_URL="${PROM_URL:-http://obs-prometheus.prod:9090/}"
DIR="$(cd "$(dirname "$0")" && pwd)"

api() { # method path [json-file]
  local method="$1" path="$2" body="${3:-}"
  local args=(-sS -u "${GRAFANA_USER}:${GRAFANA_PASSWORD}" -H 'Content-Type: application/json'
              -X "$method" "${GRAFANA_URL%/}${path}" -w '\n%{http_code}')
  [ -n "$body" ] && args+=(--data-binary "@${body}")
  curl "${args[@]}"
}

check() { # "label" response  → fails the script on non-2xx
  local label="$1" resp="$2"
  local code="${resp##*$'\n'}" body="${resp%$'\n'*}"
  if [ "${code:0:1}" != "2" ]; then
    echo "✗ ${label}: HTTP ${code}: ${body}" >&2
    exit 1
  fi
  echo "✓ ${label}"
}

echo "── Grafana: ${GRAFANA_URL}"

# 1. Ensure the Prometheus datasource (uid "prometheus") exists.
ds_code=$(curl -sS -o /dev/null -w '%{http_code}' -u "${GRAFANA_USER}:${GRAFANA_PASSWORD}" \
  "${GRAFANA_URL%/}/api/datasources/uid/prometheus")
if [ "$ds_code" = "404" ]; then
  tmp=$(mktemp)
  cat > "$tmp" <<EOF
{"name": "Prometheus", "uid": "prometheus", "type": "prometheus",
 "url": "${PROM_URL}", "access": "proxy", "isDefault": true}
EOF
  check "create Prometheus datasource" "$(api POST /api/datasources "$tmp")"
  rm -f "$tmp"
else
  echo "✓ Prometheus datasource present (HTTP ${ds_code})"
fi

# 2. Ensure the NebulaDB folder.
folder_code=$(curl -sS -o /dev/null -w '%{http_code}' -u "${GRAFANA_USER}:${GRAFANA_PASSWORD}" \
  "${GRAFANA_URL%/}/api/folders/nebuladb")
if [ "$folder_code" = "404" ]; then
  tmp=$(mktemp)
  echo '{"uid": "nebuladb", "title": "NebulaDB"}' > "$tmp"
  check "create NebulaDB folder" "$(api POST /api/folders "$tmp")"
  rm -f "$tmp"
else
  echo "✓ NebulaDB folder present (HTTP ${folder_code})"
fi

# 3. Upsert every dashboard (identity is the uid field inside each JSON).
fail=0
for f in "$DIR"/dashboards/*.json; do
  name=$(basename "$f" .json)
  tmp=$(mktemp)
  python3 - "$f" > "$tmp" <<'EOF'
import json, sys
d = json.load(open(sys.argv[1]))
d.pop("id", None)          # never pin a numeric id — uid is the identity
print(json.dumps({"dashboard": d, "folderUid": "nebuladb",
                  "overwrite": True, "message": "deployed by deploy/grafana/deploy.sh"}))
EOF
  resp=$(api POST /api/dashboards/db "$tmp"); rm -f "$tmp"
  code="${resp##*$'\n'}"
  if [ "${code:0:1}" = "2" ]; then
    echo "✓ dashboard ${name}"
  else
    echo "✗ dashboard ${name}: HTTP ${code}: ${resp%$'\n'*}" >&2
    fail=1
  fi
done

# 4. Smoke-check each uid is retrievable.
for f in "$DIR"/dashboards/*.json; do
  uid=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["uid"])' "$f")
  code=$(curl -sS -o /dev/null -w '%{http_code}' -u "${GRAFANA_USER}:${GRAFANA_PASSWORD}" \
    "${GRAFANA_URL%/}/api/dashboards/uid/${uid}")
  if [ "$code" = "200" ]; then
    echo "✓ smoke ${uid}"
  else
    echo "✗ smoke ${uid}: HTTP ${code}" >&2
    fail=1
  fi
done

exit $fail
