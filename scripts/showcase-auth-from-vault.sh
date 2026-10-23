#!/usr/bin/env bash
# Print the showcase basic-auth credentials from WSLVault as .env lines:
#   SHOWCASE_AUTH_USERNAME='…'
#   SHOWCASE_AUTH_PASSWORD='…'
#
# The same Vault path may also hold Google OAuth keys used by the k8s
# oauth2-proxy sidecar (not printed here):
#   GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET,
#   OAUTH2_PROXY_COOKIE_SECRET  (openssl rand -base64 32)
#
# Usage: scripts/showcase-auth-from-vault.sh <path>   (e.g. nebuladb/int/showcase-auth)
#
# Auth, first match: WSLVAULT_TOKEN, WSLVAULT_API_KEY, or a machine API key
# (no MFA) in WSLVAULT_API_KEY_FILE, exchanged at /v1/auth/api-key. The
# path is under the KV v2 mount `kv`, same as the ESO `wslvault-backend`
# store the k8s envs read. Values are single-quoted so `. ./.env` and
# compose read them literally; they must not contain `'`.
set -euo pipefail

[[ $# -eq 1 ]] || { echo "usage: $0 <wslvault path>" >&2; exit 2; }
export WSLVAULT_ADDR="${WSLVAULT_ADDR:-https://vault.workstation.co.uk}"
export VAULT_SECRET_PATH="$1"

python3 -I - <<'PY'
import json, os, sys, urllib.error, urllib.request

addr = os.environ["WSLVAULT_ADDR"].rstrip("/")

def call(method, path, body=None, token=None):
    req = urllib.request.Request(addr + path, method=method,
                                 data=json.dumps(body).encode() if body is not None else None)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("X-Vault-Token", token)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        sys.exit("wslvault %s %s -> HTTP %s" % (method, path, exc.code))
    except urllib.error.URLError as exc:
        sys.exit("wslvault %s %s -> %s" % (method, path, exc.reason))

token = os.environ.get("WSLVAULT_TOKEN", "")
if not token:
    key = os.environ.get("WSLVAULT_API_KEY", "")
    key_file = os.environ.get("WSLVAULT_API_KEY_FILE", "")
    if not key and key_file and os.path.isfile(key_file):
        key = open(key_file).read().strip()
    if not key:
        sys.exit("wslvault auth unavailable: set WSLVAULT_TOKEN, WSLVAULT_API_KEY or WSLVAULT_API_KEY_FILE")
    resp = call("POST", "/v1/auth/api-key", {"api_key": key})
    if resp.get("mfa_required") or not resp.get("token"):
        sys.exit("wslvault API key gave no token (MFA-bound keys can't be used here)")
    token = resp["token"]

path = os.environ["VAULT_SECRET_PATH"].removeprefix("kv/")
data = (call("GET", "/v1/kv/data/" + path, token=token).get("data") or {}).get("data") or {}
out = []
for k in ("SHOWCASE_AUTH_USERNAME", "SHOWCASE_AUTH_PASSWORD"):
    v = str(data.get(k) or "")
    if not v:
        sys.exit("%s is missing from %s" % (k, path))
    if "'" in v:
        sys.exit("%s in %s contains a single quote" % (k, path))
    out.append("%s='%s'" % (k, v))
if os.environ.get("GITHUB_ACTIONS") == "true":
    # Keep the password out of the job log.
    print("::add-mask::" + str(data["SHOWCASE_AUTH_PASSWORD"]), file=sys.stderr)
print("\n".join(out))
PY
