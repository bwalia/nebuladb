#!/bin/bash
# Forced command for the `nebuladb-edge-gha` SSH key on lon1 (root).
# Installed at /usr/local/bin/nebuladb-edge-install; authorized_keys entry:
#   command="/usr/local/bin/nebuladb-edge-install",no-port-forwarding,no-X11-forwarding,no-agent-forwarding,no-pty ssh-ed25519 ... nebuladb-edge-gha
#
# Reads a tar on stdin and installs ONLY the nebuladb website records into
# /opt/nginx/data. Anything else in the archive aborts the run before any
# file is written. Unchanged files are left alone; changed ones are backed
# up first. OpenResty reads servers/rules from disk per request, so no
# reload is needed. lon2 (pop0) mirrors lon1 every 5 minutes.
set -euo pipefail
DATA=/opt/nginx/data
ALLOWED="servers/prod/host:nebuladb.net.json servers/prod/host:www.nebuladb.net.json rules/prod/nebuladb-website-pages.json"

T=$(mktemp -d)
trap 'rm -rf "$T"' EXIT
tar -xf - -C "$T" --no-same-owner --no-same-permissions
cd "$T"

if [ -n "$(find . ! -type d ! -type f)" ]; then echo "refused: archive has links or special files" >&2; exit 1; fi
files=$(find . -type f | sed 's#^\./##' | sort)
[ -n "$files" ] || { echo "refused: empty archive" >&2; exit 1; }
for r in $files; do
  case " $ALLOWED " in *" $r "*) ;; *) echo "refused: $r is not a nebuladb website record" >&2; exit 1 ;; esac
  python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$r" || { echo "refused: $r is not valid JSON" >&2; exit 1; }
done

for r in $files; do
  if [ -e "$DATA/$r" ] && cmp -s "$r" "$DATA/$r"; then echo "unchanged $r"; continue; fi
  if [ -e "$DATA/$r" ]; then cp "$DATA/$r" "$DATA/$r.bak.$(date +%s)"; fi
  install -o 33 -g 0 -m 0777 "$r" "$DATA/$r.tmp"
  mv -f "$DATA/$r.tmp" "$DATA/$r"
  echo "installed $r"
done
