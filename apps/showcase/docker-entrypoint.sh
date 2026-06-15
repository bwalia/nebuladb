#!/bin/sh
# Render the nginx config template and launch OpenResty.
#
# Unlike the stock `nginx` image, the OpenResty image has NO
# envsubst-on-templates entrypoint, so we do it ourselves: substitute
# ${NEBULA_SERVER} into our template, drop it into conf.d/, and remove
# OpenResty's stock default.conf (which would otherwise bind :80 and
# shadow our server block). Only ${NEBULA_SERVER} is substituted so the
# many `$var` references in the nginx/Lua config are left intact.
set -eu

TEMPLATE=/etc/nginx/templates/default.conf.template
OUT=/etc/nginx/conf.d/default.conf

if [ -f "$TEMPLATE" ]; then
  envsubst '${NEBULA_SERVER}' <"$TEMPLATE" >"$OUT"
  echo "rendered $OUT (NEBULA_SERVER=${NEBULA_SERVER})"
fi

# Remove the stock OpenResty server block if it's still the pristine one
# (ours overwrote default.conf above, so this is belt-and-braces for the
# case where the template was missing).
# shellcheck disable=SC2016
if [ -f "$OUT" ] && grep -q 'nginx.vh.default.conf' "$OUT" 2>/dev/null; then
  echo "WARNING: conf.d/default.conf is still the stock OpenResty file" >&2
fi

# Validate before launching so a bad render fails fast with a clear
# message rather than a half-started server.
/usr/local/openresty/bin/openresty -t

exec /usr/local/openresty/bin/openresty -g 'daemon off;'
