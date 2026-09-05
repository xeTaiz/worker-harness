#!/bin/sh
set -eu

# The edge authenticates trusted Tailnet browser requests upstream. The secret
# never enters static assets, browser storage, or the container image.
token_file="${WH_OPERATOR_TOKEN_FILE:-/run/secrets/wh_operator_token}"
if [ ! -r "$token_file" ]; then
    echo "wh-web: missing operator token file: $token_file" >&2
    exit 1
fi
token="$(cat "$token_file")"
case "$token" in
    ''|*[!A-Za-z0-9_-]*) echo 'wh-web: operator token must be base64url' >&2; exit 1 ;;
esac
if [ "${#token}" -lt 43 ]; then
    echo 'wh-web: operator token must contain at least 256 bits of randomness' >&2
    exit 1
fi
umask 077
printf 'proxy_set_header Authorization "Bearer %s";\n' "$token" > /tmp/wh-operator-auth.conf
