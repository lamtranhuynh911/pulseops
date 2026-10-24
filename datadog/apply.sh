#!/usr/bin/env bash
# Create or update the PulseOps dashboard and monitors in Datadog, from datadog/dashboard.json and
# datadog/monitors/*.json. Idempotent: existing objects are found by title (dashboard) or name
# (monitors) and updated, so running it twice creates nothing new.
#
# Needs DD_API_KEY, DD_APP_KEY and DD_SITE (from .env: set -a; . ./.env; set +a). DD_NOTIFY, if set,
# is appended to every monitor message (e.g. @you@example.com). Requires curl and jq.
set -euo pipefail
cd "$(dirname "$0")"
: "${DD_API_KEY:?set DD_API_KEY}" "${DD_APP_KEY:?set DD_APP_KEY}"
api="https://api.${DD_SITE:-datadoghq.com}/api/v1"

dd() {
  curl -sS --fail-with-body -H "DD-API-KEY: ${DD_API_KEY}" -H "DD-APPLICATION-KEY: ${DD_APP_KEY}" \
    -H "Content-Type: application/json" "$@"
}

monitor_body() {
  jq --arg notify "${DD_NOTIFY:-}" 'if $notify != "" then .message += "\n\n" + $notify else . end' "$1"
}

# Validate every monitor first, so a bad file changes nothing.
for file in monitors/*.json; do
  monitor_body "$file" | dd -X POST "$api/monitor/validate" -d @- >/dev/null || { echo "invalid monitor: $file" >&2; exit 1; }
done

title=$(jq -r .title dashboard.json)
id=$(dd "$api/dashboard" | jq -r --arg t "$title" '[.dashboards[] | select(.title == $t) | .id][0] // empty')
if [ -n "$id" ]; then
  dd -X PUT "$api/dashboard/$id" -d @dashboard.json >/dev/null && echo "updated dashboard: $title ($id)"
else
  id=$(dd -X POST "$api/dashboard" -d @dashboard.json | jq -r .id) && echo "created dashboard: $title ($id)"
fi

for file in monitors/*.json; do
  name=$(jq -r .name "$file")
  id=$(dd --get "$api/monitor" --data-urlencode "name=$name" | jq -r --arg n "$name" '[.[] | select(.name == $n) | .id][0] // empty')
  if [ -n "$id" ]; then
    monitor_body "$file" | dd -X PUT "$api/monitor/$id" -d @- >/dev/null && echo "updated monitor: $name ($id)"
  else
    id=$(monitor_body "$file" | dd -X POST "$api/monitor" -d @- | jq -r .id) && echo "created monitor: $name ($id)"
  fi
done
