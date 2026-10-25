#!/usr/bin/env bash
# Tạo hoặc cập nhật 3 dashboard PulseOps trong Datadog từ dashboards/*.json.
# Idempotent: dashboard đã có (tìm theo title) sẽ được cập nhật, chạy lại không tạo trùng.
#
# Cần: curl, jq và 3 biến môi trường DD_API_KEY, DD_APP_KEY, DD_SITE (mặc định datadoghq.com).
#   Trong repo pulseops:  set -a; . ./.env; set +a; ./datadog/apply-dashboards.sh
set -euo pipefail
cd "$(dirname "$0")"
: "${DD_API_KEY:?set DD_API_KEY}" "${DD_APP_KEY:?set DD_APP_KEY}"
site="${DD_SITE:-datadoghq.com}"
api="https://api.${site}/api/v1"
app="https://app.${site}"

dd() {
  curl -sS --fail-with-body -H "DD-API-KEY: ${DD_API_KEY}" -H "DD-APPLICATION-KEY: ${DD_APP_KEY}" \
    -H "Content-Type: application/json" "$@"
}

existing=$(dd "$api/dashboard")
for file in dashboards/*.json; do
  title=$(jq -r .title "$file")
  id=$(jq -r --arg t "$title" '[.dashboards[] | select(.title == $t) | .id][0] // empty' <<<"$existing")
  if [ -n "$id" ]; then
    dd -X PUT "$api/dashboard/$id" -d @"$file" >/dev/null
    echo "updated: $title -> $app/dashboard/$id"
  else
    id=$(dd -X POST "$api/dashboard" -d @"$file" | jq -r .id)
    echo "created: $title -> $app/dashboard/$id"
  fi
done
