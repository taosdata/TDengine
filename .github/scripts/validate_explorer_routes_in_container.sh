#!/usr/bin/env bash
set -euo pipefail

PLAYWRIGHT_BASE_URL="${PLAYWRIGHT_BASE_URL:?PLAYWRIGHT_BASE_URL is required}"
CONTAINER_ID="${CONTAINER_ID:?CONTAINER_ID is required}"

echo "Validating explorer frontend routes at ${PLAYWRIGHT_BASE_URL}"

for route in / /login /dataIn/Task; do
  route_ready=false
  for _ in $(seq 1 60); do
    body_file="$(mktemp)"
    http_status="$(curl -sS -o "$body_file" -w "%{http_code}" "${PLAYWRIGHT_BASE_URL}${route}" || true)"

    if [ "$http_status" = "200" ] && grep -q '<script' "$body_file"; then
      route_ready=true
      rm -f "$body_file"
      break
    fi

    rm -f "$body_file"
    sleep 1
  done

  if [ "$route_ready" != "true" ]; then
    body_file="$(mktemp)"
    http_status="$(curl -sS -o "$body_file" -w "%{http_code}" "${PLAYWRIGHT_BASE_URL}${route}" || true)"
    echo "ERROR: Explorer route ${route} did not become ready"
    echo "HTTP status: ${http_status}"
    echo "=== Response body for ${route} ==="
    cat "$body_file" || true
    echo "=== Container explorer log ==="
    docker exec "$CONTAINER_ID" sh -c 'cat /tmp/taos-explorer.log' || true
    rm -f "$body_file"
    exit 1
  fi
done
