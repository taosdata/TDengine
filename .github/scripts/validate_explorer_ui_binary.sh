#!/usr/bin/env bash
set -euo pipefail

DIST_INDEX_PATH="${DIST_INDEX_PATH:-explorer/dist/index.html}"
EXPLORER_BINARY="${EXPLORER_BINARY:-./target/debug/taos-explorer}"
EXPLORER_ASSETS="${EXPLORER_ASSETS:-$(dirname "$DIST_INDEX_PATH")}"
EXPLORER_ADDR="${EXPLORER_ADDR:-127.0.0.1}"
EXPLORER_PORT_START="${EXPLORER_PORT_START:-16060}"
EXPLORER_PORT_END="${EXPLORER_PORT_END:-17060}"
EXPLORER_CLUSTER_URL="${EXPLORER_CLUSTER_URL:-http://127.0.0.1:6041}"
EXPLORER_X_API_URL="${EXPLORER_X_API_URL:-http://127.0.0.1:6050}"
EXPLORER_GRPC_URL="${EXPLORER_GRPC_URL:-http://127.0.0.1:6055}"
EXPLORER_LOG_FILE="${EXPLORER_LOG_FILE:-/tmp/taos-explorer-ui-validation.log}"

if [ ! -f "$DIST_INDEX_PATH" ]; then
  echo "ERROR: ${DIST_INDEX_PATH} is missing after explorer build"
  find "$(dirname "$DIST_INDEX_PATH")" -maxdepth 2 -type f | sort || true
  exit 1
fi

if [ ! -x "$EXPLORER_BINARY" ]; then
  echo "ERROR: explorer binary is missing or not executable: ${EXPLORER_BINARY}"
  exit 1
fi

find_available_port() {
  local start_port="$1"
  local end_port="$2"

  for port in $(seq "$start_port" "$end_port"); do
    if ! (echo >/dev/tcp/127.0.0.1/"$port") &>/dev/null; then
      echo "$port"
      return 0
    fi
  done

  return 1
}

PORT="$(find_available_port "$EXPLORER_PORT_START" "$EXPLORER_PORT_END")"
if [ -z "$PORT" ]; then
  echo "ERROR: failed to find an available port for explorer UI validation"
  exit 1
fi

rm -f "$EXPLORER_LOG_FILE"

EXPLORER_SKIP_REGISTER=true "$EXPLORER_BINARY" \
  --addr "$EXPLORER_ADDR" \
  --port "$PORT" \
  --cluster "$EXPLORER_CLUSTER_URL" \
  --x-api "$EXPLORER_X_API_URL" \
  --grpc "$EXPLORER_GRPC_URL" \
  --assets "$EXPLORER_ASSETS" \
  >"$EXPLORER_LOG_FILE" 2>&1 &
EXPLORER_PID=$!

cleanup() {
  if kill -0 "$EXPLORER_PID" 2>/dev/null; then
    kill "$EXPLORER_PID"
    wait "$EXPLORER_PID" || true
  fi
}
trap cleanup EXIT

READY=0
for _ in $(seq 1 30); do
  if curl -fs "http://${EXPLORER_ADDR}:${PORT}/login" -o /tmp/explorer-login-validation.html; then
    READY=1
    break
  fi
  sleep 1
done

if [ "$READY" -ne 1 ]; then
  echo "ERROR: taos-explorer failed to serve /login during post-build validation"
  cat "$EXPLORER_LOG_FILE" || true
  exit 1
fi

for route in / /login; do
  body_file="$(mktemp)"
  http_status="$(curl -sS -o "$body_file" -w "%{http_code}" "http://${EXPLORER_ADDR}:${PORT}${route}")"

  if [ "$http_status" != "200" ]; then
    echo "ERROR: taos-explorer returned HTTP ${http_status} for route ${route}"
    echo "=== Response body for ${route} ==="
    cat "$body_file" || true
    echo "=== Explorer log ==="
    cat "$EXPLORER_LOG_FILE" || true
    rm -f "$body_file"
    exit 1
  fi

  if ! grep -q '<script' "$body_file"; then
    echo "ERROR: taos-explorer returned non-UI HTML for route ${route}"
    echo "=== Response body for ${route} ==="
    cat "$body_file" || true
    echo "=== Explorer log ==="
    cat "$EXPLORER_LOG_FILE" || true
    rm -f "$body_file"
    exit 1
  fi

  rm -f "$body_file"
done
