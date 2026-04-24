#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

PLAYWRIGHT_BASE_URL="${PLAYWRIGHT_BASE_URL:?PLAYWRIGHT_BASE_URL is required}"
UI_WORKDIR="${UI_WORKDIR:-explorer}"
PLAYWRIGHT_WORKERS="${PLAYWRIGHT_WORKERS:-4}"
PLAYWRIGHT_BROWSERS_PATH="${PLAYWRIGHT_BROWSERS_PATH:-${REPO_ROOT}/.cache/ms-playwright}"

export PLAYWRIGHT_WORKERS
export PLAYWRIGHT_BROWSERS_PATH

validate_embedded_asset_paths() {
  local route="$1"
  local body_file="$2"
  local invalid_refs
  invalid_refs="$(
    grep -oE '(src|href)="[^"]+"' "$body_file" \
      | sed -E 's/^[^=]+="([^"]+)"$/\1/' \
      | grep -E -v '^(https?:)?//|^data:|^#|^/$|^/api/' \
      | grep -E -v '^/|^\./' \
      || true
  )"

  if [ -n "$invalid_refs" ]; then
    echo "ERROR: ${route} returned HTML with non-root asset references"
    printf '%s\n' "$invalid_refs"
    echo "=== Page content ==="
    cat "$body_file"
    echo "===================="
    exit 1
  fi
}

echo "Checking explorer frontend at ${PLAYWRIGHT_BASE_URL}/login"
for i in $(seq 1 30); do
  http_status="$(curl -s -o /tmp/explorer-login.html -w "%{http_code}" "${PLAYWRIGHT_BASE_URL}/login")"
  if [ "$http_status" = "200" ]; then
    if grep -q '<script' /tmp/explorer-login.html; then
      validate_embedded_asset_paths "/login" /tmp/explorer-login.html
      echo "Explorer frontend is ready (HTML contains script tags)"
      break
    fi

    echo "WARNING: Login page HTML has no script tags — frontend may not be embedded"
    echo "=== Page content ==="
    cat /tmp/explorer-login.html
    echo "===================="
    exit 1
  fi

  echo "Attempt ${i}/30: Explorer returned HTTP ${http_status}, retrying in 2s..."
  sleep 2
done

if ! grep -q '<script' /tmp/explorer-login.html 2>/dev/null; then
  echo "ERROR: /login did not return HTML with script tags after 30 attempts"
  echo "Last HTTP status: ${http_status}"
  echo "=== Page content ==="
  cat /tmp/explorer-login.html 2>/dev/null || echo "<no file>"
  echo "===================="
  exit 1
fi

cd "$UI_WORKDIR"
pnpm install --frozen-lockfile --prefer-offline
pnpm exec playwright install chromium --with-deps
echo "Re-checking deep-link HTML route at ${PLAYWRIGHT_BASE_URL}/dataIn/Task"
for i in $(seq 1 30); do
  http_status="$(curl -s -o /tmp/explorer-datain-task.html -w "%{http_code}" "${PLAYWRIGHT_BASE_URL}/dataIn/Task")"
  if [ "$http_status" = "200" ] && grep -q '<script' /tmp/explorer-datain-task.html; then
    validate_embedded_asset_paths "/dataIn/Task" /tmp/explorer-datain-task.html
    echo "Deep-link HTML route is ready"
    break
  fi

  echo "Attempt ${i}/30: /dataIn/Task returned HTTP ${http_status} or missing script tags, retrying in 2s..."
  sleep 2
done

if ! grep -q '<script' /tmp/explorer-datain-task.html; then
  echo "ERROR: /dataIn/Task did not return HTML with script tags"
  echo "=== Page content ==="
  cat /tmp/explorer-datain-task.html
  echo "===================="
  exit 1
fi

echo "Running Playwright tests with ${PLAYWRIGHT_WORKERS} workers"
if ! pnpm exec playwright test; then
  echo "=== Playwright tests FAILED — collecting diagnostics ==="

  post_status="$(curl -s -o /tmp/post-fail-login.html -w "%{http_code}" "${PLAYWRIGHT_BASE_URL}/login" || echo "000")"
  echo "Post-failure curl /login: HTTP ${post_status}"
  if [ "$post_status" != "200" ]; then
    cat /tmp/post-fail-login.html 2>/dev/null || echo "<empty>"
  fi

  if [ -n "${CONTAINER_ID:-}" ]; then
    echo "=== Container explorer process ==="
    docker exec "$CONTAINER_ID" sh -c 'ps aux | grep -E "taos-explorer" | grep -v grep' || echo "taos-explorer NOT FOUND!"
    echo "=== Container explorer log (last 50 lines) ==="
    docker exec "$CONTAINER_ID" sh -c 'tail -n 50 /tmp/taos-explorer.log 2>/dev/null' || echo "<log unavailable>"
  fi

  exit 1
fi
