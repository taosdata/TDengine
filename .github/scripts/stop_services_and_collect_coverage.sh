#!/usr/bin/env bash
set -euo pipefail

CONTAINER_ID="${CONTAINER_ID:?CONTAINER_ID is required}"
COVERAGE_BASE_DIR="${COVERAGE_BASE_DIR:?COVERAGE_BASE_DIR is required}"
DIR_PATH="${DIR_PATH:?DIR_PATH is required}"

docker exec "$CONTAINER_ID" sh <<'SH'
set -eu

stop_process() {
  name="$1"
  pids="$(pgrep -x "$name" || true)"
  if [ -n "$pids" ]; then
    for pid in $pids; do
      kill -TERM "$pid"
    done
  fi
}

stop_process taosx
stop_process taosx-agent
stop_process taos-explorer
SH

wait_for_profraw() {
  local coverage_dir="${1:?coverage_dir is required}"
  local timeout_seconds="${2:-30}"

  for _ in $(seq 1 "$timeout_seconds"); do
    if find "$coverage_dir" -maxdepth 1 -name 'coverage-*.profraw' -print -quit | grep -q .; then
      return 0
    fi
    sleep 1
  done

  return 1
}

if ! wait_for_profraw "${COVERAGE_BASE_DIR}/${DIR_PATH}" 30; then
  echo "Warning: No .profraw files found in ${COVERAGE_BASE_DIR}/${DIR_PATH} after waiting 30 seconds"
  ls -al "${COVERAGE_BASE_DIR}/${DIR_PATH}" 2>/dev/null || true
fi

echo "Converting coverage files inside container..."
docker exec "$CONTAINER_ID" \
  /usr/local/bin/convert-coverage.sh \
  "${COVERAGE_BASE_DIR}/${DIR_PATH}" \
  /tmp/integration-test-coverage.lcov

docker cp \
  "$CONTAINER_ID:/tmp/integration-test-coverage.lcov" \
  "${COVERAGE_BASE_DIR}/${DIR_PATH}/llvm-cov-integration.lcov" \
  2>/dev/null || echo "Warning: Failed to copy integration test coverage file"
