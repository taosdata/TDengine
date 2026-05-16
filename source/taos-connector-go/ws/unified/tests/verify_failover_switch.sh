#!/usr/bin/env bash

set -euo pipefail

# Verify unified driver failover using a real query loop:
# 1) start two taosadapter instances on different ports
# 2) run ws/unified failover probe program (go run, NOT go test)
# 3) stop one adapter while probe is querying
# 4) probe must keep succeeding after failover marker

ADAPTER_BIN="${ADAPTER_BIN:-taosadapter}"
TAOS_USER="${TAOS_USER:-root}"
TAOS_PASS="${TAOS_PASS:-taosdata}"
LOG_LEVEL="${LOG_LEVEL:-debug}"
PROBE_TOTAL_SEC="${PROBE_TOTAL_SEC:-25}"
PROBE_INTERVAL_MS="${PROBE_INTERVAL_MS:-200}"
PROBE_POST_SUCCESS="${PROBE_POST_SUCCESS:-5}"
READY_TIMEOUT_SEC="${READY_TIMEOUT_SEC:-15}"
PROBE_DSN="${PROBE_DSN:-}"

PORT1="${PORT1:-}"
PORT2="${PORT2:-}"

LOG_DIR="${LOG_DIR:-/tmp/unified_failover_switch_$$}"
mkdir -p "${LOG_DIR}"

MARK_FILE="${LOG_DIR}/probe_mark"
READY_FILE="${LOG_DIR}/probe_ready"
PROBE_LOG="${LOG_DIR}/probe.log"

PID1=""
PID2=""
PROBE_PID=""

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing command: $1" >&2
    exit 1
  fi
}

is_port_open() {
  local port="$1"
  (echo >/dev/tcp/127.0.0.1/"${port}") >/dev/null 2>&1
}

pick_free_port() {
  local p i
  for i in $(seq 1 200); do
    p=$((20000 + RANDOM % 20000))
    if ! is_port_open "${p}"; then
      echo "${p}"
      return 0
    fi
  done
  echo "failed to find free port" >&2
  exit 1
}

ping_adapter() {
  local port="$1"
  local status
  status="$(curl -sS -o /dev/null -w "%{http_code}" --max-time 1 "http://127.0.0.1:${port}/-/ping" || true)"
  [[ "${status}" == "200" ]]
}

start_adapter() {
  local port="$1"
  local log_file="${LOG_DIR}/taosadapter_${port}.log"
  "${ADAPTER_BIN}" --port "${port}" --logLevel "${LOG_LEVEL}" >"${log_file}" 2>&1 &
  local pid=$!

  local deadline=$((SECONDS + 15))
  while (( SECONDS < deadline )); do
    if ping_adapter "${port}"; then
      echo "${pid}"
      return 0
    fi
    sleep 0.1
  done

  kill "${pid}" >/dev/null 2>&1 || true
  wait "${pid}" >/dev/null 2>&1 || true
  echo "taosadapter on port ${port} failed to start, log: ${log_file}" >&2
  exit 1
}

stop_adapter() {
  local pid="$1"
  if [[ -z "${pid}" ]]; then
    return 0
  fi
  kill -INT "${pid}" >/dev/null 2>&1 || true
  local deadline=$((SECONDS + 5))
  while (( SECONDS < deadline )); do
    if ! kill -0 "${pid}" >/dev/null 2>&1; then
      wait "${pid}" >/dev/null 2>&1 || true
      return 0
    fi
    sleep 0.1
  done
  kill -KILL "${pid}" >/dev/null 2>&1 || true
  wait "${pid}" >/dev/null 2>&1 || true
}

wait_file() {
  local path="$1"
  local timeout="$2"
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    if [[ -f "${path}" ]]; then
      return 0
    fi
    if [[ -n "${PROBE_PID}" ]] && ! kill -0 "${PROBE_PID}" >/dev/null 2>&1; then
      return 1
    fi
    sleep 0.1
  done
  return 1
}

cleanup() {
  set +e
  stop_adapter "${PID1}"
  stop_adapter "${PID2}"
  if [[ -n "${PROBE_PID}" ]] && kill -0 "${PROBE_PID}" >/dev/null 2>&1; then
    kill -INT "${PROBE_PID}" >/dev/null 2>&1 || true
    wait "${PROBE_PID}" >/dev/null 2>&1 || true
  fi
  echo "logs: ${LOG_DIR}"
}
trap cleanup EXIT

need_cmd curl
need_cmd go
need_cmd "${ADAPTER_BIN}"

if [[ -z "${PORT1}" ]]; then
  PORT1="$(pick_free_port)"
fi
if [[ -z "${PORT2}" ]]; then
  PORT2="$(pick_free_port)"
  if [[ "${PORT2}" == "${PORT1}" ]]; then
    PORT2="$(pick_free_port)"
  fi
fi

echo "starting taosadapter: ${PORT1}, ${PORT2}"
PID1="$(start_adapter "${PORT1}")"
PID2="$(start_adapter "${PORT2}")"

if [[ -z "${PROBE_DSN}" ]]; then
  PROBE_DSN="${TAOS_USER}:${TAOS_PASS}@ws(127.0.0.1:${PORT1},127.0.0.1:${PORT2})/"
fi

echo "starting unified failover probe"
GOCACHE="$(pwd)/.cache/go-build" \
PROBE_DSN="${PROBE_DSN}" \
PROBE_MARK_FILE="${MARK_FILE}" \
PROBE_READY_FILE="${READY_FILE}" \
PROBE_TOTAL_SEC="${PROBE_TOTAL_SEC}" \
PROBE_INTERVAL_MS="${PROBE_INTERVAL_MS}" \
PROBE_POST_SUCCESS="${PROBE_POST_SUCCESS}" \
go run ./ws/unified/tests/cmd/failover_probe >"${PROBE_LOG}" 2>&1 &
PROBE_PID=$!

if ! wait_file "${READY_FILE}" "${READY_TIMEOUT_SEC}"; then
  echo "probe was not ready in ${READY_TIMEOUT_SEC}s" >&2
  if [[ -f "${PROBE_LOG}" ]]; then
    cat "${PROBE_LOG}" >&2
  fi
  exit 1
fi

echo "stopping adapter on port ${PORT1} to trigger failover"
stop_adapter "${PID1}"
PID1=""
touch "${MARK_FILE}"

if ! wait "${PROBE_PID}"; then
  echo "probe failed" >&2
  cat "${PROBE_LOG}" >&2
  exit 1
fi
PROBE_PID=""

echo "probe completed successfully"
cat "${PROBE_LOG}"
echo "PASS: unified driver failover switch verified"
