#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <stmt2Test> <taosd>" >&2
  exit 2
fi

TEST_BIN="$1"
TAOSD_BIN="$2"

if [[ ! -x "${TEST_BIN}" ]]; then
  echo "test binary is not executable: ${TEST_BIN}" >&2
  exit 2
fi
if [[ ! -x "${TAOSD_BIN}" ]]; then
  echo "taosd binary is not executable: ${TAOSD_BIN}" >&2
  exit 2
fi

RUN_DIR="$(mktemp -d "${TMPDIR:-/tmp}/stmt2-vnode-stop.XXXXXX")"
CFG_DIR="${RUN_DIR}/cfg"
DATA_DIR="${RUN_DIR}/data"
LOG_DIR="${RUN_DIR}/log"
CONTROL_DIR="${RUN_DIR}/control"
CONSOLE_LOG="${LOG_DIR}/taosd.console.log"
mkdir -p "${CFG_DIR}" "${DATA_DIR}" "${LOG_DIR}" "${CONTROL_DIR}"

TAOSD_PID=""
TEST_PID=""

process_is_running() {
  local pid="$1"
  local state
  kill -0 "${pid}" 2>/dev/null || return 1
  state="$(ps -p "${pid}" -o stat= 2>/dev/null || true)"
  [[ -n "${state}" && "${state}" != Z* ]]
}

terminate_process() {
  local pid="$1"
  local i
  process_is_running "${pid}" || return 0
  kill -TERM "${pid}" 2>/dev/null || true
  for ((i = 0; i < 100; ++i)); do
    process_is_running "${pid}" || return 0
    sleep 0.05
  done
  kill -KILL "${pid}" 2>/dev/null || true
}

cleanup() {
  local status=$?
  trap - EXIT INT TERM HUP
  set +e
  if [[ -n "${TEST_PID}" ]]; then
    terminate_process "${TEST_PID}"
    wait "${TEST_PID}" 2>/dev/null
  fi
  if [[ -n "${TAOSD_PID}" ]]; then
    terminate_process "${TAOSD_PID}"
    wait "${TAOSD_PID}" 2>/dev/null
  fi
  if [[ ${status} -ne 0 ]]; then
    echo "stmt2 vnode-stop test failed; taosd log follows:" >&2
    tail -n 200 "${CONSOLE_LOG}" >&2 2>/dev/null
  fi
  if [[ ${status} -ne 0 || "${TAOS_STMT2_KEEP_RUN_DIR:-0}" == "1" ]]; then
    echo "stmt2 vnode-stop run directory retained at ${RUN_DIR}" >&2
  else
    rm -rf "${RUN_DIR}"
  fi
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

port_is_free() {
  local port="$1"
  if (exec 9<>"/dev/tcp/127.0.0.1/${port}") >/dev/null 2>&1; then
    exec 9>&-
    return 1
  fi
  return 0
}

choose_port() {
  local candidate
  local i
  if [[ -n "${TAOS_STMT2_TEST_PORT:-}" ]]; then
    candidate="${TAOS_STMT2_TEST_PORT}"
    if [[ ! "${candidate}" =~ ^[0-9]+$ ]] || ((candidate < 1 || candidate > 65056)); then
      echo "invalid TAOS_STMT2_TEST_PORT: ${candidate}" >&2
      return 1
    fi
    port_is_free "${candidate}" || {
      echo "TAOS_STMT2_TEST_PORT is already in use: ${candidate}" >&2
      return 1
    }
    echo "${candidate}"
    return 0
  fi

  candidate=$((16030 + ($$ % 20000)))
  for ((i = 0; i < 200; ++i)); do
    if ((candidate > 65056)); then
      candidate=16030
    fi
    if port_is_free "${candidate}"; then
      echo "${candidate}"
      return 0
    fi
    candidate=$((candidate + 1))
  done
  echo "could not find an unused server port" >&2
  return 1
}

PORT="$(choose_port)"
cat >"${CFG_DIR}/taos.cfg" <<EOF
firstEP localhost:${PORT}
fqdn localhost
serverPort ${PORT}
dataDir ${DATA_DIR}
logDir ${LOG_DIR}
asyncLog 0
monitor 0
statusInterval 1
supportVnodes 16
telemetryReporting 0
EOF

start_taosd() {
  local deadline=$((SECONDS + 90))
  "${TAOSD_BIN}" -c "${CFG_DIR}" </dev/null >>"${CONSOLE_LOG}" 2>&1 &
  TAOSD_PID=$!
  while ((SECONDS < deadline)); do
    if ! process_is_running "${TAOSD_PID}"; then
      wait "${TAOSD_PID}" 2>/dev/null || true
      TAOSD_PID=""
      echo "taosd exited before its server port became ready" >&2
      return 1
    fi
    if ! port_is_free "${PORT}"; then
      return 0
    fi
    sleep 0.05
  done
  echo "taosd did not listen within 90 seconds" >&2
  return 1
}

stop_taosd() {
  local i
  if [[ -z "${TAOSD_PID}" ]] || ! process_is_running "${TAOSD_PID}"; then
    echo "test-owned taosd is not running" >&2
    return 1
  fi
  kill -TERM "${TAOSD_PID}"
  for ((i = 0; i < 1200; ++i)); do
    if ! process_is_running "${TAOSD_PID}"; then
      wait "${TAOSD_PID}" 2>/dev/null || true
      TAOSD_PID=""
      return 0
    fi
    sleep 0.05
  done
  echo "taosd did not stop gracefully within 60 seconds" >&2
  return 1
}

wait_for_request() {
  local marker="$1"
  local deadline=$((SECONDS + 240))
  local test_status
  while ((SECONDS < deadline)); do
    if ! process_is_running "${TEST_PID}"; then
      set +e
      wait "${TEST_PID}"
      test_status=$?
      set -e
      TEST_PID=""
      echo "test binary exited before creating ${marker} (status ${test_status})" >&2
      return 1
    fi
    [[ -f "${marker}" ]] && return 0
    sleep 0.05
  done
  echo "timed out waiting for ${marker}" >&2
  return 1
}

handle_restart_phase() {
  local phase="$1"
  wait_for_request "${CONTROL_DIR}/${phase}.stop.request"
  # Acknowledge only after exit: the failed batch cannot commit and lose its reply.
  stop_taosd
  : >"${CONTROL_DIR}/${phase}.stop.ack"

  wait_for_request "${CONTROL_DIR}/${phase}.start.request"
  start_taosd
  : >"${CONTROL_DIR}/${phase}.start.ack"
}

start_taosd
TAOS_STMT2_CONTROL_DIR="${CONTROL_DIR}" TAOS_STMT2_TEST_PORT="${PORT}" \
  "${TEST_BIN}" --gtest_filter='stmt2Case.same_stmt_recovers_after_restart' &
TEST_PID=$!

handle_restart_phase sync
handle_restart_phase async

set +e
wait "${TEST_PID}"
TEST_STATUS=$?
set -e
TEST_PID=""
if [[ ${TEST_STATUS} -ne 0 ]]; then
  exit "${TEST_STATUS}"
fi

echo "stmt2 vnode-stop recovery test passed"
