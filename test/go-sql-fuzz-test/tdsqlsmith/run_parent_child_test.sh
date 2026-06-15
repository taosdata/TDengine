#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./run_parent_child_test.sh <duration>

Examples:
  ./run_parent_child_test.sh 30s
  ./run_parent_child_test.sh 10m
  ./run_parent_child_test.sh 2h

Notes:
  - Only one positional argument is required: duration.
  - Output is always written under: ./out
  - Optional env overrides: TDSQLSMITH_BIN, DSN, STMT_TIMEOUT, MUTATION_LEVEL, EXEC_PROFILE, CHILD_CASES
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || "${1:-}" == "help" ]]; then
  usage
  exit 0
fi

if [[ $# -ne 1 ]]; then
  echo "error: exactly one argument is required: <duration>" >&2
  usage
  exit 1
fi

DURATION="$1"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${ROOT_DIR}/out"
RUN_TAG="pc_$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${OUT_DIR}/${RUN_TAG}"
LOG_FILE="${RUN_DIR}/parent_child.log"

TDSQLSMITH_BIN="${TDSQLSMITH_BIN:-${ROOT_DIR}/tdsqlsmith}"
DSN="${DSN:-root:taosdata@tcp(127.0.0.1:6030)/}"
STMT_TIMEOUT="${STMT_TIMEOUT:-2s}"
MUTATION_LEVEL="${MUTATION_LEVEL:-1}"
EXEC_PROFILE="${EXEC_PROFILE:-balanced}"
CHILD_CASES="${CHILD_CASES:-1000000000}"

if [[ ! -x "${TDSQLSMITH_BIN}" ]]; then
  if command -v "${TDSQLSMITH_BIN}" >/dev/null 2>&1; then
    TDSQLSMITH_BIN="$(command -v "${TDSQLSMITH_BIN}")"
  else
    echo "error: tdsqlsmith binary not found: ${TDSQLSMITH_BIN}" >&2
    exit 1
  fi
fi

mkdir -p "${RUN_DIR}"

LOG_FILE="${RUN_DIR}/parent_child.log"
REPORT_PATH="${RUN_DIR}/run_report.json"
: > "${LOG_FILE}"

log_line() {
  local msg="$1"
  echo "${msg}"
  echo "${msg}" >> "${LOG_FILE}"
}

log_line "[run] bin=${TDSQLSMITH_BIN}"
log_line "[run] duration=${DURATION}"
log_line "[run] out_dir=${OUT_DIR}"
log_line "[run] run_dir=${RUN_DIR}"
log_line "[run] streaming disabled; follow log with: tail -f ${LOG_FILE}"

if TDSQLSMITH_RUN_ID="${RUN_TAG}" \
  TDSQLSMITH_RUN_DIR="${RUN_DIR}" \
  "${TDSQLSMITH_BIN}" run \
    --dsn="${DSN}" \
    --duration="${DURATION}" \
    --cases="${CHILD_CASES}" \
    --stmt-timeout="${STMT_TIMEOUT}" \
    --mutation-level="${MUTATION_LEVEL}" \
    --exec-profile="${EXEC_PROFILE}" \
    --out-dir="${OUT_DIR}" \
    --cleanup-success-run-dir=true \
    --stop-when-covered=false \
    --verbose >> "${LOG_FILE}" 2>&1; then
  :
else
  run_status=$?
  log_line "[done] log=${LOG_FILE}"
  log_line "[done] report=${REPORT_PATH}"
  log_line "[done] tdsqlsmith exited with status=${run_status}"
  exit "${run_status}"
fi

log_line "[done] log=${LOG_FILE}"
log_line "[done] report=${REPORT_PATH}"
