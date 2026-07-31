#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./run_web_service.sh [start|stop|status|restart] [--daemon]

Examples:
  ./run_web_service.sh
  ./run_web_service.sh start --daemon
  LISTEN=0.0.0.0:18081 ./run_web_service.sh start --daemon
  ./run_web_service.sh status
  ./run_web_service.sh stop

Env overrides:
  TDSQLSMITH_BIN   default: tdsqlsmith (from PATH)
  LISTEN           default: 0.0.0.0:18080
  API_TOKEN        default: tdsqlsmith-dev-token
  DATA_DIR         default: $(pwd)/data
  OUT_DIR          default: $(pwd)/out
  ALLOW_ORIGIN     default: *
  LOG_FILE         default: $(pwd)/tdsqlsmith-web.log
  PID_FILE         default: $(pwd)/tdsqlsmith-web.pid
  PUBLIC_HOST      default: empty (optional, e.g. 43.130.228.76)
EOF
}

ACTION="start"
MODE="foreground"

while (($# > 0)); do
  case "$1" in
    start|stop|status|restart)
      ACTION="$1"
      shift
      ;;
    --daemon)
      MODE="daemon"
      shift
      ;;
    -h|--help|help)
      usage
      exit 0
      ;;
    *)
      echo "unknown arg: $1" >&2
      usage
      exit 1
      ;;
  esac
done

RUN_CWD="$(pwd)"
TDSQLSMITH_BIN="${TDSQLSMITH_BIN:-tdsqlsmith}"
LISTEN="${LISTEN:-0.0.0.0:18080}"
API_TOKEN="${API_TOKEN:-tdsqlsmith-dev-token}"
DATA_DIR="${DATA_DIR:-${RUN_CWD}/data}"
OUT_DIR="${OUT_DIR:-${RUN_CWD}/out}"
ALLOW_ORIGIN="${ALLOW_ORIGIN:-*}"
LOG_FILE="${LOG_FILE:-${RUN_CWD}/tdsqlsmith-web.log}"
PID_FILE="${PID_FILE:-${RUN_CWD}/tdsqlsmith-web.pid}"
PUBLIC_HOST="${PUBLIC_HOST:-}"

log() {
  printf '%s [web] %s\n' "$(date '+%F %T')" "$*"
}

listen_host() {
  printf '%s' "${LISTEN%:*}"
}

listen_port() {
  printf '%s' "${LISTEN##*:}"
}

local_health_url() {
  local host
  local port
  host="$(listen_host)"
  port="$(listen_port)"
  if [[ "${host}" == "0.0.0.0" || "${host}" == "" || "${host}" == "::" || "${host}" == "[::]" ]]; then
    host="127.0.0.1"
  fi
  if [[ "${host}" == *:* && "${host}" != \[*\] ]]; then
    host="[${host}]"
  fi
  printf 'http://%s:%s/api/v1/health' "${host}" "${port}"
}

health_ok() {
  if ! command -v curl >/dev/null 2>&1; then
    return 1
  fi
  local url
  url="$(local_health_url)"
  curl -fsS --max-time 2 "${url}" 2>/dev/null | grep -q '"status":"ok"'
}

pid_from_file() {
  if [[ ! -f "${PID_FILE}" ]]; then
    return 1
  fi
  local pid
  pid="$(tr -d '[:space:]' <"${PID_FILE}" 2>/dev/null || true)"
  if [[ -z "${pid}" ]] || ! [[ "${pid}" =~ ^[0-9]+$ ]]; then
    return 1
  fi
  printf '%s' "${pid}"
}

pid_from_port() {
  local pid
  local port
  port="$(listen_port)"
  if command -v ss >/dev/null 2>&1; then
    pid="$(ss -lntpH "( sport = :${port} )" 2>/dev/null | sed -n 's/.*pid=\([0-9]\+\).*/\1/p' | head -n 1)"
    if [[ -n "${pid}" ]]; then
      printf '%s' "${pid}"
      return 0
    fi
  fi
  if command -v lsof >/dev/null 2>&1; then
    pid="$(lsof -nP -iTCP:"${port}" -sTCP:LISTEN -t 2>/dev/null | head -n 1)"
    if [[ -n "${pid}" ]]; then
      printf '%s' "${pid}"
      return 0
    fi
  fi
  return 1
}

pid_from_cmdline() {
  local pid
  local bin_name
  bin_name="$(basename "${TDSQLSMITH_BIN}")"
  pid="$(ps -eo pid=,args= | awk -v bin="${bin_name}" -v listen="--listen=${LISTEN}" '
    index($0, " serve ") && index($0, listen) && index($0, bin) { print $1; exit }
  ')"
  if [[ -z "${pid}" ]]; then
    return 1
  fi
  printf '%s' "${pid}"
}

pid_matches_serve() {
  local pid
  local cmdline
  local bin_name
  pid="$1"
  if [[ -z "${pid}" ]] || ! [[ "${pid}" =~ ^[0-9]+$ ]]; then
    return 1
  fi
  if [[ ! -r "/proc/${pid}/cmdline" ]]; then
    return 1
  fi
  cmdline="$(tr '\0' ' ' <"/proc/${pid}/cmdline" 2>/dev/null || true)"
  if [[ -z "${cmdline}" ]]; then
    return 1
  fi
  bin_name="$(basename "${TDSQLSMITH_BIN}")"
  [[ "${cmdline}" == *"${bin_name}"* ]] || return 1
  [[ "${cmdline}" == *" serve "* ]] || return 1
  [[ "${cmdline}" == *"--listen=${LISTEN}"* ]] || return 1
}

discover_running_pid() {
  local pid
  pid="$(pid_from_cmdline || true)"
  if [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1; then
    printf '%s' "${pid}"
    return 0
  fi
  pid="$(pid_from_port || true)"
  if [[ -z "${pid}" ]] || ! kill -0 "${pid}" >/dev/null 2>&1; then
    return 1
  fi
  if ! pid_matches_serve "${pid}"; then
    return 1
  fi
  printf '%s' "${pid}"
}

is_running() {
  local pid
  pid="$(pid_from_file || true)"
  if [[ -z "${pid}" ]]; then
    return 1
  fi
  kill -0 "${pid}" >/dev/null 2>&1
}

require_bin() {
  if ! command -v "${TDSQLSMITH_BIN}" >/dev/null 2>&1; then
    echo "required command not found: ${TDSQLSMITH_BIN}" >&2
    echo "set TDSQLSMITH_BIN to installed binary path if needed" >&2
    exit 1
  fi
}

start_foreground() {
  require_bin
  mkdir -p "${DATA_DIR}" "${OUT_DIR}"
  log "starting tdsqlsmith serve in foreground"
  log "listen=${LISTEN} data_dir=${DATA_DIR} out_dir=${OUT_DIR}"
  exec "${TDSQLSMITH_BIN}" serve \
    --listen="${LISTEN}" \
    --api-token="${API_TOKEN}" \
    --data-dir="${DATA_DIR}" \
    --out-dir="${OUT_DIR}" \
    --allow-origin="${ALLOW_ORIGIN}"
}

start_daemon() {
  require_bin
  mkdir -p "${DATA_DIR}" "${OUT_DIR}"
  if is_running; then
    log "already running: pid=$(pid_from_file)"
    return 0
  fi
  if [[ -f "${PID_FILE}" ]]; then
    rm -f "${PID_FILE}"
  fi
  log "starting tdsqlsmith serve in daemon mode"
  log "listen=${LISTEN} data_dir=${DATA_DIR} out_dir=${OUT_DIR} log=${LOG_FILE}"
  nohup "${TDSQLSMITH_BIN}" serve \
    --listen="${LISTEN}" \
    --api-token="${API_TOKEN}" \
    --data-dir="${DATA_DIR}" \
    --out-dir="${OUT_DIR}" \
    --allow-origin="${ALLOW_ORIGIN}" \
    >>"${LOG_FILE}" 2>&1 &
  local pid="$!"
  local recovered_pid
  local started=0
  echo "${pid}" >"${PID_FILE}"
  for _ in $(seq 1 30); do
    if ! kill -0 "${pid}" >/dev/null 2>&1; then
      break
    fi
    if health_ok; then
      started=1
      break
    fi
    sleep 0.1
  done
  if [[ "${started}" -eq 1 ]]; then
    log "started: pid=${pid}"
    return 0
  fi
  if kill -0 "${pid}" >/dev/null 2>&1; then
    log "started: pid=${pid} (health check still warming up)"
    return 0
  fi
  recovered_pid="$(discover_running_pid || true)"
  if [[ -n "${recovered_pid}" ]]; then
    echo "${recovered_pid}" >"${PID_FILE}"
    log "started: pid=${recovered_pid} (recovered)"
    return 0
  fi
  echo "failed to start, check log: ${LOG_FILE}" >&2
  rm -f "${PID_FILE}"
  exit 1
}

stop_daemon() {
  local pid
  pid="$(pid_from_file || true)"
  if [[ -z "${pid}" ]] || ! kill -0 "${pid}" >/dev/null 2>&1; then
    pid="$(discover_running_pid || true)"
    if [[ -z "${pid}" ]]; then
      if [[ -f "${PID_FILE}" ]]; then
        rm -f "${PID_FILE}"
      fi
      log "not running"
      return 0
    fi
    echo "${pid}" >"${PID_FILE}"
    log "pid file missing/stale, recovered pid=${pid}"
  fi
  log "stopping pid=${pid}"
  kill "${pid}" >/dev/null 2>&1 || true
  for _ in $(seq 1 30); do
    if ! kill -0 "${pid}" >/dev/null 2>&1; then
      rm -f "${PID_FILE}"
      log "stopped"
      return 0
    fi
    sleep 0.1
  done
  echo "process did not stop in time: pid=${pid}" >&2
  exit 1
}

status_daemon() {
  local pid
  local host
  local port
  local external_host
  local health
  pid="$(pid_from_file || true)"
  if [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1; then
    :
  else
    pid="$(discover_running_pid || true)"
    if [[ -z "${pid}" ]]; then
      log "not running"
      return 1
    fi
    echo "${pid}" >"${PID_FILE}"
    log "running: pid=${pid} (recovered)"
  fi
  host="$(listen_host)"
  port="$(listen_port)"
  health="$(local_health_url)"
  log "running: pid=${pid}"
  log "health(local): curl -sS ${health}"
  if [[ "${host}" == "0.0.0.0" || "${host}" == "" ]]; then
    external_host="${PUBLIC_HOST:-<public-ip>}"
    log "health(external): curl -sS http://${external_host}:${port}/api/v1/health"
  fi
  return 0
}

case "${ACTION}" in
  start)
    if [[ "${MODE}" == "daemon" ]]; then
      start_daemon
    else
      start_foreground
    fi
    ;;
  stop)
    stop_daemon
    ;;
  status)
    status_daemon
    ;;
  restart)
    stop_daemon || true
    if [[ "${MODE}" == "daemon" ]]; then
      start_daemon
    else
      start_foreground
    fi
    ;;
  *)
    echo "unknown action: ${ACTION}" >&2
    usage
    exit 1
    ;;
esac
