#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

MODE="${1:-all}"
if [[ "${MODE}" == "-h" || "${MODE}" == "--help" ]]; then
  cat <<'USAGE'
Usage:
  scripts/verify_taosd_core.sh [all|probe|repro]

Modes:
  all   : probe core-producing signal, then run fixed repro (default)
  probe : only probe the signal
  repro : run fixed repro using CORE_SIGNAL or saved probe result

Environment variables:
  DSN                 TDengine DSN (default: root:taosdata@tcp(127.0.0.1:6030)/)
  OUT_DIR             output dir for repro artifacts (default: /home/ubuntu/tdsqlsmith/out)
  DURATION            tdsqlsmith run duration (default: 120s)
  SEED                run seed (default: current epoch seconds)
  INJECT_INTERVAL     seconds between signal injections (default: 2)
  REPRO_GRACE         seconds to wait after run starts before first injection (default: 25)
  PROBE_TIMEOUT       seconds waiting for new core after signal (default: 60)
  STARTUP_GRACE       seconds to wait after taosd starts before sending signal (default: 3)
  SIGNALS             comma-separated probe candidates (default: SEGV,ABRT,BUS,FPE,ILL,QUIT)
  TAOSD_CMD           taosd launch command (default: taosd)
  CORE_SIGNAL         fixed signal for repro mode (overrides probe result)
  SIGNAL_FILE         path to save/load probed signal (default: /home/ubuntu/tdsqlsmith/out/probed_core_signal.txt)
USAGE
  exit 0
fi

DSN="${DSN:-root:taosdata@tcp(127.0.0.1:6030)/}"
OUT_DIR="${OUT_DIR:-${ROOT_DIR}/out}"
DURATION="${DURATION:-120s}"
SEED="${SEED:-$(date +%s)}"
INJECT_INTERVAL="${INJECT_INTERVAL:-2}"
REPRO_GRACE="${REPRO_GRACE:-25}"
PROBE_TIMEOUT="${PROBE_TIMEOUT:-60}"
STARTUP_GRACE="${STARTUP_GRACE:-3}"
SIGNALS="${SIGNALS:-SEGV,ABRT,BUS,FPE,ILL,QUIT}"
TAOSD_CMD="${TAOSD_CMD:-taosd}"
SIGNAL_FILE="${SIGNAL_FILE:-${OUT_DIR}/probed_core_signal.txt}"
CORE_SIGNAL="${CORE_SIGNAL:-}"

RUN_LOG="${OUT_DIR}/repro_run.log"
INJECT_LOG="${OUT_DIR}/inject.log"

CORE_DIRS=(
  /var/lib/apport/coredump
  /var/lib/systemd/coredump
  /var/crash
)

log() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*" >&2
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

core_count() {
  sudo -n find "${CORE_DIRS[@]}" -maxdepth 1 -type f -name 'core*' 2>/dev/null | wc -l
}

latest_core() {
  sudo -n find "${CORE_DIRS[@]}" -maxdepth 1 -type f -name 'core*' -printf '%T@ %p\n' 2>/dev/null \
    | sort -nr \
    | head -n 1 \
    | awk '{ $1=""; sub(/^ /, ""); print }'
}

wait_for_new_core() {
  local before_count="$1"
  local before_latest="$2"
  local timeout_sec="$3"
  local deadline=$((SECONDS + timeout_sec))
  while (( SECONDS < deadline )); do
    local now_count
    local now_latest
    now_count=$(core_count)
    now_latest=$(latest_core)
    if (( now_count > before_count )); then
      printf '%s\n' "$now_latest"
      return 0
    fi
    if [[ -n "$now_latest" && "$now_latest" != "$before_latest" ]]; then
      printf '%s\n' "$now_latest"
      return 0
    fi
    sleep 1
  done
  return 1
}

stop_all_taosd() {
  sudo -n systemctl stop taosd >/dev/null 2>&1 || true
  sudo -n pkill -9 -x taosd >/dev/null 2>&1 || true
  for _ in $(seq 1 30); do
    if ! pgrep -x taosd >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  pgrep -x taosd >/dev/null 2>&1 && {
    echo "failed to stop taosd" >&2
    exit 1
  }
}

wait_taosd_pid() {
  local timeout_sec="$1"
  local deadline=$((SECONDS + timeout_sec))
  while (( SECONDS < deadline )); do
    local pid
    pid=$(pgrep -x taosd | head -n 1 || true)
    if [[ -n "$pid" ]]; then
      printf '%s\n' "$pid"
      return 0
    fi
    sleep 0.2
  done
  return 1
}

launch_probe_taosd() {
  # Run taosd directly, avoid systemctl start to align with repro path.
  sudo -n bash -lc "nohup ${TAOSD_CMD} >/tmp/tdsqlsmith-taosd-probe.log 2>&1 &"
}

probe_core_signal() {
  local probe_csv="$1"
  local selected=""
  local before_count
  local before_latest
  local pid
  local sig
  local core_path

  IFS=',' read -r -a signal_arr <<<"$probe_csv"

  for raw in "${signal_arr[@]}"; do
    sig=$(echo "$raw" | xargs)
    [[ -z "$sig" ]] && continue

    stop_all_taosd
    before_count=$(core_count)
    before_latest=$(latest_core)
    log "probe signal=${sig} before_count=${before_count} before_latest=${before_latest:-<none>}"

    launch_probe_taosd
    if ! pid=$(wait_taosd_pid 15); then
      log "probe signal=${sig} taosd did not start"
      continue
    fi

    # Give taosd a short warm-up window; sending too early is flaky.
    sleep "$STARTUP_GRACE"

    sudo -n kill -s "$sig" "$pid" >/dev/null 2>&1 || true
    if core_path=$(wait_for_new_core "$before_count" "$before_latest" "$PROBE_TIMEOUT"); then
      selected="$sig"
      log "probe success signal=${sig} new_core=${core_path}"
      break
    fi
    log "probe miss signal=${sig} no new core"
  done

  stop_all_taosd

  if [[ -z "$selected" ]]; then
    return 1
  fi

  mkdir -p "$(dirname "$SIGNAL_FILE")"
  printf '%s\n' "$selected" >"$SIGNAL_FILE"
  printf '%s\n' "$selected"
}

clear_out_dir() {
  sudo -n rm -rf "$OUT_DIR"
  sudo -n mkdir -p "$OUT_DIR"
  sudo -n chown -R "$(id -un):$(id -gn)" "$OUT_DIR"
}

latest_report_path() {
  find "$OUT_DIR" -maxdepth 2 -type f -name run_report.json -printf '%T@ %p\n' \
    | sort -nr \
    | head -n 1 \
    | awk '{ $1=""; sub(/^ /, ""); print }'
}

run_fixed_repro() {
  local sig="$1"
  local before_count
  local before_latest
  local after_count
  local after_latest
  local report_path
  local run_pid
  local inj_pid
  local run_rc
  local incident_len
  local total_executed
  local new_core="no"

  stop_all_taosd
  clear_out_dir

  before_count=$(core_count)
  before_latest=$(latest_core)

  log "repro start signal=${sig} duration=${DURATION} seed=${SEED} out=${OUT_DIR}"

  sudo -n env TDSQLSMITH_TAOSD_COMMAND="${TAOSD_CMD}" \
    "${ROOT_DIR}/bin/tdsqlsmith" run \
    --dsn="${DSN}" \
    --duration="${DURATION}" \
    --seed="${SEED}" \
    --out-dir="${OUT_DIR}" \
    --stop-when-covered=false \
    --verbose >"${RUN_LOG}" 2>&1 &
  run_pid=$!

  (
    sleep "$REPRO_GRACE"
    while kill -0 "$run_pid" >/dev/null 2>&1; do
      pid=$(pgrep -x taosd | head -n 1 || true)
      if [[ -n "$pid" ]]; then
        sudo -n kill -s "$sig" "$pid" >/dev/null 2>&1 || true
        printf '[%s] signal=%s pid=%s\n' "$(date '+%F %T')" "$sig" "$pid" >>"${INJECT_LOG}"
      else
        printf '[%s] pid_missing\n' "$(date '+%F %T')" >>"${INJECT_LOG}"
      fi
      sleep "$INJECT_INTERVAL"
    done
  ) &
  inj_pid=$!

  set +e
  wait "$run_pid"
  run_rc=$?
  set -e
  wait "$inj_pid" || true

  after_count=$(core_count)
  after_latest=$(latest_core)
  if (( after_count > before_count )); then
    new_core="yes"
  elif [[ -n "$after_latest" && "$after_latest" != "$before_latest" ]]; then
    new_core="yes"
  fi

  report_path=$(latest_report_path)
  if [[ -z "$report_path" || ! -f "$report_path" ]]; then
    echo "run report not found under ${OUT_DIR}" >&2
    return 1
  fi

  incident_len=$(jq '.taosd_incidents|length' "$report_path")
  total_executed=$(jq '.total_executed' "$report_path")

  log "repro summary run_rc=${run_rc} total_executed=${total_executed} taosd_incidents=${incident_len} new_core=${new_core} report=${report_path}"
  log "core before_count=${before_count} after_count=${after_count} before_latest=${before_latest:-<none>} after_latest=${after_latest:-<none>}"

  if [[ "$run_rc" -ne 0 ]]; then
    echo "tdsqlsmith run failed, see ${RUN_LOG}" >&2
    return 2
  fi
  if [[ "$incident_len" -le 0 ]]; then
    echo "taosd incident not recorded, see ${report_path}" >&2
    return 3
  fi
  if [[ "$new_core" != "yes" ]]; then
    echo "no new core observed during repro, probe succeeded but repro did not produce new core" >&2
    return 4
  fi

  return 0
}

main() {
  require_cmd sudo
  require_cmd pgrep
  require_cmd jq
  require_cmd sort
  require_cmd awk
  require_cmd find

  if ! sudo -n true >/dev/null 2>&1; then
    echo "sudo -n unavailable" >&2
    exit 1
  fi

  local signal="${CORE_SIGNAL}"
  case "$MODE" in
    all)
      if [[ -z "$signal" ]]; then
        signal=$(probe_core_signal "$SIGNALS") || {
          echo "failed to probe core-producing signal" >&2
          exit 2
        }
      fi
      log "selected core signal=${signal}"
      run_fixed_repro "$signal"
      ;;
    probe)
      if [[ -z "$signal" ]]; then
        signal=$(probe_core_signal "$SIGNALS") || {
          echo "failed to probe core-producing signal" >&2
          exit 2
        }
      fi
      log "selected core signal=${signal}"
      printf '%s\n' "$signal"
      ;;
    repro)
      if [[ -z "$signal" && -f "$SIGNAL_FILE" ]]; then
        signal=$(tr -d '[:space:]' <"$SIGNAL_FILE")
      fi
      if [[ -z "$signal" ]]; then
        echo "CORE_SIGNAL is empty and no saved signal at ${SIGNAL_FILE}" >&2
        exit 1
      fi
      run_fixed_repro "$signal"
      ;;
    *)
      echo "unsupported mode: ${MODE}" >&2
      exit 1
      ;;
  esac
}

main "$@"
