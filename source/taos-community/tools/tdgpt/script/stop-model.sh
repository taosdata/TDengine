#!/bin/bash
# Wrapper that preserves the Linux stop-model.sh entry point while delegating
# model lifecycle management to taosanode_service.py.

set -e

resolve_script_path() {
  local source_path="$1"
  if command -v readlink >/dev/null 2>&1; then
    local resolved
    resolved=$(readlink -f "${source_path}" 2>/dev/null || true)
    if [ -n "${resolved}" ]; then
      printf '%s\n' "${resolved}"
      return 0
    fi
  fi
  printf '%s\n' "${source_path}"
}

SCRIPT_PATH=$(resolve_script_path "$0")
SCRIPT_DIR=$(cd "$(dirname "${SCRIPT_PATH}")" && pwd)
INSTALL_DIR=$(cd "${SCRIPT_DIR}/.." && pwd)
SERVICE_SCRIPT="${SCRIPT_DIR}/taosanode_service.py"
CONFIG_FILE="${TAOSANODE_CONFIG:-}"
MODEL_TARGET=""

show_help() {
  echo "Usage: $0 [-c config_file] [model_name|all]"
  echo "Supported model_name: tdtsfm, timesfm, timemoe, moirai, chronos, moment"
  echo "Use '$0 all' to stop all models."
  echo "Options:"
  echo "  -c config_file   Specify config file (default: <install_dir>/cfg/taosanode.config.py or /etc/taos/taosanode.config.py)"
  echo "  -h, --help       Show this help message"
}

parse_options() {
  while getopts ":c:h" opt; do
    case "$opt" in
      c) CONFIG_FILE="$OPTARG" ;;
      h) show_help; exit 0 ;;
      :) echo "Option -$OPTARG requires an argument." >&2; exit 2 ;;
      \?) echo "Invalid option: -$OPTARG" >&2; exit 2 ;;
    esac
  done
  shift $((OPTIND-1))

  if [ $# -ne 1 ]; then
    show_help
    exit 1
  fi

  MODEL_TARGET="$1"
}

resolve_config() {
  if [ -n "${CONFIG_FILE}" ]; then
    printf '%s\n' "${CONFIG_FILE}"
    return 0
  fi

  for candidate in \
    "${INSTALL_DIR}/cfg/taosanode.config.py" \
    "/etc/taos/taosanode.config.py"
  do
    if [ -f "${candidate}" ]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done

  printf '%s\n' ""
  return 0
}

resolve_python() {
  local candidate
  for candidate in \
    "${INSTALL_DIR}/venv/bin/python3" \
    "${INSTALL_DIR}/venv/bin/python"
  do
    if [ -x "${candidate}" ]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done

  return 1
}

main() {
  parse_options "$@"

  if [ ! -f "${SERVICE_SCRIPT}" ]; then
    echo "ERROR: Service manager not found: ${SERVICE_SCRIPT}" >&2
    exit 1
  fi

  local python_cmd
  python_cmd=$(resolve_python) || {
    echo "ERROR: Main Python environment not found under ${INSTALL_DIR}/venv/bin/." >&2
    echo "Run install.sh again to recreate the TDgpt environment." >&2
    exit 1
  }

  local config_path
  config_path=$(resolve_config)

  cmd=("${python_cmd}" "${SERVICE_SCRIPT}" "model-stop")
  if [ -n "${config_path}" ]; then
    cmd+=("-c" "${config_path}")
  fi
  cmd+=("${MODEL_TARGET}")

  exec "${cmd[@]}"
}

main "$@"
