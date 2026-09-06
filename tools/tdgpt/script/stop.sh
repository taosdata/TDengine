#!/bin/bash
# Wrapper that preserves the Linux stop.sh entry point while delegating
# service lifecycle management to taosanode_service.py.

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

resolve_config() {
  if [ -n "${CONFIG_FILE}" ]; then
    printf '%s\n' "${CONFIG_FILE}"
    return 0
  fi
  for candidate in "${INSTALL_DIR}/cfg/taosanode.config.py" "/etc/taos/taosanode.config.py"; do
    if [ -f "${candidate}" ]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done
  printf '%s\n' ""
}

resolve_python() {
  local candidate
  for candidate in \
    "${INSTALL_DIR}/data/venv/bin/python3" \
    "${INSTALL_DIR}/data/venv/bin/python" \
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
  [ -f "${SERVICE_SCRIPT}" ] || {
    echo "ERROR: Service manager not found: ${SERVICE_SCRIPT}" >&2
    exit 1
  }
  local python_cmd
  python_cmd=$(resolve_python) || {
    echo "ERROR: Main Python environment not found under ${INSTALL_DIR}/data/venv or ${INSTALL_DIR}/venv." >&2
    exit 1
  }
  local config_path
  config_path=$(resolve_config)
  cmd=("${python_cmd}" "${SERVICE_SCRIPT}" "stop")
  if [ -n "${config_path}" ]; then
    cmd+=("-c" "${config_path}")
  fi
  exec "${cmd[@]}"
}

main "$@"
