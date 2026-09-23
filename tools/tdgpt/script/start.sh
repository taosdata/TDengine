#!/bin/bash

# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

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
  cmd=("${python_cmd}" "${SERVICE_SCRIPT}" "start")
  if [ -n "${config_path}" ]; then
    cmd+=("-c" "${config_path}")
  fi
  exec "${cmd[@]}"
}

main "$@"
