#!/usr/bin/env bash

# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

set -euo pipefail

script_path=${BASH_SOURCE[0]}
while [[ -L "$script_path" ]]; do
  script_dir=$(CDPATH= cd -- "$(dirname -- "$script_path")" && pwd -P)
  link_target=$(readlink -- "$script_path")
  if [[ "$link_target" == /* ]]; then
    script_path=$link_target
  else
    script_path=$script_dir/$link_target
  fi
done
script_dir=$(CDPATH= cd -- "$(dirname -- "$script_path")" && pwd -P)

# shellcheck source=qst-runtime-env.sh
source "$script_dir/qst-runtime-env.sh"
if ! qst_runtime_activate_tdengine; then
  printf 'taosQueryTest: cannot locate a compatible TDengine runtime; set QST_TDENGINE_HOME or QST_TAOSD\n' >&2
  exit 1
fi
# shellcheck source=dataset/find-client.sh
source "$script_dir/dataset/find-client.sh"
# shellcheck source=qst-external-env.sh
source "$script_dir/qst-external-env.sh"
if ! qst_runtime_prepare_temp_root; then
  printf 'taosQueryTest: cannot prepare an isolated runtime temporary directory\n' >&2
  exit 1
fi
QST_PORT_LEASE_DIR=$(qst_port_lease_directory)
QST_PAIR_DATA_ROOT=$QST_RUNTIME_TEMP_ROOT
QST_PAIR_PORT_LEASE_DIR=$QST_PORT_LEASE_DIR
export QST_RUNTIME_TEMP_ROOT TMPDIR QST_PORT_LEASE_DIR QST_PAIR_DATA_ROOT \
  QST_PAIR_PORT_LEASE_DIR
qst_cleanup() {
  qst_external_cleanup
  qst_runtime_cleanup
  qst_runtime_result_store_cleanup
  qst_runtime_release_run_lock || true
}
qst_signal_cleanup() {
  qst_cleanup
  trap - EXIT
  exit 128
}
trap qst_cleanup EXIT
trap qst_signal_cleanup HUP INT TERM
if ! qst_runtime_acquire_run_lock "$script_dir/results"; then
  printf 'taosQueryTest: another invocation is active for package root=%s\n' "$script_dir" >&2
  exit 1
fi
if [[ -n "${QST_RESULT_STORE_CONFIG_DIR:-}" || -n "${TAOS_CONFIG_DIR:-}" ]]; then
  if ! qst_runtime_resolve_config; then
    printf 'taosQueryTest: explicit result-store configuration is invalid\n' >&2
    exit 1
  fi
  if [[ -z "${QST_RESULT_STORE_CONFIG_DIR:-}" ]]; then
    QST_RESULT_STORE_CONFIG_DIR=$TAOS_CONFIG_DIR
  fi
  if [[ -z "${QST_RESULT_STORE_ENDPOINT:-}" ]]; then
    result_store_port=$(awk '$1 == "serverPort" && $2 ~ /^[0-9]+$/ { print $2; exit }' \
      "$QST_RESULT_STORE_CONFIG_DIR/taos.cfg")
    if [[ ! "$result_store_port" =~ ^[1-9][0-9]{0,4}$ ]] ||
        ((result_store_port > 65535)); then
      printf 'taosQueryTest: invalid result-store serverPort configuration path=%s\n' \
        "$QST_RESULT_STORE_CONFIG_DIR/taos.cfg" >&2
      exit 1
    fi
    QST_RESULT_STORE_ENDPOINT="127.0.0.1:$result_store_port"
  fi
else
  if ! qst_runtime_start_result_store; then
    printf 'taosQueryTest: managed result-store preparation failed\n' >&2
    exit 1
  fi
fi
export QST_RESULT_STORE_CONFIG_DIR QST_RESULT_STORE_ENDPOINT
unset TAOS_CONFIG_DIR TAOS_HOST TAOS_PORT
if [[ "${1:-}" == "--audit-run" || "${1:-}" == "--reset-result-store" ]]; then
  "$script_dir/taosQueryTest.bin" "$@"
  exit $?
fi
if [[ "${1:-}" == "-v" || "${1:-}" == "-av" ]]; then
  "$script_dir/taosQueryTest.bin" "$@"
  exit $?
fi
generate_only=0
for argument in "$@"; do
  if [[ "$argument" == "-g" ]]; then
    generate_only=1
    break
  fi
done
if ((generate_only)); then
  # -g creates only a real fixture snapshot and JSONL evidence. It must not
  # start the private execution server or create any result-store ledger rows.
  export TAOS_CONFIG_DIR="$QST_RESULT_STORE_CONFIG_DIR"
else
  if ! qst_runtime_start_private; then
    printf 'taosQueryTest: private TDengine environment preparation failed\n' >&2
    exit 1
  fi
fi
targets=$(qst_external_targets "$@")
if [[ "$targets" == *mysql* || "$targets" == *pg* || "$targets" == *influx* ]]; then
  QST_EXTERNAL_ENV_DIR=$(mktemp -d "${TMPDIR:-/tmp}/taosQueryTest-external.XXXXXX")
  if ! qst_external_prepare "$targets"; then
    printf 'taosQueryTest: external test environment preparation failed\n' >&2
    exit 1
  fi
fi
"$script_dir/taosQueryTest.bin" "$@"
