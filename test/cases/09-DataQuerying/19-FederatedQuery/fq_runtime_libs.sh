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

_fq_runtime_prepend_path() {
  local name="$1"
  local dir="$2"
  local cur="${!name:-}"

  case ":${cur}:" in
    *":${dir}:"*) ;;
    *) export "${name}=${dir}${cur:+:${cur}}" ;;
  esac
}

fq_ci_needs_databases() {
  local arg
  # Explicit audited tasks still stage provider libraries, but need no servers.
  [[ "$*" == *FQ_REQUIRED_SERVICES=none* ]] && return 1
  for arg in "$@"; do
    case "${arg}" in
      *19-FederatedQuery*|*05-VirtualTables/*03-ExtSource*|\
      *test_vtable_auth_ext_source.py*|\
      *test_vtable_plan_agg_federated.py*|\
      *test_vtable_plan_pushdown_agg_ext_source.py*|\
      *test_vtable_plan_pushdown_ext_source_ts_precision.py*|\
      *test_vtable_plan_window_ext_source.py*|\
      *test_vtable_plan_window_federated_batch.py*|\
      *test_vtable_series_meta.py*|*test_vtable_series_smoke.py*|\
      *test_virtual_stream_mixed_external.py*|*test_virtual_stream_mixed_external_perf.py*|\
      *test_fs_0[1-7]_*.py*|*test_meta_batch_txn_series.py*)
        return 0
        ;;
    esac
  done
  return 1
}

_fq_runtime_should_enable() {
  local arg

  case "${FQ_RUNTIME_LIBS:-auto}" in
    1|on|ON|true|TRUE) return 0 ;;
    0|off|OFF|false|FALSE) return 1 ;;
  esac

  fq_ci_needs_databases "$@" && return 0

  for arg in "$@"; do
    case "${arg}" in
      *19-FederatedQuery*|*05-VirtualTables/*ExtSource*|*05-VirtualTables/*ext_source*|*05-VirtualTables/*federated*|*05-VirtualTables/06-Meta/test_vtable_series*)
        return 0
        ;;
      # Federated-stream suites drive the same external sources (SQL Server
      # included) and therefore need the FQ runtime staging exactly like the
      # federated-query cases.  Without this an fs-only CI job on a fresh
      # worker never staged the msodbcsql resources and every case died in
      # the first sqlcmd with SQLAllocHandle(HENV).
      *18-StreamProcessing/99-Others/test_fs_*|*99-Others/test_fs_*)
        return 0
        ;;
    esac
  done

  return 1
}

fq_runtime_libs_setup() {
  local build_dir="${1:-${BUILD_DIR:-}}"
  local runtime_dir
  shift || true

  _fq_runtime_should_enable "$@" || return 0

  if [ -n "${build_dir}" ]; then
    runtime_dir="${build_dir%/}/build/lib"
  else
    runtime_dir=""
  fi
  if [ -z "${runtime_dir}" ] || [ ! -d "${runtime_dir}" ]; then
    echo "[fq-runtime] WARN: normal runtime library dir not found: ${runtime_dir:-<unset>}" >&2
    return 0
  fi
  _fq_runtime_prepend_path LD_LIBRARY_PATH "${runtime_dir}"
  if [ "$(uname -s 2>/dev/null)" = "Darwin" ]; then
    _fq_runtime_prepend_path DYLD_LIBRARY_PATH "${runtime_dir}"
  fi
  echo "[fq-runtime] using provider runtime libraries: ${runtime_dir}"
}
