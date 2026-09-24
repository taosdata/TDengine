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

QST_RUNTIME_ENV_DIR=
QST_RUNTIME_TAOSD_PID=
QST_RUNTIME_PORT_LEASE_FD=
QST_RUNTIME_PORT=
QST_PORT_LEASE_FD=
QST_RESULT_STORE_RUNTIME_DIR=${QST_RESULT_STORE_RUNTIME_DIR:-}
QST_RESULT_STORE_TAOSD_PID=
QST_RESULT_STORE_PORT_LEASE_FD=
QST_RESULT_STORE_PORT=
QST_RESULT_STORE_RUNTIME_OWNED=0
QST_RUN_LOCK_FD=

qst_runtime_temp_root() {
  if [[ -n "${QST_RUNTIME_TEMP_ROOT:-}" ]]; then
    printf '%s\n' "$QST_RUNTIME_TEMP_ROOT"
  elif [[ -n "${TMPDIR:-}" ]]; then
    printf '%s\n' "$TMPDIR"
  else
    printf '%s\n' /tmp
  fi
}

qst_runtime_prepare_temp_root() {
  local runtime_root=

  runtime_root=$(qst_runtime_temp_root) || return 1
  [[ "$runtime_root" == /* ]] || return 1
  mkdir -p -- "$runtime_root" || return 1
  [[ -d "$runtime_root" && ! -L "$runtime_root" && -w "$runtime_root" ]] || return 1
  QST_RUNTIME_TEMP_ROOT=$runtime_root
  TMPDIR=$runtime_root
  export QST_RUNTIME_TEMP_ROOT TMPDIR
}

qst_port_lease_directory() {
  if [[ -n "${QST_PORT_LEASE_DIR:-}" ]]; then
    printf '%s\n' "$QST_PORT_LEASE_DIR"
  else
    printf '%s/taosQueryTest-port-leases\n' "$(qst_runtime_temp_root)"
  fi
}

qst_port_lease_acquire() {
  local port=$1
  local lease_directory=
  local lease_path=
  local lease_fd=

  [[ "$port" =~ ^[0-9]+$ ]] || return 1
  ((port >= 20000 && port < 60000)) || return 1
  command -v flock >/dev/null 2>&1 || return 1
  lease_directory=$(qst_port_lease_directory) || return 1
  # Use the pair supervisor's lock naming so every self-hosted endpoint shares
  # one cross-process port reservation domain.
  mkdir -m 0700 "$lease_directory" 2>/dev/null ||
      [[ -d "$lease_directory" && ! -L "$lease_directory" ]] || return 1
  [[ -d "$lease_directory" && ! -L "$lease_directory" ]] || return 1
  lease_path="$lease_directory/port-$port.lock"
  (umask 077; : >>"$lease_path") || return 1
  [[ -f "$lease_path" && ! -L "$lease_path" ]] || return 1
  exec {lease_fd}>"$lease_path" || return 1
  if ! flock -n "$lease_fd"; then
    eval "exec ${lease_fd}>&-"
    return 1
  fi
  QST_PORT_LEASE_FD=$lease_fd
  return 0
}

qst_port_lease_release() {
  local lease_fd=$1

  [[ "$lease_fd" =~ ^[0-9]+$ ]] || return 1
  flock -u "$lease_fd" || return 1
  eval "exec ${lease_fd}>&-"
}

qst_runtime_release_port_lease() {
  if [[ -n "${QST_RUNTIME_PORT_LEASE_FD:-}" ]]; then
    qst_port_lease_release "$QST_RUNTIME_PORT_LEASE_FD" || return 1
  fi
  QST_RUNTIME_PORT_LEASE_FD=
  QST_RUNTIME_PORT=
  return 0
}

qst_runtime_port() {
  local port=

  for ((port = 20000 + RANDOM % 20000; port < 60000; ++port)); do
    if ! ss -ltn "sport = :$port" 2>/dev/null | grep -q LISTEN &&
        qst_port_lease_acquire "$port"; then
      QST_RUNTIME_PORT_LEASE_FD=$QST_PORT_LEASE_FD
      QST_RUNTIME_PORT=$port
      return 0
    fi
  done
  return 1
}

qst_result_store_port() {
  local port=

  for ((port = 20000 + RANDOM % 20000; port < 60000; ++port)); do
    if ! ss -ltn "sport = :$port" 2>/dev/null | grep -q LISTEN &&
        qst_port_lease_acquire "$port"; then
      QST_RESULT_STORE_PORT_LEASE_FD=$QST_PORT_LEASE_FD
      QST_RESULT_STORE_PORT=$port
      return 0
    fi
  done
  return 1
}

qst_result_store_release_port_lease() {
  if [[ -n "${QST_RESULT_STORE_PORT_LEASE_FD:-}" ]]; then
    qst_port_lease_release "$QST_RESULT_STORE_PORT_LEASE_FD" || return 1
  fi
  QST_RESULT_STORE_PORT_LEASE_FD=
  QST_RESULT_STORE_PORT=
  return 0
}

qst_runtime_native_ready() {
  local config_dir=$1
  local port=$2
  local readiness_attempt=

  command -v taos >/dev/null 2>&1 || return 0
  taos -c "$config_dir" -h 127.0.0.1 -P "$port" \
    -s 'CREATE DATABASE IF NOT EXISTS qst_ready PRECISION "ms"' \
    -o /dev/null >/dev/null 2>&1 || return 1
  for ((readiness_attempt = 0; readiness_attempt < 30;
        ++readiness_attempt)); do
    if taos -c "$config_dir" -h 127.0.0.1 -P "$port" \
        -s 'USE qst_ready' -o /dev/null >/dev/null 2>&1; then
      taos -c "$config_dir" -h 127.0.0.1 -P "$port" \
        -s 'DROP DATABASE qst_ready' -o /dev/null >/dev/null 2>&1 || return 1
      return 0
    fi
    sleep 1
  done
  return 1
}

qst_runtime_set_candidate() {
  local taosd_path=$1
  local bin_dir=
  local lib_path=

  [[ -x "$taosd_path" ]] || return 1
  taosd_path=$(readlink -f "$taosd_path") || return 1
  bin_dir=$(CDPATH= cd -- "$(dirname -- "$taosd_path")" && pwd -P)
  for lib_path in "$bin_dir/../lib/libtaos.so" "$bin_dir/../lib64/libtaos.so"; do
    if [[ -r "$lib_path" ]]; then
      QST_RESOLVED_TAOSD="$taosd_path"
      QST_RESOLVED_TDENGINE_BIN="$bin_dir"
      QST_RESOLVED_LIBTAOS="$lib_path"
      export QST_RESOLVED_TAOSD QST_RESOLVED_TDENGINE_BIN QST_RESOLVED_LIBTAOS
      return 0
    fi
  done
  return 1
}

qst_runtime_resolve_tdengine() {
  local candidate=
  local path_taosd=

  if [[ -n "${QST_TDENGINE_HOME:-}" ]]; then
    qst_runtime_set_candidate "$QST_TDENGINE_HOME/bin/taosd" && return 0
  fi
  if [[ -n "${QST_TAOSD:-}" ]]; then
    qst_runtime_set_candidate "$QST_TAOSD" && return 0
  fi
  path_taosd=$(command -v taosd 2>/dev/null || true)
  if [[ -n "$path_taosd" ]]; then
    qst_runtime_set_candidate "$path_taosd" && return 0
  fi
  for candidate in /usr/local/taos/bin/taosd /usr/local/taos/taosd \
      /opt/taos/bin/taosd /local/tsdb/debug*/build/bin/taosd; do
    qst_runtime_set_candidate "$candidate" && return 0
  done
  return 1
}

qst_runtime_activate_tdengine() {
  qst_runtime_resolve_tdengine || return 1
  PATH="$QST_RESOLVED_TDENGINE_BIN:$PATH"
  LD_LIBRARY_PATH="$(dirname -- "$QST_RESOLVED_LIBTAOS")${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
  export PATH LD_LIBRARY_PATH
}

qst_runtime_start_result_store() {
  local startup_attempt=
  local readiness_attempt=
  local runtime_root=
  local config_dir=
  local log_dir=
  local port=

  [[ -n "${QST_RESOLVED_TAOSD:-}" ]] || return 1
  [[ -z "${QST_RESULT_STORE_TAOSD_PID:-}" ]] || return 1
  qst_runtime_prepare_temp_root || return 1
  if [[ -z "${QST_RESULT_STORE_RUNTIME_DIR:-}" ]]; then
    runtime_root=$(qst_runtime_temp_root) || return 1
    QST_RESULT_STORE_RUNTIME_DIR=$(mktemp -d "$runtime_root/taosQueryTest-result-store.XXXXXX") ||
        return 1
    QST_RESULT_STORE_RUNTIME_OWNED=1
  else
    [[ ! -L "$QST_RESULT_STORE_RUNTIME_DIR" ]] || return 1
    mkdir -p "$QST_RESULT_STORE_RUNTIME_DIR" || return 1
    QST_RESULT_STORE_RUNTIME_OWNED=0
  fi
  config_dir="$QST_RESULT_STORE_RUNTIME_DIR/cfg"
  log_dir="$QST_RESULT_STORE_RUNTIME_DIR/log"
  if [[ -r "$config_dir/taos.cfg" ]]; then
    port=$(awk '$1 == "serverPort" && $2 ~ /^[0-9]+$/ { print $2; exit }' \
      "$config_dir/taos.cfg")
    [[ "$port" =~ ^[1-9][0-9]{0,4}$ ]] && ((port <= 65535)) || return 1
    qst_port_lease_acquire "$port" || return 1
    QST_RESULT_STORE_PORT_LEASE_FD=$QST_PORT_LEASE_FD
    QST_RESULT_STORE_PORT=$port
  else
    qst_result_store_port || return 1
    port=$QST_RESULT_STORE_PORT
    mkdir -p "$config_dir" "$log_dir" "$QST_RESULT_STORE_RUNTIME_DIR/data" \
      "$QST_RESULT_STORE_RUNTIME_DIR/tmp" || {
      qst_result_store_release_port_lease
      return 1
    }
    cat >"$config_dir/taos.cfg" <<EOF
fqdn localhost
serverPort $port
firstEp localhost:$port
secondEp localhost:$port
dataDir $QST_RESULT_STORE_RUNTIME_DIR/data
logDir $log_dir
tempDir $QST_RESULT_STORE_RUNTIME_DIR/tmp
federatedQueryEnable 1
EOF
  fi
  for ((startup_attempt = 0; startup_attempt < 2; ++startup_attempt)); do
    "$QST_RESOLVED_TAOSD" -c "$config_dir" -o "$log_dir/taosd.log" \
      >"$log_dir/taosd.stdout.log" 2>&1 &
    QST_RESULT_STORE_TAOSD_PID=$!
    for ((readiness_attempt = 0; readiness_attempt < 30;
          ++readiness_attempt)); do
      if ! kill -0 "$QST_RESULT_STORE_TAOSD_PID" 2>/dev/null; then
        break
      fi
      if ss -ltn "sport = :$port" 2>/dev/null | grep -q LISTEN &&
          qst_runtime_native_ready "$config_dir" "$port"; then
        QST_RESULT_STORE_CONFIG_DIR=$config_dir
        QST_RESULT_STORE_ENDPOINT="127.0.0.1:$port"
        export QST_RESULT_STORE_CONFIG_DIR QST_RESULT_STORE_ENDPOINT \
          QST_RESULT_STORE_TAOSD_PID
        return 0
      fi
      sleep 1
    done
    wait "$QST_RESULT_STORE_TAOSD_PID" 2>/dev/null || true
    QST_RESULT_STORE_TAOSD_PID=
  done
  qst_result_store_release_port_lease || true
  if ((QST_RESULT_STORE_RUNTIME_OWNED != 0)); then
    rm -rf -- "$QST_RESULT_STORE_RUNTIME_DIR"
    QST_RESULT_STORE_RUNTIME_DIR=
  fi
  return 1
}

qst_runtime_result_store_cleanup() {
  if [[ -n "${QST_RESULT_STORE_TAOSD_PID:-}" ]]; then
    kill "$QST_RESULT_STORE_TAOSD_PID" 2>/dev/null || true
    wait "$QST_RESULT_STORE_TAOSD_PID" 2>/dev/null || true
  fi
  QST_RESULT_STORE_TAOSD_PID=
  qst_result_store_release_port_lease || true
  if ((QST_RESULT_STORE_RUNTIME_OWNED != 0)) &&
      [[ -n "${QST_RESULT_STORE_RUNTIME_DIR:-}" ]]; then
    rm -rf -- "$QST_RESULT_STORE_RUNTIME_DIR"
    QST_RESULT_STORE_RUNTIME_DIR=
  fi
  QST_RESULT_STORE_RUNTIME_OWNED=0
}

qst_runtime_acquire_run_lock() {
  local results_root=$1
  local lock_path=
  local lock_fd=

  [[ -d "$results_root" && ! -L "$results_root" ]] || return 1
  command -v flock >/dev/null 2>&1 || return 1
  lock_path="$results_root/.taosQueryTest-run.lock"
  (umask 077; : >>"$lock_path") || return 1
  [[ -f "$lock_path" && ! -L "$lock_path" ]] || return 1
  exec {lock_fd}>"$lock_path" || return 1
  if ! flock -n "$lock_fd"; then
    eval "exec ${lock_fd}>&-"
    return 1
  fi
  QST_RUN_LOCK_FD=$lock_fd
  return 0
}

qst_runtime_release_run_lock() {
  if [[ -n "${QST_RUN_LOCK_FD:-}" ]]; then
    qst_port_lease_release "$QST_RUN_LOCK_FD" || return 1
  fi
  QST_RUN_LOCK_FD=
  return 0
}

qst_runtime_start_private() {
  local startup_attempt=
  local readiness_attempt=
  local port=
  local config_dir=
  local log_dir=
  local runtime_root=

  [[ -n "${QST_RESOLVED_TAOSD:-}" ]] || return 1
  [[ -z "${QST_RUNTIME_TAOSD_PID:-}" ]] || return 1
  qst_runtime_prepare_temp_root || return 1
  runtime_root=$(qst_runtime_temp_root) || return 1
  for ((startup_attempt = 0; startup_attempt < 32; ++startup_attempt)); do
    qst_runtime_port || return 1
    port=$QST_RUNTIME_PORT
    QST_RUNTIME_ENV_DIR=$(mktemp -d "$runtime_root/taosQueryTest-tdengine.XXXXXX") || {
      qst_runtime_release_port_lease
      return 1
    }
    config_dir="$QST_RUNTIME_ENV_DIR/cfg"
    log_dir="$QST_RUNTIME_ENV_DIR/log"
    mkdir -p "$config_dir" "$log_dir" "$QST_RUNTIME_ENV_DIR/data" "$QST_RUNTIME_ENV_DIR/tmp" || {
      qst_runtime_cleanup
      return 1
    }
    cat >"$config_dir/taos.cfg" <<EOF
fqdn localhost
serverPort $port
firstEp localhost:$port
secondEp localhost:$port
dataDir $QST_RUNTIME_ENV_DIR/data
logDir $log_dir
tempDir $QST_RUNTIME_ENV_DIR/tmp
federatedQueryEnable 1
EOF
    "$QST_RESOLVED_TAOSD" -c "$config_dir" -o "$log_dir/taosd.log" \
      >"$log_dir/taosd.stdout.log" 2>&1 &
    QST_RUNTIME_TAOSD_PID=$!
    for ((readiness_attempt = 0; readiness_attempt < 30;
          ++readiness_attempt)); do
      if ! kill -0 "$QST_RUNTIME_TAOSD_PID" 2>/dev/null; then
        break
      fi
      if ss -ltn "sport = :$port" 2>/dev/null | grep -q LISTEN &&
          qst_runtime_native_ready "$config_dir" "$port"; then
        TAOS_CONFIG_DIR=$config_dir
        TAOS_HOST=127.0.0.1
        TAOS_PORT=$port
        export TAOS_CONFIG_DIR TAOS_HOST TAOS_PORT
        return 0
      fi
      sleep 1
    done
    qst_runtime_cleanup || return 1
  done
  return 1
}

qst_runtime_cleanup() {
  if [[ -n "${QST_RUNTIME_TAOSD_PID:-}" ]]; then
    kill "$QST_RUNTIME_TAOSD_PID" 2>/dev/null || true
    wait "$QST_RUNTIME_TAOSD_PID" 2>/dev/null || true
  fi
  QST_RUNTIME_TAOSD_PID=
  if [[ -n "${QST_RUNTIME_ENV_DIR:-}" ]]; then
    rm -rf -- "$QST_RUNTIME_ENV_DIR"
  fi
  QST_RUNTIME_ENV_DIR=
  qst_runtime_release_port_lease
}

qst_runtime_resolve_config() {
  local process_dir=
  local process_executable=
  local process_args=
  local argument=
  local next_is_config=0

  if [[ -n "${QST_RESULT_STORE_CONFIG_DIR:-}" ]]; then
    [[ -r "$QST_RESULT_STORE_CONFIG_DIR/taos.cfg" ]] || return 1
    TAOS_CONFIG_DIR=$QST_RESULT_STORE_CONFIG_DIR
    export TAOS_CONFIG_DIR
    return 0
  fi
  if [[ -n "${TAOS_CONFIG_DIR:-}" ]]; then
    [[ -r "$TAOS_CONFIG_DIR/taos.cfg" ]] && return 0
    return 1
  fi
  [[ -n "${QST_RESOLVED_TAOSD:-}" ]] || return 1
  for process_dir in /proc/[0-9]*; do
    process_executable=$(readlink -f "$process_dir/exe" 2>/dev/null || true)
    [[ "$process_executable" == "$QST_RESOLVED_TAOSD" ]] || continue
    process_args=$(tr '\0' '\n' < "$process_dir/cmdline" 2>/dev/null || true)
    next_is_config=0
    while IFS= read -r argument; do
      if [[ "$next_is_config" == 1 ]]; then
        if [[ -r "$argument/taos.cfg" ]]; then
          TAOS_CONFIG_DIR="$argument"
          export TAOS_CONFIG_DIR
          return 0
        fi
        break
      fi
      [[ "$argument" == -c ]] && next_is_config=1
    done <<< "$process_args"
  done
  for argument in /etc/taos /usr/local/taos/cfg; do
    if [[ -r "$argument/taos.cfg" ]]; then
      TAOS_CONFIG_DIR="$argument"
      export TAOS_CONFIG_DIR
      return 0
    fi
  done
  return 1
}
