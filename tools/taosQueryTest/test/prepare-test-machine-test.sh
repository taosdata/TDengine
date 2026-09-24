#!/usr/bin/env bash

set -euo pipefail

script_path=${1:?prepare-test-machine.sh path is required}
tdengine_home=${2:?TDengine runtime root is required}
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/taosQueryTest-preflight-test-XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

make_stub() {
  local name=$1

  printf '#!/bin/sh\nexit 0\n' >"$work_dir/$name"
  chmod 0750 "$work_dir/$name"
}

QST_TDENGINE_HOME=$tdengine_home PATH=$work_dir:/usr/bin:/bin /bin/bash "$script_path" --targets td \
  >"$work_dir/present.log" 2>&1
grep -F 'OK   taosd' "$work_dir/present.log" >/dev/null
grep -F 'OK   libtaos.so' "$work_dir/present.log" >/dev/null
grep -F 'SKIP TAOS_CONFIG_DIR' "$work_dir/present.log" >/dev/null
grep -F 'test-machine preparation check passed' "$work_dir/present.log" >/dev/null

make_stub mysql
QST_TDENGINE_HOME=$tdengine_home QST_MYSQL_CLIENT="$work_dir/mysql" PATH=$work_dir:/usr/bin:/bin \
  /bin/bash "$script_path" --targets td,mysql >"$work_dir/mysql.log" 2>&1
grep -F "OK   mysql            $work_dir/mysql" "$work_dir/mysql.log" >/dev/null

if QST_TDENGINE_HOME=$tdengine_home QST_MYSQL_CLIENT="$work_dir/missing-mysql" \
    PATH=$work_dir:/usr/bin:/bin /bin/bash "$script_path" --targets td,mysql \
    >"$work_dir/invalid-mysql.log" 2>&1; then
  printf '%s\n' 'expected invalid explicit MySQL client to fail' >&2
  exit 1
fi
grep -F 'MISS mysql' "$work_dir/invalid-mysql.log" >/dev/null
