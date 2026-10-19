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
