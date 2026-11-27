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

usage() {
  printf '%s\n' "Usage: $0 [--targets td,mysql,pg,influx] [--require-benchmark]"
}

targets=td
require_benchmark=0
script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
while (($# > 0)); do
  case "$1" in
    --targets)
      (($# >= 2)) || { usage >&2; exit 2; }
      targets=$2
      shift 2
      ;;
    --require-benchmark)
      require_benchmark=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
done

missing=0
check_command() {
  local name=$1
  if command -v "$name" >/dev/null 2>&1; then
    printf 'OK   %-16s %s\n' "$name" "$(command -v "$name")"
  else
    printf 'MISS %-16s command is not present in PATH\n' "$name"
    missing=1
  fi
}

if source "$script_dir/qst-runtime-env.sh" && qst_runtime_activate_tdengine; then
  printf 'OK   %-16s %s\n' taosd "$QST_RESOLVED_TAOSD"
  printf 'OK   %-16s %s\n' libtaos.so "$QST_RESOLVED_LIBTAOS"
  printf 'SKIP %-16s managed result-store is created for each invocation\n' TAOS_CONFIG_DIR
else
  printf 'MISS %-16s compatible taosd and libtaos.so were not found\n' taosd
  missing=1
fi
printf 'SKIP %-16s native client calls use libtaos.so directly\n' taos
printf 'SKIP %-16s not used by taosQueryTest query pairs\n' taosudf
if ((require_benchmark != 0)); then
  check_command taosBenchmark
else
  printf 'SKIP %-16s not required for the built-in default dataset\n' taosBenchmark
fi

. "$script_dir/dataset/find-client.sh"
check_external_client() {
  local label=$1
  local override_name=$2
  local command_name=$3
  local standard_pattern=$4
  local legacy_override=${5:-}
  local resolved=

  if resolved=$(qst_find_client "$override_name" "$command_name" \
      "$standard_pattern" "$legacy_override"); then
    printf 'OK   %-16s %s\n' "$label" "$resolved"
  else
    printf 'MISS %-16s client was not found\n' "$label"
    missing=1
  fi
}

case ",$targets," in
  *,mysql,*) check_external_client mysql QST_MYSQL_CLIENT mysql \
    '/opt/taostest/fq/mysql/*/bin/mysql' ;;
esac
case ",$targets," in
  *,pg,*) check_command psql ;;
esac
case ",$targets," in
  *,influx,*) check_command curl
    check_external_client influxdb3 QST_INFLUXDB3_CLIENT influxdb3 \
      '/opt/taostest/fq/influxdb/*/bin/influxdb3' QST_INFLUX_ADMIN_CLIENT ;;
esac

if ((missing != 0)); then
  printf 'test-machine preparation check failed; install missing components and retry\n' >&2
  exit 1
fi
printf 'test-machine preparation check passed\n'
