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

if (($# != 3)); then
  printf 'Usage: %s CORE_TEST FAKE_NATIVE_DIR FAKE_BIN_DIR\n' "$0" >&2
  exit 2
fi

core_test=$1
fake_native=$2
fake_bin=$3
worker_count=12
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/taosQueryTest-port-lease-XXXXXX")
port_log="$work_dir/ports.log"
log_dir="$work_dir/logs"
mkdir "$log_dir"

cleanup() {
  status=$?
  if ((status == 0)); then
    rm -rf "$work_dir"
  else
    printf 'port lease test logs retained at %s\n' "$work_dir" >&2
  fi
}
trap cleanup EXIT

test -x "$core_test"
test -f "$fake_native/libtaos.so"
test -x "$fake_bin/taosd"

pids=()
for ((index = 0; index < worker_count; ++index)); do
  QST_CORE_PORT_LEASE_ONLY=1 \
  QST_TEST_PORT_LEASE_WORKER_INDEX="$index" \
  QST_TEST_PORT_LEASE_PORT_LOG="$port_log" \
  LD_LIBRARY_PATH="$fake_native${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
  PATH="$fake_bin:$PATH" \
    "$core_test" >"$log_dir/worker-$index.log" 2>&1 &
  pids+=("$!")
done

for pid in "${pids[@]}"; do
  wait "$pid"
done

test -f "$port_log"
mapfile -t ports < "$port_log"
if ((${#ports[@]} != worker_count)); then
  printf 'expected %d port observations, got %d\n' "$worker_count" "${#ports[@]}" >&2
  exit 1
fi

mapfile -t unique_ports < <(LC_ALL=C sort -n -u "$port_log")
if ((${#unique_ports[@]} != worker_count)); then
  printf 'port lease collision: expected %d unique ports, got %d\n' \
    "$worker_count" "${#unique_ports[@]}" >&2
  exit 1
fi
