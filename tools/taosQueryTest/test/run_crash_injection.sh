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

test -x "$core_test"
test -f "$fake_native/libtaos.so"
test -x "$fake_bin/taosd"

# The focused C regression injects one pair-supervisor crash, verifies recovery
# and the reaped pair facts, and runs the supported coverage path only.
QST_CORE_PAIR_CRASH_ONLY=1 \
LD_LIBRARY_PATH="$fake_native${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
PATH="$fake_bin:$PATH" \
  "$core_test"
