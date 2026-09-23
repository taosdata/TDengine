#!/bin/bash

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

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TAOSD_BIN="$(find "$SCRIPT_DIR/.." -path '*/build/bin/taosd' -not -path '*/packaging/*' 2>/dev/null | head -n1)"
BUILD_BIN="$(dirname "$TAOSD_BIN")"
BUILD_LIB="$(dirname "$BUILD_BIN")/lib"

if [[ -n "$BUILD_BIN" && -d "$BUILD_BIN" ]]; then
  export PATH="$PATH:$BUILD_BIN"
  export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:+$LD_LIBRARY_PATH:}$BUILD_LIB"
fi

python3 "$SCRIPT_DIR/auto_crash_gen_valgrind_cluster.py"
