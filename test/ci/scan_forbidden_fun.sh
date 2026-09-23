#!/bin/bash

# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

if [ "$#" -ne 1 ]; then
    echo "plz use <path>"
    exit 1
fi

SEARCH_PATH=$1

FORBIDDEN_FUNCS=(
    strcpy
    strncpy
    strcat
    sprintf
    vsprintf
    gets
    strtok
    asctime
    localtime
    atoi
    atol
    atof
)

# 黑名单：不检测的文件或目录
BLACKLIST_FILES=(
    "ignore_this.c"
    "legacy_code.c"
    "tmsg.c"
    "localtime.c" 
    "strftime.c"
    "osTimezone.c"
    "taos_map.c" 
)
BLACKLIST_DIRS=(
    "third_party"
    "vendor"
    "test"
    "contrib"
    "docs"
    "utils"
    "source/libs/*/test/*"
    "example*"
    "tools" 
    "utils/tsim/src/*"
    ".externals/"
)

# 构造 grep 的 --exclude 和 --exclude-dir 参数
EXCLUDE_ARGS=()
for f in "${BLACKLIST_FILES[@]}"; do
    EXCLUDE_ARGS+=(--exclude="$f")
done
for d in "${BLACKLIST_DIRS[@]}"; do
    EXCLUDE_ARGS+=(--exclude-dir="$d")
done

for FUNC in "${FORBIDDEN_FUNCS[@]}"; do
    grep -rnw --include="*.c" --exclude-dir={test,tests} "${EXCLUDE_ARGS[@]}" "$SEARCH_PATH" -e "$FUNC" | sed "s|$SEARCH_PATH/||g"
done | sort