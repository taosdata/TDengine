#!/bin/bash

# Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
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

PRJ_ROOT_DIR=$(readlink -f -- "$(dirname $0)/../..")
FORMAT_BIN=clang-format-14

ORIGIN_DIR=$(pwd)

cd ${PRJ_ROOT_DIR}

FORMAT_DIR_LIST=(
    "${PRJ_ROOT_DIR}/include"
    "${PRJ_ROOT_DIR}/source/os"
    "${PRJ_ROOT_DIR}/source/util"
    "${PRJ_ROOT_DIR}/source/common"
    "${PRJ_ROOT_DIR}/source/libs"
    "${PRJ_ROOT_DIR}/source/client/inc"
    "${PRJ_ROOT_DIR}/source/client/src"
    "${PRJ_ROOT_DIR}/source/client/test"
    "${PRJ_ROOT_DIR}/source/dnode"
)

EXCLUDE_DIR_LIST=(
)

EXCLUDE_FILE_LIST=(
    "source/libs/parser/sql.c"
)

for d in ${FORMAT_DIR_LIST[@]}; do
    for f in $(find $d -type f -not -name '*sql.c' -regex '.*\.\(cpp\|hpp\|c\|h\)'); do
        ${FORMAT_BIN} -i $f
    done
done

cd ${ORIGIN_DIR}

# find source -type f -regex '.*\.\(cpp\|hpp\|c\|h\)' ! -name
