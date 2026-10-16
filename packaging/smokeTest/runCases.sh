#!/bin/bash

# Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
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

ulimit -c unlimited

rm -rf ${TDENGINE_ROOT_DIR}/debug
mkdir ${TDENGINE_ROOT_DIR}/debug
mkdir ${TDENGINE_ROOT_DIR}/debug/build
mkdir ${TDENGINE_ROOT_DIR}/debug/build/bin

systemType=`uname`
if [ ${systemType} == "Darwin" ]; then
    cp /usr/local/bin/taos*  ${TDENGINE_ROOT_DIR}/debug/build/bin/
else
    cp /usr/bin/taos*  ${TDENGINE_ROOT_DIR}/debug/build/bin/
fi

case_out_file=`pwd`/case.out
python3 -m pip install -r ${TDENGINE_ROOT_DIR}/tests/requirements.txt >> $case_out_file
python3 -m pip install taos-ws-py taospy >> $case_out_file

cd ${TDENGINE_ROOT_DIR}/tests/army
python3 ./test.py -f query/query_basic.py -N 3 >> $case_out_file

cd ${TDENGINE_ROOT_DIR}/tests/system-test
python3 ./test.py -f 1-insert/insert_column_value.py >> $case_out_file
python3 ./test.py -f 2-query/primary_ts_base_5.py >> $case_out_file
python3 ./test.py -f 2-query/case_when.py >> $case_out_file
python3 ./test.py -f 2-query/partition_limit_interval.py >> $case_out_file
python3 ./test.py -f 2-query/join.py >> $case_out_file
python3 ./test.py -f 2-query/fill.py >> $case_out_file
