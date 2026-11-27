#!/bin/bash

# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

set +e
#set -x

echo "Executing copy_udf.sh"
SCRIPT_DIR=`dirname $0`
cd $SCRIPT_DIR/../
SCRIPT_DIR=`pwd`
echo "SCRIPT_DIR: ${SCRIPT_DIR}"

IN_TDINTERNAL="community"
if [[ "$SCRIPT_DIR" == *"$IN_TDINTERNAL"* ]]; then
  cd ../../..
else
  cd ../../
fi

TAOS_DIR=`pwd`
echo "find udf library in $TAOS_DIR"
UDF1_DIR=`find $TAOS_DIR -name "libudf1.so"|grep lib|head -n1`
UDF2_DIR=`find $TAOS_DIR -name "libudf2.so"|grep lib|head -n1`

echo $UDF1_DIR
echo $UDF2_DIR

UDF_TMP=/tmp/udf
rm -rf $UDF_TMP
mkdir $UDF_TMP

echo "Copy udf shared library files to $UDF_TMP"

cp $UDF1_DIR $UDF_TMP
echo "copy udf1 result: $?" 
cp $UDF2_DIR $UDF_TMP
echo "copy udf2 result: $?"
