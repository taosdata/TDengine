#!/bin/bash

# Copyright (c) 2020 TAOS Data, Inc. <jhtao@taosdata.com>
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

log_dir=$1
result_file=$2

if [ ! -n "$1" ];then
  echo "Pleas input the director of taosdlog."
  echo "usage: ./get_client.sh <taosdlog directory> <result file>"
  exit 1
else
  log_dir=$1
fi

if [ ! -n "$2" ];then
  result_file=clientInfo.txt
else
  result_file=$2
fi

grep "new TCP connection" ${log_dir}/taosdlog.* | sed -e "s/0x.* from / /"|sed -e "s/,.*$//"|sed -e "s/:[0-9]*$//"|sort -r|uniq -f 2|sort -k 3 -r|uniq -f 2 > ${result_file}
