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

set -e
#set -x

CMD_NAME=
LOOP_TIMES=5
SLEEP_TIME=0

while getopts "hf:t:s:" arg
do
  case $arg in
    f)
      CMD_NAME=$OPTARG
      ;;
    t)
      LOOP_TIMES=$OPTARG
      ;;
    s)
      SLEEP_TIME=$OPTARG
      ;;
    h)
      echo "Usage: $(basename $0) -f [cmd name] "
      echo "                  -t [loop times] "
      echo "                  -s [sleep time] "
      exit 0
      ;;
    ?)
      echo "unknow argument"
      ;;
  esac
done

echo LOOP_TIMES ${LOOP_TIMES}
echo CMD_NAME ${CMD_NAME}
echo SLEEP_TIME ${SLEEP_TIME}

GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

for ((i=0; i<$LOOP_TIMES; i++ ))
do
    echo -e $GREEN loop $i $NC
    echo -e $GREEN cmd $CMD_NAME $NC
    $CMD_NAME
    sleep ${SLEEP_TIME}
done
