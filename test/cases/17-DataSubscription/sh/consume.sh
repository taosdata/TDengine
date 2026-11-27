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

# set default value for parameters
EXEC_OPTON=start
DB_NAME=db 
CDB_NAME=db
POLL_DELAY=5
VALGRIND=0
SIGNAL=SIGINT
SHOW_MSG=0
SHOW_ROW=0
EXP_USE_SNAPSHOT=0

while getopts "d:s:v:y:x:g:r:w:e:" arg
do
  case $arg in
    d)
      DB_NAME=$OPTARG
      ;;
    g)
      SHOW_MSG=$OPTARG
      ;;
    r)
      SHOW_ROW=$OPTARG
      ;;
    s)
      EXEC_OPTON=$OPTARG
      ;;
    v)
      VALGRIND=1
      ;;
    y)
      POLL_DELAY=$OPTARG
      ;;
    x)
      SIGNAL=$OPTARG
      ;;
    w)
      CDB_NAME=$OPTARG
      ;;
    e)
      EXP_USE_SNAPSHOT=$OPTARG
      ;;
    ?)
      echo "unkown argument"
      ;;
  esac
done

SCRIPT_DIR=`pwd`

IN_TDINTERNAL="community"
if [[ "$SCRIPT_DIR" == *"$IN_TDINTERNAL"* ]]; then
  cd ../..
else
  cd ../
fi

TOP_DIR=`pwd`

BIN_DIR=`find . -name "tmq_sim"|grep bin|head -n1|cut -d '/' -f 2`

declare -x BUILD_DIR=$TOP_DIR/$BIN_DIR

declare -x SIM_DIR=$TOP_DIR/sim

PROGRAM=$BUILD_DIR/build/bin/tmq_sim

PRG_DIR=$SIM_DIR/dnode1
CFG_DIR=$PRG_DIR/cfg
LOG_DIR=$PRG_DIR/log

echo "------------------------------------------------------------------------"
echo "TOP_DIR: $TOP_DIR"
echo "BUILD_DIR: $BUILD_DIR"
echo "SIM_DIR  : $SIM_DIR"
echo "CFG_DIR  : $CFG_DIR"

echo "PROGRAM: $PROGRAM"
echo "CFG_DIR: $CFG_DIR"
echo "POLL_DELAY: $POLL_DELAY"
echo "DB_NAME: $DB_NAME"

echo "------------------------------------------------------------------------"
if [ "$EXEC_OPTON" = "start" ]; then 
  if [ $VALGRIND -eq 1 ]; then
    echo nohup valgrind --tool=memcheck --leak-check=full --show-reachable=no --track-origins=yes --show-leak-kinds=all -v --workaround-gcc296-bugs=yes --log-file=${LOG_DIR}/valgrind-tmq_sim.log $PROGRAM -c $CFG_DIR -y $POLL_DELAY -d $DB_NAME -g $SHOW_MSG -r $SHOW_ROW > /dev/null 2>&1 &
    nohup valgrind --tool=memcheck --leak-check=full --show-reachable=no --track-origins=yes --show-leak-kinds=all -v --workaround-gcc296-bugs=yes --log-file=${LOG_DIR}/valgrind-tmq_sim.log $PROGRAM -c $CFG_DIR -y $POLL_DELAY -d $DB_NAME -g $SHOW_MSG -r $SHOW_ROW > /dev/null 2>&1 &
  else
    echo  "nohup $PROGRAM -c $CFG_DIR -y $POLL_DELAY -d $DB_NAME -g $SHOW_MSG -r $SHOW_ROW -w $CDB_NAME -e $EXP_USE_SNAPSHOT > /dev/null 2>&1 &"
    nohup $PROGRAM -c $CFG_DIR -y $POLL_DELAY -d $DB_NAME -g $SHOW_MSG -r $SHOW_ROW -w $CDB_NAME -e $EXP_USE_SNAPSHOT > /dev/null 2>&1 &
  fi
else
  PID=`ps -ef|grep tmq_sim | grep -v grep | awk '{print $2}'`
  while [ -n "$PID" ]
  do
    if [ "$SIGNAL" = "SIGKILL" ]; then
      echo try to kill by signal SIGKILL
      kill -9 $PID
    else
      echo try to kill by signal SIGINT
      kill -SIGINT $PID
    fi
    sleep 1
    PID=`ps -ef|grep tmq_sim | grep -v grep | awk '{print $2}'`
  done 
fi
