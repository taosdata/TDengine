#!/bin/bash

# if [ $# != 2 || $# != 3 ]; then
  # echo "argument list need input : "
  # echo "  -s start/stop"
  # exit 1
# fi

UNAME_BIN=`which uname`
OS_TYPE=`$UNAME_BIN`
EXEC_OPTON=
while getopts "n:s:u:x:ct" arg
do
  case $arg in
    n)
      NODE_NAME=$OPTARG
      ;;
    s)
      EXEC_OPTON=$OPTARG
      ;;
    c)
      CLEAR_OPTION="clear"
      ;;
    t)
      SHELL_OPTION="true"
      ;;
    u)
      USERS=$OPTARG
      ;;
    x)
      SIGNAL=$OPTARG
      ;;
    ?)
      echo "unkown argument"
      ;;
  esac
done


SCRIPT_DIR=`dirname $0`
cd $SCRIPT_DIR/../
SCRIPT_DIR=`pwd`

IN_TDINTERNAL="community"
if [[ "$SCRIPT_DIR" == *"$IN_TDINTERNAL"* ]]; then
  cd ../../..
else
  cd ../../
fi

TAOS_DIR=`pwd`
TAOSD_DIR=`find . -name "taosd"|grep bin|head -n1`

cut_opt="-f "

if [[ "$TAOSD_DIR" == *"$IN_TDINTERNAL"* ]]; then
  BIN_DIR=`find . -name "taosd"|grep bin|head -n1|cut -d '/' ${cut_opt}2,3`
else
  BIN_DIR=`find . -name "taosd"|grep bin|head -n1|cut -d '/' ${cut_opt}2`
fi

BUILD_DIR=$TAOS_DIR/$BIN_DIR/build

SIM_DIR=$TAOS_DIR/sim
NODE_DIR=$SIM_DIR/arbitrator
EXE_DIR=$BUILD_DIR/bin
LOG_DIR=$NODE_DIR/log

echo "------------ $EXEC_OPTON tarbitrator"

if [ "$EXEC_OPTON" = "start" ]; then 
  echo "------------ log path: $LOG_DIR"
  nohup $EXE_DIR/tarbitrator -p 8000 -d 135 -g $LOG_DIR > /dev/null 2>&1 & 
else
  #relative path
  PID=`ps -ef|grep tarbitrator | grep -v grep | awk '{print $2}'`
  if [ -n "$PID" ]; then   
    kill -9 $PID
    pkill -9 tarbitrator
  fi 
fi

