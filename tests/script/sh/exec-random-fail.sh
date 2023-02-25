#!/bin/bash

# if [ $# != 4 || $# != 5 ]; then
  # echo "argument list need input : "
  # echo "  -n nodeName"
  # echo "  -s start/stop"
  # echo "  -c clear"
  # exit 1
# fi

UNAME_BIN=`which uname`
OS_TYPE=`$UNAME_BIN`
NODE_NAME=
EXEC_OPTON=
CLEAR_OPTION="false"
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
NODE_DIR=$SIM_DIR/$NODE_NAME
EXE_DIR=$BUILD_DIR/bin
CFG_DIR=$NODE_DIR/cfg
LOG_DIR=$NODE_DIR/log
DATA_DIR=$NODE_DIR/data
MGMT_DIR=$NODE_DIR/data/mgmt
TSDB_DIR=$NODE_DIR/data/tsdb

TAOS_CFG=$NODE_DIR/cfg/taos.cfg

echo ------------ $EXEC_OPTON $NODE_NAME

TAOS_FLAG=$SIM_DIR/tsim/flag
if [ -f "$TAOS_FLAG" ]; then 
  EXE_DIR=/usr/local/bin/taos
fi

if [ "$CLEAR_OPTION" = "clear" ]; then 
  echo rm -rf $MGMT_DIR $TSDB_DIR  
  rm -rf $TSDB_DIR
  rm -rf $MGMT_DIR
fi

if [ "$EXEC_OPTON" = "start" ]; then 
  echo "ExcuteCmd:" $EXE_DIR/taosd -c $CFG_DIR
  
  if [ "$SHELL_OPTION" = "true" ]; then 
    TT=`date +%s`
    mkdir ${LOG_DIR}/${TT}
    nohup valgrind --log-file=${LOG_DIR}/${TT}/valgrind.log --tool=memcheck --leak-check=full --show-reachable=no  --track-origins=yes --show-leak-kinds=all  -v  --workaround-gcc296-bugs=yes   $EXE_DIR/taosd -c $CFG_DIR > /dev/null 2>&1 &   
  else
    nohup $EXE_DIR/taosd -c $CFG_DIR  --alloc-random-fail \
      --random-file-fail-factor 5 > /dev/null 2>&1 & 
  fi
  
else
  #relative path
  RCFG_DIR=sim/$NODE_NAME/cfg
  PID=`ps -ef|grep taosd | grep $RCFG_DIR | grep -v grep | awk '{print $2}'`
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
    PID=`ps -ef|grep taosd | grep $RCFG_DIR | grep -v grep | awk '{print $2}'`
  done 
fi

