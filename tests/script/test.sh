#!/bin/bash

##################################################
#
# Do simulation test
#
##################################################

set +e
#set -x

FILE_NAME=
VALGRIND=0
TEST=0
UNAME_BIN=`which uname`
OS_TYPE=`$UNAME_BIN`
while getopts "f:tgv" arg
do
  case $arg in
    f)
      FILE_NAME=$OPTARG
      ;;
    v)
      VALGRIND=1
      ;;
    t)
      TEST=1
      ;;
    g)
      VALGRIND=2
      ;;  
    ?)
      echo "unknow argument"
      ;;
  esac
done

cd .

# Get responsible directories
CODE_DIR=`dirname $0`
CODE_DIR=`pwd`

IN_TDINTERNAL="community"
if [[ "$CODE_DIR" == *"$IN_TDINTERNAL"* ]]; then
  cd ../../..
else
  cd ../../
fi

TOP_DIR=`pwd`
TAOSD_DIR=`find . -name "taosd"|grep bin|head -n1`

cut_opt="-f "

if [[ "$TAOSD_DIR" == *"$IN_TDINTERNAL"* ]]; then
  BIN_DIR=`find . -name "taosd"|grep bin|head -n1|cut -d '/' ${cut_opt}2,3`
else
  BIN_DIR=`find . -name "taosd"|grep bin|head -n1|cut -d '/' ${cut_opt}2`
fi

declare -x BUILD_DIR=$TOP_DIR/$BIN_DIR
declare -x SIM_DIR=$TOP_DIR/sim
PROGRAM=$BUILD_DIR/build/bin/tsim
PRG_DIR=$SIM_DIR/tsim
CFG_DIR=$PRG_DIR/cfg
LOG_DIR=$PRG_DIR/log
DATA_DIR=$PRG_DIR/data
ASAN_DIR=$SIM_DIR/asan

chmod -R 777 $PRG_DIR
echo "------------------------------------------------------------------------"
echo "Start TDengine Testing Case ..."
echo "BUILD_DIR: $BUILD_DIR"
echo "SIM_DIR  : $SIM_DIR"
echo "CODE_DIR : $CODE_DIR"
echo "CFG_DIR  : $CFG_DIR"
echo "ASAN_DIR  : $ASAN_DIR"

rm -rf $SIM_DIR/*
rm -rf $LOG_DIR
rm -rf $CFG_DIR
rm -rf $ASAN_DIR

mkdir -p $PRG_DIR
mkdir -p $LOG_DIR
mkdir -p $CFG_DIR
mkdir -p $ASAN_DIR

TAOS_CFG=$PRG_DIR/cfg/taos.cfg
touch -f $TAOS_CFG
TAOS_FLAG=$PRG_DIR/flag

#HOSTNAME=`hostname -f`
HOSTNAME=localhost

echo " "                                          >> $TAOS_CFG
echo "firstEp            ${HOSTNAME}:7100"        >> $TAOS_CFG
echo "secondEp           ${HOSTNAME}:7200"        >> $TAOS_CFG
echo "serverPort         7100"                    >> $TAOS_CFG
echo "dataDir            $DATA_DIR"               >> $TAOS_CFG
echo "logDir             $LOG_DIR"                >> $TAOS_CFG
echo "scriptDir          ${CODE_DIR}"             >> $TAOS_CFG
echo "numOfLogLines      100000000"               >> $TAOS_CFG
echo "rpcDebugFlag       143"                     >> $TAOS_CFG
echo "tmrDebugFlag       131"                     >> $TAOS_CFG
echo "cDebugFlag         143"                     >> $TAOS_CFG
echo "udebugFlag         143"                     >> $TAOS_CFG
echo "debugFlag          143"                     >> $TAOS_CFG
echo "wal                0"                       >> $TAOS_CFG
echo "asyncLog           0"                       >> $TAOS_CFG
echo "locale             en_US.UTF-8"             >> $TAOS_CFG
echo "enableCoreFile     1"                       >> $TAOS_CFG
echo " "                                          >> $TAOS_CFG

ulimit -n 600000
ulimit -c unlimited

#sudo sysctl -w kernel.core_pattern=$TOP_DIR/core.%p.%e

if [ -n "$FILE_NAME" ]; then
  echo "------------------------------------------------------------------------"
  if [ $VALGRIND -eq 1 ]; then
    FLAG="-v"
    echo valgrind --tool=memcheck --leak-check=full --show-reachable=no  --track-origins=yes --child-silent-after-fork=yes --show-leak-kinds=all --num-callers=20 -v  --workaround-gcc296-bugs=yes  --log-file=${LOG_DIR}/valgrind-tsim.log $PROGRAM -c $CFG_DIR -f $FILE_NAME $FLAG
    valgrind --tool=memcheck --leak-check=full --show-reachable=no  --track-origins=yes --child-silent-after-fork=yes --show-leak-kinds=all --num-callers=20 -v  --workaround-gcc296-bugs=yes  --log-file=${LOG_DIR}/valgrind-tsim.log $PROGRAM -c $CFG_DIR -f $FILE_NAME $FLAG
  elif [ $VALGRIND -eq 2 ]; then
    echo "ExcuteCmd:" $PROGRAM -c $CFG_DIR -f $FILE_NAME -v
    $PROGRAM -c $CFG_DIR -f $FILE_NAME -v
  else
    echo "ExcuteCmd:" $PROGRAM -c $CFG_DIR -f $FILE_NAME
    echo "AsanDir:" $ASAN_DIR/tsim.asan
    eval $PROGRAM -c $CFG_DIR -f $FILE_NAME 2> $ASAN_DIR/tsim.asan
    result=$?
    echo "Execute result:" $result

    if [ $TEST -eq 1 ]; then
      echo "Exit without check asan errors"
      exit 1
    fi

    if [ $result -eq 0 ]; then
      $CODE_DIR/sh/sigint_stop_dnodes.sh
      $CODE_DIR/sh/checkAsan.sh
    else
      echo "TSIM has asan errors"
      sleep 1
      $CODE_DIR/sh/checkAsan.sh
      exit 1
    fi
  fi
else
  echo "ExcuteCmd:" $PROGRAM -c $CFG_DIR -f basicSuite.sim
  echo "------------------------------------------------------------------------"
  $PROGRAM -c $CFG_DIR -f basicSuite.sim
fi

