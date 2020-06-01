#!/bin/bash

##################################################
# 
# Do simulation test 
#
##################################################

set +e
#set -x

FILE_NAME=
RELEASE=0
ASYNC=0
VALGRIND=0
UNIQUE=0
while getopts "f:avu" arg
do
  case $arg in
    f)
      FILE_NAME=$OPTARG
      ;;
    a)
      ASYNC=1
      ;;
    v)
      VALGRIND=1
      ;;
    u)
      UNIQUE=1
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

TDINTERNAL="TDinternal"
if [[ "$CODE_DIR" == *"$TDINTERNAL"* ]]; then
  cd ../../..
else
  cd ../../
fi

TOP_DIR=`pwd`

BIN_DIR=`find . -name "taosd"|grep bin| cut -d '/' --fields=2,3`

BUILD_DIR=$TOP_DIR/$BIN_DIR

SIM_DIR=$TOP_DIR/sim

if [ $ASYNC -eq 0 ]; then
  PROGRAM=$BUILD_DIR/bin/tsim
else
  PROGRAM="$BUILD_DIR/bin/tsim -a"
fi


PRG_DIR=$SIM_DIR/tsim
CFG_DIR=$PRG_DIR/cfg
LOG_DIR=$PRG_DIR/log
DATA_DIR=$PRG_DIR/data


ARBITRATOR_PRG_DIR=$SIM_DIR/arbitrator
ARBITRATOR_LOG_DIR=$ARBITRATOR_PRG_DIR/log


chmod -R 777 $PRG_DIR
echo "------------------------------------------------------------------------"
echo "Start TDengine Testing Case ..."
echo "BUILD_DIR: $BUILD_DIR"
echo "SIM_DIR  : $SIM_DIR"
echo "CODE_DIR : $CODE_DIR"
echo "CFG_DIR  : $CFG_DIR"

rm -rf $LOG_DIR
rm -rf $CFG_DIR
rm -rf $ARBITRATOR_LOG_DIR

mkdir -p $PRG_DIR
mkdir -p $LOG_DIR
mkdir -p $CFG_DIR
mkdir -p $ARBITRATOR_LOG_DIR

TAOS_CFG=$PRG_DIR/cfg/taos.cfg
touch -f $TAOS_CFG
TAOS_FLAG=$PRG_DIR/flag

HOSTNAME=`hostname -f`

echo " "                                     >> $TAOS_CFG
echo "first            ${HOSTNAME}:7100"     >> $TAOS_CFG
echo "second           ${HOSTNAME}:7200"     >> $TAOS_CFG
echo "serverPort       7100"                 >> $TAOS_CFG
echo "dataDir          $DATA_DIR"            >> $TAOS_CFG
echo "logDir           $LOG_DIR"             >> $TAOS_CFG
echo "scriptDir        ${CODE_DIR}/../script">> $TAOS_CFG
echo "numOfLogLines    100000000"            >> $TAOS_CFG
echo "dDebugFlag       135"                  >> $TAOS_CFG
echo "mDebugFlag       135"                  >> $TAOS_CFG
echo "sdbDebugFlag     135"                  >> $TAOS_CFG
echo "rpcDebugFlag     151"                  >> $TAOS_CFG
echo "tmrDebugFlag     131"                  >> $TAOS_CFG
echo "cDebugFlag       135"                  >> $TAOS_CFG
echo "httpDebugFlag    135"                  >> $TAOS_CFG
echo "monitorDebugFlag 135"                  >> $TAOS_CFG
echo "udebugFlag       135"                  >> $TAOS_CFG
echo "tablemetakeeptimer 5"                  >> $TAOS_CFG
echo "wal              0"                    >> $TAOS_CFG
echo "asyncLog         0"                    >> $TAOS_CFG
echo "locale           en_US.UTF-8"          >> $TAOS_CFG
echo " "                                     >> $TAOS_CFG

ulimit -n 600000
ulimit -c unlimited

#sudo sysctl -w kernel.core_pattern=$TOP_DIR/core.%p.%e

if [ -n "$FILE_NAME" ]; then
  echo "------------------------------------------------------------------------"
  if [ $VALGRIND -eq 1 ]; then
    echo valgrind --tool=memcheck --leak-check=full --show-reachable=no  --track-origins=yes --show-leak-kinds=all  -v  --workaround-gcc296-bugs=yes  --log-file=${CODE_DIR}/../script/valgrind.log $PROGRAM -c $CFG_DIR -f $FILE_NAME
    valgrind --tool=memcheck --leak-check=full --show-reachable=no  --track-origins=yes --show-leak-kinds=all  -v  --workaround-gcc296-bugs=yes  --log-file=${CODE_DIR}/../script/valgrind.log $PROGRAM -c $CFG_DIR -f $FILE_NAME
  else
    echo "ExcuteCmd:" $PROGRAM -c $CFG_DIR -f $FILE_NAME
    $PROGRAM -c $CFG_DIR -f $FILE_NAME
  fi
else
  echo "ExcuteCmd:" $PROGRAM -c $CFG_DIR -f basicSuite.sim
  echo "------------------------------------------------------------------------"
  $PROGRAM -c $CFG_DIR -f basicSuite.sim
fi

