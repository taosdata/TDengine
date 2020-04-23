#!/bin/bash

##################################################
# 
# Do simulation test 
#
##################################################

set +e

FILE_NAME=
RELEASE=0
ASYNC=0
VALGRIND=0
while getopts "f:av" arg
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
    ?)
      echo "unknow argument"
      ;;
  esac
done

cd .
sh/ip.sh -i 1 -s up > /dev/null 2>&1 & 
sh/ip.sh -i 2 -s up > /dev/null 2>&1 & 
sh/ip.sh -i 3 -s up > /dev/null 2>&1 & 
sh/ip.sh -i 4 -s up > /dev/null 2>&1 & 
sh/ip.sh -i 5 -s up > /dev/null 2>&1 & 

# Get responsible directories
CODE_DIR=`dirname $0`
CODE_DIR=`pwd`
cd ../../
TOP_DIR=`pwd`
BUILD_DIR=$TOP_DIR/debug/build
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

chmod -R 777 $PRG_DIR
echo "------------------------------------------------------------------------"
echo "Start TDengine Testing Case ..."
echo "BUILD_DIR: $BUILD_DIR"
echo "SIM_DIR  : $SIM_DIR"
echo "CODE_DIR : $CODE_DIR"
echo "CFG_DIR  : $CFG_DIR"

rm -rf $LOG_DIR
rm -rf $CFG_DIR
mkdir -p $PRG_DIR
mkdir -p $LOG_DIR
mkdir -p $CFG_DIR

TAOS_CFG=$PRG_DIR/cfg/taos.cfg
touch -f $TAOS_CFG
TAOS_FLAG=$PRG_DIR/flag

echo " "                                    >> $TAOS_CFG
echo "scriptDir        ${CODE_DIR}/../script">> $TAOS_CFG
echo "masterIp         192.168.0.1"          >> $TAOS_CFG
echo "secondIp         192.168.0.2"          >> $TAOS_CFG
echo "localIp          127.0.0.1"            >> $TAOS_CFG
echo "dataDir          $DATA_DIR"            >> $TAOS_CFG
echo "logDir           $LOG_DIR"             >> $TAOS_CFG
echo "numOfLogLines    100000000"            >> $TAOS_CFG
echo "dDebugFlag       135"                  >> $TAOS_CFG
echo "mDebugFlag       135"                  >> $TAOS_CFG
echo "sdbDebugFlag     135"                  >> $TAOS_CFG
echo "rpcDebugFlag     135"                  >> $TAOS_CFG
echo "tmrDebugFlag     131"                  >> $TAOS_CFG
echo "cDebugFlag       135"                  >> $TAOS_CFG
echo "httpDebugFlag    135"                  >> $TAOS_CFG
echo "monitorDebugFlag 135"                 >> $TAOS_CFG
echo "udebugFlag       135"                  >> $TAOS_CFG
echo "clog             0"                    >> $TAOS_CFG
echo "asyncLog         0"                    >> $TAOS_CFG
echo "locale           en_US.UTF-8"          >> $TAOS_CFG
echo " "                                    >> $TAOS_CFG

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

