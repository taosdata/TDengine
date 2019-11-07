#!/bin/bash

##################################################
# 
# Do simulation test 
#
##################################################

set +e

FILE_NAME=
while getopts "f:" arg 
do
  case $arg in
    f)
      FILE_NAME=$OPTARG
      ;;
    ?)
      echo "unknow argument"
      ;;
  esac
done

# Get responsible directories
TOOL_DIR=`dirname $0`
cd $TOOL_DIR
TOOL_DIR=`pwd`
cd ../
CODE_DIR=`pwd`
cd ../
TOP_DIR=`pwd`
BUILD_DIR=$TOP_DIR/build
SIM_DIR=$TOP_DIR/../sim

PROGRAM=$BUILD_DIR/bin/tsim
PRG_DIR=$SIM_DIR/tsim
CFG_DIR=$PRG_DIR/cfg
LOG_DIR=$PRG_DIR/log
DATA_DIR=$PRG_DIR/data

chmod -R 777 $PRG_DIR
echo "------------------------------------------------------------------------"
echo "Start TDengine Testing Case ..."
echo "BuildPath: $BUILD_DIR"
echo "SimPath  : $SIM_DIR"
echo "CodePath : $CODE_DIR"
echo "CfgPath  : $CFG_DIR"

rm -rf $LOG_DIR
rm -rf $CFG_DIR
rm -rf $DATA_DIR
mkdir -p $PRG_DIR
mkdir -p $LOG_DIR
mkdir -p $DATA_DIR
mkdir -p $CFG_DIR

TAOS_CFG=$PRG_DIR/cfg/taos.cfg
touch -f $TAOS_CFG
TAOS_FLAG=$PRG_DIR/flag

echo " "                                    >> $TAOS_CFG
echo "scriptDir       ${CODE_DIR}/script"   >> $TAOS_CFG
echo "masterIp        192.168.0.1"          >> $TAOS_CFG
echo "secondIp        192.168.0.2"          >> $TAOS_CFG
echo "localIp         127.0.0.1"            >> $TAOS_CFG
echo "dataDir         $DATA_DIR"            >> $TAOS_CFG
echo "logDir          $LOG_DIR"             >> $TAOS_CFG
echo "numOfLogLines   100000000"            >> $TAOS_CFG
echo "dDebugFlag      135"                  >> $TAOS_CFG
echo "mDebugFlag      135"                  >> $TAOS_CFG
echo "sdbDebugFlag    135"                  >> $TAOS_CFG
echo "rpcDebugFlag   135"                  >> $TAOS_CFG
echo "tmrDebugFlag    131"                  >> $TAOS_CFG
echo "cDebugFlag      135"                  >> $TAOS_CFG
echo "httpDebugFlag   135"                  >> $TAOS_CFG
echo "monitorDebugFlag  135"                  >> $TAOS_CFG
echo "debugFlag       135"                  >> $TAOS_CFG
echo "meterMetaKeepTimer     2"             >> $TAOS_CFG
echo "metricMetaKeepTimer    2"             >> $TAOS_CFG

#echo "rpcDebugFlag   131"                  >> $TAOS_CFG
#echo "cDebugFlag      131"                  >> $TAOS_CFG
#echo "debugFlag       131"                  >> $TAOS_CFG
#echo "meterMetaKeepTimer     3600"          >> $TAOS_CFG
#echo "metricMetaKeepTimer    3600"          >> $TAOS_CFG

echo " "                                    >> $TAOS_CFG

ulimit -c unlimited
sudo sysctl -w kernel.core_pattern=$TOP_DIR/core.%p.%e

sudo mkdir -p /usr/local/bin/taos/connector/
sudo mkdir -p /usr/local/lib/taos/

sudo cp -rf $BUILD_DIR/lib/JDBCDriver-1.0.0-dist.jar  /usr/local/bin/taos/connector/
sudo cp -rf $BUILD_DIR/lib/libtaos.so                 /usr/local/lib/taos/
sudo cp -rf $BUILD_DIR/bin/taos                      /usr/local/lib/taos/
sudo cp -rf $BUILD_DIR/bin/taosd                     /usr/local/lib/taos/
sudo cp -rf $TAOS_CFG                                 /etc/taos/

if [ -n "$FILE_NAME" ]; then
  echo "ExcuteCmd:" $PROGRAM -c $CFG_DIR -f $FILE_NAME
  echo "------------------------------------------------------------------------"
  $PROGRAM -c $CFG_DIR -f $FILE_NAME
else
  echo "ExcuteCmd:" $PROGRAM -c $CFG_DIR -f test/dnode1.sim
  echo "------------------------------------------------------------------------"
  $PROGRAM -c $CFG_DIR -f test/dnode1.sim
fi

