#!/bin/bash

set +e
#set -x

echo "Executing deploy.sh"

UNAME_BIN=`which uname`
OS_TYPE=`$UNAME_BIN`
NODE_NAME=
NODE=
while getopts "n:i:m" arg
do
  case $arg in
    n)
      NODE_NAME=$OPTARG
      ;;
    i)
      NODE=$OPTARG
      ;;
    ?)
      echo "unkonw argument"
      ;;
  esac
done

SCRIPT_DIR=`dirname $0`
cd $SCRIPT_DIR/../
SCRIPT_DIR=`pwd`
echo "SCRIPT_DIR: $SCRIPT_DIR"

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

BUILD_DIR=$TAOS_DIR/$BIN_DIR

SIM_DIR=$TAOS_DIR/sim

NODE_DIR=$SIM_DIR/$NODE_NAME
EXE_DIR=$BUILD_DIR/bin
CFG_DIR=$NODE_DIR/cfg
LOG_DIR=$NODE_DIR/log
DATA_DIR=$NODE_DIR/data

rm -rf $NODE_DIR

mkdir -p $SIM_DIR
mkdir -p $NODE_DIR
mkdir -p $LOG_DIR
mkdir -p $DATA_DIR

#cp -rf $TAOS_DIR/cfg  $NODE_DIR/
mkdir -p $CFG_DIR

#allow normal user to read/write log
chmod -R 777 $NODE_DIR

TAOS_CFG=$NODE_DIR/cfg/taos.cfg
touch -f $TAOS_CFG

TAOS_FLAG=$SIM_DIR/tsim/flag
if [ -f "$TAOS_FLAG" ] ; then 
  TAOS_CFG=/etc/taos/taos.cfg
  DATA_DIR=/var/lib/taos
  LOG_DIR=/var/log/taos
  sudo rm -f /etc/taos/*.cfg
  sudo cp -rf $TAOS_DIR/cfg/*.cfg /etc/taos
  sudo rm -rf $DATA_DIR
  sudo rm -rf $LOG_DIR
fi

HOSTNAME=`hostname -f`

if [ $NODE -eq 1 ]; then
  NODE=7100
elif [ $NODE -eq 2 ]; then
  NODE=7200
elif [ $NODE -eq 3 ]; then
  NODE=7300
elif [ $NODE -eq 4 ]; then
  NODE=7400
elif [ $NODE -eq 5 ]; then
  NODE=7500
elif [ $NODE -eq 6 ]; then
  NODE=7600  
elif [ $NODE -eq 7 ]; then
  NODE=7700  
elif [ $NODE -eq 8 ]; then
  NODE=7800    
elif [ $NODE -eq 9 ]; then
  NODE=7900    
fi

HOSTNAME=localhost

echo " "                                         >> $TAOS_CFG   
echo "firstEp                ${HOSTNAME}:7100"   >> $TAOS_CFG
echo "secondEp               ${HOSTNAME}:7200"   >> $TAOS_CFG
echo "fqdn                   ${HOSTNAME}"        >> $TAOS_CFG
echo "serverPort             ${NODE}"            >> $TAOS_CFG
echo "supportVnodes          1024"                >> $TAOS_CFG
echo "statusInterval         1"                  >> $TAOS_CFG
echo "dataDir                $DATA_DIR"          >> $TAOS_CFG
echo "logDir                 $LOG_DIR"           >> $TAOS_CFG
echo "debugFlag              0"                  >> $TAOS_CFG
echo "tmrDebugFlag           131"                >> $TAOS_CFG
echo "uDebugFlag             143"                >> $TAOS_CFG
echo "rpcDebugFlag           143"                >> $TAOS_CFG
echo "jniDebugFlag           143"                >> $TAOS_CFG
echo "qDebugFlag             143"                >> $TAOS_CFG
echo "cDebugFlag             143"                >> $TAOS_CFG
echo "dDebugFlag             143"                >> $TAOS_CFG
echo "vDebugFlag             143"                >> $TAOS_CFG
echo "mDebugFlag             143"                >> $TAOS_CFG
echo "wDebugFlag             143"                >> $TAOS_CFG
echo "sDebugFlag             143"                >> $TAOS_CFG
echo "tsdbDebugFlag          143"                >> $TAOS_CFG
echo "tdbDebugFlag           143"                >> $TAOS_CFG
echo "tqDebugFlag            143"                >> $TAOS_CFG
echo "fsDebugFlag            143"                >> $TAOS_CFG
echo "idxDebugFlag           143"                >> $TAOS_CFG
echo "udfDebugFlag           143"                >> $TAOS_CFG
echo "smaDebugFlag           143"                >> $TAOS_CFG
echo "metaDebugFlag          143"                >> $TAOS_CFG
echo "stDebugFlag            143"                >> $TAOS_CFG
echo "numOfLogLines          20000000"           >> $TAOS_CFG
echo "asyncLog               0"                  >> $TAOS_CFG
echo "locale                 en_US.UTF-8"        >> $TAOS_CFG
echo "telemetryReporting     0"                  >> $TAOS_CFG
echo "querySmaOptimize       1"                  >> $TAOS_CFG
echo " "                                         >> $TAOS_CFG  
