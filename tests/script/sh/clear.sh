#!/bin/bash

echo "Executing clear.sh"

if [ $# != 6 ]; then
  echo "argument list need input : "
  echo "  -n nodeName"
  echo "  -i nodeIp"
  echo "  -m masterIp"
  exit 1
fi

UNAME_BIN=`which uname`
OS_TYPE=`$UNAME_BIN`
NODE_NAME=
NODE_IP=
MSATER_IP=
while getopts "n:i:m:" arg
do
  case $arg in
    n)
      NODE_NAME=$OPTARG
      ;;
    i)
      NODE_IP=$OPTARG
      ;;
    m)
      MASTER_IP=$OPTARG
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

BUILD_DIR=$TAOS_DIR/$BIN_DIR/build

SIM_DIR=$TAOS_DIR/sim
NODE_DIR=$SIM_DIR/$NODE_NAME
EXE_DIR=$BUILD_DIR/bin
CFG_DIR=$NODE_DIR/cfg
LOG_DIR=$NODE_DIR/log
DATA_DIR=$NODE_DIR/data

#echo ============ deploy $NODE_NAME
#echo === masterIp : $MASTER_IP
#echo === nodeIp : $NODE_IP
#echo === nodePath : $EXE_DIR
#echo === cfgPath : $CFG_DIR
#echo === logPath : $LOG_DIR
#echo === dataPath : $DATA_DIR

# rm -rf $NODE_DIR

mkdir -p $SIM_DIR
mkdir -p $NODE_DIR
mkdir -p $LOG_DIR
rm -rf $DATA_DIR
mkdir -p $DATA_DIR

#cp -rf $TAOS_DIR/cfg  $NODE_DIR/
rm -rf $CFG_DIR
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

echo " "                                  >> $TAOS_CFG
echo "masterIp            $MASTER_IP"     >> $TAOS_CFG
echo "dataDir             $DATA_DIR"      >> $TAOS_CFG
echo "logDir              $LOG_DIR"       >> $TAOS_CFG
echo "publicIp            $NODE_IP"       >> $TAOS_CFG
echo "internalIp          $NODE_IP"       >> $TAOS_CFG
echo "privateIp           $NODE_IP"       >> $TAOS_CFG
echo "dDebugFlag          135"            >> $TAOS_CFG
echo "mDebugFlag          135"            >> $TAOS_CFG
echo "sdbDebugFlag        135"            >> $TAOS_CFG
echo "rpcDebugFlag        131"            >> $TAOS_CFG
echo "tmrDebugFlag        131"            >> $TAOS_CFG
echo "cDebugFlag          135"            >> $TAOS_CFG
echo "httpDebugFlag       131"            >> $TAOS_CFG
echo "monitorDebugFlag    131"            >> $TAOS_CFG
echo "udebugFlag          131"            >> $TAOS_CFG
echo "jnidebugFlag        131"            >> $TAOS_CFG
echo "monitor             0"              >> $TAOS_CFG
echo "numOfThreadsPerCore 2.0"            >> $TAOS_CFG
echo "defaultPass         taosdata"       >> $TAOS_CFG
echo "numOfLogLines       100000000"      >> $TAOS_CFG
echo "mnodeEqualVnodeNum   0"              >> $TAOS_CFG
echo "clog                0"              >> $TAOS_CFG
echo "statusInterval      1"              >> $TAOS_CFG
echo "asyncLog            0"              >> $TAOS_CFG
echo "numOfMnodes         1"              >> $TAOS_CFG
echo "locale    en_US.UTF-8"              >> $TAOS_CFG


