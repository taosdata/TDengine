#!/bin/sh

echo "Executing deploy.sh"

if [ $# != 6 ]; then 
  echo "argument list need input : "
  echo "  -n nodeName"
  echo "  -i nodeIp"
  echo "  -m masterIp"
  exit 1
fi

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

cd ../../
TAOS_DIR=`pwd`

BUILD_DIR=$TAOS_DIR/debug/build
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

echo " "                                  >> $TAOS_CFG   
echo "masterIp            $MASTER_IP"     >> $TAOS_CFG
echo "dataDir             $DATA_DIR"      >> $TAOS_CFG
echo "logDir              $LOG_DIR"       >> $TAOS_CFG
echo "publicIp            $NODE_IP"       >> $TAOS_CFG
echo "internalIp          $NODE_IP"       >> $TAOS_CFG
echo "privateIp           $NODE_IP"       >> $TAOS_CFG
echo "dDebugFlag          199"            >> $TAOS_CFG
echo "mDebugFlag          199"            >> $TAOS_CFG
echo "sdbDebugFlag        199"            >> $TAOS_CFG
echo "rpcDebugFlag        135"            >> $TAOS_CFG
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
echo "mgmtEqualVnodeNum   0"              >> $TAOS_CFG
echo "clog                2"              >> $TAOS_CFG
echo "statusInterval      1"              >> $TAOS_CFG
echo "numOfTotalVnodes    4"              >> $TAOS_CFG
echo "asyncLog            0"              >> $TAOS_CFG
echo "numOfMPeers         1"              >> $TAOS_CFG
echo "locale    en_US.UTF-8"              >> $TAOS_CFG
echo "anyIp               0"              >> $TAOS_CFG


