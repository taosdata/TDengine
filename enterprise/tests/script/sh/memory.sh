#!/bin/sh

# if [ $# != 4 || $# != 5 ]; then 
  # echo "argument list need input : "
  # echo "  -n nodeName"
  # echo "  -s start/stop"
  # echo "  -c clear"
  # exit 1
# fi

NODE_NAME=
while getopts "n:s:u:x:ct" arg 
do
  case $arg in
    n)
      NODE_NAME=$OPTARG
      ;;
    ?)
      echo "unkonw argument"
      ;;
  esac
done

SCRIPT_DIR=`dirname $0`
cd $SCRIPT_DIR
SCRIPT_DIR=`pwd`

cd ../../
TAOS_DIR=`pwd`

cd ../
PARENT_DIR=`pwd`

BUILD_DIR=$PARENT_DIR/build
cd ../
TOP_TOP_DIR=`pwd`
SIM_DIR=$TOP_TOP_DIR/sim

NODE_DIR=$SIM_DIR/$NODE_NAME
EXE_DIR=$BUILD_DIR/bin
CFG_DIR=$NODE_DIR/cfg
LOG_DIR=$NODE_DIR/log
DATA_DIR=$NODE_DIR/data
MGMT_DIR=$NODE_DIR/data/mgmt
TSDB_DIR=$NODE_DIR/data/tsdb

TAOS_CFG=$NODE_DIR/cfg/taos.cfg



RCFG_DIR=sim/$NODE_NAME/cfg
PID=`ps -ef|grep taosd | grep $RCFG_DIR | grep -v grep | awk '{print $2}'`

VSS=`cat /proc/$PID/status | grep VmRSS `

echo ---  thread  ---  $PID  ----  $VSS
