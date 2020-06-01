#!/bin/bash

if [ $# != 6 ]; then 
  echo "argument list need input : "
  echo "  -n nodeName"
  echo "  -c configName"
  echo "  -v configValue"
  exit 1
fi

NODE_NAME=
CONFIG_NAME=
CONFIG_VALUE=
while getopts "n:v:c:" arg 
do
  case $arg in
    n)
      NODE_NAME=$OPTARG
      ;;
    c)
      CONFIG_NAME=$OPTARG
      ;;
    v)
      CONFIG_VALUE=$OPTARG
      ;;
    ?)
      echo "unkonw argument"
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

BIN_DIR=`find . -name "taosd"|grep bin| cut -d '/' --fields=2,3`

BUILD_DIR=$TAOS_DIR/$BIN_DIR

SIM_DIR=$TAOS_DIR/sim

NODE_DIR=$SIM_DIR/$NODE_NAME
TAOS_CFG=$NODE_DIR/cfg/taos.cfg
TAOS_FLAG=$SIM_DIR/tsim/flag
if [ -f "$TAOS_FLAG" ] ; then 
  TAOS_CFG=/etc/taos/taos.cfg
fi

echo "$CONFIG_NAME  $CONFIG_VALUE"        >> $TAOS_CFG
