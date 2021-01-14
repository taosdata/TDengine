#!/bin/bash

echo "Executing move_dnode.sh"

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

if [[ "$TAOSD_DIR" == *"$IN_TDINTERNAL"* ]]; then
  BIN_DIR=`find . -name "taosd"|grep bin|head -n1|cut -d '/' --fields=2,3`
else
  BIN_DIR=`find . -name "taosd"|grep bin|head -n1|cut -d '/' --fields=2`
fi

BUILD_DIR=$TAOS_DIR/$BIN_DIR/build

SIM_DIR=$TAOS_DIR/sim

NODE_DIR=$SIM_DIR/$NODE_NAME

if [ -d "$SIM_DIR/$2" ];then
  rm -rf $SIM_DIR/$2
fi
mv $SIM_DIR/$1 $SIM_DIR/$2

if [[ $2 =~ "dnode2" ]];then
  sed -i 's/serverPort             7100/serverPort             7200/g' $SIM_DIR/$2/cfg/taos.cfg
  sed -i 's/dnode1/dnode2/g' $SIM_DIR/$2/cfg/taos.cfg
  sed -i 's/7100/7200/g' $SIM_DIR/$2/data/dnode/dnodeEps.json
elif [[ $2 =~ "dnode4" ]];then
  sed -i 's/serverPort             7100/serverPort             7400/g' $SIM_DIR/$2/cfg/taos.cfg
  sed -i 's/dnode1/dnode4/g' $SIM_DIR/$2/cfg/taos.cfg
  sed -i 's/7100/7400/g' $SIM_DIR/dnode2/data/dnode/dnodeEps.json
  sed -i 's/7100/7400/g' $SIM_DIR/dnode3/data/dnode/dnodeEps.json
  sed -i 's/7100/7400/g' $SIM_DIR/$2/data/dnode/dnodeEps.json
fi
