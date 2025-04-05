#!/bin/bash

echo "Executing mv_old_data.sh"

UNAME_BIN=`which uname`
OS_TYPE=`$UNAME_BIN`

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

rm -rf $SIM_DIR/dnode1
rm -rf $SIM_DIR/dnode2
rm -rf $SIM_DIR/dnode3

tar zxf $SCRIPT_DIR/general/connection/sim.tar.gz -C $SIM_DIR/../
cd $SIM_DIR/../sim
fqdn=`hostname -f || hostname`
grep 'test4' -l -r ./* | xargs sed -i "s/test4/${fqdn}/g"
grep 'dataDir' -l -r ./* | xargs sed -i "s#/root/TDengine#${TAOS_DIR}#g"
