#!/bin/bash

##################################################
#
# Do simulation test
#
##################################################

set +e
#set -x

UNAME_BIN=`which uname`
OS_TYPE=`$UNAME_BIN`

cd .

# Get responsible directories
CODE_DIR=`dirname $0`
CODE_DIR=`pwd`

IN_TDINTERNAL="community"
if [[ "$CODE_DIR" == *"$IN_TDINTERNAL"* ]]; then
  cd ../../..
else
  cd ../../
fi

TOP_DIR=`pwd`
TAOSD_DIR=`find . -name "taosd"|grep bin|head -n1`

if [[ "$OS_TYPE" != "Darwin" ]]; then
  cut_opt="--field="
else
  cut_opt="-f "
fi

if [[ "$TAOSD_DIR" == *"$IN_TDINTERNAL"* ]]; then
  BIN_DIR=`find . -name "taosd"|grep bin|head -n1|cut -d '/' ${cut_opt}2,3`
else
  BIN_DIR=`find . -name "taosd"|grep bin|head -n1|cut -d '/' ${cut_opt}2`
fi

declare -x BUILD_DIR=$TOP_DIR/$BIN_DIR
declare -x SIM_DIR=$TOP_DIR/sim
PROGRAM=$BUILD_DIR/build/bin/tsim
PRG_DIR=$SIM_DIR/tsim
ASAN_DIR=$SIM_DIR/asan
SYSTEM_TEST_DIR=$CODE_DIR/tests/system-test

chmod -R 777 $PRG_DIR
echo "------------------------------------------------------------------------"
echo "Start TDengine Testing Case ..."
echo "BUILD_DIR: $BUILD_DIR"
echo "SYSTEM_TEST_DIR : $SYSTEM_TEST_DIR"
echo "SIM_DIR  : $SIM_DIR"
echo "CODE_DIR : $CODE_DIR"
echo "ASAN_DIR  : $ASAN_DIR"

rm -rf $SIM_DIR/*

mkdir -p $PRG_DIR
mkdir -p $ASAN_DIR

cd $SYSTEM_TEST_DIR
ulimit -n 600000
ulimit -c unlimited

#sudo sysctl -w kernel.core_pattern=$TOP_DIR/core.%p.%e

echo "ExcuteCmd:" $*
echo "AsanDir:" $ASAN_DIR/psim.info

export LD_PRELOAD=libasan.so.5
$* -a 2> $ASAN_DIR/psim.info

result=$?
echo "Execute result:" $result

if [ $result -eq 0 ]; then
  $CODE_DIR/tests/script/sh/checkAsan.sh
else
  exit 1
fi

