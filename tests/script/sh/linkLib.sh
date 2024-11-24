#!/bin/bash

set +e
#set -x

unset LD_PRELOAD
UNAME_BIN=`which uname`
OS_TYPE=`$UNAME_BIN`

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
TAOSD_DIR=`find . -name "taosd"|grep bin|head -n1`

cut_opt="-f "

if [[ "$TAOSD_DIR" == *"$IN_TDINTERNAL"* ]]; then
  BIN_DIR=`find . -name "taosd"|grep bin|head -n1|cut -d '/' ${cut_opt}2,3`
else
  BIN_DIR=`find . -name "taosd"|grep bin|head -n1|cut -d '/' ${cut_opt}2`
fi

BUILD_DIR=$TAOS_DIR/$BIN_DIR
LIB_DIR=$BUILD_DIR/build/lib

if [[ "$OSTYPE" == "darwin"* ]]; then
  TD_OS="Darwin"
  INTERNAL_LIB_FILE=$LIB_DIR/libtaosinternal.dylib
  LIB_FILE=$LIB_DIR/libtaos.dylib
else
  OS=$(cat /etc/*-release | grep "^NAME=" | cut -d= -f2)
  len=$(echo ${#OS})
  len=$((len-2))
  TD_OS=$(echo -ne ${OS:1:${len}} | cut -d" " -f1)
  INTERNAL_LIB_FILE=$LIB_DIR/libtaosinternal.so
  LIB_FILE=$LIB_DIR/libtaos.so
fi

if [ ! -f "$INTERNAL_LIB_FILE" ]; then 
  echo $INTERNAL_LIB_FILE not exist
  exit -1
fi

if [ -f "$LIB_FILE" ]; then 
  if [ -L "$LIB_FILE" ]; then 
    echo $LIB_FILE is symbolic link
    rm -f $LIB_FILE
  else
    echo $LIB_FILE already exist and not symbolic link
    exit -1
  fi
else  
  echo $LIB_FILE not exist
fi

echo $INTERNAL_LIB_FILE linkto $LIB_FILE
ln -sf $INTERNAL_LIB_FILE $LIB_FILE
