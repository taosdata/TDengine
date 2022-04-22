#!/bin/bash

set +e
#set -x

NODE_NAME=

while getopts "n:" arg
do
  case $arg in
    n)
      NODE_NAME=$OPTARG
      ;;
    ?)
      echo "unkown argument"
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
LOG_DIR=$TAOS_DIR/sim/$NODE_NAME/log
#CFG_DIR=$TAOS_DIR/sim/$NODE_NAME/cfg

#echo ---- $LOG_DIR

errors=`grep "ERROR SUMMARY:" ${LOG_DIR}/valgrind-taosd-*.log | cut -d ' ' -f 2,3,4,5 | tr -d "\n"`
echo $errors
