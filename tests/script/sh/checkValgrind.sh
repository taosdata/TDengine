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

#errors=`grep "ERROR SUMMARY:" ${LOG_DIR}/valgrind-taosd-*.log | cut -d ' ' -f 2,3,4,5 | tr -d "\n"`

error_summary=`cat ${LOG_DIR}/valgrind-taosd-*.log | grep "ERROR SUMMARY:" | awk '{print $4}' | awk '{sum+=$1}END{print sum}'`
still_reachable=`cat ${LOG_DIR}/valgrind-taosd-*.log | grep "still reachable in" | wc -l`
definitely_lost=`cat ${LOG_DIR}/valgrind-taosd-*.log | grep "definitely lost in" | wc -l`

let "errors=$still_reachable+$error_summary+$definitely_lost"
echo $errors
