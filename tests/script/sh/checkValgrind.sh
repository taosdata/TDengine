#!/bin/bash

set +e
#set -x

NODE_NAME=
DETAIL=0

while getopts "n:d" arg
do
  case $arg in
    n)
      NODE_NAME=$OPTARG
      ;;
    d)
      DETAIL=1
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

error_summary=`cat ${LOG_DIR}/valgrind-taosd-*.log | grep "ERROR SUMMARY:" | awk '{print $4}' | awk '{sum+=$1}END{print sum}'`
still_reachable=`cat ${LOG_DIR}/valgrind-taosd-*.log | grep "still reachable in" | wc -l`
definitely_lost=`cat ${LOG_DIR}/valgrind-taosd-*.log | grep "definitely lost in" | wc -l`
indirectly_lost=`cat ${LOG_DIR}/valgrind-taosd-*.log | grep "indirectly lost in " | wc -l`
possibly_lost=`cat ${LOG_DIR}/valgrind-taosd-*.log | grep "possibly lost in " | wc -l`

if [ $DETAIL -eq 1 ]; then
  echo error_summary: $error_summary
  echo still_reachable: $still_reachable
  echo definitely_lost: $definitely_lost
  echo indirectly_lost: $indirectly_lost
  echo possibly_lost: $possibly_lost
fi

let "errors=$still_reachable+$error_summary+$definitely_lost+$indirectly_lost+$possibly_lost"
echo $errors
