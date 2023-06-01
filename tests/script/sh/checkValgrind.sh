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
definitely_lost=`cat ${LOG_DIR}/valgrind-taosd-*.log | grep "definitely lost in" | wc -l`
indirectly_lost=`cat ${LOG_DIR}/valgrind-taosd-*.log | grep "indirectly lost in " | wc -l`
possibly_lost=`cat ${LOG_DIR}/valgrind-taosd-*.log | grep "possibly lost in " | wc -l`
invalid_read=`cat ${LOG_DIR}/valgrind-taosd-*.log | grep "Invalid read of " | wc -l`
invalid_write=`cat ${LOG_DIR}/valgrind-taosd-*.log | grep "Invalid write of " | wc -l`
invalid_free=`cat ${LOG_DIR}/valgrind-taosd-*.log | grep "Invalid free() " | wc -l`

if [ $DETAIL -eq 1 ]; then
  echo error_summary: $error_summary
  echo definitely_lost: $definitely_lost
  echo indirectly_lost: $indirectly_lost
  echo possibly_lost: $possibly_lost
  echo invalid_read: $invalid_read
  echo invalid_write: $invalid_write
  echo invalid_free: $invalid_free
fi

let "errors=$error_summary+$definitely_lost+$indirectly_lost+$possibly_lost+$invalid_read+$invalid_write+$invalid_free"
echo $errors
