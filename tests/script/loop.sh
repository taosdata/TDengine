#!/bin/bash

##################################################
# 
# Do simulation test 
#
##################################################

set -e
#set -x

CMD_NAME=
LOOP_TIMES=5

while getopts "f:t:" arg
do
  case $arg in
    f)
      CMD_NAME=$OPTARG
      ;;
    t)
      LOOP_TIMES=$OPTARG
      ;;
    ?)
      echo "unknow argument"
      ;;
  esac
done

echo LOOP_TIMES ${LOOP_TIMES}
echo CMD_NAME ${CMD_NAME}

for ((i=0; i<$LOOP_TIMES; i++ ))
do
    echo loop $i
    echo cmd $CMD_NAME
    $CMD_NAME
    sleep 2
done
