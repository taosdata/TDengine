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
SLEEP_TIME=0

while getopts "hf:t:s:" arg
do
  case $arg in
    f)
      CMD_NAME=$OPTARG
      ;;
    t)
      LOOP_TIMES=$OPTARG
      ;;
    s)
      SLEEP_TIME=$OPTARG
      ;;
    h)
      echo "Usage: $(basename $0) -f [cmd name] "
      echo "                  -t [loop times] "
      echo "                  -s [sleep time] "
      exit 0
      ;;
    ?)
      echo "unknow argument"
      ;;
  esac
done

echo LOOP_TIMES ${LOOP_TIMES}
echo CMD_NAME ${CMD_NAME}
echo SLEEP_TIME ${SLEEP_TIME}

GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

for ((i=0; i<$LOOP_TIMES; i++ ))
do
    echo -e $GREEN loop $i $NC
    echo -e $GREEN cmd $CMD_NAME $NC
    $CMD_NAME
    sleep ${SLEEP_TIME}
done
