#!/bin/bash

##################################################
#
# Do tmq test
#
##################################################

set +e

# set default value for parameters
EXEC_OPTON=start
DB_NAME=db 
POLL_DELAY=5
VALGRIND=0
SIGNAL=SIGINT

while getopts "d:s:v:y:x:" arg
do
  case $arg in
    d)
      DB_NAME=$OPTARG
      ;;
    s)
      EXEC_OPTON=$OPTARG
      ;;
    v)
      VALGRIND=1
      ;;
    y)
      POLL_DELAY=$OPTARG
      ;;
    x)
      SIGNAL=$OPTARG
      ;;
    ?)
      echo "unkown argument"
      ;;
  esac
done

SCRIPT_DIR=`pwd`

IN_TDINTERNAL="community"
if [[ "$SCRIPT_DIR" == *"$IN_TDINTERNAL"* ]]; then
  cd ../../..
else
  cd ../../
fi

TOP_DIR=`pwd`

if [[ "$SCRIPT_DIR" == *"$IN_TDINTERNAL"* ]]; then
  BIN_DIR=`find . -name "tmq_sim"|grep bin|head -n1|cut -d '/' -f 2,3`
else
  BIN_DIR=`find . -name "tmq_sim"|grep bin|head -n1|cut -d '/' -f 2`
fi

declare -x BUILD_DIR=$TOP_DIR/$BIN_DIR

declare -x SIM_DIR=$TOP_DIR/sim

PROGRAM=$BUILD_DIR/build/bin/tmq_sim

PRG_DIR=$SIM_DIR/tsim
CFG_DIR=$PRG_DIR/cfg
LOG_DIR=$PRG_DIR/log

echo "------------------------------------------------------------------------"
echo "BUILD_DIR: $BUILD_DIR"
echo "SIM_DIR  : $SIM_DIR"
echo "CFG_DIR  : $CFG_DIR"


echo "PROGRAM: $PROGRAM
echo "CFG_DIR: $CFG_DIR 
echo "POLL_DELAY: $POLL_DELAY 
echo "DB_NAME: $DB_NAME

echo "------------------------------------------------------------------------"
if [ "$EXEC_OPTON" = "start" ]; then 
  if [ $VALGRIND -eq 1 ]; then
    echo nohup valgrind --tool=memcheck --leak-check=full --show-reachable=no --track-origins=yes --show-leak-kinds=all -v --workaround-gcc296-bugs=yes --log-file=${LOG_DIR}/valgrind-tmq_sim.log $PROGRAM -c $CFG_DIR -d $DB_NAME -y $POLL_DELAY > /dev/null 2>&1 &
    nohup valgrind --tool=memcheck --leak-check=full --show-reachable=no --track-origins=yes --show-leak-kinds=all -v --workaround-gcc296-bugs=yes --log-file=${LOG_DIR}/valgrind-tmq_sim.log $PROGRAM -c $CFG_DIR -d $DB_NAME -y $POLL_DELAY > /dev/null 2>&1 &
  else
    echo  "nohup $PROGRAM -c $CFG_DIR -d $DB_NAME -y $POLL_DELAY > /dev/null 2>&1 &"
    nohup $PROGRAM -c $CFG_DIR -y $POLL_DELAY -d $DB_NAME > /dev/null 2>&1 &
  fi
else
  PID=`ps -ef|grep tmq_sim | grep -v grep | awk '{print $2}'`
  while [ -n "$PID" ]
  do
    if [ "$SIGNAL" = "SIGKILL" ]; then
      echo try to kill by signal SIGKILL
      kill -9 $PID
    else
      echo try to kill by signal SIGINT
      kill -SIGINT $PID
    fi
    sleep 1
    PID=`ps -ef|grep tmq_sim | grep -v grep | awk '{print $2}'`
  done 
fi
