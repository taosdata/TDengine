#!/bin/sh

# if [ $# != 2 || $# != 3 ]; then 
  # echo "argument list need input : "
  # echo "  -s start/stop"
  # exit 1
# fi

EXEC_OPTON=
while getopts "n:s:u:x:ct" arg 
do
  case $arg in
    n)
      NODE_NAME=$OPTARG
      ;;
    s)
      EXEC_OPTON=$OPTARG
      ;;
    c)
      CLEAR_OPTION="clear"
      ;;
    t)
      SHELL_OPTION="true"
      ;;
    u)
      USERS=$OPTARG
      ;;
    x)
      SIGNAL=$OPTARG
      ;;
    ?)
      echo "unkown argument"
      ;;
  esac
done


SCRIPT_DIR=`dirname $0`
cd $SCRIPT_DIR/../
SCRIPT_DIR=`pwd`

cd ../../
TAOS_DIR=`pwd`

BUILD_DIR=$TAOS_DIR/../debug/build
SIM_DIR=$TAOS_DIR/sim
NODE_DIR=$SIM_DIR/arbitrator
EXE_DIR=$BUILD_DIR/bin
LOG_DIR=$NODE_DIR/log

echo "------------ $EXEC_OPTON tarbitrator"

if [ "$EXEC_OPTON" = "start" ]; then 
  echo "------------ log path: $LOG_DIR"
  nohup $EXE_DIR/tarbitrator -p 8000 -d 135 -g $LOG_DIR > /dev/null 2>&1 & 
else
  #relative path
  PID=`ps -ef|grep tarbitrator | grep -v grep | awk '{print $2}'`
  if [ -n "$PID" ]; then   
  	sudo kill -9 $PID
	  sudo pkill -9 tarbitrator
  fi 
fi

