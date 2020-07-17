#!/bin/bash

DATA_DIR=/mnt/root/testdata
NUM_LOOP=5
NUM_OF_FILES=100

rowsPerRequest=(1 100 500 1000 2000)
numOfClients=(1 2 3 4 5 6 7)

function printTo {
  if $verbose ; then
    echo $1
  fi
}

function runTest {
  printf "R/R, "
  for c in ${numOfClients[@]}; do
    if [ "$c" == "1" ]; then
      printf "$c client, "
    else
      printf "$c clients, "
    fi
  done
  printf "\n"

  for r in ${rowsPerRequest[@]}; do
    printf "$r, "
    for c in ${numOfClients[@]}; do
      totalRPR=0
      for i in `seq 1 $NUM_LOOP`; do
	restartTaosd
        $TAOSD_DIR/taos -s "drop database db" > /dev/null 2>&1
        printTo "loop i:$i, $TDTEST_DIR/tdengineTest \
	      -dataDir $DATA_DIR \
	      -numOfFiles $NUM_OF_FILES \
	      -writeClients $c \
	      -rowsPerRequest $r"
        RPR=`$TDTEST_DIR/tdengineTest \
          -dataDir $DATA_DIR \
          -numOfFiles 1 \
          -writeClients $c \
          -rowsPerRequest $r \
          | grep speed | awk '{print $(NF-1)}'`
        totalRPR=`echo "scale=4; $totalRPR + $RPR" | bc`
        printTo "rows:$r, clients:$c, i:$i RPR:$RPR"
      done
      avgRPR=`echo "scale=4; $totalRPR / $NUM_LOOP" | bc`
      printf "$avgRPR, "
    done
    printf "\n"
  done
}

function restartTaosd {
  printTo "Stop taosd"
  systemctl stop taosd
  PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
  while [ -n "$PID" ]
  do
    pkill -TERM -x taosd
    sleep 1
    PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
  done

  printTo "Start taosd"
  $TAOSD_DIR/taosd > /dev/null 2>&1 &
  sleep 10
}

################ Main ################

master=false
develop=true
verbose=false

for arg in "$@"
do
  case $arg in
    -v)
      verbose=true
      ;;

    master)
      master=true
      develop=false
      ;;

    develop)
      master=false
      develop=true
      ;;
    *)
      ;;
  esac
done

if $master ; then
  echo "Test master branch.."
  cp /mnt/root/cfg/master/taos.cfg /etc/taos/taos.cfg
  WORK_DIR=/mnt/root/TDengine.master
else
  echo "Test develop branch.."
  cp /mnt/root/cfg/10billion/taos.cfg /etc/taos/taos.cfg
  WORK_DIR=/mnt/root/TDengine
fi

TAOSD_DIR=$WORK_DIR/debug/build/bin
TDTEST_DIR=$WORK_DIR/tests/comparisonTest/tdengine

runTest

echo "Test done!"
