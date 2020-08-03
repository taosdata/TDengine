#!/bin/bash

DATA_DIR=/mnt/root/testdata
NUM_LOOP=1
NUM_OF_FILES=100

rowsPerRequest=(1 100 500 1000 2000)

function printTo {
  if $verbose ; then
    echo $1
  fi
}

function runTest {
  printf "R/R, "
  for c in `seq 1 $clients`; do
    if [ "$c" == "1" ]; then
      printf "$c client, "
    else
      printf "$c clients, "
    fi
  done
  printf "\n"

  for r in ${rowsPerRequest[@]}; do
    printf "$r, "
    for c in `seq 1 $clients`; do
      totalRPR=0
      for i in `seq 1 $NUM_LOOP`; do
	restartTaosd
        $TAOSD_DIR/taos -s "drop database db" > /dev/null 2>&1
        printTo "loop i:$i, $TDTEST_DIR/tdengineTest \
	      -dataDir $DATA_DIR \
	      -numOfFiles $NUM_OF_FILES \
	      -w -clients $c \
	      -rowsPerRequest $r"
        RPR=`$TDTEST_DIR/tdengineTest \
          -dataDir $DATA_DIR \
          -numOfFiles $NUM_OF_FILES \
          -w -clients $c \
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
clients=1

while : ; do
  case $1 in
    -v)
      verbose=true
      shift ;;

    -n)
      NUM_LOOP=$2
      shift 2;;

    master)
      master=true
      develop=false
      shift ;;

    develop)
      master=false
      develop=true
      shift ;;

    -c)
      clients=$2
      shift 2;;

    *)
      break ;;
  esac
done

if $master ; then
  printTo "Test master branch.."
  cp /mnt/root/cfg/master/taos.cfg /etc/taos/taos.cfg
  WORK_DIR=/mnt/root/TDengine.master
else
  printTo "Test develop branch.."
  cp /mnt/root/cfg/perftest/taos.cfg /etc/taos/taos.cfg
  WORK_DIR=/mnt/root/TDengine
fi

TAOSD_DIR=$WORK_DIR/debug/build/bin
TDTEST_DIR=$WORK_DIR/tests/comparisonTest/tdengine

runTest

printTo "Test done!"
