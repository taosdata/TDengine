#!/bin/bash

DATA_DIR=/mnt/root/testdata
NUM_LOOP=5
NUM_OF_FILES=100

rowsPerRequest=(1 100 500 1000 2000)

function printTo {
  if $verbose ; then
    echo $1
  fi
}

function runTest {
  declare -A avgRPR

  for r in ${!rowsPerRequest[@]}; do
    for c in `seq 1 $clients`; do
      avgRPR[$r,$c]=0
    done
  done

  for r in ${!rowsPerRequest[@]}; do
    for c in `seq 1 $clients`; do
      totalRPR=0
      OUTPUT_FILE=tdengineTestWrite-RPR${rowsPerRequest[$r]}-clients$c.out

      for i in `seq 1 $NUM_LOOP`; do
	restartTaosd
        $TAOSD_DIR/taos -s "drop database db" > /dev/null 2>&1
        printTo "loop i:$i, $TDTEST_DIR/tdengineTest \
	      -dataDir $DATA_DIR \
	      -numOfFiles $NUM_OF_FILES \
	      -w -clients $c \
	      -rowsPerRequest ${rowsPerRequest[$r]}"
        $TDTEST_DIR/tdengineTest \
          -dataDir $DATA_DIR \
          -numOfFiles $NUM_OF_FILES \
          -w -clients $c \
          -rowsPerRequest ${rowsPerRequest[$r]} \
	  | tee $OUTPUT_FILE
        RPR=`cat $OUTPUT_FILE  | grep speed | awk '{print $(NF-1)}'`
        totalRPR=`echo "scale=4; $totalRPR + $RPR" | bc`
        printTo "rows:${rowsPerRequest[$r]}, clients:$c, i:$i RPR:$RPR"
      done
      avgRPR[$r,$c]=`echo "scale=4; $totalRPR / $NUM_LOOP" | bc`
      printTo "r:${rowsPerRequest[$r]} c:$c avgRPR:${avgRPR[$r,$c]}"
    done
  done

  printf "R/R, "
  for c in `seq 1 $clients`; do
    if [ "$c" == "1" ]; then
      printf "$c client, "
    else
      printf "$c clients, "
    fi
  done
  printf "\n"

  for r in ${!rowsPerRequest[@]}; do
    printf "${rowsPerRequest[$r]}, "
    for c in `seq 1 $clients`; do
      printf "${avgRPR[$r,$c]}, "
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

    -n)
      NUM_LOOP=$2
      shift 2;;

    *)
      break ;;
  esac
done

if $master ; then
  echo "Test master branch.."
  cp /mnt/root/cfg/master/taos.cfg /etc/taos/taos.cfg
  WORK_DIR=/mnt/root/TDengine.master
else
  echo "Test develop branch.."
  cp /mnt/root/cfg/develop/taos.cfg /etc/taos/taos.cfg
  WORK_DIR=/mnt/root/TDengine
fi

TAOSD_DIR=$WORK_DIR/debug/build/bin
TDTEST_DIR=$WORK_DIR/tests/comparisonTest/tdengine

runTest

echo "Test done!"
