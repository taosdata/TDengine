#!/bin/bash

DATA_DIR=/mnt/root/testdata
NUM_LOOP=5

function printTo {
  if $verbose ; then
    echo $1
  fi
}

TDTESTQ5OUT=tdengineTestQ5.out

function runTest {
  totalThroughput=0
  for i in `seq 1 $NUM_LOOP`; do
    for c in `seq 1 $clients`; do
      records[$c]=0
      spentTime[$c]=0
      throughput[$c]=0
    done
    printTo "loop i:$i, $TDTEST_DIR/tdengineTest \
	      -clients $clients -sql q5.txt"
    restartTaosd
    beginMS=`date +%s%3N`
    $TDTEST_DIR/tdengineTest \
      -clients $clients -sql $TDTEST_DIR/q5.txt > $TDTESTQ5OUT
    endMS=`date +%s%3N`
    totalRecords=0
    for c in `seq 1 $clients`; do
      records[$c]=`grep Thread:$c $TDTESTQ5OUT | awk '{print $7}'`
      totalRecords=`echo "$totalRecords + ${records[$c]}"|bc`
    done
    spending=`echo "scale=4; x = ($endMS - $beginMS)/1000; if (x<1) print 0; x"|bc`
    throughput=`echo "scale=4; x= $totalRecords / $spending; if (x<1) print 0; x" | bc`
    printTo "spending: $spending sec, throughput: $throughput"
    totalThroughput=`echo "scale=4; x = $totalThroughput + $throughput; if(x<1) print 0; x"|bc`
  done
  avgThrougput=`echo "scale=4; x = $totalThroughput / $NUM_LOOP; if (x<1) print 0; x"|bc`
  echo "avg Throughput: $avgThrougput"
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
  cp /mnt/root/cfg/10billion/taos.cfg /etc/taos/taos.cfg
  WORK_DIR=/mnt/root/TDengine
fi

TAOSD_DIR=$WORK_DIR/debug/build/bin
TDTEST_DIR=$WORK_DIR/tests/comparisonTest/tdengine

runTest

printTo "Test done!"
