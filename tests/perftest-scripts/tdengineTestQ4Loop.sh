#!/bin/bash

DATA_DIR=/mnt/root/testdata
NUM_LOOP=5

function printTo {
  if $verbose ; then
    echo $1
  fi
}

TDTESTQ4OUT=tdengineTestQ4.out

function runTest {
  totalG10=0
  totalG20=0
  totalG30=0
  totalG40=0
  totalG50=0
  totalG60=0
  totalG70=0
  totalG80=0
  totalG90=0
  totalG100=0
  for i in `seq 1 $NUM_LOOP`; do
    printTo "loop i:$i, $TDTEST_DIR/tdengineTest \
	      -sql q4.txt"
    restartTaosd
    $TDTEST_DIR/tdengineTest \
      -sql $TDTEST_DIR/q4.txt > $TDTESTQ4OUT
    G10=`grep "devgroup<10" $TDTESTQ4OUT| awk '{print $3}'`
    totalG10=`echo "scale=4; $totalG10 + $G10" | bc`
    G20=`grep "devgroup<20" $TDTESTQ4OUT| awk '{print $3}'`
    totalG20=`echo "scale=4; $totalG20 + $G20" | bc`
    G30=`grep "devgroup<30" $TDTESTQ4OUT| awk '{print $3}'`
    totalG30=`echo "scale=4; $totalG30 + $G30" | bc`
    G40=`grep "devgroup<40" $TDTESTQ4OUT| awk '{print $3}'`
    totalG40=`echo "scale=4; $totalG40 + $G40" | bc`
    G50=`grep "devgroup<50" $TDTESTQ4OUT| awk '{print $3}'`
    totalG50=`echo "scale=4; $totalG50 + $G50" | bc`
    G60=`grep "devgroup<60" $TDTESTQ4OUT| awk '{print $3}'`
    totalG60=`echo "scale=4; $totalG60 + $G60" | bc`
    G70=`grep "devgroup<70" $TDTESTQ4OUT| awk '{print $3}'`
    totalG70=`echo "scale=4; $totalG70 + $G70" | bc`
    G80=`grep "devgroup<80" $TDTESTQ4OUT| awk '{print $3}'`
    totalG80=`echo "scale=4; $totalG80 + $G80" | bc`
    G90=`grep "devgroup<90" $TDTESTQ4OUT| awk '{print $3}'`
    totalG90=`echo "scale=4; $totalG90 + $G90" | bc`
    G100=`grep "db.devices interval" $TDTESTQ4OUT| awk '{print $3}'`
    totalG100=`echo "scale=4; $totalG100 + $G100" | bc`
  done
  avgG10=`echo "scale=4; x = $totalG10 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgG20=`echo "scale=4; x = $totalG20 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgG30=`echo "scale=4; x = $totalG30 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgG40=`echo "scale=4; x = $totalG40 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgG50=`echo "scale=4; x = $totalG50 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgG60=`echo "scale=4; x = $totalG60 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgG70=`echo "scale=4; x = $totalG70 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgG80=`echo "scale=4; x = $totalG80 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgG90=`echo "scale=4; x = $totalG90 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgG100=`echo "scale=4; x = $totalG100 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  echo "Latency, 10%, 20%, 30%, 40%, 50%, 60%, 70%, 80%, 90%, 100%"
  echo "TDengine, $avgG10, $avgG20, $avgG30, $avgG40, $avgG50, $avgG60, $avgG70, $avgG80, $avgG90, $avgG100"
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
