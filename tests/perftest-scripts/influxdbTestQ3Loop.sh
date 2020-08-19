#!/bin/bash

NUM_LOOP=5

function printTo {
  if $verbose ; then
    echo $1
  fi
}

INFLUXDBTESTQ3OUT=opentsdbTestQ3.out

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
    printTo "loop i:$i, $INFLUXDBTEST_DIR/influxdbTest \
      -sql $INFLUXDBTEST_DIR/q3.txt"
    $INFLUXDBTEST_DIR/influxdbTest \
      -sql $INFLUXDBTEST_DIR/q3.txt 2>&1 \
      | tee $INFLUXDBTESTQ3OUT
    G10=`grep -w "devgroup=~\/\[1-1\]" $INFLUXDBTESTQ3OUT| awk '{print $5}'`
    totalG10=`echo "scale=4; $totalG10 + $G10" | bc`
    G20=`grep -w "devgroup=~\/\[1-2\]" $INFLUXDBTESTQ3OUT| awk '{print $5}'`
    totalG20=`echo "scale=4; $totalG20 + $G20" | bc`
    G30=`grep -w "devgroup=~\/\[1-3\]" $INFLUXDBTESTQ3OUT| awk '{print $5}'`
    totalG30=`echo "scale=4; $totalG30 + $G30" | bc`
    G40=`grep -w "devgroup=~\/\[1-4\]" $INFLUXDBTESTQ3OUT| awk '{print $5}'`
    totalG40=`echo "scale=4; $totalG40 + $G40" | bc`
    G50=`grep -w "devgroup=~\/\[1-5\]" $INFLUXDBTESTQ3OUT| awk '{print $5}'`
    totalG50=`echo "scale=4; $totalG50 + $G50" | bc`
    G60=`grep -w "devgroup=~\/\[1-6\]" $INFLUXDBTESTQ3OUT| awk '{print $5}'`
    totalG60=`echo "scale=4; $totalG60 + $G60" | bc`
    G70=`grep -w "devgroup=~\/\[1-7\]" $INFLUXDBTESTQ3OUT| awk '{print $5}'`
    totalG70=`echo "scale=4; $totalG70 + $G70" | bc`
    G80=`grep -w "devgroup=~\/\[1-8\]" $INFLUXDBTESTQ3OUT| awk '{print $5}'`
    totalG80=`echo "scale=4; $totalG80 + $G80" | bc`
    G90=`grep -w "devgroup=~\/\[1-9\]" $INFLUXDBTESTQ3OUT| awk '{print $5}'`
    totalG90=`echo "scale=4; $totalG90 + $G90" | bc`
    G100=`grep -w "devices group by devgroup;" $INFLUXDBTESTQ3OUT| awk '{print $5}'`
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
  echo "InfluxDB, $avgG10, $avgG20, $avgG30, $avgG40, $avgG50, $avgG60, $avgG70, $avgG80, $avgG90, $avgG100"
}

################ Main ################

verbose=false

for arg in "$@"
do
  case $arg in
    -v)
      verbose=true
      shift ;;

    -c)
      clients=$2
      shift 2;;

    -n)
      NUM_LOOP=$2
      shift 2;;

    *)
      ;;
  esac
done

WORK_DIR=/mnt/root/TDengine
INFLUXDBTEST_DIR=$WORK_DIR/tests/comparisonTest/influxdb

runTest

printTo "Test done!"
