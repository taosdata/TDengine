#!/bin/bash

NUM_LOOP=5

function printTo {
  if $verbose ; then
    echo $1
  fi
}

INFLUXDBTESTQ1OUT=influxdbTestQ1.out

function runTest {
  totalG0=0
  totalG10=0
  totalG20=0
  totalG30=0
  totalG40=0
  totalG50=0
  totalG60=0
  totalG70=0
  totalG80=0
  totalG90=0
  for i in `seq 1 $NUM_LOOP`; do
    printTo "loop i:$i, $INFLUXDBTEST_DIR/infludbTest \
      -sql $INFLUXDBTEST_DIR/q1.txt"
    $INFLUXDBTEST_DIR/influxdbTest \
      -sql $INFLUXDBTEST_DIR/q1.txt 2>&1 \
      | tee $INFLUXDBTESTQ1OUT
    G0=`grep "devgroup='0'" $INFLUXDBTESTQ1OUT| awk '{print $5}'`
    totalG0=`echo "scale=4; $totalG0 + $G0" | bc`
    G10=`grep "devgroup='10'" $INFLUXDBTESTQ1OUT| awk '{print $5}'`
    totalG10=`echo "scale=4; $totalG10 + $G10" | bc`
    G20=`grep "devgroup='20'" $INFLUXDBTESTQ1OUT| awk '{print $5}'`
    totalG20=`echo "scale=4; $totalG20 + $G20" | bc`
    G30=`grep "devgroup='30'" $INFLUXDBTESTQ1OUT| awk '{print $5}'`
    totalG30=`echo "scale=4; $totalG30 + $G30" | bc`
    G40=`grep "devgroup='40'" $INFLUXDBTESTQ1OUT| awk '{print $5}'`
    totalG40=`echo "scale=4; $totalG40 + $G40" | bc`
    G50=`grep "devgroup='50'" $INFLUXDBTESTQ1OUT| awk '{print $5}'`
    totalG50=`echo "scale=4; $totalG50 + $G50" | bc`
    G60=`grep "devgroup='60'" $INFLUXDBTESTQ1OUT| awk '{print $5}'`
    totalG60=`echo "scale=4; $totalG60 + $G60" | bc`
    G70=`grep "devgroup='70'" $INFLUXDBTESTQ1OUT| awk '{print $5}'`
    totalG70=`echo "scale=4; $totalG70 + $G70" | bc`
    G80=`grep "devgroup='80'" $INFLUXDBTESTQ1OUT| awk '{print $5}'`
    totalG80=`echo "scale=4; $totalG80 + $G80" | bc`
    G90=`grep "devgroup='90'" $INFLUXDBTESTQ1OUT| awk '{print $5}'`
    totalG90=`echo "scale=4; $totalG90 + $G90" | bc`
  done
  avgG0=`echo "scale=4; x = $totalG0 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgG10=`echo "scale=4; x = $totalG10 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgG20=`echo "scale=4; x = $totalG20 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgG30=`echo "scale=4; x = $totalG30 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgG40=`echo "scale=4; x = $totalG40 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgG50=`echo "scale=4; x = $totalG50 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgG60=`echo "scale=4; x = $totalG60 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgG70=`echo "scale=4; x = $totalG70 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgG80=`echo "scale=4; x = $totalG80 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgG90=`echo "scale=4; x = $totalG90 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  echo "Latency, G-0, G-10, G-20, G-30, G-40, G-50, G-60, G-70, G-80, G-90"
  echo "InfluxDB, $avgG0, $avgG10, $avgG20, $avgG30, $avgG40, $avgG50, $avgG60, $avgG70, $avgG80, $avgG90"
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

WORK_DIR=/mnt/root/TDengine
INFLUXDBTEST_DIR=$WORK_DIR/tests/comparisonTest/influxdb

runTest

printTo "Test done!"
