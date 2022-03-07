#!/bin/bash

NUM_LOOP=5

function printTo {
  if $verbose ; then
    echo $1
  fi
}

TSDBTESTQ3OUT=opentsdbTestQ3.out

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
    printTo "loop i:$i, java -jar \
      $TSDBTEST_DIR/opentsdbtest/target/opentsdbtest-1.0-SNAPSHOT-jar-with-dependencies.jar \
      -sql q3"
    java -jar \
      $TSDBTEST_DIR/opentsdbtest/target/opentsdbtest-1.0-SNAPSHOT-jar-with-dependencies.jar \
      -sql q3 2>&1 \
      | tee $TSDBTESTQ3OUT
    G10=`grep -w "devgroup < 10" $TSDBTESTQ3OUT| awk '{print $2}'`
    totalG10=`echo "scale=4; $totalG10 + $G10" | bc`
    G20=`grep -w "devgroup < 20" $TSDBTESTQ3OUT| awk '{print $2}'`
    totalG20=`echo "scale=4; $totalG20 + $G20" | bc`
    G30=`grep -w "devgroup < 30" $TSDBTESTQ3OUT| awk '{print $2}'`
    totalG30=`echo "scale=4; $totalG30 + $G30" | bc`
    G40=`grep -w "devgroup < 40" $TSDBTESTQ3OUT| awk '{print $2}'`
    totalG40=`echo "scale=4; $totalG40 + $G40" | bc`
    G50=`grep -w "devgroup < 50" $TSDBTESTQ3OUT| awk '{print $2}'`
    totalG50=`echo "scale=4; $totalG50 + $G50" | bc`
    G60=`grep -w "devgroup < 60" $TSDBTESTQ3OUT| awk '{print $2}'`
    totalG60=`echo "scale=4; $totalG60 + $G60" | bc`
    G70=`grep -w "devgroup < 70" $TSDBTESTQ3OUT| awk '{print $2}'`
    totalG70=`echo "scale=4; $totalG70 + $G70" | bc`
    G80=`grep -w "devgroup < 80" $TSDBTESTQ3OUT| awk '{print $2}'`
    totalG80=`echo "scale=4; $totalG80 + $G80" | bc`
    G90=`grep -w "devgroup < 90" $TSDBTESTQ3OUT| awk '{print $2}'`
    totalG90=`echo "scale=4; $totalG90 + $G90" | bc`
    G100=`grep -w "devgroup < 100" $TSDBTESTQ3OUT| awk '{print $2}'`
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
  echo "OpenTSDB, $avgG10, $avgG20, $avgG30, $avgG40, $avgG50, $avgG60, $avgG70, $avgG80, $avgG90, $avgG100"
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
TSDBTEST_DIR=$WORK_DIR/tests/comparisonTest/opentsdb

runTest

printTo "Test done!"
