#!/bin/bash

DATA_DIR=/mnt/root/testdata
NUM_LOOP=5

function printTo {
  if $verbose ; then
    echo $1
  fi
}

CASTESTQ4OUT=cassandraTestQ4.out

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
    if $regeneratedata ; then
      printTo "java -jar $CASTEST_DIR/cassandratest-1.0-SNAPSHOT-jar-with-dependencies.jar \
        -datadir $DATA_DIR \
        -numofFiles 100 \
        -rowsperrequest 100 \
        -writeclients 4 \
        -conf $CASTEST_DIR/application.conf \
        -timetest"
      java -jar $CASTEST_DIR/cassandratest-1.0-SNAPSHOT-jar-with-dependencies.jar \
        -datadir $DATA_DIR \
        -numofFiles 100 \
        -rowsperrequest 100 \
        -writeclients 4 \
        -conf $CASTEST_DIR/application.conf \
        -timetest
    fi 

    printTo "loop i:$i, java -jar \
      $CASTEST_DIR/cassandratest/target/cassandratest-1.0-SNAPSHOT-jar-with-dependencies.jar \
      -conf $CASTEST_DIR/application.conf \
      -sql $CASTEST_DIR/q4.txt"
    java -jar \
      $CASTEST_DIR/cassandratest/target/cassandratest-1.0-SNAPSHOT-jar-with-dependencies.jar \
      -conf $CASTEST_DIR/application.conf \
      -sql $CASTEST_DIR/q4.txt \
      > $CASTESTQ4OUT
    G10=`grep "devgroup<10" $CASTESTQ4OUT| awk '{print $2}'`
    totalG10=`echo "scale=4; $totalG10 + $G10" | bc`
    G20=`grep "devgroup<20" $CASTESTQ4OUT| awk '{print $2}'`
    totalG20=`echo "scale=4; $totalG20 + $G20" | bc`
    G30=`grep "devgroup<30" $CASTESTQ4OUT| awk '{print $2}'`
    totalG30=`echo "scale=4; $totalG30 + $G30" | bc`
    G40=`grep "devgroup<40" $CASTESTQ4OUT| awk '{print $2}'`
    totalG40=`echo "scale=4; $totalG40 + $G40" | bc`
    G50=`grep "devgroup<50" $CASTESTQ4OUT| awk '{print $2}'`
    totalG50=`echo "scale=4; $totalG50 + $G50" | bc`
    G60=`grep "devgroup<60" $CASTESTQ4OUT| awk '{print $2}'`
    totalG60=`echo "scale=4; $totalG60 + $G60" | bc`
    G70=`grep "devgroup<70" $CASTESTQ4OUT| awk '{print $2}'`
    totalG70=`echo "scale=4; $totalG70 + $G70" | bc`
    G80=`grep "devgroup<80" $CASTESTQ4OUT| awk '{print $2}'`
    totalG80=`echo "scale=4; $totalG80 + $G80" | bc`
    G90=`grep "devgroup<90" $CASTESTQ4OUT| awk '{print $2}'`
    totalG90=`echo "scale=4; $totalG90 + $G90" | bc`
    G100=`grep "test group by minute;" $CASTESTQ4OUT| awk '{print $2}'`
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
  echo "Cassandra, $avgG10, $avgG20, $avgG30, $avgG40, $avgG50, $avgG60, $avgG70, $avgG80, $avgG90, $avgG100"
}

################ Main ################

master=false
develop=true
verbose=false
regeneratedata=false

for arg in "$@"
do
  case $arg in
    -v)
      verbose=true
      shift ;;

    -c)
      clients=$2
      shift 2;;

    -r)
      regeneratedata=true
      ;;

    -n)
      NUM_LOOP=$2
      shift 2;;

    *)
      ;;
  esac
done

WORK_DIR=/mnt/root/TDengine
CASTEST_DIR=$WORK_DIR/tests/comparisonTest/cassandra

runTest

printTo "Test done!"
