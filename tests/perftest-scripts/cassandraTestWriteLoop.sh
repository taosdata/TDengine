#!/bin/bash

DATA_DIR=/mnt/root/testdata
NUM_LOOP=1
NUM_OF_FILES=100
OUT_FILE=cassandraWrite.out

rowsPerRequest=(1 10 50 100 500 1000 2000)

function printTo {
  if $verbose ; then
    echo $1
  fi
}

function runTest {
  for c in `seq 1 $clients`; do
    avgRPR[$c]=0
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

  for r in ${rowsPerRequest[@]}; do
    printf "$r, "
    for c in `seq 1 $clients`; do
      totalRPR=0
      for i in `seq 1 $NUM_LOOP`; do
        printTo "loop i:$i, java -jar $CAS_TEST_DIR/cassandratest/target/cassandratest-1.0-SNAPSHOT-jar-with-dependencies.jar \
            -datadir $DATA_DIR \
            -numofFiles $NUM_OF_FILES \
            -rowsperrequest $r \
            -writeclients $c \
            -conf $CAS_TEST_DIR/application.conf"
        java -jar $CAS_TEST_DIR/cassandratest/target/cassandratest-1.0-SNAPSHOT-jar-with-dependencies.jar \
            -datadir $DATA_DIR \
            -numofFiles $NUM_OF_FILES \
            -rowsperrequest $r \
            -writeclients $c \
            -conf $CAS_TEST_DIR/application.conf \
            2>&1 > $OUT_FILE
        RPR=`cat $OUT_FILE | grep "insertation speed:" | awk '{print $(NF-1)}'`
        totalRPR=`echo "scale=4; $totalRPR + $RPR" | bc`
        printTo "rows:$r, clients:$c, i:$i RPR:$RPR"
      done
      avgRPR[$c]=`echo "scale=4; $totalRPR / $NUM_LOOP" | bc`
    done
    for c in `seq 1 $clients`; do
      printf "${avgRPR[$c]}, "
    done
    printf "\n"
  done
}

################ Main ################

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
    *)
      break ;;
  esac
done

printTo "Cassandra Test begin.."

WORK_DIR=/mnt/root/TDengine
CAS_TEST_DIR=$WORK_DIR/tests/comparisonTest/cassandra

runTest

printTo "Cassandra Test done!"
