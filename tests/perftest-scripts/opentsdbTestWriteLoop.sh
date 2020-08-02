#!/bin/bash

DATA_DIR=/mnt/root/testdata
NUM_LOOP=1
NUM_OF_FILES=100

rowsPerRequest=(1 10 50 100 500 1000 2000)

function printTo {
  if $verbose ; then
    echo $1
  fi
}

function runTest {
  declare -A avgRPR

  for r in ${!rowsPerRequest[@]}; do
    for c in `seq 1 $clients`; do
      avgRPR[$r, $c]=0
    done
  done

  for r in ${!rowsPerRequest[@]}; do
    for c in `seq 1 $clients`; do
      totalRPR=0
      OUT_FILE=opentsdbWrite-rows${rowsPerRequest[$r]}-clients$c.out
      for i in `seq 1 $NUM_LOOP`; do
        printTo "loop i:$i java -jar \
          $TSDBTEST_DIR/opentsdbtest/target/opentsdbtest-1.0-SNAPSHOT-jar-with-dependencies.jar \
          -dataDir $DATA_DIR \
          -numOfFiles $NUM_OF_FILES \
          -writeClients $c \
          -rowsPerRequest $r"
        java -jar \
          $TSDBTEST_DIR/opentsdbtest/target/opentsdbtest-1.0-SNAPSHOT-jar-with-dependencies.jar \
          -dataDir $DATA_DIR \
          -numOfFiles $NUM_OF_FILES \
          -writeClients $c \
          -rowsPerRequest ${rowsPerRequest[$r]} \
          2>&1 | tee $OUT_FILE
        RPR=`cat $OUT_FILE | grep speed | awk '{print $(NF-1)}'`
        totalRPR=`echo "scale=4; $totalRPR + $RPR" | bc`
        printTo "r:$r rows:${rowsPerRequest[$r]}, clients:$c, i:$i RPR:$RPR"
      done
      avgRPR[$r,$c]=`echo "scale=4; $totalRPR / $NUM_LOOP" | bc`
      printTo "r:$r c:$c avgRPR:${avgRPR[$r, $c]}"
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

################ Main ################

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

    -c)
      clients=$2
      shift 2;;

    *)
      break ;;
  esac
done

WORK_DIR=/mnt/root/TDengine
TSDBTEST_DIR=$WORK_DIR/tests/comparisonTest/opentsdb

runTest

printTo "Test done!"
