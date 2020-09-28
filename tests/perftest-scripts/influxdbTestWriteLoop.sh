#!/bin/bash

DATA_DIR=/mnt/root/testdata
NUM_LOOP=1
NUM_OF_FILES=100

rowsPerRequest=(1 100 1000 10000 20000 50000 100000)

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
    if [ "$r" == "1" ] || [ "$r" == "100" ] || [ "$r" == "1000" ]; then
      NUM_OF_FILES=$clients
    else
      NUM_OF_FILES=100
    fi

    for c in `seq 1 $clients`; do
      totalRPR=0
      OUTPUT_FILE=influxdbTestWrite-RPR${rowsPerRequest[$r]}-clients$c.out
      for i in `seq 1 $NUM_LOOP`; do
        printTo "loop i:$i, $INF_TEST_DIR/influxdbTest \
	      -dataDir $DATA_DIR \
	      -numOfFiles $NUM_OF_FILES \
	      -writeClients $c \
	      -rowsPerRequest $r"
        $INF_TEST_DIR/influxdbTest \
          -dataDir $DATA_DIR \
          -numOfFiles $NUM_OF_FILES \
          -writeClients $c \
          -rowsPerRequest $r 2>&1 \
          | tee $OUTPUT_FILE
        RPR=`cat $OUTPUT_FILE  | grep speed | awk '{print $(NF-1)}'`
        totalRPR=`echo "scale=4; $totalRPR + $RPR" | bc`
        printTo "rows:$r, clients:$c, i:$i RPR:$RPR"
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

INF_TEST_DIR=$WORK_DIR/tests/comparisonTest/influxdb

runTest

echo "Test done!"
