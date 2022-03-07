#!/bin/bash 

NUM_LOOP=5

function printTo {
  if $verbose ; then
    echo $1
  fi
}

INFLUXDBTESTQ2OUT=influxdbTestQ2.out

function runTest {
  totalCount10=0
  totalCount20=0
  totalCount30=0
  totalCount40=0
  totalCount50=0
  totalCount60=0
  totalCount70=0
  totalCount80=0
  totalCount90=0
  totalCount100=0

  totalMean10=0
  totalMean20=0
  totalMean30=0
  totalMean40=0
  totalMean50=0
  totalMean60=0
  totalMean70=0
  totalMean80=0
  totalMean90=0
  totalMean100=0

  totalSum10=0
  totalSum20=0
  totalSum30=0
  totalSum40=0
  totalSum50=0
  totalSum60=0
  totalSum70=0
  totalSum80=0
  totalSum90=0
  totalSum100=0

  totalMax10=0
  totalMax20=0
  totalMax30=0
  totalMax40=0
  totalMax50=0
  totalMax60=0
  totalMax70=0
  totalMax80=0
  totalMax90=0
  totalMax100=0

  totalMin10=0
  totalMin20=0
  totalMin30=0
  totalMin40=0
  totalMin50=0
  totalMin60=0
  totalMin70=0
  totalMin80=0
  totalMin90=0
  totalMin100=0

  totalSpread10=0
  totalSpread20=0
  totalSpread30=0
  totalSpread40=0
  totalSpread50=0
  totalSpread60=0
  totalSpread70=0
  totalSpread80=0
  totalSpread90=0
  totalSpread100=0

  for i in `seq 1 $NUM_LOOP`; do
    printTo "loop i:$i, $INFLUXDBTEST_DIR/influxdbTest \
      -sql $INFLUXDBTEST_DIR/q2.txt"
    $INFLUXDBTEST_DIR/influxdbTest \
      -sql $INFLUXDBTEST_DIR/q2.txt 2>&1 \
      | tee $INFLUXDBTESTQ2OUT

    Count10=`cat $INFLUXDBTESTQ2OUT | grep count | grep "devgroup=~\/\[1-1\]" | awk '{print $5}'`
    totalCount10=`echo "scale=4; $totalCount10 + $Count10" | bc`
    Count20=`cat $INFLUXDBTESTQ2OUT | grep count | grep "devgroup=~\/\[1-2\]" | awk '{print $5}'`
    totalCount20=`echo "scale=4; $totalCount20 + $Count20" | bc`
    Count30=`cat $INFLUXDBTESTQ2OUT | grep count | grep "devgroup=~\/\[1-3\]" | awk '{print $5}'`
    totalCount30=`echo "scale=4; $totalCount30 + $Count30" | bc`
    Count40=`cat $INFLUXDBTESTQ2OUT | grep count | grep "devgroup=~\/\[1-4\]" | awk '{print $5}'`
    totalCount40=`echo "scale=4; $totalCount40 + $Count40" | bc`
    Count50=`cat $INFLUXDBTESTQ2OUT | grep count | grep "devgroup=~\/\[1-5\]" | awk '{print $5}'`
    totalCount50=`echo "scale=4; $totalCount50 + $Count50" | bc`
    Count60=`cat $INFLUXDBTESTQ2OUT | grep count | grep "devgroup=~\/\[1-6\]" | awk '{print $5}'`
    totalCount60=`echo "scale=4; $totalCount60 + $Count60" | bc`
    Count70=`cat $INFLUXDBTESTQ2OUT | grep count | grep "devgroup=~\/\[1-7\]" | awk '{print $5}'`
    totalCount70=`echo "scale=4; $totalCount70 + $Count70" | bc`
    Count80=`cat $INFLUXDBTESTQ2OUT | grep count | grep "devgroup=~\/\[1-8\]" | awk '{print $5}'`
    totalCount80=`echo "scale=4; $totalCount80 + $Count80" | bc`
    Count90=`cat $INFLUXDBTESTQ2OUT | grep count | grep "devgroup=~\/\[1-9\]" | awk '{print $5}'`
    totalCount90=`echo "scale=4; $totalCount90 + $Count90" | bc`
    Count100=`cat $INFLUXDBTESTQ2OUT | grep count | grep "devices;" | awk '{print $5}'`
    totalCount100=`echo "scale=4; $totalCount100 + $Count100" | bc`

    Mean10=`cat $INFLUXDBTESTQ2OUT | grep mean | grep "devgroup=~\/\[1-1\]" | awk '{print $5}'`
    totalMean10=`echo "scale=4; $totalMean10 + $Mean10" | bc`
    Mean20=`cat $INFLUXDBTESTQ2OUT | grep mean | grep "devgroup=~\/\[1-2\]" | awk '{print $5}'`
    totalMean20=`echo "scale=4; $totalMean20 + $Mean20" | bc`
    Mean30=`cat $INFLUXDBTESTQ2OUT | grep mean | grep "devgroup=~\/\[1-3\]" | awk '{print $5}'`
    totalMean30=`echo "scale=4; $totalMean30 + $Mean30" | bc`
    Mean40=`cat $INFLUXDBTESTQ2OUT | grep mean | grep "devgroup=~\/\[1-4\]" | awk '{print $5}'`
    totalMean40=`echo "scale=4; $totalMean40 + $Mean40" | bc`
    Mean50=`cat $INFLUXDBTESTQ2OUT | grep mean | grep "devgroup=~\/\[1-5\]" | awk '{print $5}'`
    totalMean50=`echo "scale=4; $totalMean50 + $Mean50" | bc`
    Mean60=`cat $INFLUXDBTESTQ2OUT | grep mean | grep "devgroup=~\/\[1-6\]" | awk '{print $5}'`
    totalMean60=`echo "scale=4; $totalMean60 + $Mean60" | bc`
    Mean70=`cat $INFLUXDBTESTQ2OUT | grep mean | grep "devgroup=~\/\[1-7\]" | awk '{print $5}'`
    totalMean70=`echo "scale=4; $totalMean70 + $Mean70" | bc`
    Mean80=`cat $INFLUXDBTESTQ2OUT | grep mean | grep "devgroup=~\/\[1-8\]" | awk '{print $5}'`
    totalMean80=`echo "scale=4; $totalMean80 + $Mean80" | bc`
    Mean90=`cat $INFLUXDBTESTQ2OUT | grep mean | grep "devgroup=~\/\[1-9\]" | awk '{print $5}'`
    totalMean90=`echo "scale=4; $totalMean90 + $Mean90" | bc`
    Mean100=`cat $INFLUXDBTESTQ2OUT | grep mean | grep "devices;" | awk '{print $5}'`
    totalMean100=`echo "scale=4; $totalMean100 + $Mean100" | bc`

    Sum10=`cat $INFLUXDBTESTQ2OUT | grep sum | grep "devgroup=~\/\[1-1\]" | awk '{print $5}'`
    totalSum10=`echo "scale=4; $totalSum10 + $Sum10" | bc`
    Sum20=`cat $INFLUXDBTESTQ2OUT | grep sum | grep "devgroup=~\/\[1-2\]" | awk '{print $5}'`
    totalSum20=`echo "scale=4; $totalSum20 + $Sum20" | bc`
    Sum30=`cat $INFLUXDBTESTQ2OUT | grep sum | grep "devgroup=~\/\[1-3\]" | awk '{print $5}'`
    totalSum30=`echo "scale=4; $totalSum30 + $Sum30" | bc`
    Sum40=`cat $INFLUXDBTESTQ2OUT | grep sum | grep "devgroup=~\/\[1-4\]" | awk '{print $5}'`
    totalSum40=`echo "scale=4; $totalSum40 + $Sum40" | bc`
    Sum50=`cat $INFLUXDBTESTQ2OUT | grep sum | grep "devgroup=~\/\[1-5\]" | awk '{print $5}'`
    totalSum50=`echo "scale=4; $totalSum50 + $Sum50" | bc`
    Sum60=`cat $INFLUXDBTESTQ2OUT | grep sum | grep "devgroup=~\/\[1-6\]" | awk '{print $5}'`
    totalSum60=`echo "scale=4; $totalSum60 + $Sum60" | bc`
    Sum70=`cat $INFLUXDBTESTQ2OUT | grep sum | grep "devgroup=~\/\[1-7\]" | awk '{print $5}'`
    totalSum70=`echo "scale=4; $totalSum70 + $Sum70" | bc`
    Sum80=`cat $INFLUXDBTESTQ2OUT | grep sum | grep "devgroup=~\/\[1-8\]" | awk '{print $5}'`
    totalSum80=`echo "scale=4; $totalSum80 + $Sum80" | bc`
    Sum90=`cat $INFLUXDBTESTQ2OUT | grep sum | grep "devgroup=~\/\[1-9\]" | awk '{print $5}'`
    totalSum90=`echo "scale=4; $totalSum90 + $Sum90" | bc`
    Sum100=`cat $INFLUXDBTESTQ2OUT | grep sum | grep "devices;" | awk '{print $5}'`
    totalSum100=`echo "scale=4; $totalSum100 + $Sum100" | bc`

    Max10=`cat $INFLUXDBTESTQ2OUT | grep max | grep "devgroup=~\/\[1-1\]" | awk '{print $5}'`
    totalMax10=`echo "scale=4; $totalMax10 + $Max10" | bc`
    Max20=`cat $INFLUXDBTESTQ2OUT | grep max | grep "devgroup=~\/\[1-2\]" | awk '{print $5}'`
    totalMax20=`echo "scale=4; $totalMax20 + $Max20" | bc`
    Max30=`cat $INFLUXDBTESTQ2OUT | grep max | grep "devgroup=~\/\[1-3\]" | awk '{print $5}'`
    totalMax30=`echo "scale=4; $totalMax30 + $Max30" | bc`
    Max40=`cat $INFLUXDBTESTQ2OUT | grep max | grep "devgroup=~\/\[1-4\]" | awk '{print $5}'`
    totalMax40=`echo "scale=4; $totalMax40 + $Max40" | bc`
    Max50=`cat $INFLUXDBTESTQ2OUT | grep max | grep "devgroup=~\/\[1-5\]" | awk '{print $5}'`
    totalMax50=`echo "scale=4; $totalMax50 + $Max50" | bc`
    Max60=`cat $INFLUXDBTESTQ2OUT | grep max | grep "devgroup=~\/\[1-6\]" | awk '{print $5}'`
    totalMax60=`echo "scale=4; $totalMax60 + $Max60" | bc`
    Max70=`cat $INFLUXDBTESTQ2OUT | grep max | grep "devgroup=~\/\[1-7\]" | awk '{print $5}'`
    totalMax70=`echo "scale=4; $totalMax70 + $Max70" | bc`
    Max80=`cat $INFLUXDBTESTQ2OUT | grep max | grep "devgroup=~\/\[1-8\]" | awk '{print $5}'`
    totalMax80=`echo "scale=4; $totalMax80 + $Max80" | bc`
    Max90=`cat $INFLUXDBTESTQ2OUT | grep max | grep "devgroup=~\/\[1-9\]" | awk '{print $5}'`
    totalMax90=`echo "scale=4; $totalMax90 + $Max90" | bc`
    Max100=`cat $INFLUXDBTESTQ2OUT | grep max | grep "devices;" | awk '{print $5}'`
    totalMax100=`echo "scale=4; $totalMax100 + $Max100" | bc`

    Min10=`cat $INFLUXDBTESTQ2OUT | grep min | grep "devgroup=~\/\[1-1\]" | awk '{print $5}'`
    totalMin10=`echo "scale=4; $totalMin10 + $Min10" | bc`
    Min20=`cat $INFLUXDBTESTQ2OUT | grep min | grep "devgroup=~\/\[1-2\]" | awk '{print $5}'`
    totalMin20=`echo "scale=4; $totalMin20 + $Min20" | bc`
    Min30=`cat $INFLUXDBTESTQ2OUT | grep min | grep "devgroup=~\/\[1-3\]" | awk '{print $5}'`
    totalMin30=`echo "scale=4; $totalMin30 + $Min30" | bc`
    Min40=`cat $INFLUXDBTESTQ2OUT | grep min | grep "devgroup=~\/\[1-4\]" | awk '{print $5}'`
    totalMin40=`echo "scale=4; $totalMin40 + $Min40" | bc`
    Min50=`cat $INFLUXDBTESTQ2OUT | grep min | grep "devgroup=~\/\[1-5\]" | awk '{print $5}'`
    totalMin50=`echo "scale=4; $totalMin50 + $Min50" | bc`
    Min60=`cat $INFLUXDBTESTQ2OUT | grep min | grep "devgroup=~\/\[1-6\]" | awk '{print $5}'`
    totalMin60=`echo "scale=4; $totalMin60 + $Min60" | bc`
    Min70=`cat $INFLUXDBTESTQ2OUT | grep min | grep "devgroup=~\/\[1-7\]" | awk '{print $5}'`
    totalMin70=`echo "scale=4; $totalMin70 + $Min70" | bc`
    Min80=`cat $INFLUXDBTESTQ2OUT | grep min | grep "devgroup=~\/\[1-8\]" | awk '{print $5}'`
    totalMin80=`echo "scale=4; $totalMin80 + $Min80" | bc`
    Min90=`cat $INFLUXDBTESTQ2OUT | grep min | grep "devgroup=~\/\[1-9\]" | awk '{print $5}'`
    totalMin90=`echo "scale=4; $totalMin90 + $Min90" | bc`
    Min100=`cat $INFLUXDBTESTQ2OUT | grep min | grep "devices;" | awk '{print $5}'`
    totalMin100=`echo "scale=4; $totalMin100 + $Min100" | bc`

    Spread10=`cat $INFLUXDBTESTQ2OUT | grep spread | grep "devgroup=~\/\[1-1\]" | awk '{print $5}'`
    totalSpread10=`echo "scale=4; $totalSpread10 + $Spread10" | bc`
    Spread20=`cat $INFLUXDBTESTQ2OUT | grep spread | grep "devgroup=~\/\[1-2\]" | awk '{print $5}'`
    totalSpread20=`echo "scale=4; $totalSpread20 + $Spread20" | bc`
    Spread30=`cat $INFLUXDBTESTQ2OUT | grep spread | grep "devgroup=~\/\[1-3\]" | awk '{print $5}'`
    totalSpread30=`echo "scale=4; $totalSpread30 + $Spread30" | bc`
    Spread40=`cat $INFLUXDBTESTQ2OUT | grep spread | grep "devgroup=~\/\[1-4\]" | awk '{print $5}'`
    totalSpread40=`echo "scale=4; $totalSpread40 + $Spread40" | bc`
    Spread50=`cat $INFLUXDBTESTQ2OUT | grep spread | grep "devgroup=~\/\[1-5\]" | awk '{print $5}'`
    totalSpread50=`echo "scale=4; $totalSpread50 + $Spread50" | bc`
    Spread60=`cat $INFLUXDBTESTQ2OUT | grep spread | grep "devgroup=~\/\[1-6\]" | awk '{print $5}'`
    totalSpread60=`echo "scale=4; $totalSpread60 + $Spread60" | bc`
    Spread70=`cat $INFLUXDBTESTQ2OUT | grep spread | grep "devgroup=~\/\[1-7\]" | awk '{print $5}'`
    totalSpread70=`echo "scale=4; $totalSpread70 + $Spread70" | bc`
    Spread80=`cat $INFLUXDBTESTQ2OUT | grep spread | grep "devgroup=~\/\[1-8\]" | awk '{print $5}'`
    totalSpread80=`echo "scale=4; $totalSpread80 + $Spread80" | bc`
    Spread90=`cat $INFLUXDBTESTQ2OUT | grep spread | grep "devgroup=~\/\[1-9\]" | awk '{print $5}'`
    totalSpread90=`echo "scale=4; $totalSpread90 + $Spread90" | bc`
    Spread100=`cat $INFLUXDBTESTQ2OUT | grep spread | grep "devices;" | awk '{print $5}'`
    totalSpread100=`echo "scale=4; $totalSpread100 + $Spread100" | bc`

  done
  avgCount10=`echo "scale=4; x = $totalCount10 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgCount20=`echo "scale=4; x = $totalCount20 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgCount30=`echo "scale=4; x = $totalCount30 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgCount40=`echo "scale=4; x = $totalCount40 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgCount50=`echo "scale=4; x = $totalCount50 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgCount60=`echo "scale=4; x = $totalCount60 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgCount70=`echo "scale=4; x = $totalCount70 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgCount80=`echo "scale=4; x = $totalCount80 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgCount90=`echo "scale=4; x = $totalCount90 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgCount100=`echo "scale=4; x = $totalCount100 / $NUM_LOOP; if(x<1) print 0; x" | bc`

  avgMean10=`echo "scale=4; x = $totalMean10 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMean20=`echo "scale=4; x = $totalMean20 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMean30=`echo "scale=4; x = $totalMean30 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMean40=`echo "scale=4; x = $totalMean40 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMean50=`echo "scale=4; x = $totalMean50 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMean60=`echo "scale=4; x = $totalMean60 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMean70=`echo "scale=4; x = $totalMean70 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMean80=`echo "scale=4; x = $totalMean80 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMean90=`echo "scale=4; x = $totalMean90 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMean100=`echo "scale=4; x = $totalMean100 / $NUM_LOOP; if(x<1) print 0; x" | bc`

  avgSum10=`echo "scale=4; x = $totalSum10 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgSum20=`echo "scale=4; x = $totalSum20 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgSum30=`echo "scale=4; x = $totalSum30 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgSum40=`echo "scale=4; x = $totalSum40 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgSum50=`echo "scale=4; x = $totalSum50 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgSum60=`echo "scale=4; x = $totalSum60 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgSum70=`echo "scale=4; x = $totalSum70 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgSum80=`echo "scale=4; x = $totalSum80 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgSum90=`echo "scale=4; x = $totalSum90 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgSum100=`echo "scale=4; x = $totalSum100 / $NUM_LOOP; if(x<1) print 0; x" | bc`

  avgMax10=`echo "scale=4; x = $totalMax10 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMax20=`echo "scale=4; x = $totalMax20 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMax30=`echo "scale=4; x = $totalMax30 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMax40=`echo "scale=4; x = $totalMax40 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMax50=`echo "scale=4; x = $totalMax50 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMax60=`echo "scale=4; x = $totalMax60 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMax70=`echo "scale=4; x = $totalMax70 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMax80=`echo "scale=4; x = $totalMax80 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMax90=`echo "scale=4; x = $totalMax90 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMax100=`echo "scale=4; x = $totalMax100 / $NUM_LOOP; if(x<1) print 0; x" | bc`

  avgMin10=`echo "scale=4; x = $totalMin10 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMin20=`echo "scale=4; x = $totalMin20 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMin30=`echo "scale=4; x = $totalMin30 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMin40=`echo "scale=4; x = $totalMin40 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMin50=`echo "scale=4; x = $totalMin50 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMin60=`echo "scale=4; x = $totalMin60 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMin70=`echo "scale=4; x = $totalMin70 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMin80=`echo "scale=4; x = $totalMin80 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMin90=`echo "scale=4; x = $totalMin90 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgMin100=`echo "scale=4; x = $totalMin100 / $NUM_LOOP; if(x<1) print 0; x" | bc`

  avgSpread10=`echo "scale=4; x = $totalSpread10 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgSpread20=`echo "scale=4; x = $totalSpread20 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgSpread30=`echo "scale=4; x = $totalSpread30 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgSpread40=`echo "scale=4; x = $totalSpread40 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgSpread50=`echo "scale=4; x = $totalSpread50 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgSpread60=`echo "scale=4; x = $totalSpread60 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgSpread70=`echo "scale=4; x = $totalSpread70 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgSpread80=`echo "scale=4; x = $totalSpread80 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgSpread90=`echo "scale=4; x = $totalSpread90 / $NUM_LOOP; if(x<1) print 0; x" | bc`
  avgSpread100=`echo "scale=4; x = $totalSpread100 / $NUM_LOOP; if(x<1) print 0; x" | bc`

  echo "Latency, 10%, 20%, 30%, 40%, 50%, 60%, 70%, 80%, 90%, 100%"
  echo "Count, $avgCount10, $avgCount20, $avgCount30, $avgCount40, $avgCount50, $avgCount60, $avgCount70, $avgCount80, $avgCount90, $avgCount100"
  echo "Mean, $avgMean10, $avgMean20, $avgMean30, $avgMean40, $avgMean50, $avgMean60, $avgMean70, $avgMean80, $avgMean90, $avgMean100"
  echo "Sum, $avgSum10, $avgSum20, $avgSum30, $avgSum40, $avgSum50, $avgSum60, $avgSum70, $avgSum80, $avgSum90, $avgSum100"
  echo "Max, $avgMax10, $avgMax20, $avgMax30, $avgMax40, $avgMax50, $avgMax60, $avgMax70, $avgMax80, $avgMax90, $avgMax100"
  echo "Min, $avgMin10, $avgMin20, $avgMin30, $avgMin40, $avgMin50, $avgMin60, $avgMin70, $avgMin80, $avgMin90, $avgMin100"
  echo "Spread, $avgSpread10, $avgSpread20, $avgSpread30, $avgSpread40, $avgSpread50, $avgSpread60, $avgSpread70, $avgSpread80, $avgSpread90, $avgSpread100"
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
