#!/bin/bash

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

TDENGINE_DIR=/root/TDinternal/community


#echo "TDENGINE_DIR = $TDENGINE_DIR"
today=`date +"%Y%m%d"`
TDENGINE_ALLCI_REPORT=$TDENGINE_DIR/tests/all-ci-report-$today.log


function runCasesOneByOne () {
    while read -r line; do
        if [[ "$line" != "#"* ]]; then
            cmd=`echo $line | cut -d',' -f 5`
            if [[ "$2" == "sim" ]] && [[ $line == *"script"* ]]; then
                case=`echo $cmd | cut -d' ' -f 3`
                start_time=`date +%s`
                date +%F\ %T | tee -a  $TDENGINE_ALLCI_REPORT  && timeout 20m $cmd > /dev/null 2>&1 && \
                echo -e "${GREEN}$case success${NC}" | tee -a  $TDENGINE_ALLCI_REPORT \
                || echo -e "${RED}$case failed${NC}" | tee -a  $TDENGINE_ALLCI_REPORT
                end_time=`date +%s`
                echo execution time of $case was `expr $end_time - $start_time`s. | tee -a $TDENGINE_ALLCI_REPORT
                
            elif [[ "$line" == *"$2"* ]]; then
                if [[ "$cmd" == *"pytest.sh"* ]]; then
                    cmd=`echo $cmd | cut -d' ' -f 2-20`
                fi
                case=`echo $cmd | cut -d' ' -f 4-20`
                start_time=`date +%s`
                date +%F\ %T | tee -a $TDENGINE_ALLCI_REPORT && timeout 20m $cmd > /dev/null 2>&1 && \
                echo -e "${GREEN}$case success${NC}" | tee -a $TDENGINE_ALLCI_REPORT || \
                echo -e "${RED}$case failed${NC}" | tee -a $TDENGINE_ALLCI_REPORT
                end_time=`date +%s`
                echo execution time of $case was `expr $end_time - $start_time`s. | tee -a $TDENGINE_ALLCI_REPORT
            fi
        fi
    done < $1
}

function runUnitTest() {
    echo "=== Run unit test case ==="
    echo " $TDENGINE_DIR/debug"
    cd $TDENGINE_DIR/debug
    ctest -j12
    echo "3.0 unit test done"
}

function runSimCases() {
    echo "=== Run sim cases ==="

    cd $TDENGINE_DIR/tests/script
    runCasesOneByOne $TDENGINE_DIR/tests/parallel_test/cases-test.task sim

    totalSuccess=`grep 'sim success' $TDENGINE_ALLCI_REPORT | wc -l`
    if [ "$totalSuccess" -gt "0" ]; then
        echo "### Total $totalSuccess SIM test case(s) succeed! ###" | tee -a $TDENGINE_ALLCI_REPORT
    fi

    totalFailed=`grep 'sim failed\|fault' $TDENGINE_ALLCI_REPORT | wc -l`
    if [ "$totalFailed" -ne "0" ]; then
        echo "### Total $totalFailed SIM test case(s) failed! ###" | tee -a $TDENGINE_ALLCI_REPORT
    fi
}

function runPythonCases() {
    echo "=== Run python cases ==="

    cd $TDENGINE_DIR/tests/parallel_test
    sed -i '/compatibility.py/d' cases-test.task

    # army
    cd $TDENGINE_DIR/tests/army
    runCasesOneByOne ../parallel_test/cases-test.task army

    # system-test
    cd $TDENGINE_DIR/tests/system-test
    runCasesOneByOne ../parallel_test/cases-test.task system-test

    # develop-test
    cd $TDENGINE_DIR/tests/develop-test
    runCasesOneByOne ../parallel_test/cases-test.task develop-test

    totalSuccess=`grep 'py success' $TDENGINE_ALLCI_REPORT | wc -l`
    if [ "$totalSuccess" -gt "0" ]; then
        echo "### Total $totalSuccess python test case(s) succeed! ###" | tee -a $TDENGINE_ALLCI_REPORT
    fi

    totalFailed=`grep 'py failed\|fault' $TDENGINE_ALLCI_REPORT | wc -l`
    if [ "$totalFailed" -ne "0" ]; then
        echo "### Total $totalFailed python test case(s) failed! ###" | tee -a $TDENGINE_ALLCI_REPORT
    fi
}


function runTest() {
    echo "run Test"

    cd $TDENGINE_DIR
    [ -d sim ] && rm -rf sim
    [ -f $TDENGINE_ALLCI_REPORT ] && rm $TDENGINE_ALLCI_REPORT

    runUnitTest
    runSimCases
    runPythonCases

    stopTaosd
    cd $TDENGINE_DIR/tests/script
    find . -name '*.sql' | xargs rm -f

    cd $TDENGINE_DIR/tests/pytest
    find . -name '*.sql' | xargs rm -f
}

function stopTaosd {
    echo "Stop taosd start"
        systemctl stop taosd
      PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
    while [ -n "$PID" ]
    do
    pkill -TERM -x taosd
    sleep 1
      PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
    done
    echo "Stop tasod end"
}

function stopTaosadapter {
    echo "Stop taosadapter"
    systemctl stop taosadapter.service
    PID=`ps -ef|grep -w taosadapter | grep -v grep | awk '{print $2}'`
    while [ -n "$PID" ]
    do
        pkill -TERM -x taosadapter
        sleep 1
        PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
    done
    echo "Stop tasoadapter end"

}

WORK_DIR=/root/

date >> $WORK_DIR/date.log
echo "Run ALL CI Test Cases" | tee -a $WORK_DIR/date.log

stopTaosd

runTest

date >> $WORK_DIR/date.log
echo "End of CI Test Cases" | tee -a $WORK_DIR/date.log