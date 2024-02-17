#!/bin/bash

branch=
if [ x$1 != x ];then
        branch=$1
        echo "Testing branch: $branch"
else
        echo "Please enter branch name as a parameter"
        exit 1
fi

# work main path
TDENGINE_DIR=/root/TDinternal/community
if [ x$2 != x ];then
  TDENGINE_DIR=$2
fi

echo "TDENGINE_DIR=$TDENGINE_DIR"
today=`date +"%Y%m%d"`
JDBC_DIR=/root/taos-connector-jdbc
TDENGINE_COVERAGE_REPORT=$TDENGINE_DIR/tests/coverage-report-$today.log

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

function buildTDengine() {
    echo "check if TDengine need build"
    
    # pull parent code
    cd "$TDENGINE_DIR/../"
    echo "git pull parent code..."
    git remote prune origin > /dev/null
    git remote update > /dev/null

    # pull tdengine code
    cd $TDENGINE_DIR
    echo "git pull tdengine code..."
    git remote prune origin > /dev/null
    git remote update > /dev/null
    REMOTE_COMMIT=`git rev-parse --short remotes/origin/$branch`
    LOCAL_COMMIT=`git rev-parse --short @`
    echo " LOCAL: $LOCAL_COMMIT"
    echo "REMOTE: $REMOTE_COMMIT"

    # reset counter
    lcov -d . --zerocounters

    if [ "$LOCAL_COMMIT" == "$REMOTE_COMMIT" ]; then
        echo "repo up-to-date"
    else
        echo "repo need to pull"
    fi

    git reset --hard
    git checkout -- .
    git checkout $branch
    git checkout -- .
    git clean -dfx
    git pull

      [ -d $TDENGINE_DIR/debug ] || mkdir $TDENGINE_DIR/debug
      cd $TDENGINE_DIR/debug

    echo "rebuild.."
    LOCAL_COMMIT=`git rev-parse --short @`

    rm -rf *
    makecmd="cmake -DCOVER=true -DBUILD_TEST=true -DBUILD_HTTP=false -DBUILD_TOOLS=true -DBUILD_GEOS=true -DBUILD_CONTRIB=true ../../"
    echo "$makecmd"
    $makecmd

    make -j 8 install
}

function runCasesOneByOne () {
    while read -r line; do
        if [[ "$line" != "#"* ]]; then
            cmd=`echo $line | cut -d',' -f 5`
            if [[ "$2" == "sim" ]] && [[ $line == *"script"* ]]; then
                case=`echo $cmd | cut -d' ' -f 3`
                start_time=`date +%s`
                date +%F\ %T | tee -a  $TDENGINE_COVERAGE_REPORT  && timeout 20m $cmd > /dev/null 2>&1 && \
                echo -e "${GREEN}$case success${NC}" | tee -a  $TDENGINE_COVERAGE_REPORT \
                || echo -e "${RED}$case failed${NC}" | tee -a  $TDENGINE_COVERAGE_REPORT
                end_time=`date +%s`
                echo execution time of $case was `expr $end_time - $start_time`s. | tee -a $TDENGINE_COVERAGE_REPORT
            elif [[ "$line" == *"$2"* ]]; then
                if [[ "$cmd" == *"pytest.sh"* ]]; then
                    cmd=`echo $cmd | cut -d' ' -f 2-20`
                fi
                case=`echo $cmd | cut -d' ' -f 4-20`
                start_time=`date +%s`
                date +%F\ %T | tee -a $TDENGINE_COVERAGE_REPORT && timeout 20m $cmd > /dev/null 2>&1 && \
                echo -e "${GREEN}$case success${NC}" | tee -a $TDENGINE_COVERAGE_REPORT || \
                echo -e "${RED}$case failed${NC}" | tee -a $TDENGINE_COVERAGE_REPORT
                end_time=`date +%s`
                echo execution time of $case was `expr $end_time - $start_time`s. | tee -a $TDENGINE_COVERAGE_REPORT
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
    runCasesOneByOne $TDENGINE_DIR/tests/parallel_test/cases.task sim

    totalSuccess=`grep 'sim success' $TDENGINE_COVERAGE_REPORT | wc -l`
    if [ "$totalSuccess" -gt "0" ]; then
        echo "### Total $totalSuccess SIM test case(s) succeed! ###" | tee -a $TDENGINE_COVERAGE_REPORT
    fi

    totalFailed=`grep 'sim failed\|fault' $TDENGINE_COVERAGE_REPORT | wc -l`
    if [ "$totalFailed" -ne "0" ]; then
        echo "### Total $totalFailed SIM test case(s) failed! ###" | tee -a $TDENGINE_COVERAGE_REPORT
    fi
}

function runPythonCases() {
    echo "=== Run python cases ==="

    cd $TDENGINE_DIR/tests/parallel_test
    sed -i '/compatibility.py/d' cases.task

    # army
    cd $TDENGINE_DIR/tests/army
    runCasesOneByOne ../parallel_test/cases.task army

    # system-test
    cd $TDENGINE_DIR/tests/system-test
    runCasesOneByOne ../parallel_test/cases.task system-test

    # develop-test
    cd $TDENGINE_DIR/tests/develop-test
    runCasesOneByOne ../parallel_test/cases.task develop-test

    totalSuccess=`grep 'py success' $TDENGINE_COVERAGE_REPORT | wc -l`
    if [ "$totalSuccess" -gt "0" ]; then
        echo "### Total $totalSuccess python test case(s) succeed! ###" | tee -a $TDENGINE_COVERAGE_REPORT
    fi

    totalFailed=`grep 'py failed\|fault' $TDENGINE_COVERAGE_REPORT | wc -l`
    if [ "$totalFailed" -ne "0" ]; then
        echo "### Total $totalFailed python test case(s) failed! ###" | tee -a $TDENGINE_COVERAGE_REPORT
    fi
}

function runJDBCCases() {
    echo "=== Run JDBC cases ==="

    cd $JDBC_DIR
    git checkout -- .
    git reset --hard HEAD
    git checkout main
    git pull

    stopTaosd
    stopTaosadapter

    nohup $TDENGINE_DIR/debug/build/bin/taosd -c /etc/taos >> /dev/null 2>&1 &
    nohup taosadapter >> /dev/null 2>&1 &

    mvn clean test > result.txt 2>&1
    summary=`grep "Tests run:" result.txt | tail -n 1`
    echo -e "### JDBC test result: $summary ###" | tee -a $TDENGINE_COVERAGE_REPORT
}

function runTest() {
    echo "run Test"

    cd $TDENGINE_DIR
    [ -d sim ] && rm -rf sim
    [ -f $TDENGINE_COVERAGE_REPORT ] && rm $TDENGINE_COVERAGE_REPORT

    runUnitTest
    runSimCases
    runPythonCases
    runJDBCCases

    stopTaosd
    cd $TDENGINE_DIR/tests/script
    find . -name '*.sql' | xargs rm -f

    cd $TDENGINE_DIR/tests/pytest
    find . -name '*.sql' | xargs rm -f
}

function lcovFunc {
    echo "collect data by lcov"
    cd $TDENGINE_DIR

    # collect data
    lcov --capture -d $TDENGINE_DIR -b $TDENGINE_DIR -o coverage.info --ignore-errors negative,mismatch,inconsistent,source --branch-coverage --function-coverage --no-external

    # remove exclude paths
    lcov --remove coverage.info \
        '*/contrib/*' '*/tests/*' '*/test/*' '*/packaging/*' '*/taos-tools/*' '*/taosadapter/*' '*/TSZ/*' \
        '*/AccessBridgeCalls.c' '*/ttszip.c' '*/dataInserter.c' '*/tlinearhash.c' '*/tsimplehash.c' '*/tsdbDiskData.c'\
        '*/texpr.c' '*/runUdf.c' '*/schDbg.c' '*/syncIO.c' '*/tdbOs.c' '*/pushServer.c' '*/osLz4.c'\
        '*/tbase64.c' '*/tbuffer.c' '*/tdes.c' '*/texception.c' '*/examples/*' '*/tidpool.c' '*/tmempool.c'\
        '*/clientJniConnector.c' '*/clientTmqConnector.c' '*/version.c' '*/build_version.cc'\
        '*/tthread.c' '*/tversion.c'  '*/ctgDbg.c' '*/schDbg.c' '*/qwDbg.c' '*/tencode.h' \
        '*/shellAuto.c' '*/shellTire.c' '*/shellCommand.c'\
        '*/sql.c' '*/sql.y'\
         --branch-coverage --function-coverage -o coverage.info

    # generate result
    echo "generate result"
    lcov -l coverage.info --branch-coverage --function-coverage | tee -a $TDENGINE_COVERAGE_REPORT

    sed -i 's/\/root\/TDengine\/sql.c/\/root\/TDengine\/source\/libs\/parser\/inc\/sql.c/g' coverage.info
    sed -i 's/\/root\/TDengine\/sql.y/\/root\/TDengine\/source\/libs\/parser\/inc\/sql.y/g' coverage.info

    # push result to coveralls.io
    echo "push result to coveralls.io"
    /usr/local/bin/coveralls-lcov coverage.info -b $branch -t o7uY02qEAgKyJHrkxLGiCOTfL3IGQR2zm | tee -a $TDENGINE_COVERAGE_REPORT

    #/root/pxiao/checkCoverageFile.sh -s $TDENGINE_DIR/source -f $TDENGINE_COVERAGE_REPORT
    #cat /root/pxiao/fileListNoCoverage.log | tee -a $TDENGINE_COVERAGE_REPORT
    cat $TDENGINE_COVERAGE_REPORT | grep "| 0.0%" | awk -F "%" '{print $1}' | awk -F "|" '{if($2==0.0)print $1}' | tee -a $TDENGINE_COVERAGE_REPORT
}

function sendReport {
    echo "send report"
    receiver="develop@taosdata.com"
    mimebody="MIME-Version: 1.0\nContent-Type: text/html; charset=utf-8\n"

    cd $TDENGINE_DIR

    sed -i 's/\x1b\[[0-9;]*m//g' $TDENGINE_COVERAGE_REPORT
    BODY_CONTENT=`cat $TDENGINE_COVERAGE_REPORT`
    echo -e "from: <support@taosdata.com>\nto: ${receiver}\nsubject: Coverage test report ${branch} ${today}, commit ID: ${LOCAL_COMMIT}\n\n${today}:\n${BODY_CONTENT}" | \
    (cat - && uuencode $TDENGINE_COVERAGE_REPORT coverage-report-$today.log) | \
    /usr/sbin/ssmtp "${receiver}" && echo "Report Sent!"
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

date >> $WORK_DIR/cron.log
echo "Run Coverage Test" | tee -a $WORK_DIR/cron.log

stopTaosd

buildTDengine
runTest
lcovFunc

#sendReport
stopTaosd

date >> $WORK_DIR/cron.log
echo "End of Coverage Test" | tee -a $WORK_DIR/cron.log
