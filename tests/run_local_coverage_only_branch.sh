#!/bin/bash

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

function print_color() {
    local color="$1"
    local message="$2"
    echo -e "${color}${message}${NC}"
}

# Initialization parameter
TDENGINE_DIR="/root/TDinternal/community"
BRANCH=""
TDENGINE_GCDA_DIR="/root/TDinternal/community/debug/"

# Parse command line parameters
while getopts "hd:b:f:c:u:i:" arg; do
  case $arg in
    d)
      TDENGINE_DIR=$OPTARG
      ;;
    b)
      BRANCH=$OPTARG
      ;;
    f)
      TDENGINE_GCDA_DIR=$OPTARG
      ;;
    c)
      TEST_CASE=$OPTARG
      ;;
    u)
      UNIT_TEST_CASE=$OPTARG
      ;;
    i)
      BRANCH_BUILD=$OPTARG
      ;;
    h)
      echo "Usage: $(basename $0) -d [TDengine dir] -b [Test branch] -i [Build test branch] -f [TDengine gcda dir] -c [Test single case/all cases] -u [Unit test case]"
      echo "                  -d [TDengine dir] [default /root/TDinternal/community; eg: /home/TDinternal/community] "
      echo "                  -b [Test branch] [default local branch; eg:cover/3.0] "
      echo "                  -i [Build test branch] [default no:not build, but still install ;yes:will build and install ] "
      echo "                  -f [TDengine gcda dir] [default /root/TDinternal/community/debug; eg:/root/TDinternal/community/debug/community/source/dnode/vnode/CMakeFiles/vnode.dir/src/tq/] "
      echo "                  -c [Test single case/all cases] [default null; -c all : include parallel_test/longtimeruning_cases.task and all unit cases; -c task : include parallel_test/longtimeruning_cases.task; single case: eg: -c './test.sh -f tsim/stream/streamFwcIntervalFill.sim' ] "
      echo "                  -u [Unit test case] [default null;  eg: './schedulerTest' ] "
      exit 0
      ;;
    ?)
      echo "Usage: ./$(basename $0) -h"
      exit 1
      ;;
  esac
done

# Check if the command name is provided
if [ -z "$TDENGINE_DIR" ]; then
  echo "Error: TDengine dir is required."
  echo "Usage: $(basename $0) -d [TDengine dir] -b [Test branch] -i [Build test branch]  -f [TDengine gcda dir] -c [Test single case/all cases] -u [Unit test case]  "
  echo "                        -d [TDengine dir] [default /root/TDinternal/community; eg: /home/TDinternal/community] "
  echo "                        -b [Test branch] [default local branch; eg:cover/3.0] "   
  echo "                        -i [Build test branch] [default no:not build, but still install ;yes:will build and install ] "
  echo "                        -f [TDengine gcda dir] [default /root/TDinternal/community/debug; eg:/root/TDinternal/community/debug/community/source/dnode/vnode/CMakeFiles/vnode.dir/src/tq/]  " 
  echo "                        -c [Test casingle case/all casesse] [default null; -c all : include parallel_test/longtimeruning_cases.task and all unit cases; -c task : include parallel_test/longtimeruning_cases.task; single case: eg: -c './test.sh -f tsim/stream/streamFwcIntervalFill.sim' ]  " 
  echo "                        -u [Unit test case] [default null;  eg: './schedulerTest' ] "
  exit 1
fi


echo "TDENGINE_DIR = $TDENGINE_DIR"
today=`date +"%Y%m%d"`
TDENGINE_ALLCI_REPORT="$TDENGINE_DIR/tests/all-ci-report-$today.log"

function pullTDengine() {
    print_color "$GREEN" "TDengine pull start"
    
    # pull parent code
    cd "$TDENGINE_DIR/../"
    print_color "$GREEN" "git pull parent code..."

    git reset --hard
    git checkout -- .
    git checkout $branch
    git checkout -- .
    git clean -f
    git pull

    # pull tdengine code
    cd $TDENGINE_DIR
    print_color "$GREEN" "git pull tdengine code..."

    git reset --hard
    git checkout -- .
    git checkout $branch
    git checkout -- .
    git clean -f
    git pull

    print_color "$GREEN" "TDengine pull end"
}

function buildTDengine() {
    print_color "$GREEN" "TDengine build start"

    [ -d $TDENGINE_DIR/debug ] || mkdir $TDENGINE_DIR/debug
    cd $TDENGINE_DIR/debug

    print_color "$GREEN" "rebuild.."
    rm -rf *
    makecmd="cmake -DCOVER=true -DBUILD_TEST=false -DBUILD_HTTP=false -DBUILD_DEPENDENCY_TESTS=0 -DBUILD_TOOLS=true -DBUILD_GEOS=true -DBUILD_TEST=true -DBUILD_CONTRIB=false ../../"
    print_color "$GREEN" "$makecmd"
    $makecmd        
    make -j 8 install
}

# Check and get the branch name and build branch
if [ -n "$BRANCH" ] && [ -z "$BRANCH_BUILD" ] ; then
    branch="$BRANCH"
    print_color "$GREEN" "Testing branch: $branch "
    print_color "$GREEN" "Build is required for this test!"
    pullTDengine
    buildTDengine
elif [ -n "$BRANCH_BUILD" ] && [ "$BRANCH_BUILD" == "yes" ] ; then
    branch="$BRANCH"
    print_color "$GREEN" "Testing branch: $branch "
    print_color "$GREEN" "Build is required for this test!"
    pullTDengine
    buildTDengine
elif [ -n "$BRANCH_BUILD" ] && [ "$BRANCH_BUILD" == "no" ] ; then
    branch="$BRANCH"
    print_color "$GREEN" "Testing branch: $branch "
    print_color "$GREEN" "not build,only install!"
    cd "$TDENGINE_DIR/../"
    git pull
    cd "$TDENGINE_DIR/"
    git pull
    cd $TDENGINE_DIR/debug
    make -j 8 install 
else
    print_color "$GREEN" "Build is not required for this test!"
fi

function runCasesOneByOne () {
    while read -r line; do
        if [[ "$line" != "#"* ]]; then
            cmd=`echo $line | cut -d',' -f 5`
            if [[ "$2" == "sim" ]] && [[ $line == *"script"* ]]; then
                echo $cmd
                case=`echo $cmd | cut -d' ' -f 3`
                case_file=`echo $case | tr -d ' /' `
                start_time=`date +%s`
                date +%F\ %T | tee -a  $TDENGINE_ALLCI_REPORT  && timeout 20m $cmd > $TDENGINE_DIR/tests/$case_file.log 2>&1 && \
                echo -e "${GREEN}$case success${NC}" | tee -a  $TDENGINE_ALLCI_REPORT || \
                echo -e "${RED}$case failed${NC}" | tee -a $TDENGINE_ALLCI_REPORT
                end_time=`date +%s`
                echo execution time of $case was `expr $end_time - $start_time`s. | tee -a $TDENGINE_ALLCI_REPORT

            elif [[ "$line" == *"$2"* ]]; then
                echo $cmd
                if [[ "$cmd" == *"pytest.sh"* ]]; then
                    cmd=`echo $cmd | cut -d' ' -f 2-20`
                fi
                case=`echo $cmd | cut -d' ' -f 4-20`           
                case_file=`echo $case | tr -d ' /' `
                start_time=`date +%s`
                date +%F\ %T | tee -a $TDENGINE_ALLCI_REPORT && timeout 20m $cmd > $TDENGINE_DIR/tests/$case_file.log 2>&1 && \
                echo -e "${GREEN}$case success${NC}" | tee -a $TDENGINE_ALLCI_REPORT || \
                echo -e "${RED}$case failed${NC}" | tee -a $TDENGINE_ALLCI_REPORT               
                end_time=`date +%s`
                echo execution time of $case was `expr $end_time - $start_time`s. | tee -a $TDENGINE_ALLCI_REPORT
            fi
        fi
    done < $1
}

function runUnitTest() {
    print_color "$GREEN" "=== Run unit test case ==="
    print_color "$GREEN" " $TDENGINE_DIR/debug"
    cd $TDENGINE_DIR/debug
    ctest -j12
    print_color "$GREEN" "3.0 unit test done"
}

function runSimCases() {
    print_color "$GREEN" "=== Run sim cases ==="

    cd $TDENGINE_DIR/tests/script
    runCasesOneByOne $TDENGINE_DIR/tests/parallel_test/longtimeruning_cases.task sim

    totalSuccess=`grep 'sim success' $TDENGINE_ALLCI_REPORT | wc -l`
    if [ "$totalSuccess" -gt "0" ]; then
        print_color "$GREEN" "### Total $totalSuccess SIM test case(s) succeed! ###" | tee -a $TDENGINE_ALLCI_REPORT
    fi

    totalFailed=`grep 'sim failed\|fault' $TDENGINE_ALLCI_REPORT | wc -l`
    if [ "$totalFailed" -ne "0" ]; then
        print_color "$RED" "### Total $totalFailed SIM test case(s) failed! ###" | tee -a $TDENGINE_ALLCI_REPORT
    fi
}

function runPythonCases() {
    print_color "$GREEN" "=== Run python cases ==="

    cd $TDENGINE_DIR/tests/parallel_test
    sed -i '/compatibility.py/d' longtimeruning_cases.task

    # army
    cd $TDENGINE_DIR/tests/army
    runCasesOneByOne ../parallel_test/longtimeruning_cases.task army

    # system-test
    cd $TDENGINE_DIR/tests/system-test
    runCasesOneByOne ../parallel_test/longtimeruning_cases.task system-test

    # develop-test
    cd $TDENGINE_DIR/tests/develop-test
    runCasesOneByOne ../parallel_test/longtimeruning_cases.task develop-test

    totalSuccess=`grep 'py success' $TDENGINE_ALLCI_REPORT | wc -l`
    if [ "$totalSuccess" -gt "0" ]; then
        print_color "$GREEN" "### Total $totalSuccess python test case(s) succeed! ###" | tee -a $TDENGINE_ALLCI_REPORT
    fi

    totalFailed=`grep 'py failed\|fault' $TDENGINE_ALLCI_REPORT | wc -l`
    if [ "$totalFailed" -ne "0" ]; then
        print_color "$RED" "### Total $totalFailed python test case(s) failed! ###" | tee -a $TDENGINE_ALLCI_REPORT
    fi
}


function runTest_all() {
    print_color "$GREEN" "run Test"

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


function runTest() {
    print_color "$GREEN" "run Test"

    cd $TDENGINE_DIR
    [ -d sim ] && rm -rf sim
    [ -f $TDENGINE_ALLCI_REPORT ] && rm $TDENGINE_ALLCI_REPORT

    if [ -n "$TEST_CASE" ] && [ "$TEST_CASE" != "all" ] && [ "$TEST_CASE" != "task" ]; then
        TEST_CASE="$TEST_CASE"
        print_color "$GREEN" "Test case: $TEST_CASE "
        cd $TDENGINE_DIR/tests/script/ && $TEST_CASE
        cd $TDENGINE_DIR/tests/army/ && $TEST_CASE
        cd $TDENGINE_DIR/tests/system-test/ && $TEST_CASE
        cd $TDENGINE_DIR/tests/develop-test/ && $TEST_CASE
    elif [ "$TEST_CASE" == "all" ]; then
        print_color "$GREEN" "Test case is : parallel_test/longtimeruning_cases.task and all unit cases"
        runTest_all
    elif [ "$TEST_CASE" == "task" ]; then
        print_color "$GREEN" "Test case is only: parallel_test/longtimeruning_cases.task "
        runSimCases
        runPythonCases
    elif [ -n "$UNIT_TEST_CASE" ]; then
        UNIT_TEST_CASE="$UNIT_TEST_CASE"
        cd $TDENGINE_DIR/debug/build/bin/ && $UNIT_TEST_CASE
    else
        print_color "$GREEN" "Test case is null"
    fi


    stopTaosd
    cd $TDENGINE_DIR/tests/script
    find . -name '*.sql' | xargs rm -f

    cd $TDENGINE_DIR/tests/pytest
    find . -name '*.sql' | xargs rm -f
}

function lcovFunc {
    echo "collect data by lcov"
    cd $TDENGINE_DIR

    if [ -n "$TDENGINE_GCDA_DIR" ]; then
        TDENGINE_GCDA_DIR="$TDENGINE_GCDA_DIR"
        print_color "$GREEN" "Test gcda file dir: $TDENGINE_GCDA_DIR "
    else
        print_color "$GREEN" "Test gcda file dir is default: /root/TDinternal/community/debug"
    fi

    # collect data
    lcov -d "$TDENGINE_GCDA_DIR" -capture --rc lcov_branch_coverage=0 --rc genhtml_branch_coverage=1 --no-external -b $TDENGINE_DIR -o coverage.info

    # remove exclude paths
    lcov --remove coverage.info \
        '*/contrib/*' '*/test/*' '*/packaging/*' '*/taos-tools/*' '*/taosadapter/*' '*/TSZ/*' \
        '*/AccessBridgeCalls.c' '*/ttszip.c' '*/dataInserter.c' '*/tlinearhash.c' '*/tsimplehash.c' '*/tsdbDiskData.c' '/*/enterprise/*' '*/docs/*' '*/sim/*'\
        '*/texpr.c' '*/runUdf.c' '*/schDbg.c' '*/syncIO.c' '*/tdbOs.c' '*/pushServer.c' '*/osLz4.c'\
        '*/tbase64.c' '*/tbuffer.c' '*/tdes.c' '*/texception.c' '*/examples/*' '*/tidpool.c' '*/tmempool.c'\
        '*/clientJniConnector.c' '*/clientTmqConnector.c' '*/version.cc' '*/branch/*'\
        '*/tthread.c' '*/tversion.c'  '*/ctgDbg.c' '*/schDbg.c' '*/qwDbg.c' '*/tencode.h' \
        '*/shellAuto.c' '*/shellTire.c' '*/shellCommand.c'\
        '*/sql.c' '*/sql.y' '*/smaSnapshot.c' '*/smaCommit.c' '*/debug/*' '*/tests/*'\
         --rc lcov_branch_coverage=1  -o coverage.info

    # generate result
    echo "generate result"
    lcov -l --rc lcov_branch_coverage=1 coverage.info | tee -a $TDENGINE_COVERAGE_REPORT

}

function stopTaosd {
    print_color "$GREEN" "Stop taosd start"
    systemctl stop taosd
    PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
    while [ -n "$PID" ]
    do
        pkill -TERM -x taosd
        sleep 1
        PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
    done
    print_color "$GREEN" "Stop tasod end"
}

function stopTaosadapter {
    print_color "$GREEN" "Stop taosadapter"
    systemctl stop taosadapter.service
    PID=`ps -ef|grep -w taosadapter | grep -v grep | awk '{print $2}'`
    while [ -n "$PID" ]
    do
        pkill -TERM -x taosadapter
        sleep 1
        PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
    done
    print_color "$GREEN" "Stop tasoadapter end"

}

WORK_DIR=/root

date >> $WORK_DIR/date.log
print_color "$GREEN" "Run local coverage test cases" | tee -a $WORK_DIR/date.log

stopTaosd

runTest

lcovFunc


date >> $WORK_DIR/date.log
print_color "$GREEN" "End of local coverage test cases" | tee -a $WORK_DIR/date.log


# Define coverage information files and output directories
COVERAGE_INFO="$TDENGINE_DIR/coverage.info"
OUTPUT_DIR="$WORK_DIR/coverage_report"

# Check whether the coverage information file exists
if [ ! -f "$COVERAGE_INFO" ]; then
    echo "Error: $COVERAGE_INFO not found!"
    exit 1
fi

# Generate local HTML reports
genhtml "$COVERAGE_INFO"  --branch-coverage --function-coverage --output-directory "$OUTPUT_DIR"

# Check whether the report was generated successfully
if [ $? -eq 0 ]; then
    echo "HTML coverage report generated successfully in $OUTPUT_DIR"
    echo "For more details : "
    echo "http://192.168.1.61:7000/"
else
    echo "Error generating HTML coverage report"
    exit 1
fi

