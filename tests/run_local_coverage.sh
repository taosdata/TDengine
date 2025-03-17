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

function printHelp() {
    echo "Usage: $(basename $0) [options]"
      echo
      echo "Options:"
      echo "    -d [Project dir]            Project directory (default: outermost project directory)"
      echo "                                    e.g., -d /root/TDinternal/community"
      echo "    -b [Build specify branch]   Build specify branch (default: main)"
      echo "                                    Options: "
      echo "                                    e.g., -b main (pull main branch, build and install)[Branches need to be specified for the first run]"
      echo "    -i [Build develop branch]   Build develop branch (default: null)"
      echo "                                    Options: "
      echo "                                    yes/YES (pull , build and install)"
      echo "                                    only_install/ONLY_INSTALL (only install)"
      echo "    -f [Capture gcda dir]       Capture gcda directory (default: <project dir>/debug)"
      echo "    -c [Test case]              Test single case or all cases (default: null)"
      echo "                                    Options:"
      echo "                                    -c all  : run all python, sim cases in longtimeruning_cases.task and unit cases"
      echo "                                    -c task : run all python and sim cases in longtimeruning_cases.task "
      echo "                                    -c cmd  : run the specified test command"
      echo "                                    e.g., -c './test.sh -f tsim/stream/streamFwcIntervalFill.sim'"
      echo "    -u [Unit test case]         Unit test case (default: null)"
      echo "                                    e.g., -u './schedulerTest'"
      echo "    -l [Lcov bin dir]           Lcov bin dir (default: /usr/local/bin)"
      echo "                                    e.g., -l '/root/TDinternal/community/tests/lcov-1.16/bin'"
      exit 0
}


# Find the project/tdengine/build/capture directory 
function get_DIR() {
    today=`date +"%Y%m%d"`
    if [ -z "$PROJECT_DIR" ]; then
        CODE_DIR=$(dirname $0)
        cd $CODE_DIR
        CODE_DIR=$(pwd)
        if [[ "$CODE_DIR" == *"/community/"*  ]]; then
            PROJECT_DIR=$(realpath ../..)
            TDENGINE_DIR="$PROJECT_DIR"
            BUILD_DIR="$PROJECT_DIR/debug"
            TDENGINE_ALLCI_REPORT="$TDENGINE_DIR/tests/all-ci-report-$today.log"
            CAPTURE_GCDA_DIR="$BUILD_DIR"
        else
            PROJECT_DIR=$(realpath ..)
            TDENGINE_DIR="$PROJECT_DIR"
            BUILD_DIR="$PROJECT_DIR/debug"
            TDENGINE_ALLCI_REPORT="$TDENGINE_DIR/tests/all-ci-report-$today.log"
            CAPTURE_GCDA_DIR="$BUILD_DIR"
        fi
    elif [[ "$PROJECT_DIR" == *"/TDinternal" ]]; then
        TDENGINE_DIR="$PROJECT_DIR/community"
        BUILD_DIR="$PROJECT_DIR/debug"
        TDENGINE_ALLCI_REPORT="$TDENGINE_DIR/tests/all-ci-report-$today.log"
        CAPTURE_GCDA_DIR="$BUILD_DIR"
    elif [[ "$PROJECT_DIR" == *"/TDengine" ]]; then
        TDENGINE_DIR="$PROJECT_DIR"
        BUILD_DIR="$PROJECT_DIR/debug"
        TDENGINE_ALLCI_REPORT="$TDENGINE_DIR/tests/all-ci-report-$today.log"
        CAPTURE_GCDA_DIR="$BUILD_DIR"
    fi
}


function buildTDengine() {
    print_color "$GREEN" "TDengine build start"
    
    if [[ "$PROJECT_DIR" == *"/TDinternal" ]]; then
        TDENGINE_DIR="$PROJECT_DIR/community"

        # pull tdinternal code
        cd "$TDENGINE_DIR/../"
        print_color "$GREEN" "Git pull TDinternal code..."
        # git remote prune origin > /dev/null
        # git remote update > /dev/null

        # pull tdengine code
        cd $TDENGINE_DIR
        print_color "$GREEN" "Git pull TDengine code..."
        # git remote prune origin > /dev/null
        # git remote update > /dev/null
        REMOTE_COMMIT=`git rev-parse --short remotes/origin/$branch`
        LOCAL_COMMIT=`git rev-parse --short @`
        print_color "$GREEN" " LOCAL: $LOCAL_COMMIT"
        print_color "$GREEN" "REMOTE: $REMOTE_COMMIT"

        if [ "$LOCAL_COMMIT" == "$REMOTE_COMMIT" ]; then
            print_color "$GREEN" "Repo up-to-date"
        else
            print_color "$GREEN" "Repo need to pull"
        fi

        # git reset --hard
        # git checkout -- .
        git checkout $branch
        # git checkout -- .
        # git clean -f
        # git pull

        [ -d $TDENGINE_DIR/../debug ] || mkdir $TDENGINE_DIR/../debug
        cd $TDENGINE_DIR/../debug

        print_color "$GREEN" "Rebuild.."
        LOCAL_COMMIT=`git rev-parse --short @`

        rm -rf *
        makecmd="cmake -DCOVER=true -DBUILD_HTTP=false -DBUILD_DEPENDENCY_TESTS=false -DBUILD_TOOLS=true -DBUILD_GEOS=true -DBUILD_TEST=true -DBUILD_CONTRIB=false ../ "
        print_color "$GREEN" "$makecmd"
        $makecmd

        make -j $(nproc) install  

    else
        TDENGINE_DIR="$PROJECT_DIR"
        # pull tdengine code
        cd $TDENGINE_DIR
        print_color "$GREEN" "Git pull TDengine code..."
        # git remote prune origin > /dev/null
        # git remote update > /dev/null
        REMOTE_COMMIT=`git rev-parse --short remotes/origin/$branch`
        LOCAL_COMMIT=`git rev-parse --short @`
        print_color "$GREEN" " LOCAL: $LOCAL_COMMIT"
        print_color "$GREEN" "REMOTE: $REMOTE_COMMIT"

        if [ "$LOCAL_COMMIT" == "$REMOTE_COMMIT" ]; then
            print_color "$GREEN" "Repo up-to-date"
        else
            print_color "$GREEN" "Repo need to pull"
        fi

        # git reset --hard
        # git checkout -- .
        git checkout $branch
        # git checkout -- .
        # git clean -f
        # git pull

        [ -d $TDENGINE_DIR/debug ] || mkdir $TDENGINE_DIR/debug
        cd $TDENGINE_DIR/debug

        print_color "$GREEN" "Rebuild.."
        LOCAL_COMMIT=`git rev-parse --short @`

        rm -rf *
        makecmd="cmake -DCOVER=true -DBUILD_HTTP=false -DBUILD_DEPENDENCY_TESTS=0 -DBUILD_TOOLS=true -DBUILD_GEOS=true -DBUILD_TEST=true -DBUILD_CONTRIB=false ../ "
        print_color "$GREEN" "$makecmd"
        $makecmd

        make -j $(nproc) install  
    fi

    print_color "$GREEN" "TDengine build end"
}

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
    get_DIR
    print_color "$GREEN" "cd $BUILD_DIR"
    cd $BUILD_DIR
    pgrep taosd || taosd >> /dev/null 2>&1 &
    sleep 10
    ctest -E "cunit_test" -j $(nproc)
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
        PYTHON_SIM=$(echo $TEST_CASE | awk '{print $1}' | xargs basename)
        echo "PYTHON_SIM: $PYTHON_SIM"
        if [[ "$PYTHON_SIM" == "python3" ]]; then
            CASE_FILENAME=$(echo $TEST_CASE | awk -F' ' '{print $4}' | xargs basename)
            echo "CASE_FILENAME: $CASE_FILENAME"
            CASE_DIR=$(find $TDENGINE_DIR/tests -name $CASE_FILENAME)
            echo "CASE_DIR: $CASE_DIR"
            if [[ "$CASE_DIR" == *"/army/"*  ]]; then
                cd $TDENGINE_DIR/tests/army/ && $TEST_CASE
            elif [[ "$CASE_DIR" == *"/system-test/"*  ]]; then
                cd $TDENGINE_DIR/tests/system-test/ && $TEST_CASE
            elif [[ "$CASE_DIR" == *"/develop-test/"*  ]]; then
                cd $TDENGINE_DIR/tests/develop-test/ && $TEST_CASE
            fi
        else
            CASE_FILENAME=$(echo $TEST_CASE | awk -F' ' '{print $3}' | xargs basename)
            echo "CASE_FILENAME: $CASE_FILENAME"
            CASE_DIR=$(find $TDENGINE_DIR/tests -name $CASE_FILENAME)
            echo "CASE_DIR: $CASE_DIR"
            cd $TDENGINE_DIR/tests/script/ && $TEST_CASE
        fi
    elif [ "$TEST_CASE" == "all" ]; then
        print_color "$GREEN" "Test case is : parallel_test/longtimeruning_cases.task and all unit cases"
        runTest_all
    elif [ "$TEST_CASE" == "task" ]; then
        print_color "$GREEN" "Test case is only: parallel_test/longtimeruning_cases.task "
        runSimCases
        runPythonCases
    elif [ -n "$UNIT_TEST_CASE" ]; then
        UNIT_TEST_CASE="$UNIT_TEST_CASE"
        cd $BUILD_DIR/build/bin/ && $UNIT_TEST_CASE
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

    if [ -n "$LCOV_DIR" ]; then
        LCOV_DIR="$LCOV_DIR"
        print_color "$GREEN" "Lcov bin dir: $LCOV_DIR "
    else
        print_color "$GREEN" "Lcov bin dir is default"
    fi

    # collect data
    $LCOV_DIR/lcov -d "$CAPTURE_GCDA_DIR" -capture --rc lcov_branch_coverage=1 --rc genhtml_branch_coverage=1 --no-external -b $TDENGINE_DIR -o coverage.info

    # remove exclude paths
    $LCOV_DIR/lcov --remove coverage.info \
        '*/contrib/*' '*/test/*' '*/packaging/*' '*/taos-tools/deps/*' '*/taosadapter/*' '*/TSZ/*' \
        '*/AccessBridgeCalls.c' '*/ttszip.c' '*/dataInserter.c' '*/tlinearhash.c' '*/tsimplehash.c' '*/tsdbDiskData.c' '/*/enterprise/*' '*/docs/*' '*/sim/*'\
        '*/texpr.c' '*/runUdf.c' '*/schDbg.c' '*/syncIO.c' '*/tdbOs.c' '*/pushServer.c' '*/osLz4.c'\
        '*/tbase64.c' '*/tbuffer.c' '*/tdes.c' '*/texception.c' '*/examples/*' '*/tidpool.c' '*/tmempool.c'\
        '*/clientJniConnector.c' '*/clientTmqConnector.c' '*/version.cc'\
        '*/tthread.c' '*/tversion.c'  '*/ctgDbg.c' '*/schDbg.c' '*/qwDbg.c' '*/tencode.h' \
        '*/shellAuto.c' '*/shellTire.c' '*/shellCommand.c'\
        '*/sql.c' '*/sql.y' '*/smaSnapshot.c' '*/smaCommit.c' '*/debug/*' '*/tests/*'\
         --rc lcov_branch_coverage=1  -o coverage.info

    # generate result
    echo "generate result"
    $LCOV_DIR/lcov -l --rc lcov_branch_coverage=1 coverage.info | tee -a $TDENGINE_COVERAGE_REPORT

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

######################
# main entry
######################

# Initialization parameter
PROJECT_DIR=""
CAPTURE_GCDA_DIR=""
TEST_CASE="task"
UNIT_TEST_CASE=""
BRANCH=""
BRANCH_BUILD=""
LCOV_DIR="/usr/local/bin"

# Parse command line parameters
while getopts "hd:b:f:c:u:i:l:" arg; do
  case $arg in
    d)
      PROJECT_DIR=$OPTARG
      ;;
    b)
      BRANCH=$OPTARG
      ;;
    f)
      CAPTURE_GCDA_DIR=$OPTARG
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
    l)
      LCOV_DIR=$OPTARG
      ;;
    h)
      printHelp
      ;;
    ?)
      echo "Usage: ./$(basename $0) -h"
      exit 1
      ;;
  esac
done


# Show all parameters
get_DIR
echo "PROJECT_DIR = $PROJECT_DIR"
echo "TDENGINE_DIR = $TDENGINE_DIR"
echo "BUILD_DIR = $BUILD_DIR"
echo "CAPTURE_GCDA_DIR = $CAPTURE_GCDA_DIR"
echo "TEST_CASE = $TEST_CASE"
echo "UNIT_TEST_CASE = $UNIT_TEST_CASE"
echo "BRANCH_BUILD = $BRANCH_BUILD"
echo "LCOV_DIR = $LCOV_DIR"


date >> $TDENGINE_DIR/date.log
print_color "$GREEN" "Run local coverage test cases" | tee -a $TDENGINE_DIR/date.log


# Check and get the branch name and build branch
if [ -n "$BRANCH" ] && [ -z "$BRANCH_BUILD" ] ; then
    branch="$BRANCH"
    print_color "$GREEN" "Testing branch: $branch "
    print_color "$GREEN" "Build is required for this test!"
    buildTDengine
elif [ -n "$BRANCH_BUILD" ] && [ "$BRANCH_BUILD" = "YES" -o "$BRANCH_BUILD" = "yes" ] ; then
    CURRENT_DIR=$(pwd)
    echo "CURRENT_DIR: $CURRENT_DIR"
    if [ -d .git ]; then
        CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
        echo "CURRENT_BRANCH: $CURRENT_BRANCH"
    else
        echo "The current directory is not a Git repository"
    fi
    branch="$CURRENT_BRANCH"
    print_color "$GREEN" "Testing branch: $branch "
    print_color "$GREEN" "Build is required for this test!"
    buildTDengine
elif [ -n "$BRANCH_BUILD" ] && [ "$BRANCH_BUILD" = "ONLY_INSTALL" -o "$BRANCH_BUILD" = "only_install" ] ; then
    CURRENT_DIR=$(pwd)
    echo "CURRENT_DIR: $CURRENT_DIR"
    if [ -d .git ]; then
        CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
        echo "CURRENT_BRANCH: $CURRENT_BRANCH"
    else
        echo "The current directory is not a Git repository"
    fi
    branch="$CURRENT_BRANCH"
    print_color "$GREEN" "Testing branch: $branch "
    print_color "$GREEN" "not build,only install!"
    cd $TDENGINE_DIR/debug
    make -j $(nproc) install 
elif [ -z "$BRANCH" ] && [ -z "$BRANCH_BUILD" ] ; then
    print_color "$GREEN" "Build is not required for this test!"
fi


stopTaosd

runTest

lcovFunc


date >> $TDENGINE_DIR/date.log
print_color "$GREEN" "End of local coverage test cases" | tee -a $TDENGINE_DIR/date.log


# Define coverage information files and output directories
COVERAGE_INFO="$TDENGINE_DIR/coverage.info"
OUTPUT_DIR="$TDENGINE_DIR/coverage_report"

# Check whether the coverage information file exists
if [ ! -f "$COVERAGE_INFO" ]; then
    echo "Error: $COVERAGE_INFO not found!"
    exit 1
fi

if [ -n "$LCOV_DIR" ]; then
    LCOV_DIR="$LCOV_DIR"
    print_color "$GREEN" "Lcov bin dir: $LCOV_DIR "
else
    print_color "$GREEN" "Lcov bin dir is default"
fi
# Generate local HTML reports
$LCOV_DIR/genhtml "$COVERAGE_INFO"  --branch-coverage --function-coverage --output-directory "$OUTPUT_DIR"

# Check whether the report was generated successfully
if [ $? -eq 0 ]; then
    echo "HTML coverage report generated successfully in $OUTPUT_DIR"
    echo "For more details : use 'python3 -m http.server port' in $OUTPUT_DIR"
    echo "eg: http://IP:PORT/"
else
    echo "Error generating HTML coverage report"
    exit 1
fi

