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
      echo "                                    Options: "
      echo "                                    e.g., -d /root/TDengine or -d /root/TDinternal"
      echo "    -b [Build test branch]      Build test branch (default: null)"
      echo "                                    Options: "
      echo "                                    e.g., -b main (pull main branch, build and install)"
      echo "    -t [Run test cases]         Run test cases type(default: all)"
      echo "                                    Options: "
      echo "                                    e.g., -t all/python/legacy"
      echo "    -s [Save cases log]         Save cases log(default: notsave)"
      echo "                                    Options:"
      echo "                                    e.g., -s notsave : do not save the log "
      echo "                                    -s save : default save ci case log in Project dir/tests/ci_bak"
      exit 0
}

function get_DIR() {
    today=`date +"%Y%m%d"`
    if [ -z "$PROJECT_DIR" ]; then
        CODE_DIR=$(dirname $0)
        cd $CODE_DIR
        CODE_DIR=$(pwd)
        if [[ "$CODE_DIR" == *"/community/"*  ]]; then
            PROJECT_DIR=$(realpath ../..)
            TDENGINE_DIR="$PROJECT_DIR/community"
            BUILD_DIR="$PROJECT_DIR/debug"
            TDENGINE_ALLCI_REPORT="$TDENGINE_DIR/tests/all-ci-report-$today.log"
            BACKUP_DIR="$TDENGINE_DIR/tests/ci_bak"
            mkdir -p "$BACKUP_DIR"
        else
            PROJECT_DIR=$(realpath ..)
            TDENGINE_DIR="$PROJECT_DIR"
            BUILD_DIR="$PROJECT_DIR/debug"
            TDENGINE_ALLCI_REPORT="$TDENGINE_DIR/tests/all-ci-report-$today.log"
            BACKUP_DIR="$TDENGINE_DIR/tests/ci_bak"
            mkdir -p "$BACKUP_DIR"
            cp $TDENGINE_DIR/tests/parallel_test/cases.task $TDENGINE_DIR/tests/parallel_test/cases_tdengine.task 
        fi
    elif [[ "$PROJECT_DIR" == *"/TDinternal" ]]; then
        TDENGINE_DIR="$PROJECT_DIR/community"
        BUILD_DIR="$PROJECT_DIR/debug"
        TDENGINE_ALLCI_REPORT="$TDENGINE_DIR/tests/all-ci-report-$today.log"
        BACKUP_DIR="$TDENGINE_DIR/tests/ci_bak"
        mkdir -p "$BACKUP_DIR"
        cp $TDENGINE_DIR/tests/parallel_test/cases.task $TDENGINE_DIR/tests/parallel_test/cases_tdengine.task 
    elif [[ "$PROJECT_DIR" == *"/TDengine" ]]; then
        TDENGINE_DIR="$PROJECT_DIR"
        BUILD_DIR="$PROJECT_DIR/debug"
        TDENGINE_ALLCI_REPORT="$TDENGINE_DIR/tests/all-ci-report-$today.log"
        BACKUP_DIR="$TDENGINE_DIR/tests/ci_bak"
        mkdir -p "$BACKUP_DIR"
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
        makecmd="cmake -DBUILD_HTTP=false -DBUILD_DEPENDENCY_TESTS=false -DBUILD_TOOLS=true -DBUILD_GEOS=true -DBUILD_TEST=true -DBUILD_CONTRIB=false ../ "
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
        makecmd="cmake -DBUILD_HTTP=false -DBUILD_DEPENDENCY_TESTS=0 -DBUILD_TOOLS=true -DBUILD_GEOS=true -DBUILD_TEST=true -DBUILD_CONTRIB=false ../ "
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
                
                if [ "$SAVE_LOG" == "save" ]; then
                    mkdir -p "$BACKUP_DIR/$case_file"
                    tar --exclude='*.sock*' -czf "$BACKUP_DIR/$case_file/sim.tar.gz" -C "$TDENGINE_DIR/.." sim
                    mv "$TDENGINE_DIR/tests/$case_file.log" "$BACKUP_DIR/$case_file"
                fi
                
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
                
                if [ "$SAVE_LOG" == "save" ]; then
                    mkdir -p "$BACKUP_DIR/$case_file"
                    tar --exclude='*.sock*' -czf "$BACKUP_DIR/$case_file/sim.tar.gz" -C "$TDENGINE_DIR/.." sim
                    mv "$TDENGINE_DIR/tests/$case_file.log" "$BACKUP_DIR/$case_file"
                fi
                
                end_time=`date +%s`
                echo execution time of $case was `expr $end_time - $start_time`s. | tee -a $TDENGINE_ALLCI_REPORT
            fi
        fi
    done < $1
}

function runUnitTest() {
    get_DIR
    print_color "$GREEN" "cd $BUILD_DIR"
    cd $BUILD_DIR
    pgrep taosd || taosd >> /dev/null 2>&1 &
    sleep 10
    ctest -E "cunit_test" -j4
    print_color "$GREEN" "3.0 unit test done"
}

function runSimCases() {
    print_color "$GREEN" "=== Run sim cases ==="

    cd $TDENGINE_DIR/tests/script
    runCasesOneByOne $TDENGINE_DIR/tests/parallel_test/cases_tdengine.task sim

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
    sed -i '/compatibility.py/d' cases_tdengine.task

    # army
    cd $TDENGINE_DIR/tests/army
    runCasesOneByOne ../parallel_test/cases_tdengine.task army

    # system-test
    cd $TDENGINE_DIR/tests/system-test
    runCasesOneByOne ../parallel_test/cases_tdengine.task system-test

    # develop-test
    cd $TDENGINE_DIR/tests/develop-test
    runCasesOneByOne ../parallel_test/cases_tdengine.task develop-test

    totalSuccess=`grep 'py success' $TDENGINE_ALLCI_REPORT | wc -l`
    if [ "$totalSuccess" -gt "0" ]; then
        print_color "$GREEN" "### Total $totalSuccess python test case(s) succeed! ###" | tee -a $TDENGINE_ALLCI_REPORT
    fi

    totalFailed=`grep 'py failed\|fault' $TDENGINE_ALLCI_REPORT | wc -l`
    if [ "$totalFailed" -ne "0" ]; then
        print_color "$RED" "### Total $totalFailed python test case(s) failed! ###" | tee -a $TDENGINE_ALLCI_REPORT
    fi
}

function runTest() {
    print_color "$GREEN" "run Test"

    cd $TDENGINE_DIR
    [ -d sim ] && rm -rf sim
    [ -f $TDENGINE_ALLCI_REPORT ] && rm $TDENGINE_ALLCI_REPORT

    runSimCases
    runPythonCases
    runUnitTest

    stopTaosd
    cd $TDENGINE_DIR/tests/script
    find . -name '*.sql' | xargs rm -f

    cd $TDENGINE_DIR/tests/pytest
    find . -name '*.sql' | xargs rm -f
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
    print_color "$GREEN" "Stop taosd end"
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
    print_color "$GREEN" "Stop taosadapter end"

}

######################
# main entry
######################

# Initialization parameter
PROJECT_DIR=""
BRANCH=""
TEST_TYPE=""
SAVE_LOG="notsave"

# Parse command line parameters
while getopts "hb:d:t:s:" arg; do
  case $arg in
    d)
      PROJECT_DIR=$OPTARG
      ;;
    b)
      BRANCH=$OPTARG
      ;;
    t)
      TEST_TYPE=$OPTARG
      ;;
    s)
      SAVE_LOG=$OPTARG
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

get_DIR
echo "PROJECT_DIR = $PROJECT_DIR"
echo "TDENGINE_DIR = $TDENGINE_DIR"
echo "BUILD_DIR = $BUILD_DIR"
echo "BACKUP_DIR = $BACKUP_DIR"

# Run all ci case
WORK_DIR=$TDENGINE_DIR

date >> $WORK_DIR/date.log
print_color "$GREEN" "Run all ci test cases" | tee -a $WORK_DIR/date.log

stopTaosd

# Check and get the branch name
if [ -n "$BRANCH" ] ; then
    branch="$BRANCH"
    print_color "$GREEN" "Testing branch: $branch "
    print_color "$GREEN" "Build is required for this test!"
    buildTDengine
else
    print_color "$GREEN" "Build is not required for this test!"
fi

# Run different types of case
if [ -z "$TEST_TYPE" -o "$TEST_TYPE" = "all" -o "$TEST_TYPE" = "ALL" ]; then
    runTest
elif [ "$TEST_TYPE" = "python" -o "$TEST_TYPE" = "PYTHON" ]; then
    runPythonCases
elif [ "$TEST_TYPE" = "legacy" -o "$TEST_TYPE" = "LEGACY" ]; then
    runSimCases
fi

date >> $WORK_DIR/date.log
print_color "$GREEN" "End of ci test cases" | tee -a $WORK_DIR/date.log
