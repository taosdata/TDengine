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

# 初始化参数
TDENGINE_DIR="/root/TDinternal/community"
BRANCH=""
SAVE_LOG="notsave"

# 解析命令行参数
while getopts "hd:b:t:s:" arg; do
  case $arg in
    d)
      TDENGINE_DIR=$OPTARG
      ;;
    b)
      BRANCH=$OPTARG
      ;;
    s)
      SAVE_LOG=$OPTARG
      ;;
    h)
      echo "Usage: $(basename $0) -d [TDengine_dir] -b [branch] -s [save ci case log]"
      echo "                  -d [TDengine_dir] [default /root/TDinternal/community] "
      echo "                  -b [branch] [default local branch] "
      echo "                  -s [save/notsave] [default save ci case log in TDengine_dir/tests/ci_bak] "
      exit 0
      ;;
    ?)
      echo "Usage: ./$(basename $0) -h"
      exit 1
      ;;
  esac
done

# 检查是否提供了命令名称
if [ -z "$TDENGINE_DIR" ]; then
  echo "Error: TDengine dir is required."
  echo "Usage: $(basename $0) -d [TDengine_dir] -b [branch] -s [save ci case log] "
  echo "                        -d [TDengine_dir] [default /root/TDinternal/community] "
  echo "                        -b [branch] [default local branch] "
  echo "                        -s [save/notsave] [default save ci case log in TDengine_dir/tests/ci_bak] "    
  exit 1
fi


echo "TDENGINE_DIR = $TDENGINE_DIR"
today=`date +"%Y%m%d"`
TDENGINE_ALLCI_REPORT="$TDENGINE_DIR/tests/all-ci-report-$today.log"
BACKUP_DIR="$TDENGINE_DIR/tests/ci_bak"
mkdir -p "$BACKUP_DIR"
#cd $BACKUP_DIR && rm -rf *


function buildTDengine() {
    print_color "$GREEN" "TDengine build start"
    
    # pull parent code
    cd "$TDENGINE_DIR/../"
    print_color "$GREEN" "git pull parent code..."
    git remote prune origin > /dev/null
    git remote update > /dev/null

    # pull tdengine code
    cd $TDENGINE_DIR
    print_color "$GREEN" "git pull tdengine code..."
    git remote prune origin > /dev/null
    git remote update > /dev/null
    REMOTE_COMMIT=`git rev-parse --short remotes/origin/$branch`
    LOCAL_COMMIT=`git rev-parse --short @`
    print_color "$GREEN" " LOCAL: $LOCAL_COMMIT"
    print_color "$GREEN" "REMOTE: $REMOTE_COMMIT"

    if [ "$LOCAL_COMMIT" == "$REMOTE_COMMIT" ]; then
        print_color "$GREEN" "repo up-to-date"
    else
        print_color "$GREEN" "repo need to pull"
    fi

    git reset --hard
    git checkout -- .
    git checkout $branch
    git checkout -- .
    git clean -f
    git pull

    [ -d $TDENGINE_DIR/debug ] || mkdir $TDENGINE_DIR/debug
    cd $TDENGINE_DIR/debug

    print_color "$GREEN" "rebuild.."
    LOCAL_COMMIT=`git rev-parse --short @`

    rm -rf *
    makecmd="cmake -DBUILD_TEST=false -DBUILD_HTTP=false -DBUILD_DEPENDENCY_TESTS=0 -DBUILD_TOOLS=true -DBUILD_GEOS=true -DBUILD_TEST=true -DBUILD_CONTRIB=false ../../"
    print_color "$GREEN" "$makecmd"
    $makecmd

    make -j 8 install

    print_color "$GREEN" "TDengine build end"
}


# 检查并获取分支名称
if [ -n "$BRANCH" ]; then
    branch="$BRANCH"
    print_color "$GREEN" "Testing branch: $branch "
    print_color "$GREEN" "Build is required for this test！"
    buildTDengine
else
    print_color "$GREEN" "Build is not required for this test！"
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

                # # 记录日志和备份
                # mkdir -p "$BACKUP_DIR/$case_file"
                # tar --exclude='*.sock*' -czf "$BACKUP_DIR/$case_file/sim.tar.gz" -C "$TDENGINE_DIR/.." sim
                # mv "$TDENGINE_DIR/tests/$case_file.log" "$BACKUP_DIR/$case_file"
                
                if [ "$SAVE_LOG" == "save" ]; then
                    mkdir -p "$BACKUP_DIR/$case_file"
                    tar --exclude='*.sock*' -czf "$BACKUP_DIR/$case_file/sim.tar.gz" -C "$TDENGINE_DIR/.." sim
                    mv "$TDENGINE_DIR/tests/$case_file.log" "$BACKUP_DIR/$case_file"
                else
                    echo "This case not save log!"
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
                else
                    echo "This case not save log!"
                fi
                
                end_time=`date +%s`
                echo execution time of $case was `expr $end_time - $start_time`s. | tee -a $TDENGINE_ALLCI_REPORT
            fi
        fi
    done < $1
}

function runUnitTest() {
    print_color "$GREEN" "=== Run unit test case ==="
    print_color "$GREEN" " $TDENGINE_DIR/../debug"
    cd $TDENGINE_DIR/../debug
    ctest -j12
    print_color "$GREEN" "3.0 unit test done"
}

function runSimCases() {
    print_color "$GREEN" "=== Run sim cases ==="

    cd $TDENGINE_DIR/tests/script
    runCasesOneByOne $TDENGINE_DIR/tests/parallel_test/cases.task sim

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

WORK_DIR=/root/

date >> $WORK_DIR/date.log
print_color "$GREEN" "Run all ci test cases" | tee -a $WORK_DIR/date.log

stopTaosd

runTest

date >> $WORK_DIR/date.log
print_color "$GREEN" "End of ci test cases" | tee -a $WORK_DIR/date.log