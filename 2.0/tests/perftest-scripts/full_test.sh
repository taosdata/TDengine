#!/bin/bash

today=`date +"%Y%m%d"`
TDENGINE_DIR=/home/shuduo/work/taosdata/TDengine.orig
TDENGINE_FULLTEST_REPORT=$TDENGINE_DIR/tests/full-report-$today.log

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

function buildTDengine {
  echo "check if TDengine need build"

  need_rebuild=false

  if [ ! -d $TDENGINE_DIR ]; then
    echo "No TDengine source code found!"
    git clone https://github.com/taosdata/TDengine $TDENGINE_DIR
    need_rebuild=true
  fi

  cd $TDENGINE_DIR
  git remote prune origin > /dev/null
  git remote update > /dev/null
  REMOTE_COMMIT=`git rev-parse --short remotes/origin/develop`
  LOCAL_COMMIT=`git rev-parse --short @`
  echo " LOCAL: $LOCAL_COMMIT"
  echo "REMOTE: $REMOTE_COMMIT"

  if [ "$LOCAL_COMMIT" == "$REMOTE_COMMIT" ]; then
    echo "repo up-to-date"
  else
    echo "repo need to pull"
    git pull
    need_rebuild=true
  fi

  [ -d $TDENGINE_DIR/debug ] || mkdir $TDENGINE_DIR/debug
  cd $TDENGINE_DIR/debug
  [ -f $TDENGINE_DIR/debug/build/bin/taosd ] || need_rebuild=true

  if $need_rebuild ; then
    echo "rebuild.."

    LOCAL_COMMIT=`git rev-parse --short @`
    rm -rf *
    cmake .. > /dev/null
    make > /dev/null
  fi

  make install > /dev/null
}

function runGeneralCaseOneByOne {
  while read -r line; do
    if [[ $line =~ ^./test.sh* ]]; then
      general_case=`echo $line | grep -w general`

      if [ -n "$general_case" ]; then
        case=`echo $line | awk '{print $NF}'`

        start_time=`date +%s`
        ./test.sh -f $case > /dev/null 2>&1 && ret=0 || ret = 1
        end_time=`date +%s`

        if [[ ret -eq 0 ]]; then
          echo -e "${GREEN}$case success${NC}" | tee -a $TDENGINE_FULLTEST_REPORT
        else
          casename=`echo $case|sed 's/\//\-/g'`
          find $TDENGINE_DIR/sim -name "*log" -exec tar czf $TDENGINE_DIR/fulltest-$today-$casename.log.tar.gz {} +
          echo -e "${RED}$case failed and log saved${NC}" | tee -a $TDENGINE_FULLTEST_REPORT
        fi
        echo execution time of $case was `expr $end_time - $start_time`s. | tee -a $TDENGINE_FULLTEST_REPORT
      fi
    fi
  done < $1
}

function runPyCaseOneByOne {
  while read -r line; do
    if [[ $line =~ ^python.* ]]; then
      if [[ $line != *sleep* ]]; then
        case=`echo $line|awk '{print $NF}'`
        start_time=`date +%s`
        $line > /dev/null 2>&1 && ret=0 || ret=1
        end_time=`date +%s`

        if [[ ret -eq 0 ]]; then
          echo -e "${GREEN}$case success${NC}" | tee -a pytest-out.log
        else
          casename=`echo $case|sed 's/\//\-/g'`
          find $TDENGINE_DIR/sim -name "*log" -exec tar czf $TDENGINE_DIR/fulltest-$today-$casename.log.tar.gz {} +
          echo -e "${RED}$case failed and log saved${NC}" | tee -a pytest-out.log
        fi
        echo execution time of $case was `expr $end_time - $start_time`s. | tee -a pytest-out.log
      else
        $line > /dev/null 2>&1
      fi
    fi
  done < $1
}

function runTest {
  echo "Run Test"
  cd $TDENGINE_DIR/tests/script

  [ -d $TDENGINE_DIR/sim ] && rm -rf $TDENGINE_DIR/sim
  [ -f $TDENGINE_FULLTEST_REPORT ] && rm $TDENGINE_FULLTEST_REPORT

  runGeneralCaseOneByOne jenkins/basic.txt

  totalSuccess=`grep 'success' $TDENGINE_FULLTEST_REPORT | wc -l`

  if [ "$totalSuccess" -gt "0" ]; then
    echo -e "\n${GREEN} ### Total $totalSuccess SIM case(s) succeed! ### ${NC}" \
      | tee -a $TDENGINE_FULLTEST_REPORT
  fi

  totalFailed=`grep 'failed\|fault' $TDENGINE_FULLTEST_REPORT | wc -l`
  if [ "$totalFailed" -ne "0" ]; then
    echo -e "${RED} ### Total $totalFailed SIM case(s) failed! ### ${NC}\n" \
    | tee -a $TDENGINE_FULLTEST_REPORT
  fi

  cd $TDENGINE_DIR/tests/pytest
  [ -d $TDENGINE_DIR/sim ] && rm -rf $TDENGINE_DIR/sim
  [ -f pytest-out.log ] && rm -f pytest-out.log
  runPyCaseOneByOne fulltest.sh

  totalPySuccess=`grep 'success' pytest-out.log | wc -l`
  totalPyFailed=`grep 'failed\|fault' pytest-out.log | wc -l`

  cat pytest-out.log >> $TDENGINE_FULLTEST_REPORT
  if [ "$totalPySuccess" -gt "0" ]; then
    echo -e "\n${GREEN} ### Total $totalPySuccess python case(s) succeed! ### ${NC}" \
      | tee -a $TDENGINE_FULLTEST_REPORT
  fi

  if [ "$totalPyFailed" -ne "0" ]; then
    echo -e "\n${RED} ### Total $totalPyFailed python case(s) failed! ### ${NC}" \
      | tee -a $TDENGINE_FULLTEST_REPORT
  fi
}

function sendReport {
  echo "Send Report"
  receiver="sdsang@taosdata.com, sangshuduo@gmail.com, pxiao@taosdata.com"
  mimebody="MIME-Version: 1.0\nContent-Type: text/html; charset=utf-8\n"

  cd $TDENGINE_DIR/tests

  sed -i 's/\x1b\[[0-9;]*m//g' $TDENGINE_FULLTEST_REPORT
  BODY_CONTENT=`cat $TDENGINE_FULLTEST_REPORT`

  cd $TDENGINE_DIR
  tar czf fulltest-$today.tar.gz fulltest-$today-*.log.tar.gz

  echo -e "to: ${receiver}\nsubject: Full test report ${today}, commit ID: ${LOCAL_COMMIT}\n\n${today}:\n${BODY_CONTENT}" | \
  (cat - && uuencode $TDENGINE_FULLTEST_REPORT fulltest-report-$today.log) | \
  (cat - && uuencode $TDENGINE_DIR/fulltest-$today.tar.gz fulltest-$today.tar.gz) | \
  ssmtp "${receiver}" && echo "Report Sent!"
}

function stopTaosd {
  echo "Stop taosd"
  systemctl stop taosd
  PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
  while [ -n "$PID" ]
  do
    pkill -TERM -x taosd
    sleep 1
    PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
  done
}

WORK_DIR=/home/shuduo/work/taosdata

date >> $WORK_DIR/cron.log
echo "Run Full Test" | tee -a $WORK_DIR/cron.log

stopTaosd
buildTDengine
runTest
sendReport
stopTaosd

date >> $WORK_DIR/cron.log
echo "End of Full Test" | tee -a $WORK_DIR/cron.log
