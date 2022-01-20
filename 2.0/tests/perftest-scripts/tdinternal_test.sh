#!/bin/bash

today=`date +"%Y%m%d"`
TDINTERNAL_DIR=/home/shuduo/work/taosdata/TDinternal
TDINTERNAL_TEST_REPORT=$TDINTERNAL_DIR/community/tests/tdinternal-report-$today.log

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

function buildTDinternal {
	echo "check if TDinternal need build"
	cd $TDINTERNAL_DIR
	NEED_COMPILE=0
#	git remote update
	REMOTE_COMMIT=`git rev-parse --short remotes/origin/develop`
	LOCAL_COMMIT=`git rev-parse --short @`
	echo " LOCAL: $LOCAL_COMMIT"
	echo "REMOTE: $REMOTE_COMMIT"
	if [ "$LOCAL_COMMIT" == "$REMOTE_COMMIT" ]; then
		echo "TDinternal repo is up-to-date"
	else
		echo "repo need to pull"
#		git pull

#		NEED_COMPILE=1
	fi

#	git submodule update --init --recursive
	cd $TDINTERNAL_DIR/community
	TDENGINE_REMOTE_COMMIT=`git rev-parse --short remotes/origin/develop`
	TDENGINE_LOCAL_COMMIT=`git rev-parse --short @`
	if [ "$TDENGINE_LOCAL_COMMIT" == "$TDENGINE_REMOTE_COMMIT" ]; then
		echo "community repo is up-to-date"
	else
		echo "repo need to pull"
#		git checkout develop
#		git pull
#		NEED_COMPILE=1
	fi

	cd $TDINTERNAL_DIR/debug

	if [[ $NEED_COMPILE -eq 1 ]]; then
		LOCAL_COMMIT=`git rev-parse --short @`
		rm -rf *
		cmake .. > /dev/null
		make > /dev/null
	fi

	make install > /dev/null
}

function runUniqueCaseOneByOne {
  while read -r line; do
    if [[ $line =~ ^./test.sh* ]]; then
      case=`echo $line | awk '{print $NF}'`
      start_time=`date +%s`
      ./test.sh -f $case > /dev/null 2>&1 && \
        echo -e "${GREEN}$case success${NC}" | tee -a $TDINTERNAL_TEST_REPORT || \
        echo -e "${RED}$case failed${NC}" | tee -a $TDINTERNAL_TEST_REPORT
      end_time=`date +%s`
      echo execution time of $case was `expr $end_time - $start_time`s. | tee -a $TDINTERNAL_TEST_REPORT
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
          find $TDINTERNAL_DIR/community/sim -name "*log" -exec tar czf $TDINTERNAL_DIR/fulltest-$today-$casename.log.tar.gz {} +
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
	cd $TDINTERNAL_DIR/community/tests/script
	[ -d $TDINTERNAL_DIR/sim ] && rm -rf $TDINTERNAL_DIR/sim

	[ -f $TDINTERNAL_TEST_REPORT ] && rm $TDINTERNAL_TEST_REPORT

	runUniqueCaseOneByOne jenkins/basic.txt

	totalSuccess=`grep 'success' $TDINTERNAL_TEST_REPORT | wc -l`

	if [ "$totalSuccess" -gt "0" ]; then
		echo -e "\n${GREEN} ### Total $totalSuccess TDinternal case(s) succeed! ### ${NC}" | tee -a $TDINTERNAL_TEST_REPORT
	fi

	totalFailed=`grep 'failed\|fault' $TDINTERNAL_TEST_REPORT | wc -l`
	if [ "$totalFailed" -ne "0" ]; then
		echo -e "${RED} ### Total $totalFailed TDinternal case(s) failed! ### ${NC}\n" | tee -a $TDINTERNAL_TEST_REPORT
#  exit $totalPyFailed
	fi

	cd $TDINTERNAL_DIR/community/tests/pytest
  [ -d $TDINTERNAL_DIR/community/sim ] && rm -rf $TDINTERNAL_DIR/community/sim
  [ -f pytest-out.log ] && rm -f pytest-out.log

	/usr/bin/time -f "Total spent: %e" ./test-all.sh full python | tee -a $TDINTERNAL_TEST_REPORT
  runPyCaseOneByOne fulltest.sh

  totalPySuccess=`grep 'success' pytest-out.log | wc -l`
  totalPyFailed=`grep 'failed\|fault' pytest-out.log | wc -l`

  cat pytest-out.log >> $TDINTERNAL_TEST_REPORT
  if [ "$totalPySuccess" -gt "0" ]; then
    echo -e "\n${GREEN} ### Total $totalPySuccess python case(s) succeed! ### ${NC}" \
      | tee -a $TDINTERNAL_TEST_REPORT
  fi

  if [ "$totalPyFailed" -ne "0" ]; then
    echo -e "\n${RED} ### Total $totalPyFailed python case(s) failed! ### ${NC}" \
      | tee -a $TDINTERNAL_TEST_REPORT
  fi
}

function sendReport {
	echo "Send Report"
	receiver="sdsang@taosdata.com, sangshuduo@gmail.com, pxiao@taosdata.com"
	mimebody="MIME-Version: 1.0\nContent-Type: text/html; charset=utf-8\n"

	cd $TDINTERNAL_DIR

	sed -i 's/\x1b\[[0-9;]*m//g' $TDINTERNAL_TEST_REPORT
	BODY_CONTENT=`cat $TDINTERNAL_TEST_REPORT`

  cd $TDINTERNAL_DIR
  tar czf fulltest-$today.tar.gz fulltest-$today-*.log.tar.gz

	echo -e "to: ${receiver}\nsubject: TDinternal test report ${today}, commit ID: ${LOCAL_COMMIT}\n\n${today}:\n${BODY_CONTENT}" | \
	(cat - && uuencode $TDINTERNAL_TEST_REPORT tdinternal-report-$today.log) | \
	ssmtp "${receiver}" && echo "Report Sent!"
}

function stopTaosd {
	echo "Stop taosd"
        systemctl stop taosd
  	PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
	while [ -n "$PID" ]
	do
        	pkill -KILL -x taosd
        	sleep 1
  		PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
	done
}

WORK_DIR=/home/shuduo/work/taosdata

date >> $WORK_DIR/cron.log
echo "Run Test for TDinternal" | tee -a $WORK_DIR/cron.log

buildTDinternal
runTest
sendReport
stopTaosd

date >> $WORK_DIR/cron.log
echo "End of TDinternal Test" | tee -a $WORK_DIR/cron.log
