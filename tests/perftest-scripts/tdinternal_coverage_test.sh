#!/bin/bash

today=`date +"%Y%m%d"`
TDINTERNAL_DIR=/home/shuduo/work/taosdata/TDinternal.cover
TDINTERNAL_COVERAGE_REPORT=$TDINTERNAL_DIR/community/tests/tdinternal-coverage-report-$today.log

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

  lcov -d . --zerocounters
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
        echo -e "${GREEN}$case success${NC}" | tee -a $TDINTERNAL_COVERAGE_REPORT || \
        echo -e "${RED}$case failed${NC}" | tee -a $TDINTERNAL_COVERAGE_REPORT
      end_time=`date +%s`
      echo execution time of $case was `expr $end_time - $start_time`s. | tee -a $TDINTERNAL_COVERAGE_REPORT
    fi
  done < $1
}

function runTest {
	echo "Run Test"
	cd $TDINTERNAL_DIR/community/tests/script
	[ -d ../../sim ] && rm -rf ../../sim

	[ -f $TDINTERNAL_COVERAGE_REPORT ] && rm $TDINTERNAL_COVERAGE_REPORT

	runUniqueCaseOneByOne jenkins/basic.txt

	totalSuccess=`grep 'success' $TDINTERNAL_COVERAGE_REPORT | wc -l`

	if [ "$totalSuccess" -gt "0" ]; then
		echo -e "\n${GREEN} ### Total $totalSuccess TDinternal case(s) succeed! ### ${NC}" | tee -a $TDINTERNAL_COVERAGE_REPORT
	fi

	totalFailed=`grep 'failed\|fault' $TDINTERNAL_COVERAGE_REPORT | wc -l`
	if [ "$totalFailed" -ne "0" ]; then
		echo -e "${RED} ### Total $totalFailed TDinternal case(s) failed! ### ${NC}\n" | tee -a $TDINTERNAL_COVERAGE_REPORT
#  exit $totalPyFailed
	fi

	# Test Python test case
	cd $TDINTERNAL_DIR/community/tests
	/usr/bin/time -f "Total spent: %e" ./test-all.sh full python | tee -a $TDINTERNAL_COVERAGE_REPORT

	# Test Connector
	stopTaosd
	$TDINTERNAL_DIR/debug/build/bin/taosd -c $TDINTERNAL_DIR/debug/test/cfg > /dev/null &
	sleep 10

	cd $TDINTERNAL_DIR/community/src/connector/jdbc
	mvn clean package > /dev/null
	mvn test > /dev/null | tee -a $TDINTERNAL_COVERAGE_REPORT

	# Test C Demo
	stopTaosd
	$TDINTERNAL_DIR/debug/build/bin/taosd -c $TDINTERNAL_DIR/debug/test/cfg > /dev/null &
	sleep 10
	yes | $TDINTERNAL_DIR/debug/build/bin/demo 127.0.0.1 > /dev/null | tee -a $TDINTERNAL_COVERAGE_REPORT

	# Test waltest
	dataDir=`grep dataDir $TDINTERNAL_DIR/debug/test/cfg/taos.cfg|awk '{print $2}'`
	walDir=`find $dataDir -name "wal"|head -n1`
	echo "dataDir: $dataDir" | tee -a $TDINTERNAL_COVERAGE_REPORT
  echo "walDir: $walDir" | tee -a $TDINTERNAL_COVERAGE_REPORT
	if [ -n "$walDir" ]; then
		yes | $TDINTERNAL_DIR/debug/build/bin/waltest -p $walDir > /dev/null | tee -a $TDINTERNAL_COVERAGE_REPORT
	fi

  # run Unit Test
  echo "Run Unit Test: utilTest, queryTest and cliTest"
  $TDINTERNAL_DIR/debug/build/bin/utilTest > /dev/null 2>&1 && echo "utilTest pass!" || echo "utilTest failed!"
  $TDINTERNAL_DIR/debug/build/bin/queryTest > /dev/null 2>&1 && echo "queryTest pass!" || echo "queryTest failed!"
  $TDINTERNAL_DIR/debug/build/bin/cliTest > /dev/null 2>&1 && echo "cliTest pass!" || echo "cliTest failed!"

	stopTaosd
}

function sendReport {
	echo "Send Report"
	receiver="sdsang@taosdata.com, sangshuduo@gmail.com, pxiao@taosdata.com"
	mimebody="MIME-Version: 1.0\nContent-Type: text/html; charset=utf-8\n"

	cd $TDINTERNAL_DIR

	sed -i 's/\x1b\[[0-9;]*m//g' $TDINTERNAL_COVERAGE_REPORT

	BODY_CONTENT=`cat $TDINTERNAL_COVERAGE_REPORT`
	echo -e "to: ${receiver}\nsubject: TDinternal coverage test report ${today}, commit ID: ${LOCAL_COMMIT}\n\n${today}:\n${BODY_CONTENT}" | \
	(cat - && uuencode tdinternal-coverage-report-$today.tar.gz tdinternal-coverage-report-$today.tar.gz) | \
	(cat - && uuencode $TDINTERNAL_COVERAGE_REPORT tdinternal-coverage-report-$today.log) | \
	ssmtp "${receiver}" && echo "Report Sent!"
}

function lcovFunc {
	echo "collect data by lcov"
	cd $TDINTERNAL_DIR

	sed -i 's/\x1b\[[0-9;]*m//g' $TDINTERNAL_COVERAGE_REPORT
	# collect data
	lcov -d . --capture --rc lcov_branch_coverage=1 --rc genhtmml_branch_coverage=1 --no-external -b $TDINTERNAL_DIR -o coverage.info

	# remove exclude paths
	lcov --remove coverage.info '*/tests/*' '*/test/*' '*/deps/*' '*/plugins/*' '*/taosdef.h' \
    --rc lcov_branch_coverage=1 -o coverage.info

	# generate result
	lcov -l --rc lcov_branch_coverage=1 coverage.info | tee -a $TDINTERNAL_COVERAGE_REPORT

	genhtml -o html coverage.info

	tar czf tdinternal-coverage-report-$today.tar.gz html coverage.info $TDINTERNAL_COVERAGE_REPORT
	# push result to coveralls.io
#	coveralls-lcov coverage.info | tee -a tdinternal-coverage-report-$today.log
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
echo "Run Coverage Test for TDinternal" | tee -a $WORK_DIR/cron.log

stopTaosd
buildTDinternal
runTest
lcovFunc
sendReport
stopTaosd

date >> $WORK_DIR/cron.log
echo "End of TDinternal Coverage Test" | tee -a $WORK_DIR/cron.log
