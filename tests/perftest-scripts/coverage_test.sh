#!/bin/bash

today=`date +"%Y%m%d"`
TDENGINE_DIR=/home/shuduo/work/taosdata/TDengine.cover
TDENGINE_COVERAGE_REPORT=$TDENGINE_DIR/tests/coverage-report-$today.log

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

function buildTDengine {
	echo "check if TDengine need build"
	cd $TDENGINE_DIR
  git remote prune origin > /dev/null
	git remote update > /dev/null
	REMOTE_COMMIT=`git rev-parse --short remotes/origin/develop`
	LOCAL_COMMIT=`git rev-parse --short @`
	echo " LOCAL: $LOCAL_COMMIT"
	echo "REMOTE: $REMOTE_COMMIT"

	# reset counter
	lcov -d . --zerocounters

	cd $TDENGINE_DIR/debug

	if [ "$LOCAL_COMMIT" == "$REMOTE_COMMIT" ]; then
		echo "repo up-to-date"
	else
		echo "repo need to pull"
    git reset --hard
		git pull
  fi

  [ -d $TDENGINE_DIR/debug ] || mkdir $TDENGINE_DIR/debug
  cd $TDENGINE_DIR/debug
  [ -f $TDENGINE_DIR/debug/build/bin/taosd ] || need_rebuild=true

  if $need_rebuild ; then
    echo "rebuild.."
		LOCAL_COMMIT=`git rev-parse --short @`

		rm -rf *
		cmake -DCOVER=true -DRANDOM_FILE_FAIL=true .. > /dev/null
		make > /dev/null
	fi

	make install > /dev/null
}

function runGeneralCaseOneByOne {
	while read -r line; do
		if [[ $line =~ ^./test.sh* ]]; then
			case=`echo $line | grep sim$ |awk '{print $NF}'`

			if [ -n "$case" ]; then
				date +%F\ %T | tee -a  $TDENGINE_COVERAGE_REPORT  && ./test.sh -f $case > /dev/null 2>&1 && \
				( grep -q 'script.*success.*m$' ../../sim/tsim/log/taoslog0.0 && echo -e "${GREEN}$case success${NC}" | tee -a  $TDENGINE_COVERAGE_REPORT ) \
				|| echo -e "${RED}$case failed${NC}" | tee -a  $TDENGINE_COVERAGE_REPORT
			fi
		fi
	done < $1
}

function runTest {
	echo "run Test"

	cd $TDENGINE_DIR/tests/script

	[ -d ../../sim ] && rm -rf ../../sim
	[ -f $TDENGINE_COVERAGE_REPORT ] && rm $TDENGINE_COVERAGE_REPORT

	runGeneralCaseOneByOne jenkins/basic.txt

	sed -i "1i\SIM cases test result" $TDENGINE_COVERAGE_REPORT

	totalSuccess=`grep 'success' $TDENGINE_COVERAGE_REPORT | wc -l`
	if [ "$totalSuccess" -gt "0" ]; then
		sed -i -e "2i\ ### Total $totalSuccess SIM test case(s) succeed! ###"  $TDENGINE_COVERAGE_REPORT
	fi

	totalFailed=`grep 'failed\|fault' $TDENGINE_COVERAGE_REPORT | wc -l`
	if [ "$totalFailed" -ne "0" ]; then
		sed -i '3i\### Total $totalFailed SIM test case(s) failed! ###'  $TDENGINE_COVERAGE_REPORT
	else
		sed -i '3i\\n'  $TDENGINE_COVERAGE_REPORT
	fi

	cd $TDENGINE_DIR/tests
	rm -rf ../sim
	./test-all.sh full python | tee -a $TDENGINE_COVERAGE_REPORT
	
	sed -i "4i\Python cases test result" $TDENGINE_COVERAGE_REPORT
	totalPySuccess=`grep 'python case(s) succeed!' $TDENGINE_COVERAGE_REPORT | awk '{print $4}'`
	if [ "$totalPySuccess" -gt "0" ]; then
                sed -i -e "5i\ ### Total $totalPySuccess Python test case(s) succeed! ###"  $TDENGINE_COVERAGE_REPORT
        fi

	totalPyFailed=`grep 'python case(s) failed!' $TDENGINE_COVERAGE_REPORT | awk '{print $4}'`
        if [ -z $totalPyFailed ]; then
		sed -i '6i\\n'  $TDENGINE_COVERAGE_REPORT
        else
		sed -i '6i\### Total $totalPyFailed Python test case(s) failed! ###'  $TDENGINE_COVERAGE_REPORT
        fi

	# Test Connector
	stopTaosd
	$TDENGINE_DIR/debug/build/bin/taosd -c $TDENGINE_DIR/debug/test/cfg > /dev/null &
	sleep 10

	cd $TDENGINE_DIR/src/connector/jdbc
	mvn clean package > /dev/null 2>&1
	mvn test > /dev/null 2>&1 | tee -a $TDENGINE_COVERAGE_REPORT

	# Test C Demo
	stopTaosd
	$TDENGINE_DIR/debug/build/bin/taosd -c $TDENGINE_DIR/debug/test/cfg > /dev/null &
	sleep 10
	yes | $TDENGINE_DIR/debug/build/bin/demo 127.0.0.1 > /dev/null 2>&1 | tee -a $TDENGINE_COVERAGE_REPORT

	# Test waltest
	dataDir=`grep dataDir $TDENGINE_DIR/debug/test/cfg/taos.cfg|awk '{print $2}'`
	walDir=`find $dataDir -name "wal"|head -n1`
	echo "dataDir: $dataDir" | tee -a $TDENGINE_COVERAGE_REPORT
	echo "walDir: $walDir" | tee -a $TDENGINE_COVERAGE_REPORT
	if [ -n "$walDir" ]; then
		yes | $TDENGINE_DIR/debug/build/bin/waltest -p $walDir > dev/null 2>&1 | tee -a $TDENGINE_COVERAGE_REPORT
	fi

  # run Unit Test
  echo "Run Unit Test: utilTest, queryTest and cliTest"
  $TDENGINE_DIR/debug/build/bin/utilTest > /dev/null 2>&1 && echo "utilTest pass!" || echo "utilTest failed!"
  $TDENGINE_DIR/debug/build/bin/queryTest > /dev/null 2>&1 && echo "queryTest pass!" || echo "queryTest failed!"
  $TDENGINE_DIR/debug/build/bin/cliTest > /dev/null 2>&1 && echo "cliTest pass!" || echo "cliTest failed!"

	stopTaosd
}

function lcovFunc {
	echo "collect data by lcov"
	cd $TDENGINE_DIR

	# collect data
	lcov -d . --capture --rc lcov_branch_coverage=1 --rc genhtml_branch_coverage=1 --no-external -b $TDENGINE_DIR -o coverage.info

	# remove exclude paths
	lcov --remove coverage.info \
    '*/tests/*' '*/test/*' '*/deps/*' '*/plugins/*' '*/taosdef.h' \
    --rc lcov_branch_coverage=1 -o coverage.info

	# generate result
	lcov -l --rc lcov_branch_coverage=1 coverage.info | tee -a $TDENGINE_COVERAGE_REPORT

	# push result to coveralls.io
	coveralls-lcov coverage.info | tee -a $TDENGINE_COVERAGE_REPORT
}

function sendReport {
	echo "send report"
	receiver="sdsang@taosdata.com, sangshuduo@gmail.com, pxiao@taosdata.com"
	mimebody="MIME-Version: 1.0\nContent-Type: text/html; charset=utf-8\n"

	cd $TDENGINE_DIR

	sed -i 's/\x1b\[[0-9;]*m//g' $TDENGINE_COVERAGE_REPORT
	BODY_CONTENT=`cat $TDENGINE_COVERAGE_REPORT`
	echo -e "to: ${receiver}\nsubject: Coverage test report ${today}, commit ID: ${LOCAL_COMMIT}\n\n${today}:\n${BODY_CONTENT}" | \
	(cat - && uuencode $TDENGINE_COVERAGE_REPORT coverage-report-$today.log) | \
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

function runTestRandomFail {
  exec_random_fail_sh=$1
  default_exec_sh=$TDENGINE_DIR/tests/script/sh/exec.sh
	[ -f $exec_random_fail_sh ] && cp $exec_random_fail_sh $default_exec_sh || exit 1

  dnodes_random_fail_py=$TDENGINE_DIR/tests/pytest/util/dnodes-no-random-fail.py
  default_dnodes_py=$TDENGINE_DIR/tests/pytest/util/dnodes.py
	[ -f $dnodes_random_fail_py ] && cp $dnodes_random_fail_py $default_dnodes_py || exit 1

  runTest NoRandomFail
}

WORK_DIR=/home/shuduo/work/taosdata

date >> $WORK_DIR/cron.log
echo "Run Coverage Test" | tee -a $WORK_DIR/cron.log

rm /tmp/core-*

stopTaosd
buildTDengine

runTestRandomFail $TDENGINE_DIR/tests/script/sh/exec-random-fail.sh 
runTestRandomFail $TDENGINE_DIR/tests/script/sh/exec-default.sh 
runTestRandomFail $TDENGINE_DIR/tests/script/sh/exec-no-random-fail.sh 

lcovFunc
sendReport
stopTaosd

date >> $WORK_DIR/cron.log
echo "End of Coverage Test" | tee -a $WORK_DIR/cron.log
