#!/bin/bash

branch=
if [ x$1 != x ];then
        branch=$1
        echo "Testing branch: $branch"
else
        echo "Please enter branch name as a parameter"
        exit 1
fi

today=`date +"%Y%m%d"`
TDENGINE_DIR=/root/pxiao/TDengine
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
	git clean -dfx
	git pull
    	git submodule update --init --recursive -f
 	
  	[ -d $TDENGINE_DIR/debug ] || mkdir $TDENGINE_DIR/debug
  	cd $TDENGINE_DIR/debug

    	echo "rebuild.."
	LOCAL_COMMIT=`git rev-parse --short @`

	rm -rf *
	if [ "$branch" == "3.0" ]; then
		echo "3.0 ============="
		cmake -DCOVER=true -DBUILD_TEST=true .. 
	else
		cmake -DCOVER=true -DBUILD_TOOLS=true -DBUILD_HTTP=false .. > /dev/null
	fi
	make -j4
	make install
}

function runGeneralCaseOneByOne {
	while read -r line; do
		if [[ $line =~ ^./test.sh* ]]; then
			case=`echo $line | grep sim$ | awk '{print $NF}'`

			if [ -n "$case" ]; then
				date +%F\ %T | tee -a  $TDENGINE_COVERAGE_REPORT  && ./test.sh -f $case > /dev/null 2>&1 && \
				 echo -e "${GREEN}$case success${NC}" | tee -a  $TDENGINE_COVERAGE_REPORT \
				|| echo -e "${RED}$case failed${NC}" | tee -a  $TDENGINE_COVERAGE_REPORT
			fi
		fi
	done < $1
}

function runTestNGCaseOneByOne {
	while read -r line; do
		if [[ $line =~ ^taostest* ]]; then
			case=`echo $line | cut -d' ' -f 3 | cut -d'=' -f 2`
			yaml=`echo $line | cut -d' ' -f 2`

			if [ -n "$case" ]; then
				date +%F\ %T | tee -a  $TDENGINE_COVERAGE_REPORT  && taostest $yaml --case=$case --keep --disable_collection > /dev/null 2>&1 && \
				 echo -e "${GREEN}$case success${NC}" | tee -a  $TDENGINE_COVERAGE_REPORT \
				|| echo -e "${RED}$case failed${NC}" | tee -a  $TDENGINE_COVERAGE_REPORT
			fi
		fi
	done < $1
}

function runTest {
	echo "run Test"

	if [ "$branch" == "3.0" ]; then
		echo "start run unit test case ................"
		echo " $TDENGINE_DIR/debug "
		cd $TDENGINE_DIR/debug
		ctest -j12
		echo "3.0 unit test done"
	fi

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
		sed -i "3i\### Total $totalFailed SIM test case(s) failed! ###"  $TDENGINE_COVERAGE_REPORT
	fi
	sed "3G"  $TDENGINE_COVERAGE_REPORT

	stopTaosd
	echo "run TestNG cases"
	rm -rf /var/lib/taos/*
	rm -rf /var/log/taos/*
        nohup $TDENGINE_DIR/debug/build/bin/taosd -c /etc/taos > /dev/null 2>&1 &
        sleep 10
	cd $TDENGINE_DIR/../TestNG/cases
	runTestNGCaseOneByOne ../scripts/cases.txt
	echo "TestNG cases done"

	cd $TDENGINE_DIR/tests
	rm -rf ../sim
	/root/pxiao/test-all-coverage.sh full python $branch | tee -a $TDENGINE_COVERAGE_REPORT
	
	
	sed -i "4i\Python cases test result" $TDENGINE_COVERAGE_REPORT
	totalPySuccess=`grep 'python case(s) succeed!' $TDENGINE_COVERAGE_REPORT | awk '{print $4}'`
	if [ "$totalPySuccess" -gt "0" ]; then
                sed -i -e "5i\ ### Total $totalPySuccess Python test case(s) succeed! ###"  $TDENGINE_COVERAGE_REPORT
        fi

	totalPyFailed=`grep 'python case(s) failed!' $TDENGINE_COVERAGE_REPORT | awk '{print $4}'`
        if [ -z $totalPyFailed ]; then
		sed -i "6i\\n"  $TDENGINE_COVERAGE_REPORT
        else
		sed -i "6i\### Total $totalPyFailed Python test case(s) failed! ###"  $TDENGINE_COVERAGE_REPORT
        fi

	echo "### run JDBC test cases ###" | tee -a $TDENGINE_COVERAGE_REPORT
	# Test Connector
	stopTaosd
	nohup $TDENGINE_DIR/debug/build/bin/taosd -c /etc/taos > /dev/null 2>&1 &
	sleep 10

	cd $TDENGINE_DIR/src/connector/jdbc
	mvn clean package > /dev/null 2>&1
	mvn test > jdbc-out.log 2>&1
  	tail -n 20 jdbc-out.log 2>&1 | tee -a $TDENGINE_COVERAGE_REPORT

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
		yes | $TDENGINE_DIR/debug/build/bin/waltest -p $walDir > /dev/null 2>&1 | tee -a $TDENGINE_COVERAGE_REPORT
	fi

  # run Unit Test
  echo "Run Unit Test: utilTest, queryTest and cliTest"
  #$TDENGINE_DIR/debug/build/bin/utilTest > /dev/null 2>&1 && echo "utilTest pass!" || echo "utilTest failed!"
  #$TDENGINE_DIR/debug/build/bin/queryTest > /dev/null 2>&1 && echo "queryTest pass!" || echo "queryTest failed!"
  #$TDENGINE_DIR/debug/build/bin/cliTest > /dev/null 2>&1 && echo "cliTest pass!" || echo "cliTest failed!"

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
	lcov -d . --capture --rc lcov_branch_coverage=1 --rc genhtml_branch_coverage=1 --no-external -b $TDENGINE_DIR -o coverage.info

	# remove exclude paths
	if [ "$branch" == "3.0" ]; then
		lcov --remove coverage.info \
			'*/contrib/*' '*/tests/*' '*/test/*'\
		       	'*/AccessBridgeCalls.c' '*/ttszip.c' '*/dataInserter.c' '*/tlinearhash.c' '*/tsimplehash.c'\
			'*/texpr.c' '*/runUdf.c' '*/schDbg.c' '*/syncIO.c' '*/tdbOs.c' '*/pushServer.c' '*/osLz4.c'\
		       	'*/tbase64.c' '*/tbuffer.c' '*/tdes.c' '*/texception.c' '*/tidpool.c' '*/tmempool.c'\
		       	'*/tthread.c' '*/tversion.c'\
		       	--rc lcov_branch_coverage=1 -o coverage.info
	else	
		lcov --remove coverage.info \
    		'*/tests/*' '*/test/*' '*/deps/*' '*/plugins/*' '*/taosdef.h' '*/ttype.h' '*/tarithoperator.c' '*/TSDBJNIConnector.c' '*/taosdemo.c'\
    		--rc lcov_branch_coverage=1 -o coverage.info
	fi


	# generate result
	echo "generate result"
	lcov -l --rc lcov_branch_coverage=1 coverage.info | tee -a $TDENGINE_COVERAGE_REPORT

	# push result to coveralls.io
	echo "push result to coveralls.io"
	/usr/local/bin/coveralls-lcov coverage.info -t o7uY02qEAgKyJHrkxLGiCOTfL3IGQR2zm | tee -a $TDENGINE_COVERAGE_REPORT

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

function runTestRandomFail {
  exec_random_fail_sh=$1
  default_exec_sh=$TDENGINE_DIR/tests/script/sh/exec.sh
	[ -f $exec_random_fail_sh ] && cp $exec_random_fail_sh $default_exec_sh || exit 1

  dnodes_random_fail_py=$TDENGINE_DIR/tests/pytest/util/dnodes-no-random-fail.py
  default_dnodes_py=$TDENGINE_DIR/tests/pytest/util/dnodes.py
	[ -f $dnodes_random_fail_py ] && cp $dnodes_random_fail_py $default_dnodes_py || exit 1

  runTest NoRandomFail
}

WORK_DIR=/root/pxiao

date >> $WORK_DIR/cron.log
echo "Run Coverage Test" | tee -a $WORK_DIR/cron.log

stopTaosd
buildTDengine

#runTestRandomFail $TDENGINE_DIR/tests/script/sh/exec-random-fail.sh 
#runTestRandomFail $TDENGINE_DIR/tests/script/sh/exec-default.sh 
#runTestRandomFail $TDENGINE_DIR/tests/script/sh/exec-no-random-fail.sh 

runTest

lcovFunc
#sendReport
stopTaosd

date >> $WORK_DIR/cron.log
echo "End of Coverage Test" | tee -a $WORK_DIR/cron.log
