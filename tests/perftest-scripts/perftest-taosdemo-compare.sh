#!/bin/bash

WORK_DIR=/home/ubuntu/pxiao
TDENGINE_DIR=/home/ubuntu/pxiao/TDengine
NUM_OF_VERSIONS=5
CURRENT_VERSION=0
today=`date +"%Y%m%d"`
TAOSDEMO_COMPARE_TEST_REPORT=$TDENGINE_DIR/tests/taosdemo-compare-test-report-$today.log

# Coloured Echoes 
function red_echo      { echo -e "\033[31m$@\033[0m";   }
function green_echo    { echo -e "\033[32m$@\033[0m";   }
function yellow_echo   { echo -e "\033[33m$@\033[0m";   }
function white_echo    { echo -e "\033[1;37m$@\033[0m"; }
# Coloured Printfs
function red_printf    { printf "\033[31m$@\033[0m";    }
function green_printf  { printf "\033[32m$@\033[0m";    }
function yellow_printf { printf "\033[33m$@\033[0m";    }
function white_printf  { printf "\033[1;37m$@\033[0m";  }
# Debugging Outputs
function white_brackets { local args="$@"; white_printf "["; printf "${args}"; white_printf "]"; }
function echoInfo   { local args="$@"; white_brackets $(green_printf "INFO") && echo " ${args}"; }
function echoWarn   { local args="$@";  echo "$(white_brackets "$(yellow_printf "WARN")" && echo " ${args}";)" 1>&2; }
function echoError  { local args="$@"; echo "$(white_brackets "$(red_printf    "ERROR")" && echo " ${args}";)" 1>&2; }

function getCurrentVersion {
	echoInfo "Build TDengine"
	cd $WORK_DIR/TDengine

	git remote update > /dev/null
	git reset --hard HEAD
	git checkout master
	REMOTE_COMMIT=`git rev-parse --short remotes/origin/master`
	LOCAL_COMMIT=`git rev-parse --short @`

	echo " LOCAL: $LOCAL_COMMIT"
	echo "REMOTE: $REMOTE_COMMIT"
	if [ "$LOCAL_COMMIT" == "$REMOTE_COMMIT" ]; then
		echo "repo up-to-date"
	else
		echo "repo need to pull"		
		git pull > /dev/null 2>&1				
	fi
	cd debug
	rm -rf *
	cmake .. > /dev/null 2>&1
	make > /dev/null 2>&1
	make install > /dev/null 2>&1

	rm -rf $WORK_DIR/taosdemo
	cp -r $TDENGINE_DIR/src/kit/taosdemo $WORK_DIR	
	CURRENT_VERSION=`taosd -V | grep version | awk '{print $3}' | awk -F. '{print $3}'`
}

function buildTDengineByVersion() {
	echoInfo "build TDengine on branch: $1"
	git reset --hard HEAD
	git checkout $1
	git pull > /dev/null

	rm -rf $TDENGINE_DIR/src/kit/taosdemo
	cp -r $WORK_DIR/taosdemo $TDENGINE_DIR/src/kit	
	
	cd $TDENGINE_DIR/debug
	rm -rf *
	cmake .. > /dev/null 2>&1
	make > /dev/null 2>&1
	make install > /dev/null 2>&1
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

function startTaosd {
	echo "Start taosd"
	rm -rf /var/lib/perf/*
	rm -rf /var/log/perf/*
	nohup taosd -c /etc/perf/ > /dev/null 2>&1 &
	sleep 10
}

function runTaosdemoCompare {
	echoInfo "Stop Taosd"
	stopTaosd

	getCurrentVersion	
	release="master"

	[ -f $TAOSDEMO_COMPARE_TEST_REPORT ] && rm $TAOSDEMO_COMPARE_TEST_REPORT

	for((i=0;i<$NUM_OF_VERSIONS;i++))
	do
		startTaosd
		taos -s "drop database if exists demodb;"	
		taosdemo -y -d demodb > taosdemoperf.txt

		echo "==================== taosdemo performance for $release ====================" | tee -a $TAOSDEMO_COMPARE_TEST_REPORT
		CREATE_TABLE_TIME=`grep 'Spent' taosdemoperf.txt | awk 'NR==1{print $2}'`
		INSERT_RECORDS_TIME=`grep 'Spent' taosdemoperf.txt | awk 'NR==2{print $2}'`
		RECORDS_PER_SECOND=`grep 'Spent' taosdemoperf.txt | awk 'NR==2{print $16}'`
		AVG_DELAY=`grep 'delay' taosdemoperf.txt | awk '{print $4}' | awk -Fm '{print $1}'`
		MAX_DELAY=`grep 'delay' taosdemoperf.txt | awk '{print $6}' | awk -Fm '{print $1}'`
		MIN_DELAY=`grep 'delay' taosdemoperf.txt | awk '{print $8}' | awk -Fm '{print $1}'`

		echo "create table time: $CREATE_TABLE_TIME seconds" | tee -a $TAOSDEMO_COMPARE_TEST_REPORT
		echo "insert records time: $INSERT_RECORDS_TIME seconds" | tee -a $TAOSDEMO_COMPARE_TEST_REPORT
		echo "records per second: $RECORDS_PER_SECOND records/second" | tee -a $TAOSDEMO_COMPARE_TEST_REPORT
		echo "avg delay: $AVG_DELAY ms" | tee -a $TAOSDEMO_COMPARE_TEST_REPORT
		echo "max delay: $MAX_DELAY ms" | tee -a $TAOSDEMO_COMPARE_TEST_REPORT
		echo "min delay: $MIN_DELAY ms" | tee -a $TAOSDEMO_COMPARE_TEST_REPORT

		[ -f taosdemoperf.txt ] && rm taosdemoperf.txt

		stopTaosd
		version=`expr $CURRENT_VERSION - $i`
		release="release/s1$version"
		buildTDengineByVersion $release
	done
}

function sendReport {
	echo "send report"
	receiver="develop@taosdata.com"
	mimebody="MIME-Version: 1.0\nContent-Type: text/html; charset=utf-8\n"

	cd $TDENGINE_DIR

	sed -i 's/\x1b\[[0-9;]*m//g' $TAOSDEMO_COMPARE_TEST_REPORT
	BODY_CONTENT=`cat $TAOSDEMO_COMPARE_TEST_REPORT`
	echo -e "to: ${receiver}\nsubject: taosdemo performance compare test report ${today}, commit ID: ${LOCAL_COMMIT}\n\n${today}:\n${BODY_CONTENT}" | \
	(cat - && uuencode $TAOSDEMO_COMPARE_TEST_REPORT taosdemo-compare-test-report-$today.log) | \
	ssmtp "${receiver}" && echo "Report Sent!"
}

runTaosdemoCompare
sendReport

echoInfo "End of Taosdemo Compare Test" | tee -a $WORK_DIR/cron.log