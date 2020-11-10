#!/bin/bash

today=`date +"%Y%m%d"`
WORK_DIR=/home/ubuntu/pxiao/
PERFORMANCE_TEST_REPORT=$TDENGINE_DIR/tests/performance-test-report-$today.log

# Coloured Echoes                                                                                                       #
function red_echo      { echo -e "\033[31m$@\033[0m";   }                                                               #
function green_echo    { echo -e "\033[32m$@\033[0m";   }                                                               #
function yellow_echo   { echo -e "\033[33m$@\033[0m";   }                                                               #
function white_echo    { echo -e "\033[1;37m$@\033[0m"; }                                                               #
# Coloured Printfs                                                                                                      #
function red_printf    { printf "\033[31m$@\033[0m";    }                                                               #
function green_printf  { printf "\033[32m$@\033[0m";    }                                                               #
function yellow_printf { printf "\033[33m$@\033[0m";    }                                                               #
function white_printf  { printf "\033[1;37m$@\033[0m";  }                                                               #
# Debugging Outputs                                                                                                     #
function white_brackets { local args="$@"; white_printf "["; printf "${args}"; white_printf "]"; }                      #
function echoInfo   { local args="$@"; white_brackets $(green_printf "INFO") && echo " ${args}"; }                      #
function echoWarn   { local args="$@";  echo "$(white_brackets "$(yellow_printf "WARN")" && echo " ${args}";)" 1>&2; }  #
function echoError  { local args="$@"; echo "$(white_brackets "$(red_printf    "ERROR")" && echo " ${args}";)" 1>&2; }  #


function stopTaosd {
	echo "Stop taosd"
    systemctl stop taosd
	snap stop tdengine
  	PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
	while [ -n "$PID" ]
	do
    pkill -TERM -x taosd
    sleep 1
  	PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
	done
}

function buildTDengine {
	echoInfo "Build TDengine"
	cd $WORK_DIR/TDengine

	git remote update > /dev/null
	REMOTE_COMMIT=`git rev-parse --short remotes/origin/develop`
	LOCAL_COMMIT=`git rev-parse --short @`

	echo " LOCAL: $LOCAL_COMMIT"
	echo "REMOTE: $REMOTE_COMMIT"
	if [ "$LOCAL_COMMIT" == "$REMOTE_COMMIT" ]; then
		echo "repo up-to-date"
	else
		echo "repo need to pull"
		git pull > /dev/null

		LOCAL_COMMIT=`git rev-parse --short @`
		cd debug
		rm -rf *
		cmake .. > /dev/null
		make > /dev/null
		make install
	fi
}

function runQueryPerfTest {
	nohup $WORK_DIR/TDengine/debug/build/bin/taosd -c /etc/taodperf/ > /dev/null 2>&1 &
	echoInfo "Run Performance Test"
	cd $WORK_DIR/TDengine/tests/pytest
	
	python3 query/queryPerformance.py | tee -a $PERFORMANCE_TEST_REPORT
}


function sendReport {
	echo "send report"
	receiver="pxiao@taosdata.com"
	mimebody="MIME-Version: 1.0\nContent-Type: text/html; charset=utf-8\n"

	cd $TDENGINE_DIR

	sed -i 's/\x1b\[[0-9;]*m//g' $PERFORMANCE_TEST_REPORT
	BODY_CONTENT=`cat $PERFORMANCE_TEST_REPORT`
	echo -e "to: ${receiver}\nsubject: Query Performace Report ${today}, commit ID: ${LOCAL_COMMIT}\n\n${today}:\n${BODY_CONTENT}" | \
	(cat - && uuencode $PERFORMANCE_TEST_REPORT performance-test-report-$today.log) | \
	ssmtp "${receiver}" && echo "Report Sent!"
}


stopTaosd
buildTDengine
runQueryPerfTest

echoInfo "Send Report"
sendReport
echoInfo "End of Test"
