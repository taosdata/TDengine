#!/bin/bash

today=`date +"%Y%m%d"`
WORK_DIR=/home/ubuntu/pxiao
PERFORMANCE_TEST_REPORT=$WORK_DIR/TDengine/tests/performance-test-report-$today.log

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
	git reset --hard HEAD
	git checkout develop
	REMOTE_COMMIT=`git rev-parse --short remotes/origin/develop`
	LOCAL_COMMIT=`git rev-parse --short @`

	echo " LOCAL: $LOCAL_COMMIT"
	echo "REMOTE: $REMOTE_COMMIT"
	if [ "$LOCAL_COMMIT" == "$REMOTE_COMMIT" ]; then
		echo "repo up-to-date"
	else
		echo "repo need to pull"
		git pull > /dev/null 2>&1

		LOCAL_COMMIT=`git rev-parse --short @`
		cd debug
		rm -rf *
		cmake .. > /dev/null
		make && make install > /dev/null		
	fi
}

function runQueryPerfTest {
	[ -f $PERFORMANCE_TEST_REPORT ] && rm $PERFORMANCE_TEST_REPORT
	nohup $WORK_DIR/TDengine/debug/build/bin/taosd -c /etc/taosperf/ > /dev/null 2>&1 &
	echoInfo "Wait TDengine to start"
	sleep 120
	echoInfo "Run Performance Test"	
	cd $WORK_DIR/TDengine/tests/pytest
	
	python3 query/queryPerformance.py -c $LOCAL_COMMIT | tee -a $PERFORMANCE_TEST_REPORT

	python3 insert/insertFromCSVPerformance.py -c $LOCAL_COMMIT | tee -a $PERFORMANCE_TEST_REPORT

	taosdemo -f /home/ubuntu/pxiao/insert.json > taosdemoperf.txt

	CREATETABLETIME=`grep 'Spent' taosdemoperf.txt | awk 'NR==1{print $2}'`
	INSERTRECORDSTIME=`grep 'Spent' taosdemoperf.txt | awk 'NR==2{print $2}'`
	REQUESTSPERSECOND=`grep 'Spent' taosdemoperf.txt | awk 'NR==2{print $16}'`
	delay=`grep 'delay' taosdemoperf.txt | awk '{print $4}'`
	AVGDELAY=`echo ${delay:0:${#delay}-3}`
	delay=`grep 'delay' taosdemoperf.txt | awk '{print $6}'`		
	MAXDELAY=`echo ${delay:0:${#delay}-3}`	
	delay=`grep 'delay' taosdemoperf.txt | awk '{print $8}'`
	MINDELAY=`echo ${delay:0:${#delay}-2}`	
	
	python3 tools/taosdemoPerformance.py -c $LOCAL_COMMIT -t $CREATETABLETIME -i $INSERTRECORDSTIME -r $REQUESTSPERSECOND -avg $AVGDELAY -max $MAXDELAY -min $MINDELAY  | tee -a $PERFORMANCE_TEST_REPORT
	[ -f taosdemoperf.txt ] && rm taosdemoperf.txt
}


function sendReport {
	echo "send report"
	receiver="develop@taosdata.com"
	mimebody="MIME-Version: 1.0\nContent-Type: text/html; charset=utf-8\n"

	cd $TDENGINE_DIR

	sed -i 's/\x1b\[[0-9;]*m//g' $PERFORMANCE_TEST_REPORT
	BODY_CONTENT=`cat $PERFORMANCE_TEST_REPORT`
	echo -e "From: <support@taosdata.com>\nto: ${receiver}\nsubject: Query Performace Report ${today}, commit ID: ${LOCAL_COMMIT}\n\n${today}:\n${BODY_CONTENT}" | \
	(cat - && uuencode $PERFORMANCE_TEST_REPORT performance-test-report-$today.log) | \
	/usr/sbin/ssmtp "${receiver}" && echo "Report Sent!"
}


stopTaosd
buildTDengine
runQueryPerfTest
stopTaosd

echoInfo "Send Report"
sendReport
echoInfo "End of Test"
