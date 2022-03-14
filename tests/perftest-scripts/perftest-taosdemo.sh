#!/bin/bash

WORK_DIR=/mnt/root
TDENGINE_DIR=/root/TDengine

walLevel=`grep "^walLevel" /etc/taos/taos.cfg | awk '{print $2}'`
if [[ "$walLevel" -eq "2" ]]; then
	walPostfix="wal2"
elif [[ "$walLevel" -eq "1" ]]; then
	walPostfix="wal1"
else
	echo -e "${RED}wrong walLevel $walLevel found! ${NC}"
	exit 1
fi

logDir=`grep "^logDir" /etc/taos/taos.cfg | awk '{print $2}'`
dataDir=`grep "^dataDir" /etc/taos/taos.cfg | awk '{print $2}'`

[ -z "$logDir" ] && logDir="/var/log/taos"
[ -z "$dataDir" ] && dataDir="/var/lib/taos"

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

function restartTaosd {
        echo "Stop taosd"
	systemctl stop taosd
	PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
	while [ -n "$PID" ]
	do
		pkill -TERM -x taosd
		sleep 1
		PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
	done

	rm -rf $logDir/*
	rm -rf $dataDir/*
	
        echo "Start taosd"
	taosd 2>&1 > /dev/null &
	sleep 10
}

function runCreateTableOnly {
	echoInfo "Restart Taosd"
	restartTaosd

	/usr/bin/time -f "Total: %e" -o totaltime.out bash -c "yes | taosdemo -n 0 2>&1 | tee taosdemo-$walPostfix-$today.log"
	demoCreateTableOnly=`grep "Total:" totaltime.out|awk '{print $2}'`
}

function runDeleteTableOnly {
	echoInfo "Restart Taosd"
	restartTaosd

	/usr/bin/time -f "Total: %e" -o totaltime.out bash -c "yes | taosdemo -t 0 -D 1 2>&1 | tee taosdemo-$walPostfix-$today.log"
	demoDeleteTableOnly=`grep "Total:" totaltime.out|awk '{print $2}'`
}

function runCreateTableThenInsert {
	echoInfo "Restart Taosd"
	restartTaosd

	/usr/bin/time -f "Total: %e" -o totaltime.out bash -c "yes | taosdemo 2>&1 | tee -a taosdemo-$walPostfix-$today.log"
	demoTableAndInsert=`grep "Total:" totaltime.out|awk '{print $2}'`	
	demoRPS=`grep "records\/second" taosdemo-$walPostfix-$today.log | tail -n1 | awk '{print $13}'`	
}

function queryPerformance {
	echoInfo "Restart Taosd"
	restartTaosd
	
	cd $TDENGINE_DIR/tests/pytest
	python3 query/queryPerformance.py
}

function generateTaosdemoPlot {
	echo "${today} $walPostfix, demoCreateTableOnly: ${demoCreateTableOnly}, demoDeleteTableOnly: ${demoDeleteTableOnly}, demoTableAndInsert: ${demoTableAndInsert}" | tee -a taosdemo-$today.log
	echo "${today}, ${demoCreateTableOnly}, ${demoDeleteTableOnly}, ${demoTableAndInsert}">> taosdemo-$walPostfix-report.csv
	echo "${today}, ${demoRPS}" >> taosdemo-rps-$walPostfix-report.csv
	
	csvLines=`cat taosdemo-$walPostfix-report.csv | wc -l`

	if [ "$csvLines" -gt "10" ]; then
		sed -i '1d' taosdemo-$walPostfix-report.csv
	fi

	csvLines=`cat taosdemo-rps-$walPostfix-report.csv | wc -l`

	if [ "$csvLines" -gt "10" ]; then
		sed -i '1d' taosdemo-rps-$walPostfix-report.csv
	fi

	gnuplot -e "filename='taosdemo-$walPostfix-report'" -p taosdemo-csv2png.gnuplot
	gnuplot -e "filename='taosdemo-rps-$walPostfix-report'" -p taosdemo-rps-csv2png.gnuplot
}

today=`date +"%Y%m%d"`

cd $WORK_DIR
echoInfo "Test Create Table Only "
runCreateTableOnly
echoInfo "Test Delete Table Only"
runDeleteTableOnly
echoInfo "Test Create Table then Insert data"
runCreateTableThenInsert
echoInfo "Query Performance for 10 Billion Records"
queryPerformance
echoInfo "Generate plot for taosdemo"
generateTaosdemoPlot


tar czf $WORK_DIR/taos-log-taosdemo-$today.tar.gz $logDir/*

echoInfo "End of TaosDemo Test" | tee -a $WORK_DIR/cron.log
