#!/bin/bash

WORK_DIR=/mnt/root
TSDB_CMP_DIR=timeseriesdatabase-comparisons/build/tsdbcompare

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

function runPerfTest13d {
	echoInfo "Restart Taosd"
	restartTaosd

	cd $WORK_DIR/$TSDB_CMP_DIR
	./runTDengine.sh -d 13 -w -q 2>&1 | tee $WORK_DIR/perftest-13d-$walPostfix-$today.log
}

function generatePerfPlot13d {
	cd $WORK_DIR

	csvLines=`cat perftest-13d-$walPostfix-report.csv | wc -l`

	if [ "$csvLines" -gt "10" ]; then
		sed -i '1d' perftest-13d-$walPostfix-report.csv
	fi

	gnuplot -e "filename='perftest-13d-$walPostfix-report'" -p perftest-csv2png.gnuplot
}

today=`date +"%Y%m%d"`
cd $WORK_DIR

echoInfo "run Performance Test with 13 days data"
runPerfTest13d $1
echoInfo "Generate plot of 13 days data"
generatePerfPlot13d $1

tar czf $WORK_DIR/taos-log-13d-$today.tar.gz $logDir/*

echoInfo "End of TSDB-Compare 13-days-data Test" | tee -a $WORK_DIR/cron.log
