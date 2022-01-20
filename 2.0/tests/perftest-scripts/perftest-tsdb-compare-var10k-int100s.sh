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

function runPerfTestVar10K {
	echoInfo "Restart Taosd"
	restartTaosd

	cd $WORK_DIR/$TSDB_CMP_DIR
	./runTDengine.sh -v 10000 -i 100 -w -q 2>&1 | tee $WORK_DIR/perftest-var10k-int100s-$walPostfix-$today.log
}

function generatePerfPlotVar10K {
	cd $WORK_DIR

	csvLines=`cat perftest-var10k-int100s-$walPostfix-report.csv | wc -l`

	if [ "$csvLines" -gt "10" ]; then
		sed -i '1d' perftest-var10k-int100s-$walPostfix-report.csv
	fi

	gnuplot -e "filename='perftest-var10k-int100s-$walPostfix-report'" -p perftest-csv2png.gnuplot
}

today=`date +"%Y%m%d"`
cd $WORK_DIR

echoInfo "run Performance Test with 10K tables data"
runPerfTestVar10K
echoInfo "Generate plot of 10K tables data"
generatePerfPlotVar10K

tar czf $WORK_DIR/taos-log-var10k-int100s-$today.tar.gz $logDir/*

echoInfo "End of TSDB-Compare var10k-int100s-tables-data Test" | tee -a $WORK_DIR/cron.log
