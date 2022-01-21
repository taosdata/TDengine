#!/bin/bash

WORK_DIR=/mnt/root

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

function setMaxTablesPerVnode {
	echo "/etc/taos/taos.cfg maxTablesPerVnode will be set to $1"

	hasText=`grep "maxTablesPerVnode" /etc/taos/taos.cfg`
	if [[ -z "$hasText" ]]; then
		echo "maxTablesPerVnode $1" >> /etc/taos/taos.cfg
	else
		sed -i 's/^maxTablesPerVnode.*$/maxTablesPerVnode '"$1"'/g' /etc/taos/taos.cfg
	fi
}

function setMaxConnections {
	echo "/etc/taos/taos.cfg maxConnection will be set to $1"

	hasText=`grep "maxConnections" /etc/taos/taos.cfg`
	if [[ -z "$hasText" ]]; then
		echo "maxConnections $1" >> /etc/taos/taos.cfg
	else
		sed -i 's/^maxConnections.*$/maxConnections '"$1"'/g' /etc/taos/taos.cfg
	fi
}

function setQDebugFlag {
	echo "/etc/taos/taos.cfg qDebugFlag will be set to $1"

	hasText=`grep -w "qDebugFlag" /etc/taos/taos.cfg`
	if [[ -z "$hasText" ]]; then
		echo "qDebugFlag $1" >> /etc/taos/taos.cfg
	else
		sed -i 's/^qDebugFlag.*$/qDebugFlag '"$1"'/g' /etc/taos/taos.cfg
	fi
}

function setDebugFlag {
	echo "/etc/taos/taos.cfg DebugFlag will be set to $1"

	hasText=`grep -w "DebugFlag" /etc/taos/taos.cfg`
	if [[ -z "$hasText" ]]; then
		echo "DebugFlag $1" >> /etc/taos/taos.cfg
	else
		sed -i 's/^DebugFlag.*$/DebugFlag '"$1"'/g' /etc/taos/taos.cfg
	fi
}

function setWal {
	echo "/etc/taos/taos.cfg walLevel will be set to $1"

	hasText=`grep "walLevel" /etc/taos/taos.cfg`
	if [[ -z "$hasText" ]]; then
		echo "walLevel $1" >> /etc/taos/taos.cfg
	else
		sed -i 's/^walLevel.*$/walLevel '"$1"'/g' /etc/taos/taos.cfg
	fi
}

function collectSysInfo {
	rm sysinfo.log
	grep model /proc/cpuinfo | tail -n1 | tee sysinfo.log
	grep cores /proc/cpuinfo | tail -n1 | tee -a sysinfo.log
	grep MemTotal /proc/meminfo | tee -a sysinfo.log
	grep "^[^#;]" /etc/taos/taos.cfg | tee taos.cfg
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

function sendReport {
	receiver="sdsang@taosdata.com, sangshuduo@gmail.com"
	mimebody="MIME-Version: 1.0\nContent-Type: text/html; charset=utf-8\n"

	echo -e "to: ${receiver}\nsubject: Perf test report ${today}, commit ID: ${LOCAL_COMMIT}\n" | \
		(cat - && uuencode perftest-1d-wal1-$today.log perftest-1d-wal1-$today.log)| \
		(cat - && uuencode perftest-1d-wal1-report.csv perftest-1d-wal1-report-$today.csv) | \
		(cat - && uuencode perftest-1d-wal1-report.png perftest-1d-wal1-report-$today.png) | \
		(cat - && uuencode perftest-13d-wal1-$today.log perftest-13d-wal1-$today.log)| \
		(cat - && uuencode perftest-13d-wal1-report.csv perftest-13d-wal1-report-$today.csv) | \
		(cat - && uuencode perftest-13d-wal1-report.png perftest-13d-wal1-report-$today.png) | \
		(cat - && uuencode perftest-var10k-int10s-wal1-$today.log perftest-var10k-int10s-wal1-$today.log)| \
		(cat - && uuencode perftest-var10k-int10s-wal1-report.csv perftest-var10k-int10s-wal1-report-$today.csv) | \
		(cat - && uuencode perftest-var10k-int10s-wal1-report.png perftest-var10k-int10s-wal1-report-$today.png) | \
		(cat - && uuencode taosdemo-wal1-$today.log taosdemo-wal1-$today.log) | \
		(cat - && uuencode taosdemo-wal1-report.csv taosdemo-wal1-report-$today.csv) | \
		(cat - && uuencode taosdemo-wal1-report.png taosdemo-wal1-report-$today.png) | \
		(cat - && uuencode taosdemo-rps-wal1-report.csv taosdemo-rps-wal1-report-$today.csv) | \
		(cat - && uuencode taosdemo-rps-wal1-report.png taosdemo-rps-wal1-report-$today.png) | \
		(cat - && uuencode perftest-1d-wal2-$today.log perftest-1d-wal2-$today.log)| \
		(cat - && uuencode perftest-1d-wal2-report.csv perftest-1d-wal2-report-$today.csv) | \
		(cat - && uuencode perftest-1d-wal2-report.png perftest-1d-wal2-report-$today.png) | \
		(cat - && uuencode perftest-13d-wal2-$today.log perftest-13d-wal2-$today.log)| \
		(cat - && uuencode perftest-13d-wal2-report.csv perftest-13d-wal2-report-$today.csv) | \
		(cat - && uuencode perftest-13d-wal2-report.png perftest-13d-wal2-report-$today.png) | \
		(cat - && uuencode perftest-var10k-int10s-wal2-$today.log perftest-var10k-int10s-wal2-$today.log)| \
		(cat - && uuencode perftest-var10k-int10s-wal2-report.csv perftest-var10k-int10s-wal2-report-$today.csv) | \
		(cat - && uuencode perftest-var10k-int10s-wal2-report.png perftest-var10k-int10s-wal2-report-$today.png) | \
		(cat - && uuencode taosdemo-wal2-$today.log taosdemo-wal2-$today.log) | \
		(cat - && uuencode taosdemo-wal2-report.csv taosdemo-wal2-report-$today.csv) | \
		(cat - && uuencode taosdemo-wal2-report.png taosdemo-wal2-report-$today.png) | \
		(cat - && uuencode taosdemo-rps-wal2-report.csv taosdemo-rps-wal2-report-$today.csv) | \
		(cat - && uuencode taosdemo-rps-wal2-report.png taosdemo-rps-wal2-report-$today.png) | \
		(cat - && uuencode sysinfo.log sysinfo.txt) | \
		(cat - && uuencode taos.cfg taos-cfg-$today.txt) | \
		ssmtp "${receiver}"
}

today=`date +"%Y%m%d"`
cd $WORK_DIR
echo -e "cron-ran-at-${today}" >> $WORK_DIR/cron.log

buildTDengine

############################
setMaxConnections 1000
setMaxTablesPerVnode 6000
setDebugFlag 131
setQDebugFlag 131

############################
setWal "2"

cd $WORK_DIR
date >> $WORK_DIR/cron.log
./perftest-taosdemo.sh "wal2"
date >> $WORK_DIR/cron.log

cd $WORK_DIR
date >> $WORK_DIR/cron.log
./perftest-tsdb-compare-1d.sh
date >> $WORK_DIR/cron.log

cd $WORK_DIR
date >> $WORK_DIR/cron.log
./perftest-tsdb-compare-13d.sh
date >> $WORK_DIR/cron.log

cd $WORK_DIR
date >> $WORK_DIR/cron.log
./perftest-tsdb-compare-var10k-int10s.sh
date >> $WORK_DIR/cron.log

#############################
setWal "1"

cd $WORK_DIR
date >> $WORK_DIR/cron.log
./perftest-taosdemo.sh "wal1"
date >> $WORK_DIR/cron.log

cd $WORK_DIR
date >> $WORK_DIR/cron.log
./perftest-tsdb-compare-1d.sh
date >> $WORK_DIR/cron.log

cd $WORK_DIR
date >> $WORK_DIR/cron.log
./perftest-tsdb-compare-13d.sh
date >> $WORK_DIR/cron.log

cd $WORK_DIR
date >> $WORK_DIR/cron.log
./perftest-tsdb-compare-var10k-int10s.sh
date >> $WORK_DIR/cron.log

#############################
collectSysInfo

echoInfo "Send Report"
sendReport
echoInfo "End of Test"
