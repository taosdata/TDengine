#!/bin/bash

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

function set-Wal {
	echo "/etc/taos/taos.cfg walLevel will be set to $1"
	sed -i 's/^walLevel.*$/walLevel '"$1"'/g' /etc/taos/taos.cfg
}

function collectSysInfo {
	rm sysinfo.log
	grep model /proc/cpuinfo | tail -n1 | tee sysinfo.log
	grep cores /proc/cpuinfo | tail -n1 | tee -a sysinfo.log
	grep MemTotal /proc/meminfo | tee -a sysinfo.log
	grep "^[^#;]" /etc/taos/taos.cfg | tee taos.cfg
}

function buildTDengine {
	cd /root/TDengine

	git remote update
	REMOTE_COMMIT=`git rev-parse --short remotes/origin/develop`
	LOCAL_COMMIT=`git rev-parse --short @`

	echo " LOCAL: $LOCAL_COMMIT"
	echo "REMOTE: $REMOTE_COMMIT"
	if [ "$LOCAL_COMMIT" == "$REMOTE_COMMIT" ]; then
		echo "repo up-to-date"
	else
		echo "repo need to pull"
		git pull

		LOCAL_COMMIT=`git rev-parse --short @`
		cd debug
		rm -rf *
		cmake ..
		make > /dev/null
		make install
	fi
}

function restartTaosd {
	systemctl stop taosd
	pkill -KILL -x taosd
	sleep 10
	
	rm -rf /mnt/var/log/taos/*
	rm -rf /mnt/var/lib/taos/*
	
	taosd 2>&1 > /dev/null &
	sleep 10
}

function sendReport {
	receiver="sdsang@taosdata.com, sangshuduo@gmail.com"
	mimebody="MIME-Version: 1.0\nContent-Type: text/html; charset=utf-8\n"

	echo -e "to: ${receiver}\nsubject: Perf test report ${today}, commit ID: ${LOCAL_COMMIT}\n" | \
		(cat - && uuencode perftest-1d-$today.log perftest-1d-$today.log)| \
		(cat - && uuencode perftest-1d-report.csv perftest-1d-report-$today.csv) | \
		(cat - && uuencode perftest-1d-report.png perftest-1d-report-$today.png) | \
		(cat - && uuencode perftest-13d-$today.log perftest-13d-$today.log)| \
		(cat - && uuencode perftest-13d-report.csv perftest-13d-report-$today.csv) | \
		(cat - && uuencode perftest-13d-report.png perftest-13d-report-$today.png) | \
		(cat - && uuencode taosdemo-$today.log taosdemo-$today.log) | \
		(cat - && uuencode taosdemo-report.csv taosdemo-report-$today.csv) | \
		(cat - && uuencode taosdemo-report.png taosdemo-report-$today.png) | \
		(cat - && uuencode sysinfo.log sysinfo.txt) | \
		(cat - && uuencode taos.cfg taos-cfg-$today.txt) | \
		ssmtp "${receiver}"
}

today=`date +"%Y%m%d"`
cd /root
echo -e "cron-ran-at-${today}" >> cron.log

echoInfo "Build TDengine"
buildTDengine

set-Wal "2"

cd /root
./perftest-tsdb-compare-1d.sh

cd /root
./perftest-tsdb-compare-13d.sh

cd /root
./perftest-taosdemo.sh

collectSysInfo

echoInfo "Send Report"
sendReport
echoInfo "End of Test"
