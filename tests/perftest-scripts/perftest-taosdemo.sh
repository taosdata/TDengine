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

function restartTaosd {
	systemctl stop taosd
	pkill -KILL -x taosd
	sleep 10
	
	rm -rf /mnt/var/log/taos/*
	rm -rf /mnt/var/lib/taos/*
	
	taosd 2>&1 > /dev/null &
	sleep 10
}

function runCreateTableOnly {
	echoInfo "Restart Taosd"
	restartTaosd

	/usr/bin/time -f "Total: %e" -o totaltime.out bash -c "yes | taosdemo -n 0 2>&1 | tee taosdemo-$today.log"
	demoTableOnly=`grep "Total:" totaltime.out|awk '{print $2}'`
}

function runCreateTableThenInsert {
	echoInfo "Restart Taosd"
	restartTaosd

	/usr/bin/time -f "Total: %e" -o totaltime.out bash -c "yes | taosdemo 2>&1 | tee -a taosdemo-$today.log"
	demoTableAndInsert=`grep "Total:" totaltime.out|awk '{print $2}'`
	demoRPS=`grep "records\/second" taosdemo-$today.log | tail -n1 | awk '{print $13}'`
}	

function generateTaosdemoPlot {
	echo "${today}, demoTableOnly: ${demoTableOnly}, demoTableAndInsert: ${demoTableAndInsert}" | tee -a taosdemo-$today.log
	echo "${today}, ${demoTableOnly}, ${demoTableAndInsert}, ${demoRPS}" >> taosdemo-report.csv
	
	csvLines=`cat taosdemo-report.csv | wc -l`

	if [ "$csvLines" -gt "10" ]; then
		sed -i '1d' taosdemo-report.csv
	fi

	gnuplot -p taosdemo-csv2png.gnuplot
}

today=`date +"%Y%m%d"`

cd /root
echoInfo "Test Create Table Only "
runCreateTableOnly
echoInfo "Test Create Table then Insert data"
runCreateTableThenInsert
echoInfo "Generate plot for taosdemo"
generateTaosdemoPlot
echoInfo "End of TaosDemo Test"
