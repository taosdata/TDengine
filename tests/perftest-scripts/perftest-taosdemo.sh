#!/bin/bash

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

	/usr/bin/time -f "Total: %e" -o totaltime.out bash -c "yes | taosdemo -n 0 2>&1 | tee taosdemo-$1-$today.log"
	demoCreateTableOnly=`grep "Total:" totaltime.out|awk '{print $2}'`
}

function runDeleteTableOnly {
	echoInfo "Restart Taosd"
	restartTaosd

	/usr/bin/time -f "Total: %e" -o totaltime.out bash -c "yes | taosdemo -t 0 -D 1 2>&1 | tee taosdemo-$1-$today.log"
	demoDeleteTableOnly=`grep "Total:" totaltime.out|awk '{print $2}'`
}

function runCreateTableThenInsert {
	echoInfo "Restart Taosd"
	restartTaosd

	/usr/bin/time -f "Total: %e" -o totaltime.out bash -c "yes | taosdemo 2>&1 | tee -a taosdemo-$1-$today.log"
	demoTableAndInsert=`grep "Total:" totaltime.out|awk '{print $2}'`
	demoRPS=`grep "records\/second" taosdemo-$1-$today.log | tail -n1 | awk '{print $13}'`
}	

function generateTaosdemoPlot {
	echo "${today} $1, demoCreateTableOnly: ${demoCreateTableOnly}, demoDeleteTableOnly: ${demoDeleteTableOnly}, demoTableAndInsert: ${demoTableAndInsert}" | tee -a taosdemo-$today.log
	echo "${today}, ${demoCreateTableOnly}, ${demoDeleteTableOnly}, ${demoTableAndInsert}">> taosdemo-$1-report.csv
	echo "${today}, ${demoRPS}" >> taosdemo-rps-$1-report.csv
	
	csvLines=`cat taosdemo-$1-report.csv | wc -l`

	if [ "$csvLines" -gt "10" ]; then
		sed -i '1d' taosdemo-$1-report.csv
	fi

	csvLines=`cat taosdemo-rps-$1-report.csv | wc -l`

	if [ "$csvLines" -gt "10" ]; then
		sed -i '1d' taosdemo-rps-$1-report.csv
	fi

	gnuplot -e "filename='taosdemo-$1-report'" -p taosdemo-csv2png.gnuplot
	gnuplot -e "filename='taosdemo-rps-$1-report'" -p taosdemo-rps-csv2png.gnuplot
}

today=`date +"%Y%m%d"`

cd /root
echoInfo "Test Create Table Only "
runCreateTableOnly $1
echoInfo "Test Create Table then Insert data"
runDeleteTableOnly $1
echoInfo "Test Create Table then Insert data"
runCreateTableThenInsert $1
echoInfo "Generate plot for taosdemo"
generateTaosdemoPlot $1
echoInfo "End of TaosDemo Test"
