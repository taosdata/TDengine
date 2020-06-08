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

function runPerfTest13d {
	echoInfo "Restart Taosd"
	restartTaosd

	cd /home/taos/tliu/timeseriesdatabase-comparisons/build/tsdbcompare
	./runreal-13d-csv.sh $1 2>&1 | tee /root/perftest-13d-$1-$today.log
}

function generatePerfPlot13d {
	cd /root

	csvLines=`cat perftest-13d-$1-report.csv | wc -l`

	if [ "$csvLines" -gt "10" ]; then
		sed -i '1d' perftest-13d-$1-report.csv
	fi

	gnuplot -e "filename='perftest-13d-$1-report'" -p perftest-csv2png.gnuplot
}

today=`date +"%Y%m%d"`
cd /root

echoInfo "run Performance Test with 13 days data"
runPerfTest13d $1
echoInfo "Generate plot of 13 days data"
generatePerfPlot13d $1
echoInfo "End of TSDB-Compare 13-days-data Test"
