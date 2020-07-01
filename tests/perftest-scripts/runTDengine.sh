#!/bin/bash
#set -x

WORK_DIR=/mnt/root
DATA_DIR=/mnt/data

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

# default value
DEFAULT_BATCH=5000
DEFAULT_DAYS=1
DEFAULT_INTERVAL=1
DEFAULT_SCALEVAR=10
DEFAULT_DOPREPARE=false
DEFAULT_DOWRITE=false
DEFAULT_DOQUERY=false

# function 
function do_prepare {
	echo
	echo "---------------Generating Data-----------------"
	echo

	echo 
	echo "Prepare data for TDengine...."

	# bin/bulk_data_gen -seed 123 -format tdengine -tdschema-file config/TDengineSchema.toml -scale-var 100 -use-case devops -timestamp-start "2018-01-01T00:00:00Z" -timestamp-end "2018-01-02T00:00:00Z"  > $DATA_DIR/tdengine.dat
	echo "bin/bulk_data_gen -seed 123 -format tdengine -sampling-interval $interval_s \
		-tdschema-file config/TDengineSchema.toml -scale-var $scalevar \
		-use-case devops -timestamp-start $TIME_START \ 
		-timestamp-end $TIME_END \ 
	       	> $DATA_FILE"

	bin/bulk_data_gen -seed 123 -format tdengine -sampling-interval $interval_s \
		-tdschema-file config/TDengineSchema.toml -scale-var $scalevar \
		-use-case devops -timestamp-start $TIME_START \
		-timestamp-end $TIME_END  \
		> $DATA_FILE
}

function do_write {
	echo "TDENGINERES=cat $DATA_FILE |bin/bulk_load_tdengine --url 127.0.0.1:0 \
		--batch-size $batch -do-load -report-tags n1 -workers 20 -fileout=false| grep loaded"

	TDENGINERES=`cat $DATA_FILE |bin/bulk_load_tdengine --url 127.0.0.1:0 \
		--batch-size $batch -do-load -report-tags n1 -workers 20 -fileout=false| grep loaded`

	echo
	echo -e "${GREEN}TDengine writing result:${NC}"
	echo -e "${GREEN}$TDENGINERES${NC}"
	DATA=`echo $TDENGINERES|awk '{print($2)}'`
	TMP=`echo $TDENGINERES|awk '{print($5)}'`
	TDWTM=`echo ${TMP%s*}`

}

function do_query {

	echo
	echo "------------------Querying Data-----------------"
	echo

	echo 
	echo  "start query test, query max from 8 hosts group by 1 hour, TDengine"
	echo

#Test case 1
#测试用例1，查询所有数据中，用8个hostname标签进行匹配，匹配出这8个hostname对应的模拟服务器CPU数据中的usage_user这个监控数据的最大值。
#select max(usage_user) from cpu where(hostname='host_a' and hostname='host_b'and hostname='host_c'and hostname='host_d'and hostname='host_e'and hostname='host_f' and hostname='host_g'and hostname='host_h') ;
# a,b,c,d,e,f,g,h are random 8 numbers.
	echo "TDQS1=bin/bulk_query_gen  -seed 123 -format tdengine -query-type 8-host-all \
		-scale-var $scalevar -queries 1000 | bin/query_benchmarker_tdengine  \
		-urls=http://127.0.0.1:6020 -workers 50 -print-interval 0|grep wall"

	TDQS1=`bin/bulk_query_gen  -seed 123 -format tdengine -query-type 8-host-all \
		-scale-var $scalevar -queries 1000 | bin/query_benchmarker_tdengine  \
		-urls="http://127.0.0.1:6020" -workers 50 -print-interval 0|grep wall`
	echo
	echo -e "${GREEN}TDengine query test case 1 result:${NC}"
	echo -e "${GREEN}$TDQS1${NC}"
	TMP=`echo $TDQS1|awk '{print($4)}'`
	TDQ1=`echo ${TMP%s*}`

#Test case 2
#测试用例2，查询所有数据中，用8个hostname标签进行匹配，匹配出这8个hostname对应的模拟服务器CPU数据中的usage_user这个监控数据，以1小时为粒度，查询每1小时的最大值。
#select max(usage_user) from cpu where(hostname='host_a' and hostname='host_b'and hostname='host_c'and hostname='host_d'and hostname='host_e'and hostname='host_f' and hostname='host_g'and hostname='host_h') interval(1h);
# a,b,c,d,e,f,g,h are random 8 numbers
	echo "TDQS2=bin/bulk_query_gen  -seed 123 -format tdengine -query-type 8-host-allbyhr \
		-scale-var $scalevar -queries 1000 | bin/query_benchmarker_tdengine  \
		-urls=http://127.0.0.1:6020 -workers 50 -print-interval 0|grep wall"

	TDQS2=`bin/bulk_query_gen  -seed 123 -format tdengine -query-type 8-host-allbyhr \
		-scale-var $scalevar -queries 1000 | bin/query_benchmarker_tdengine  \
		-urls="http://127.0.0.1:6020" -workers 50 -print-interval 0|grep wall`

	echo
	echo -e "${GREEN}TDengine query test case 2 result:${NC}"
	echo -e "${GREEN}$TDQS2${NC}"
	TMP=`echo $TDQS2|awk '{print($4)}'`
	TDQ2=`echo ${TMP%s*}`

#Test case 3
#测试用例3，测试用例3，随机查询12个小时的数据，用8个hostname标签进行匹配，匹配出这8个hostname对应的模拟服务器CPU数据中的usage_user这个监控数据，以10分钟为粒度，查询每10分钟的最大值
#select max(usage_user) from cpu where(hostname='host_a' and hostname='host_b'and hostname='host_c'and hostname='host_d'and hostname='host_e'and hostname='host_f' and hostname='host_g'and hostname='host_h') and time >x and time <y interval(10m);
# a,b,c,d,e,f,g,h are random 8 numbers, y-x =12 hour
	echo "TDQS3=bin/bulk_query_gen  -seed 123 -format tdengine -query-type 8-host-12-hr \
		-scale-var $scalevar -queries 1000 | bin/query_benchmarker_tdengine  \
		-urls=http://127.0.0.1:6020 -workers 50 -print-interval 0|grep wall"

	TDQS3=`bin/bulk_query_gen  -seed 123 -format tdengine -query-type 8-host-12-hr \
		-scale-var $scalevar -queries 1000 | bin/query_benchmarker_tdengine  \
		-urls="http://127.0.0.1:6020" -workers 50 -print-interval 0|grep wall`
	echo
	echo -e "${GREEN}TDengine query test case 3 result:${NC}"
	echo -e "${GREEN}$TDQS3${NC}"
	TMP=`echo $TDQS3|awk '{print($4)}'`
	TDQ3=`echo ${TMP%s*}`

#Test case 4
#测试用例4，随机查询1个小时的数据，用8个hostname标签进行匹配，匹配出这8个hostname对应的模拟服务器CPU数据中的usage_user这个监控数据，以1分钟为粒度，查询每1分钟的最大值
#select max(usage_user) from cpu where(hostname='host_a' and hostname='host_b'and hostname='host_c'and hostname='host_d'and hostname='host_e'and hostname='host_f' and hostname='host_g'and hostname='host_h') and time >x and time <y interval(10m);
# a,b,c,d,e,f,g,h are random 8 numbers, y-x =1 hours
	echo "TDQS4=bin/bulk_query_gen  -seed 123 -format tdengine -query-type 8-host-1-hr \
		-scale-var $scalevar -queries 1000 | bin/query_benchmarker_tdengine  \
		-urls=http://127.0.0.1:6020 -workers 50 -print-interval 0|grep wall"

	TDQS4=`bin/bulk_query_gen  -seed 123 -format tdengine -query-type 8-host-1-hr \
		-scale-var $scalevar -queries 1000 | bin/query_benchmarker_tdengine  \
		-urls="http://127.0.0.1:6020" -workers 50 -print-interval 0|grep wall`
	echo
	echo -e "${GREEN}TDengine query test case 4 result:${NC}"
	echo -e "${GREEN}$TDQS4${NC}"
	TMP=`echo $TDQS4|awk '{print($4)}'`
	TDQ4=`echo ${TMP%s*}`

}

batch=$DEFAULT_BATCH
days=$DEFAULT_DAYS
interval=$DEFAULT_INTERVAL
scalevar=$DEFAULT_SCALEVAR
doprepare=$DEFAULT_DOPREPARE
dowrite=$DEFAULT_DOWRITE
doquery=$DEFAULT_DOQUERY

help="$(basename "$0") [-h] [-b batchsize] [-d data-of-days] [-i interval] [-v scalevar] [-p false for don't prepare] [-w false for don't do write] [-q false for don't do query]"

while getopts ':b:d:i:v:pwqh' flag; do
	case "${flag}" in
		b) batch=${OPTARG};;
		d) days=${OPTARG};;
		i) interval=${OPTARG};;
		v) scalevar=${OPTARG};;
		p) doprepare=${OPTARG};;
		w) dowrite=${OPTARG};;
		q) doquery=${OPTARG};;
		:) echo -e "{RED}Missing argument!{NC}"
			echo "$help"
			exit 1
			;;
		h) echo "$help"
			exit 1
			;;
	esac
done

walLevel=`grep "^walLevel" /etc/taos/taos.cfg | awk '{print $2}'`
if [[ "$walLevel" -eq "2" ]]; then
	walPostfix="wal2"
elif [[ "$walLevel" -eq "1" ]]; then
	walPostfix="wal1"
else
	echo -e "${RED}wrong walLevel $walLevel found! ${NC}"
	exit 1
fi

echo -e "${GREEN} $walPostfix found! ${NC}"

if [[ $scalevar -eq 10000 ]]
then
	TIME_START="2018-01-01T00:00:00Z"
	TIME_END="2018-01-02T00:00:00Z"

	if [[ $interval -eq 100 ]]
	then
		interval_s=100s
		DATA_FILE=$DATA_DIR/tdengine-var10k-int100s.dat
		RECORD_CSV_FILE=$WORK_DIR/perftest-var10k-int100s-$walPostfix-report.csv
	else
		interval_s=10s
		DATA_FILE=$DATA_DIR/tdengine-var10k-int10s.dat
		RECORD_CSV_FILE=$WORK_DIR/perftest-var10k-int10s-$walPostfix-report.csv
	fi
else
	if [[ $days -eq 1 ]]
	then
		TIME_START="2018-01-01T00:00:00Z"
		TIME_END="2018-01-02T00:00:00Z"
		DATA_FILE=$DATA_DIR/tdengine-1d.dat
		RECORD_CSV_FILE=$WORK_DIR/perftest-1d-$walPostfix-report.csv
	elif [[ $days -eq 13 ]]
	then
		TIME_START="2018-01-01T00:00:00Z"
		TIME_END="2018-01-14T00:00:00Z"
		DATA_FILE=$DATA_DIR/tdengine-13d.dat
		RECORD_CSV_FILE=$WORK_DIR/perftest-13d-$walPostfix-report.csv
	else
		echo -e "{RED} don't support input $days{NC}"
		exit 1
	fi

	interval_s=${interval}s
fi

echo "TIME_START: $TIME_START, TIME_END: $TIME_END, DATA_FILE: $DATA_FILE"
echo "doprepare: $doprepare, dowrite: $dowrite, doquery: $doquery"

if $doprepare;
then
	do_prepare
fi

echo

if $dowrite;
then
	echo -e "Start test TDengine writting, result in ${GREEN}Green line${NC}"
	do_write
fi

if $doquery;
then
	echo -e "Start test TDengine query, result in ${GREEN}Green line${NC}"
	do_query
fi

echo
echo
echo    "======================================================"
echo    "             tsdb performance comparision             "
echo    "======================================================"
if $dowrite;
then
	echo -e "       Writing $DATA records test takes:          "
	printf  "       TDengine           |       %-4.5f Seconds    \n" $TDWTM
	echo    "------------------------------------------------------"
fi

if $doquery;
then
	echo    "                   Query test cases:                "
	echo    " case 1: select the max(value) from all data    "
	echo    " filtered out 8 hosts                                 "
	echo    "       Query test case 1 takes:                      "
	printf  "       TDengine           |       %-4.5f Seconds    \n" $TDQ1
	echo    "------------------------------------------------------"
	echo    " case 2: select the max(value) from all data          "
	echo    " filtered out 8 hosts with an interval of 1 hour     "
	echo    " case 2 takes:                                       "
	printf  "       TDengine           |       %-4.5f Seconds    \n" $TDQ2
	echo    "------------------------------------------------------"
	echo    " case 3: select the max(value) from random 12 hours"
	echo    " data filtered out 8 hosts with an interval of 10 min         "
	echo    " filtered out 8 hosts interval(1h)                   "
	echo    " case 3 takes:                                       "
	printf  "       TDengine           |       %-4.5f Seconds    \n" $TDQ3
	echo    "------------------------------------------------------"
	echo    " case 4: select the max(value) from random 1 hour data  "
	echo    " data filtered out 8 hosts with an interval of 1 min         "
	echo    " case 4 takes:                                        "
	printf  "       TDengine           |       %-4.5f Seconds    \n" $TDQ4
	echo    "------------------------------------------------------"
fi

#bulk_query_gen/bulk_query_gen  -format influx-http -query-type 1-host-1-hr -scale-var 10 -queries 1000 | query_benchmarker_influxdb/query_benchmarker_influxdb  -urls="http://172.26.89.231:8086" 
#bulk_query_gen/bulk_query_gen  -format tdengine -query-type 1-host-1-hr -scale-var 10 -queries 1000 | query_benchmarker_tdengine/query_benchmarker_tdengine  -urls="http://172.26.89.231:6020" 
today=`date +"%Y%m%d"`
echo "${today}, ${TDWTM}, ${TDQ1}, ${TDQ2}, ${TDQ3}, ${TDQ4}" >> $RECORD_CSV_FILE
