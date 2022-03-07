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
	echo "Prepare data for InfluxDB...."

	echo "bin/bulk_data_gen -seed 123 -format influx-bulk -sampling-interval $interval_s \
		-scale-var $scalevar -use-case devops -timestamp-start $TIME_START \
		-timestamp-end $TIME_END > $DATA_FILE"

	bin/bulk_data_gen -seed 123 -format influx-bulk -sampling-interval $interval_s \
		-scale-var $scalevar -use-case devops -timestamp-start $TIME_START \
		-timestamp-end $TIME_END > $DATA_FILE
}

function do_write {
	echo "cat $DATA_FILE | bin/bulk_load_influx \
		--batch-size=$batch --workers=20 --urls=http://172.15.1.5:8086 | grep loaded"
	INFLUXRES=`cat $DATA_FILE | bin/bulk_load_influx \
		--batch-size=$batch --workers=20 --urls="http://172.15.1.5:8086" | grep loaded`

	echo -e "${GREEN}InfluxDB writing result:${NC}"
	echo -e "${GREEN}$INFLUXRES${NC}"
	DATA=`echo $INFLUXRES|awk '{print($2)}'`
	TMP=`echo $INFLUXRES|awk '{print($5)}'`
	IFWTM=`echo ${TMP%s*}`
}

function do_query {

	echo
	echo "------------------Querying Data-----------------"
	echo

	echo 
	echo  "start query test, query max from 8 hosts group by 1 hour, InfluxDB"
	echo

#Test case 1
#测试用例1，查询所有数据中，用8个hostname标签进行匹配，匹配出这8个hostname对应的模拟服务器CPU数据中的usage_user这个监控数据的最大值。
#select max(usage_user) from cpu where(hostname='host_a' and hostname='host_b'and hostname='host_c'and hostname='host_d'and hostname='host_e'and hostname='host_f' and hostname='host_g'and hostname='host_h') ;
# a,b,c,d,e,f,g,h are random 8 numbers.
	echo "IFQS1=bin/bulk_query_gen  -seed 123 -format influx-http -query-type 8-host-all -scale-var $scalevar -queries 1000 | bin/query_benchmarker_influxdb  -urls="http://172.15.1.5:8086"  -workers 50 -print-interval 0|grep wall"

	IFQS1=`bin/bulk_query_gen  -seed 123 -format influx-http -query-type 8-host-all -scale-var $scalevar -queries 1000 | bin/query_benchmarker_influxdb  -urls="http://172.15.1.5:8086"  -workers 50 -print-interval 0|grep wall`
	echo -e "${GREEN}InfluxDB query test case 1 result:${NC}"
	echo -e "${GREEN}$IFQS1${NC}"
	TMP=`echo $IFQS1|awk '{print($4)}'`
	IFQ1=`echo ${TMP%s*}`

#Test case 2
#测试用例2，查询所有数据中，用8个hostname标签进行匹配，匹配出这8个hostname对应的模拟服务器CPU数据中的usage_user这个监控数据，以1小时为粒度，查询每1小时的最大值。
#select max(usage_user) from cpu where(hostname='host_a' and hostname='host_b'and hostname='host_c'and hostname='host_d'and hostname='host_e'and hostname='host_f' and hostname='host_g'and hostname='host_h') interval(1h);
# a,b,c,d,e,f,g,h are random 8 numbers
	echo "IFQS2=bin/bulk_query_gen  -seed 123 -format influx-http -query-type 8-host-allbyhr -scale-var $scalevar -queries 1000 | bin/query_benchmarker_influxdb  -urls=http://172.15.1.5:8086  -workers 50 -print-interval 0|grep wall"

	IFQS2=`bin/bulk_query_gen  -seed 123 -format influx-http -query-type 8-host-allbyhr \
		-scale-var $scalevar -queries 1000 | bin/query_benchmarker_influxdb  \
		-urls="http://172.15.1.5:8086"  -workers 50 -print-interval 0|grep wall`
	echo -e "${GREEN}InfluxDB query test case 2 result:${NC}"
	echo -e "${GREEN}$IFQS2${NC}"
	TMP=`echo $IFQS2|awk '{print($4)}'`
	IFQ2=`echo ${TMP%s*}`

#Test case 3
#测试用例3，测试用例3，随机查询12个小时的数据，用8个hostname标签进行匹配，匹配出这8个hostname对应的模拟服务器CPU数据中的usage_user这个监控数据，以10分钟为粒度，查询每10分钟的最大值
#select max(usage_user) from cpu where(hostname='host_a' and hostname='host_b'and hostname='host_c'and hostname='host_d'and hostname='host_e'and hostname='host_f' and hostname='host_g'and hostname='host_h') and time >x and time <y interval(10m);
# a,b,c,d,e,f,g,h are random 8 numbers, y-x =12 hour
	echo "IFQS3=bin/bulk_query_gen  -seed 123 -format influx-http -query-type 8-host-12-hr -scale-var $scalevar -queries 1000 | bin/query_benchmarker_influxdb  -urls=http://172.15.1.5:8086  -workers 50 -print-interval 0|grep wall"

	IFQS3=`bin/bulk_query_gen  -seed 123 -format influx-http -query-type 8-host-12-hr \
		-scale-var $scalevar -queries 1000 | bin/query_benchmarker_influxdb  \
		-urls="http://172.15.1.5:8086"  -workers 50 -print-interval 0|grep wall`
	echo -e "${GREEN}InfluxDB query test case 3 result:${NC}"
	echo -e "${GREEN}$IFQS3${NC}"
	TMP=`echo $IFQS3|awk '{print($4)}'`
	IFQ3=`echo ${TMP%s*}`

#Test case 4
#测试用例4，随机查询1个小时的数据，用8个hostname标签进行匹配，匹配出这8个hostname对应的模拟服务器CPU数据中的usage_user这个监控数据，以1分钟为粒度，查询每1分钟的最大值
#select max(usage_user) from cpu where(hostname='host_a' and hostname='host_b'and hostname='host_c'and hostname='host_d'and hostname='host_e'and hostname='host_f' and hostname='host_g'and hostname='host_h') and time >x and time <y interval(10m);
# a,b,c,d,e,f,g,h are random 8 numbers, y-x =1 hours
	echo "IFQS4=bin/bulk_query_gen  -seed 123 -format influx-http -query-type 8-host-1-hr -scale-var $scalevar -queries 1000 | bin/query_benchmarker_influxdb  -urls=http://172.15.1.5:8086  -workers 50 -print-interval 0|grep wall"

	IFQS4=`bin/bulk_query_gen  -seed 123 -format influx-http -query-type 8-host-1-hr \
		-scale-var $scalevar -queries 1000 | bin/query_benchmarker_influxdb  \
		-urls="http://172.15.1.5:8086"  -workers 50 -print-interval 0|grep wall`
	echo -e "${GREEN}InfluxDB query test case 4 result:${NC}"
	echo -e "${GREEN}$IFQS4${NC}"
	TMP=`echo $IFQS4|awk '{print($4)}'`
	IFQ4=`echo ${TMP%s*}`

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

if [[ $scalevar -eq 10000 ]]
then
	TIME_START="2018-01-01T00:00:00Z"
	TIME_END="2018-01-02T00:00:00Z"

	if [[ $interval -eq 100 ]]
	then
		interval_s=100s
		DATA_FILE=$DATA_DIR/influxdb-var10k-int100s.dat
		RECORD_CSV_FILE=$WORK_DIR/perftest-influxdb-report-var10k-int100s.csv
	else
		interval_s=10s
		DATA_FILE=$DATA_DIR/influxdb-var10k-int10s.dat
		RECORD_CSV_FILE=$WORK_DIR/perftest-influxdb-report-var10k-int10s.csv
	fi
else
	if [[ $days -eq 1 ]]
	then
		TIME_START="2018-01-01T00:00:00Z"
		TIME_END="2018-01-02T00:00:00Z"
		DATA_FILE=$DATA_DIR/influxdb-1d.dat
		RECORD_CSV_FILE=$WORK_DIR/perftest-influxdb-report-1d.csv
	elif [[ $days -eq 13 ]]
	then
		TIME_START="2018-01-01T00:00:00Z"
		TIME_END="2018-01-14T00:00:00Z"
		DATA_FILE=$DATA_DIR/influxdb-13d.dat
		RECORD_CSV_FILE=$WORK_DIR/perftest-influxdb-report-13d.csv
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

docker network create --ip-range 172.15.1.255/24 --subnet 172.15.1.1/16 tsdbcomp >>/dev/null 2>&1
INFLUX=`docker run -d -p 8086:8086 --net tsdbcomp --ip 172.15.1.5 influxdb` >>/dev/null 2>&1
sleep 10

if $dowrite;
then
	echo -e "Start test InfluxDB writting, result in ${GREEN}Green line${NC}"
	do_write
fi

if $doquery;
then
	echo -e "Start test InfluxDB query, result in ${GREEN}Green line${NC}"
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
	printf  "       InfluxDB           |       %-4.5f Seconds    \n" $IFWTM
	echo    "------------------------------------------------------"
fi

if $doquery;
then
	echo    "                   Query test cases:                "
	echo    " case 1: select the max(value) from all data    "
	echo    " filtered out 8 hosts                                 "
	echo    "       Query test case 1 takes:                      "
	printf  "       InfluxDB           |       %-4.5f Seconds    \n" $IFQ1
	echo    "------------------------------------------------------"
	echo    " case 2: select the max(value) from all data          "
	echo    " filtered out 8 hosts with an interval of 1 hour     "
	echo    " case 2 takes:                                       "
	printf  "       InfluxDB           |       %-4.5f Seconds    \n" $IFQ2
	echo    "------------------------------------------------------"
	echo    " case 3: select the max(value) from random 12 hours"
	echo    " data filtered out 8 hosts with an interval of 10 min         "
	echo    " filtered out 8 hosts interval(1h)                   "
	echo    " case 3 takes:                                       "
	printf  "       InfluxDB           |       %-4.5f Seconds    \n" $IFQ3
	echo    "------------------------------------------------------"
	echo    " case 4: select the max(value) from random 1 hour data  "
	echo    " data filtered out 8 hosts with an interval of 1 min         "
	echo    " case 4 takes:                                        "
	printf  "       InfluxDB           |       %-4.5f Seconds    \n" $IFQ4
	echo    "------------------------------------------------------"
fi

docker stop $INFLUX >>/dev/null 2>&1
docker container rm -f $INFLUX >>/dev/null 2>&1
docker network rm tsdbcomp >>/dev/null 2>&1

today=`date +"%Y%m%d"`
echo "${today}, ${IFWTM}, ${IFQ1}, ${IFQ2}, ${IFQ3}, ${IFQ4}" >> $RECORD_CSV_FILE
