#!/bin/bash

TAOS_RUN_TAOSBENCHMARK_TEST_ONCE=0
#ADMIN_URL=${ADMIN_URL:-http://172.26.10.84:10001}
TAOSD_STARTUP_TIMEOUT_SECOND=${TAOSD_STARTUP_TIMEOUT_SECOND:-160}
TAOSADAPTER_STARTUP_TIMEOUT_SECOND=${TAOSADAPTER_STARTUP_TIMEOUT_SECOND:-180}
TAOS_TIMEOUT_SECOND=${TAOS_TIMEOUT_SECOND:-10}
BACKUP_CORE_FOLDER=/var/log/corefile
ALERT_URL=app/system/alert/add
ALERT_DISABLE_FILE=/var/log/disable_alert
REBOOT_COUNT_RESET_FILE=/var/log/reset_reboot
START_TAOSD_MAX_NUMBER=${START_TAOSD_MAX_NUMBER:-3}
start_taosd_count=0
START_TAOSADAPTER_MAX_NUMBER=${START_TAOSADAPTER_MAX_NUMBER:-3}
TAOSD_ERROR_MAX_NUMBER=${START_TAOSADAPTER_MAX_NUMBER:-10}
start_taosadapter_count=0
taosd_error_count=0
SLEEP_INTERVAL=${SLEEP_INTERVAL:-10}
DNODE_CREATED=0
MNODE_CREATED=0
SNODE_CREATED=0

# set the timezone for the TDengine
if [ "$TZ" != "" ]; then
    ln -sf /usr/share/zoneinfo/$TZ /etc/localtime
    echo $TZ >/etc/timezone
fi
# copy taos.cfg to /var/log
TAOSDCLOG="/var/log/taosd_c"
if [ ! -d "$TAOSDCLOG" ]; then
  mkdir -p "$TAOSDCLOG"
  echo "taosd -C log folder created: $TAOSDCLOG"
  cp /etc/taos/taos.cfg /var/log/taos.cfg
  echo "logKeepDays 3" >> /var/log/taos.cfg
  sed -i 's/\/var\/log/\/var\/log\/taosd_c/g' /var/log/taos.cfg
  cat /var/log/taos.cfg
else
  echo "taosd -C log folder already exists: $TAOSDCLOG"
fi
TAODCONF=$(taosd -C -c '/var/log')
FQDN=$(echo "$TAODCONF"|grep -E 'fqdn.*(\S+)' -o |head -n1|sed 's/fqdn *//')
FIRSET_EP=$(echo "$TAODCONF"|grep -E 'firstEp.*(\S+)' -o|head -n1|sed 's/firstEp *//')
# parse first ep host and port
FIRST_EP_HOST=${FIRSET_EP%:*}
FIRST_EP_PORT=${FIRSET_EP#*:}
SERVER_PORT=$(echo "$TAODCONF"|grep -E 'serverPort.*(\S+)' -o|head -n1|sed 's/serverPort *//')
SERVER_PORT=${SERVER_PORT:-6030}
ENDPOINT=$FQDN:$SERVER_PORT
function logger() {
    logLevel=$1
    logMsg=$2
    echo "`date \"+%Y-%m-%d %H:%M:%S.%N\"` run.sh: [$logLevel] $logMsg" 2>&1 | tee -a /var/log/run.log
}
logger "INFO" "FQDN is $FQDN, FIRSTEP is $FIRST_EP_HOST and ENDPOINT is $ENDPOINT"
                      
ulimit -c unlimited
# set core files pattern, maybe failed
sysctl -w kernel.core_pattern=/corefile/core-$FQDN-%e-%p >/dev/null >&1

logger "INFO" "ADMIN_URL: ${ADMIN_URL}"
logger "INFO" "TAOS_TIMEOUT_SECOND: ${TAOS_TIMEOUT_SECOND}"

pid=""
function sigterm_handler() {
    logger "INFO" "sigterm signal received"
    if [ ! -z "$pid" ]; then
	logger "INFO" "send sigterm to $pid"
        if [ -d "/var/log" ]; then
            logger "INFO" "send sigterm to $pid"
        fi
        kill -15 $pid
        wait $pid
    fi
}
trap "echo SIGTERM; sigterm_handler; date; exit" SIGTERM
function set_service_state() {
    service_state="$1"
    service_msg="$2"
}

set_service_state "init" "ok"
app_name=`hostname |cut -d\- -f1`

function check_taosd() {
    timeout $TAOS_TIMEOUT_SECOND taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT -w 2000 -s "show databases;">/tmp/taosd.txt 2>&1
    local ret=$?
    if [ $ret -eq 0 ]; then
        cat /tmp/taosd.txt | grep -q "Query OK"
        ret=$?
        if [ $ret -ne 0 ]; then
            cat /tmp/taosd.txt
        fi
    fi
    if [ $ret -ne 0 ]; then
        logger "INFO" "checked taosd and got error $ret for $taosd_error_count times"
        if [ "x$1" != "xignore" ]; then
            taosd_error_count=$(( taosd_error_count + 1 ))
            set_service_state "error" "taosd checking failed with $ret for $taosd_error_count times"
            if [ ${taosd_error_count} -gt ${TAOSD_ERROR_MAX_NUMBER} ]; then
                # dump the taosd
                SUFFIX=$(date +"%Y%m%d%H%M")
                gcore -a -o /var/log/corefile/taosd.core.${SUFFIX} `pidof taosd`
                # alert the message
                post_error_msg
                # kill the taosd
                kill `pidof taosd`
                logger "ERROR" "sent -15 to kill taosd"
            fi 
        fi
    else
        set_service_state "ready" "ok"
        taosd_error_count=0
    fi
}
function post_error_msg() {
    logger "ERROR" "app_name: ${app_name}"
    logger "ERROR" "service_state: ${service_state}"
    logger "ERROR" "service_msg: ${service_msg}"
    if [ ! -z "${ADMIN_URL}" ]; then
        taos_version=`taos --version`
        logger "ERROR" "${taos_version}"
        if [ -f ${ALERT_DISABLE_FILE} ]; then
            logger "WARN" "alert disabled"
        else
            curl --connect-timeout 10 --max-time 20 -X POST -H "Content-Type: application/json" \
                -d"{\"appName\":\"${app_name}\",\
                \"alertLevel\":\"${service_state}\",\
                \"taosVersion\":\"${taos_version}\",\
                \"alertMsg\":\"${service_msg}\"}" \
                ${ADMIN_URL}/${ALERT_URL} 2>&1 | tee -a /var/log/run.log
        fi
    fi
}
function check_process_exit_type() {
    local core_pattern=`cat /proc/sys/kernel/core_pattern`
    logger "ERROR" "get the corefile patttern $core_pattern"
    echo "$core_pattern" | grep -q "^/"
    if [ $? -eq 0 ]; then
        core_folder=`dirname $core_pattern`
        core_prefix=`basename $core_pattern | sed "s/%.*//"`
    else
        core_folder=`pwd`
        core_prefix="$core_pattern"
    fi
    local core_files=`ls $core_folder | grep "^${core_prefix}"`
    if [ ! -z "$core_files" ]; then
        # move core files to another folder
        mkdir -p ${BACKUP_CORE_FOLDER}
        cp ${core_folder}/${core_prefix}* ${BACKUP_CORE_FOLDER}/
        rm -f ${core_folder}/${core_prefix}*
        if [ "x$1" = "xadapter" ]; then
            set_adapter_state "error" "taosadapter exit with core file"
        else
            set_service_state "error" "taosd exit with core file"
        fi
    else
        if [ "x$1" = "xadapter" ]; then
            set_adapter_state "error" "taosadapter exit without core file"
        else
            set_service_state "error" "taosd exit without core file"
        fi
    fi
}
function set_adapter_state() {
    adapter_state="$1"
    adapter_msg="$2"
}
set_adapter_state "init" "ok"
function check_taosadapter() {
    # timeout $TAOS_TIMEOUT_SECOND taos -R -E http://127.0.0.1:6041 -s "show databases;" >/dev/null
    timeout $TAOS_TIMEOUT_SECOND curl -L -H "Authorization: Basic cm9vdDp0YW9zZGF0YQ==" -d "show databases;" localhost:6041/rest/sql >/tmp/taosadapter.json 2>&1
    local ret=$?
    if [ $ret -eq 0 ]; then
        cat /tmp/taosadapter.json |grep -q "\"code\":0"
        ret=$?
        if [ $ret -ne 0 ]; then
            cat /tmp/taosadapter.json
        fi
    fi
    if [ $ret -ne 0 ]; then
        logger "ERROR" "check taosadapter error $ret"
        if [ "x$1" != "xignore" ]; then
            set_adapter_state "error" "taosd/taosadapter check failed $ret"
        fi
    else
        set_adapter_state "ready" "ok"
    fi
}
function post_adapter_error_msg() {
    if [ ! -z "${ADMIN_URL}" ]; then
        adapter_version=`taosadapter --version`
        logger "ERROR" "app_name: ${app_name}"
        logger "ERROR" "adapter_state: ${adapter_state}"
        logger "ERROR" "adapter_msg: ${adapter_msg}"
        logger "ERROR" "adapter_version: ${adapter_version}"
        if [ -f ${ALERT_DISABLE_FILE} ]; then
            logger "WARN" "alert disabled"
        else
            curl --connect-timeout 10 --max-time 20 -X POST -H "Content-Type: application/json" \
                -d"{\"appName\":\"${app_name}\",\
                \"alertLevel\":\"${adapter_state}\",\
                \"taosVersion\":\"${adapter_version}\",\
                \"alertMsg\":\"${adapter_msg}\"}" \
                ${ADMIN_URL}/${ALERT_URL} 2>&1 | tee -a /var/log/run.log
        fi
    fi
}
function print_adapter_state_change() {
    if [ "x$1" != "x${adapter_state}" ]; then
        logger "INFO" "adapter state: ${adapter_state}, ${adapter_msg}"
    fi
}
disk_usage_level=(60 80 99)
current_disk_level=0
disk_state="ok"
disk_msg="ok"
get_usage_ok="yes"
function post_disk_error_msg() {
    if [ ! -z "${ADMIN_URL}" ]; then
        taos_version=`taos --version`
        logger "ERROR" "app_name: ${app_name}"
        logger "ERROR" "disk_state: ${disk_state}"
        logger "ERROR" "disk_msg: ${disk_msg}"
        logger "ERROR" "taos_version: ${taos_version}"
        if [ -f ${ALERT_DISABLE_FILE} ]; then
            logger "WARN" "alert disabled"
        else
            curl --connect-timeout 10 --max-time 20 -X POST -H "Content-Type: application/json" \
                -d"{\"appName\":\"${app_name}\",\
                \"alertLevel\":\"${disk_state}\",\
                \"taosVersion\":\"${taos_version}\",\
                \"alertMsg\":\"${disk_msg}\"}" \
                ${ADMIN_URL}/${ALERT_URL} 2>&1 | tee -a /var/log/run.log
        fi
    fi
}
function check_disk() {
    local folder=`cat /etc/taos/taos.cfg|grep -v "^#"|grep dataDir|awk '{print $NF}'`
    if [ -z "$folder" ]; then
        folder="/var/lib/taos"
    fi
    local mount_point="$folder"
    local usage=""
    while [ -z "$usage" ]; do
        usage=`df -h|grep -w "${mount_point}"|awk '{print $5}'|grep -v Use|sed "s/%$//"`
        if [ "x${mount_point}" = "x/" ]; then
            break
        fi
        mount_point=`dirname ${mount_point}`
    done
    if [ -z "$usage" ]; then
        disk_state="error"
        disk_msg="cannot get disk usage"
        if [ "$get_usage_ok" = "yes" ]; then
            post_disk_error_msg
            get_usage_ok="no"
        fi
    else
        get_usage_ok="yes"
        local current_level=0
        for level in ${disk_usage_level[*]}; do
            if [ ${usage} -ge ${level} ]; then
                disk_state="error"
                disk_msg="disk usage over ${level}%, current usage: ${usage}%"
                current_level=${level}
            fi
        done
        if [ ${current_level} -gt ${current_disk_level} ]; then
            post_disk_error_msg
        elif [ ${current_level} -lt ${current_disk_level} ]; then
            # hysteresis comparator
            local downgrade_usage=$(( current_disk_level - 4 ))
            if [ ${usage} -lt ${downgrade_usage} ]; then
                logger "INFO" "disk usage reduced from ${current_disk_level} to ${current_level}"
            else
                # echo "`date \"+%Y-%m-%d %H:%M:%S.%N\"` run.sh:disk usage level downgrade not ready: ${usage} still above ${downgrade_usage}"
                current_level=${current_disk_level}
            fi
        fi
        current_disk_level=${current_level}
    fi
}
function run_taosd() {
    local count=0
    trap "echo SIGTERM; sigterm_handler; exit" SIGTERM
    if [ -d "/var/log" ]; then
        logger "INFO" "taosd start"
    fi
    taosd &
    pid=$!
    wait $pid
    local ret=$?
    logger "INFO" "taosd exit $ret $pid"
    if [ -d "/var/log" ]; then
        logger "INFO" "taosd exit $ret $pid"
    fi
    if [ $ret -eq 0 ]; then
        logger "INFO" "$pid exit caused by sigterm"
        return
    fi
    set_service_state "error" "taosd $pid exit"
    logger "ERROR" "set taosd state existed"
    # post error msg
    # check crash or OOM
    check_process_exit_type "taosd"
    post_error_msg
}
function run_taosadapter() {
    taosadapter
    set_adapter_state "error" "taosadapter exit"
    # post error msg
    # check crash or OOM
    check_process_exit_type "adapter"
    post_adapter_error_msg
}
function print_service_state_change() {
    if [ "x$1" != "x${service_state}" ]; then
        logger "INFO" "service state: ${service_state}, ${service_msg}"
    fi
}
function initDnodeAndMnode {
    while true
    do 
        if [ $DNODE_CREATED -eq 1 ] && [ $MNODE_CREATED -eq 1 ] && [ $SNODE_CREATED -eq 1 ]; then
            break 
        fi
        # first check dnode created
        DNODETmp=$(timeout $TAOS_TIMEOUT_SECOND taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT -w 2000 -s "show dnodes;" | grep -E "$ENDPOINT" | awk '{split($0,a,"|");print a[1]}')
        if [[ "$DNODETmp" == "" ]]; then
            timeout $TAOS_TIMEOUT_SECOND taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT -s "create dnode \"$ENDPOINT\";create user admin_user pass 'NDS65R6t' sysinfo 0;"  
            DNODETmp=$(timeout $TAOS_TIMEOUT_SECOND taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT -w 2000 -s "show dnodes;" | grep -E "$ENDPOINT" | awk '{split($0,a,"|");print a[1]}')
            if [[ "$DNODETmp" != "" ]]; then
                DNODE_CREATED=1
                logger "INFO" "Created the dnode with endpoint $ENDPOINT"
            else
                logger "ERROR" "failed to create dnode $ENDPOINT through taos"
            fi
        else
            DNODE_CREATED=1
            logger "INFO" "Dnode $ENDPOINT already created "
        fi
        if [ $DNODE_CREATED -eq 1 ] && [ "$FQDN" == "$FIRST_EP_HOST" ]; then
            #check snode created
            SNODETmp=$(timeout $TAOS_TIMEOUT_SECOND taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT  -w 2000 -s "show snodes;" | grep -E "$ENDPOINT" | awk '{split($0,a,"|");print a[1]}')
            if [[ "$SNODETmp" == "" ]]; then
                DNODEID=$(echo "$DNODETmp" | sed -e 's/^[[:space:]]*//')
                if [[ "$DNODEID" != "" ]]; then
                    taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT -s "create snode on dnode $DNODEID;"
                    SNODETmp=$(timeout $TAOS_TIMEOUT_SECOND taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT -w 2000 -s "show snodes;" | grep -E "$ENDPOINT" | awk '{split($0,a,"|");print a[1]}')
                    if [[ "$SNODETmp" != "" ]]; then
                        SNODE_CREATED=1
                        logger "INFO" "Created the snode for dnode $DNODEID"
                    else 
                        logger "ERROR" "failed to create snode for dnode $ENDPOINT through taos"
                    fi
                fi
            else 
                SNODE_CREATED=1
                logger "INFO" "Snode $SNODETmp already created"
            fi
        else
            #snode can only be create once;
            SNODE_CREATED=1
        fi
        if [[ "$FQDN" != "$FIRST_EP_HOST" ]]; then
            # second check mnode created
            MNODETmp=$(timeout $TAOS_TIMEOUT_SECOND taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT  -w 2000 -s "show mnodes;" | grep -E "$ENDPOINT" | awk '{split($0,a,"|");print a[1]}')
            if [[ "$MNODETmp" == "" ]]; then
                DNODEID=$(echo "$DNODETmp" | sed -e 's/^[[:space:]]*//')
                if [[ "$DNODEID" != "" ]]; then
                    taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT -s "create mnode on dnode $DNODEID;"
                    MNODETmp=$(timeout $TAOS_TIMEOUT_SECOND taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT -w 2000 -s "show mnodes;" | grep -E "$ENDPOINT" | awk '{split($0,a,"|");print a[1]}')
                    if [[ "$MNODETmp" != "" ]]; then
                        MNODE_CREATED=1
                        logger "INFO" "Created the mnode for dnode $DNODEID"
                    else 
                        logger "ERROR" "failed to create mnode for dnode $ENDPOINT through taos"
                    fi
                fi
            else 
                MNODE_CREATED=1
                logger "INFO" "Mnode $MNODETmp already created"
            fi
        else
            # check admin_user created or not
            ADMINUSER=$(timeout $TAOS_TIMEOUT_SECOND taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT -s "show users;" | grep -E "admin_user" -o)
            if [[ "$ADMINUSER" == "" ]]; then
                timeout $TAOS_TIMEOUT_SECOND taos -h $FIRST_EP_HOST -P $FIRST_EP_PORT -s "create user admin_user pass 'NDS65R6t' sysinfo 0;"  
                logger "INFO" "created admin_user"
            fi
            MNODE_CREATED=1
            logger "INFO" "This is master dnode and no need to create mnode"
        fi
    done
}
taosd_start_time=`date +%s`
taosadapter_start_time=$taosd_start_time
while ((1))
do
    # check_disk disable disk alert
    # echo "`date \"+%Y-%m-%d %H:%M:%S.%N\"` run.sh:outer loop: $a"
    output=`timeout $TAOS_TIMEOUT_SECOND taos -k | tail -n 1`
    if [ -z "${output}" ]; then
        logger "ERROR" "taos -k error"
        status=""
    else
        if [[ "${output}" = *"UTL FATAL"* ]]; then
            logger "ERROR" "taos -k with error: \"${output}\""
            status=""
	    else
            status=${output:0:1}
	    fi
    fi
    # echo "`date \"+%Y-%m-%d %H:%M:%S.%N\"` run.sh:taos -k output: $output"
    # echo "`date \"+%Y-%m-%d %H:%M:%S.%N\"` run.sh:taos status: $status"
    if [ -f ${REBOOT_COUNT_RESET_FILE} ]; then
        start_taosd_count=0
        start_taosadapter_count=0
    fi
    if [ "$status"x = "0"x ]; then
        logger "INFO" "taos -k output is $output"
        td_cluster_check "CheckClusterStatus"
        if [ $? -eq 0 ]; then
            logger "INFO" "start taosd count: ${start_taosd_count}"
            if [ ${start_taosd_count} -gt ${START_TAOSD_MAX_NUMBER} ]; then
                logger "ERROR" "exceed restart max count: ${START_TAOSD_MAX_NUMBER}"
                break
            fi
            start_taosd_count=$(( start_taosd_count + 1 ))
            # taosd_start_time=`date +%s`
            run_taosd &
            pid=$!
            clustercheckneeded="0"
        fi
    fi
    # echo "`date \"+%Y-%m-%d %H:%M:%S.%N\"` run.sh:$status"x "$TAOS_RUN_TAOSBENCHMARK_TEST"x "$TAOS_RUN_TAOSBENCHMARK_TEST_ONCE"x
    if [ "$status"x = "2"x ]; then
        initDnodeAndMnode
        if [ "$clustercheckneeded"x = "0"x ]; then
            td_cluster_check "no"
            if [ $? -eq 0 ]; then
                clustercheckneeded="1"
                logger "INFO" "the cluster is ready to write/read in dnode $FQDN and set status to 6 and clustercheckneeded to 1"
            else 
                logger "ERROR" "the cluster status check failed"
            fi 
        fi
    fi
            #logger "INFO" "enable to generate test db: $TAOS_RUN_TAOSBENCHMARK_TEST; already generated test db: $TAOS_RUN_TAOSBENCHMARK_TEST_ONCE"
    if [ "$clustercheckneeded"x = "1"x ] && [ "$TAOS_RUN_TAOSBENCHMARK_TEST"x = "1"x ] && [ "$TAOS_RUN_TAOSBENCHMARK_TEST_ONCE"x = "0"x ] && [[ "$FQDN" = "$FIRST_EP_HOST" ]]; then
        logger "INFO" "begin to check test db existed or not"
        dbs=`taos -s "select name from information_schema.ins_databases where name='test';"`
        if [ $? -eq 0 ]; then
            testDB=`echo "$dbs" | grep -w -o " test"`
            createTest=""
            if [ "$testDB"x != ""x ]; then
                testStables=`taos -s "select stable_name from information_schema.ins_stables where db_name = 'test';"`
                if [ $? -eq 0 ]; then
                    testStable=`echo $testStables | grep -w -o meters`
                    if [ "$testStable"x = ""x ]; then
                        createTest="0"
                        logger "INFO" "test database existed but meters stable does not exist"
                    fi
                else 
                    createTest="2"
                    logger "ERROR" "failed to query meters stable from information_schema"
                fi
            else 
                createTest="1"
            fi
            if [ "$createTest"x = "0"x ] || [ "$createTest"x = "1"x ]; then
                if [ "$createTest"x = "0"x ]; then
                    taosBenchmark -Q -t 1000 -n 1000 -S 1000 -H 200 -y 
                else 
                    taosBenchmark -t 1000 -n 1000 -S 1000 -H 200 -y
                fi
                taos -s "alter database test WAL_RETENTION_PERIOD 3600;GRANT ALL on test.* to admin_user;"
                TAOS_RUN_TAOSBENCHMARK_TEST_ONCE=1
                logger "INFO" "taosBenchmark executed to generate test database"
            else 
                if [ "$createTest"x = ""x ]; then
                    TAOS_RUN_TAOSBENCHMARK_TEST_ONCE=1
                    logger "INFO" "test database existed and no need to check to create test database"
                fi
            fi
        else 
            logger "ERROR" "failed to show all databases"
        fi
    fi
    # check taosd status
    if [ "$service_state" = "ready" ]; then
        # check taosd status
        check_taosd
        print_service_state_change "ready"
        if [ "$service_state" = "error" ]; then
            post_error_msg
        fi
    elif [ "$service_state" = "init" ]; then
        check_taosd "ignore"
        # check timeout
        current_time=`date +%s`
        time_elapsed=$(( current_time - taosd_start_time ))
        if [ ${time_elapsed} -gt ${TAOSD_STARTUP_TIMEOUT_SECOND} ]; then
            set_service_state "error" "taosd startup timeout"
            post_error_msg
        fi
        print_service_state_change "init"
    elif [ "$service_state" = "error" ]; then
        # check taosd status
        check_taosd
        print_service_state_change "error"
    fi
    # check taosadapter
    nc -z localhost 6041
    if [ $? -ne 0 ] && [ "$clustercheckneeded"x = "1"x ]; then
        logger "INFO" "start taosadapter count: ${start_taosadapter_count}"
        if [ ${start_taosadapter_count} -gt ${START_TAOSADAPTER_MAX_NUMBER} ]; then
            logger "ERROR" "exceed restart adapter max count: ${START_TAOSADAPTER_MAX_NUMBER}"
            break
        fi
        start_taosadapter_count=$(( start_taosadapter_count + 1 ))
        run_taosadapter &
    fi
    if [ "$service_state" = "ready" ]; then
        if [ "${adapter_state}" = "ready" ]; then
            check_taosadapter
            print_adapter_state_change "ready"
            if [ "$adapter_state" = "error" ]; then
                post_adapter_error_msg
            fi
        elif [ "${adapter_state}" = "init" ]; then
            check_taosadapter "ignore"
            # check timeout
            current_time=`date +%s`
            time_elapsed=$(( current_time - taosadapter_start_time ))
            if [ ${time_elapsed} -gt ${TAOSADAPTER_STARTUP_TIMEOUT_SECOND} ]; then
                set_adapter_state "error" "taosadapter startup timeout"
                post_adapter_error_msg
            fi
            print_adapter_state_change "init"
        elif [ "${adapter_state}" = "error" ]; then
            check_taosadapter
            print_adapter_state_change "error"
        fi
    fi
    sleep ${SLEEP_INTERVAL}
done