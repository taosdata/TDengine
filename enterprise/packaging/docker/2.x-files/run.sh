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
start_taosadapter_count=0
SLEEP_INTERVAL=${SLEEP_INTERVAL:-10}

echo "ADMIN_URL: ${ADMIN_URL}"
echo "TAOS_TIMEOUT_SECOND: ${TAOS_TIMEOUT_SECOND}"

pid=""
function sigterm_handler() {
    echo "`date` sigterm received"
    if [ ! -z "$pid" ]; then
	echo "send sigterm to $pid"
        if [ -d "/var/log" ]; then
            echo "`date` send sigterm to $pid" >>/var/log/run.log
        fi
        kill -15 $pid
        wait $pid
    fi
}
trap "echo SIGTERM; sigterm_handler; date; exit" SIGTERM
function set_service_state() {
    #echo "set service state: $1, $2"
    service_state="$1"
    service_msg="$2"
}
set_service_state "init" "ok"
app_name=`hostname |cut -d\- -f1`

function check_taosd_deprecated() {
    timeout $TAOS_TIMEOUT_SECOND taos -s "show databases;" >/dev/null
    local ret=$?
    if [ $ret -ne 0 ]; then
        echo "`date` check taosd error $ret"
        if [ "x$1" != "xignore" ]; then
            set_service_state "error" "taos check failed $ret"
        fi
    else
        set_service_state "ready" "ok"
    fi
}
function check_taosd_deprecated_1() {
    local output=`timeout $TAOS_TIMEOUT_SECOND taos -k`
    if [ -z "${output}" ]; then
        echo "`date` taos -k error"
        if [ "x$1" != "xignore" ]; then
            set_service_state "error" "taos check failed (no output)"
        fi
    else
        echo "$output"|grep -q "^2"
        if [ $? -ne 0 ]; then
            if [ "x$1" != "xignore" ]; then
                set_service_state "error" "taos check failed $output"
            fi
        else
            set_service_state "ready" "ok"
        fi
    fi
}
function check_taosd() {
    # timeout $TAOS_TIMEOUT_SECOND taos -R -E http://127.0.0.1:6041 -s "show databases;" >/dev/null
    timeout $TAOS_TIMEOUT_SECOND curl -L -H "Authorization: Basic cm9vdDp0YW9zZGF0YQ==" -d "show databases;" localhost:6041/rest/sql >/tmp/taosd.json 2>&1
    local ret=$?
    if [ $ret -eq 0 ]; then
        cat /tmp/taosd.json |grep -q "\"code\":0"
        ret=$?
        if [ $ret -ne 0 ]; then
            cat /tmp/taosd.json
        fi
    fi
    if [ $ret -ne 0 ]; then
        echo "`date` check taosd error $ret"
        if [ "x$1" != "xignore" ]; then
            set_service_state "error" "taosd/taosadapter check failed $ret"
        fi
    else
        set_service_state "ready" "ok"
    fi
}
function post_error_msg() {
    echo "app_name: ${app_name}"
    echo "service_state: ${service_state}"
    echo "`date` service_msg: ${service_msg}"
    if [ ! -z "${ADMIN_URL}" ]; then
        taos_version=`taos --version`
        echo "${taos_version}"
        if [ -f ${ALERT_DISABLE_FILE} ]; then
            echo "alert disabled"
        else
            curl --connect-timeout 10 --max-time 20 -X POST -H "Content-Type: application/json" \
                -d"{\"appName\":\"${app_name}\",\
                \"alertLevel\":\"${service_state}\",\
                \"taosVersion\":\"${taos_version}\",\
                \"alertMsg\":\"${service_msg}\"}" \
                ${ADMIN_URL}/${ALERT_URL}
        fi
    fi
}
function check_process_exit_type() {
    local core_pattern=`cat /proc/sys/kernel/core_pattern`
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
    #echo "set adapter state: $1, $2"
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
        echo "`date` check taosadapter error $ret"
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
        echo "app_name: ${app_name}"
        echo "adapter_state: ${adapter_state}"
        echo "`date` adapter_msg: ${adapter_msg}"
        echo "${adapter_version}"
        if [ -f ${ALERT_DISABLE_FILE} ]; then
            echo "alert disabled"
        else
            curl --connect-timeout 10 --max-time 20 -X POST -H "Content-Type: application/json" \
                -d"{\"appName\":\"${app_name}\",\
                \"alertLevel\":\"${adapter_state}\",\
                \"taosVersion\":\"${adapter_version}\",\
                \"alertMsg\":\"${adapter_msg}\"}" \
                ${ADMIN_URL}/${ALERT_URL}
        fi
    fi
}
function print_adapter_state_change() {
    if [ "x$1" != "x${adapter_state}" ]; then
        echo "`date`   adapter state: ${adapter_state}, ${adapter_msg}"
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
        echo "app_name: ${app_name}"
        echo "disk_state: ${disk_state}"
        echo "`date` disk_msg: ${disk_msg}"
        echo "${taos_version}"
        if [ -f ${ALERT_DISABLE_FILE} ]; then
            echo "alert disabled"
        else
            curl --connect-timeout 10 --max-time 20 -X POST -H "Content-Type: application/json" \
                -d"{\"appName\":\"${app_name}\",\
                \"alertLevel\":\"${disk_state}\",\
                \"taosVersion\":\"${taos_version}\",\
                \"alertMsg\":\"${disk_msg}\"}" \
                ${ADMIN_URL}/${ALERT_URL}
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
                echo "disk usage reduced from ${current_disk_level} to ${current_level}"
            else
                # echo "disk usage level downgrade not ready: ${usage} still above ${downgrade_usage}"
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
        echo "`date` taosd start" >>/var/log/run.log
    fi
    taosd &
    pid=$!
    wait $pid
    local ret=$?
    echo "`date` taosd exit $ret"
    if [ -d "/var/log" ]; then
        echo "`date` taosd exit $ret" >>/var/log/run.log
    fi
    if [ $ret -eq 0 ]; then
        echo "`date` exit caused by sigterm"
        return
    fi
    echo "`date` set taosd state"
    set_service_state "error" "taosd exit"
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
        echo "`date`   service state: ${service_state}, ${service_msg}"
    fi
}
taosd_start_time=`date +%s`
taosadapter_start_time=$taosd_start_time
while ((1))
do
    check_disk
    # echo "outer loop: $a"
    output=`timeout $TAOS_TIMEOUT_SECOND taos -k | tail -n 1`
    if [ -z "${output}" ]; then
        echo "`date` taos -k error"
        status=""
    else
        status=${output:0:1}
    fi
    # echo "taos -k output: $output"
    # echo "taos status: $status"
    if [ -f ${REBOOT_COUNT_RESET_FILE} ]; then
        start_taosd_count=0
        start_taosadapter_count=0
    fi
    if [ "$status"x = "0"x ]
    then
        echo "start taosd count: ${start_taosd_count}"
        if [ ${start_taosd_count} -gt ${START_TAOSD_MAX_NUMBER} ]; then
            echo "exceed restart max count: ${START_TAOSD_MAX_NUMBER}"
            break
        fi
        start_taosd_count=$(( start_taosd_count + 1 ))
        # taosd_start_time=`date +%s`
        run_taosd &
        pid=$!
    fi
    # echo "$status"x "$TAOS_RUN_TAOSBENCHMARK_TEST"x "$TAOS_RUN_TAOSBENCHMARK_TEST_ONCE"x
    if [ "$status"x = "2"x ] && [ "$TAOS_RUN_TAOSBENCHMARK_TEST"x = "1"x ] && [ "$TAOS_RUN_TAOSBENCHMARK_TEST_ONCE"x = "0"x ]
    then
        TAOS_RUN_TAOSBENCHMARK_TEST_ONCE=1
        # result=`taos -s "show databases;" | grep " test "`
        # if [ "${result:0:5}"x != " test"x ]
        # then
        #     taosBenchmark -y -t 1000 -n 1000 -S 900000
        # fi
        taos -s "select stable_name from information_schema.ins_stables where db_name = 'test';"|grep -q -w meters
        if [ $? -ne 0 ]; then
            taosBenchmark -y -t 1000 -n 1000 -S 900000
            taos -s "create user admin_user pass 'NDS65R6t' sysinfo 0;"
            taos -s "GRANT ALL on test.* to admin_user;"
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
    if [ $? -ne 0 ]; then
        echo "start taosadapter count: ${start_taosadapter_count}"
        if [ ${start_taosadapter_count} -gt ${START_TAOSADAPTER_MAX_NUMBER} ]; then
            echo "exceed restart adapter max count: ${START_TAOSADAPTER_MAX_NUMBER}"
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
