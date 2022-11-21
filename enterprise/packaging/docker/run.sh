#!/bin/bash

TAOS_RUN_TAOSBENCHMARK_TEST_ONCE=0
#ADMIN_URL=${ADMIN_URL:-http://172.26.10.84:10001}
TAOSD_STARTUP_TIMEOUT_SECOND=${TAOSD_STARTUP_TIMEOUT_SECOND:-160}
TAOS_TIMEOUT_SECOND=${TAOS_TIMEOUT_SECOND:-5}
BACKUP_CORE_FOLDER=/data/corefile
ALERT_URL=app/system/alert/add

echo "ADMIN_URL: ${ADMIN_URL}"
echo "TAOS_TIMEOUT_SECOND: ${TAOS_TIMEOUT_SECOND}"

function set_service_state() {
    #echo "set service state: $1, $2"
    service_state="$1"
    service_msg="$2"
}
set_service_state "init" "ok"
app_name=`hostname |cut -d\- -f1`

function check_taosd() {
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
function post_error_msg() {
    if [ ! -z "${ADMIN_URL}" ]; then
        taos_version=`taos --version`
        echo "app_name: ${app_name}"
        echo "service_state: ${service_state}"
        echo "`date` service_msg: ${service_msg}"
        echo "${taos_version}"
        curl -X POST -H "Content-Type: application/json" \
            -d"{\"appName\":\"${app_name}\",\
            \"alertLevel\":\"${service_state}\",\
            \"taosVersion\":\"${taos_version}\",\
            \"alertMsg\":\"${service_msg}\"}" \
            ${ADMIN_URL}/${ALERT_URL}
    fi
}
function check_taosd_exit_type() {
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
        mv ${core_folder}/${core_prefix}* ${BACKUP_CORE_FOLDER}/
        set_service_state "error" "taosd exit with core file"
    else
        set_service_state "error" "taosd exit without core file"
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
        curl -X POST -H "Content-Type: application/json" \
            -d"{\"appName\":\"${app_name}\",\
            \"alertLevel\":\"${disk_state}\",\
            \"taosVersion\":\"${taos_version}\",\
            \"alertMsg\":\"${disk_msg}\"}" \
            ${ADMIN_URL}/${ALERT_URL}
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
                disk_msg="disk usage over ${level}%"
                current_level=${level}
            fi
        done
        if [ ${current_level} -gt ${current_disk_level} ]; then
            post_disk_error_msg
        elif [ ${current_level} -lt ${current_disk_level} ]; then
            echo "disk usage reduced from ${current_disk_level} to ${current_level}"
        fi
        current_disk_level=${current_level}
    fi
}
function run_taosd() {
    taosd
    set_service_state "error" "taosd exit"
    # post error msg
    # check crash or OOM
    check_taosd_exit_type
    post_error_msg
}
function print_service_state_change() {
    if [ "x$1" != "x${service_state}" ]; then
        echo "`date`   service state: ${service_state}, ${service_msg}"
    fi
}
taosd_start_time=`date +%s`
while ((1))
do
    check_disk
    # echo "outer loop: $a"
    output=`timeout $TAOS_TIMEOUT_SECOND taos -k`
    if [ -z "${output}" ]; then
        echo "`date` taos -k error"
        status=""
    else
        status=${output:0:1}
    fi
    # echo $output
    # echo $status
    if [ "$status"x = "0"x ]
    then
        # taosd_start_time=`date +%s`
        run_taosd &
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
        taosadapter &
    fi
    sleep 30
done
