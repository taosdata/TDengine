#!/bin/bash

machine_index=$1
thread_no=$2
task_file=$3
lock_file=$4
index_file=$5
log_dir=$6
workdir=$7
username=$8
password=$9
host=${10}
ent=${11}
timeout_param=${12}
success_case_file=${13}
failed_case_file=${14}
web_server=${15}
test_log_dir=${16}

# 判断是否本地主机
function is_local_host() {
    local check_host="$1"
    local local_hostnames=("127.0.0.1" "localhost" "::1" "$(hostname)" "$(hostname -I | awk '{print $1}')")
    for lh in "${local_hostnames[@]}"; do
        if [[ "$check_host" == "$lh" ]]; then
            return 0
        fi
    done
    return 1
}

# 获取远程SSH命令
function get_remote_ssh_command() {
    if [ -z "$password" ]; then
        echo "ssh -o StrictHostKeyChecking=no $username@$host"
    else
        echo "sshpass -p $password ssh -o StrictHostKeyChecking=no $username@$host"
    fi
}

# 运行测试脚本
runcase_script=""
if ! is_local_host "$host"; then
    runcase_script=$(get_remote_ssh_command)
fi

script="$workdir/TDengine/test/ci/run_container.sh"
if [ "$ent" -eq 1 ]; then
    script="$workdir/TDinternal/community/test/ci/run_container.sh -e"
fi

count=0
while true; do
    line=$(flock -x "$lock_file" -c "head -n1 $task_file; sed -i '1d' $task_file")
    if [ "x$line" = "x%%FINISHED%%" ]; then
        break
    fi
    if [ -z "$line" ]; then
        continue
    fi
    if echo "$line" | grep -q "^#"; then
        continue
    fi
    
    # 解析用例信息
    case_redo_time=$(echo "$line" | cut -d, -f2)
    if [ -z "$case_redo_time" ]; then
        case_redo_time=2
    fi
    
    case_build_san=$(echo "$line" | cut -d, -f3)
    if [ "$case_build_san" == "y" ]; then
        case_build_san="y"
        DEBUGPATH="debugSan"
    elif [[ "$case_build_san" == "n" ]] || [[ "$case_build_san" == "" ]]; then
        case_build_san="n"
        DEBUGPATH="debugNoSan"
    else
        continue
    fi
    
    exec_dir=$(echo "$line" | cut -d, -f4)
    
    case_cmd=$(echo "$line" | cut -d, -f5)
    
    # 提取用例文件名
    case_file=""
    if echo "$case_cmd" | grep -q "\.sh"; then
        case_file=$(echo "$case_cmd" | grep -o ".*\.sh" | awk '{print $NF}')
    elif echo "$case_cmd" | grep -q "^./ci/pytest.sh"; then
        case_file=$(echo "$case_cmd" | grep -o ".*\.py" | awk '{print $NF}')
    elif echo "$case_cmd" | grep -q "^pytest"; then
        if [[ "$case_cmd" == *".py"* ]]; then
            case_file=$(echo "$case_cmd" | grep -o ".*\.py" | awk '{print $NF}')
        fi
    fi
    
    if [ -z "$case_file" ]; then
        case_file=$(echo "$case_cmd" | awk '{print $NF}')
    fi
    
    if [ -z "$case_file" ]; then
        continue
    fi
    
    if [ "$exec_dir" == "." ]; then
        case_file="${case_file}.${machine_index}.${thread_no}.${count}"
    else
        case_file="${exec_dir}/${case_file}.${machine_index}.${thread_no}.${count}"
    fi
    
    count=$((count + 1))
    
    # 创建日志目录
    case_path=$(dirname "$case_file")
    if [ -n "$case_path" ]; then
        mkdir -p "$log_dir"/"$case_path"
    fi
    
    # 构建命令
    cmd="${runcase_script} ${script} -w ${workdir} -c \"${case_cmd}\" -t ${thread_no} -d ${exec_dir} -s ${case_build_san} ${timeout_param}"
    
    # 运行测试
    case_log_file="$log_dir/${case_file}.txt"
    case_index=$(flock -x "$lock_file" -c "sh -c \"echo \$(( \$(cat $index_file) + 1 )) | tee $index_file\"")
    case_index=$(printf "%5d" "$case_index")
    
    echo "Running case $case_index on machine $machine_index thread $thread_no: $case_cmd"
    
    # 执行命令
    ret=0
    redo_count=1
    start_time=$(date +%s)
    while [ "${redo_count}" -le "$case_redo_time" ]; do
        if [ -f "$case_log_file" ]; then
            cp "$case_log_file" "$log_dir"/"$case_file".${redo_count}.redotxt
        fi
        echo "${host}-${thread_no} order:${count}, redo:${redo_count} task:${line}" >"$case_log_file"
        current_time=$(date "+%Y-%m-%d %H:%M:%S")
        echo -e "$case_index \e[33m START >>>>> \e[0m ${case_cmd} \e[33m[$current_time]\e[0m"
        echo "$current_time" >>"$case_log_file"
        real_start_time=$(date +%s)
        
        if [ -z "$runcase_script" ]; then
            $cmd >>"$case_log_file" 2>&1
        else
            $cmd >>"$case_log_file" 2>&1
        fi
        
        ret=$?
        real_end_time=$(date +%s)
        time_elapsed=$((real_end_time - real_start_time))
        echo "execute time: ${time_elapsed}s" >>"$case_log_file"
        current_time=$(date "+%Y-%m-%d %H:%M:%S")
        echo "${host} $current_time exit code:${ret}" >>"$case_log_file"
        
        if [ "$ret" -eq 0 ]; then
            break
        fi
        
        redo=0
        if grep -q "wait too long for taosd start" "$case_log_file"; then
            redo=1
        fi
        if grep -q "kex_exchange_identification: Connection closed by remote host" "$case_log_file"; then
            redo=1
        fi
        if grep -q "ssh_exchange_identification: Connection closed by remote host" "$case_log_file"; then
            redo=1
        fi
        if grep -q "kex_exchange_identification: read: Connection reset by peer" "$case_log_file"; then
            redo=1
        fi
        if grep -q "Database not ready" "$case_log_file"; then
            redo=1
        fi
        if grep -q "Unable to establish connection" "$case_log_file"; then
            redo=1
        fi
        if [ "$redo_count" -lt "$case_redo_time" ]; then
            redo=1
        fi
        if [ "$redo" -eq 0 ]; then
            break
        fi
        redo_count=$((redo_count + 1))
    done
    
    end_time=$(date +%s)
    echo >>"$case_log_file"
    total_time=$((end_time - start_time))
    echo "${host} total time: ${total_time}s" >>"$case_log_file"
    
    # 处理测试结果
    if [ "$ret" -eq 0 ]; then
        echo -e "$case_index \e[34m DONE  <<<<< \e[0m ${case_cmd} \e[34m[${total_time}s]\e[0m \e[32m success\e[0m"
        flock -x "$lock_file" -c "echo \"${case_cmd}|success|${total_time}\" >> ${success_case_file}"
    else
        if [ -n "${web_server}" ]; then
            flock -x "$lock_file" -c "echo -e \"${host} ret:${ret} ${line}\n  ${web_server}/${test_log_dir}/${case_file}.txt\" >> ${failed_case_file}"
        else
            flock -x "$lock_file" -c "echo -e \"${host} ret:${ret} ${line}\n  log file: ${case_log_file}\" >> ${failed_case_file}"
        fi
        
        echo -e "$case_index \e[34m DONE  <<<<< \e[0m ${case_cmd} \e[34m[${total_time}s]\e[0m \e[31m failed\e[0m"
        echo "=========================log==========================="
        cat "$case_log_file"
        echo "====================================================="
        echo -e "\e[34m log file: $case_log_file \e[0m"
        
        if [ -n "${web_server}" ]; then
            echo "${web_server}/${test_log_dir}/${case_file}.txt"
        fi
    fi
done

echo "Thread $thread_no on machine $machine_index finished"
