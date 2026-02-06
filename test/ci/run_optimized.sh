#!/bin/bash
# 优化后的测试运行脚本

set -e

function usage() {
    echo "$0"
    echo -e "\t -m vm config file"
    echo -e "\t -t task file"
    echo -e "\t -b branch"
    echo -e "\t -l log dir"
    echo -e "\t -e enterprise edition"
    echo -e "\t -o default timeout value"
    echo -e "\t -w log web server"
    echo -e "\t -h help"
}

ent=0
config_file=""
task_file=""
branch=""
log_dir=""
timeout_param=""
web_server=""

while getopts "m:t:b:l:o:w:eh" opt; do
    case $opt in
    m)
        config_file=$OPTARG
        ;;
    t)
        task_file=$OPTARG
        ;;
    b)
        branch=$OPTARG
        ;;
    l)
        log_dir=$OPTARG
        ;;
    e)
        ent=1
        ;;
    o)
        timeout_param="-o $OPTARG"
        ;;
    w)
        web_server=$OPTARG
        ;;
    h)
        usage
        exit 0
        ;;
    \?)
        echo "Invalid option: -$OPTARG"
        usage
        exit 1
        ;;
    esac
done

if [ -z "$config_file" ]; then
    usage
    exit 1
fi

if [ ! -f "$config_file" ]; then
    echo "$config_file not found"
    exit 1
fi

if [ -z "$task_file" ]; then
    usage
    exit 1
fi

if [ ! -f "$task_file" ]; then
    echo "$task_file not found"
    exit 1
fi

# 自动运行优化分配脚本
echo "Running optimization allocation..."
python3 "$(dirname "$0")/optimize_allocation.py" "$(dirname "$task_file")/cases-list.txt" "$task_file"

if [ $? -ne 0 ]; then
    echo "Error running optimize_allocation.py"
    exit 1
fi

echo "Optimization allocation completed"

# 读取机器配置
hosts=()
usernames=()
passwords=()
workdirs=()
threads=()

i=0
while true; do
    host=$(jq .[$i].host "$config_file")
    if [ "$host" = "null" ]; then
        break
    fi
    username=$(jq .[$i].username "$config_file")
    if [ "$username" = "null" ]; then
        break
    fi
    password=$(jq .[$i].password "$config_file")
    if [ "$password" = "null" ]; then
        password=""
    fi
    workdir=$(jq .[$i].workdir "$config_file")
    if [ "$workdir" = "null" ]; then
        break
    fi
    thread=$(jq .[$i].thread "$config_file")
    if [ "$thread" = "null" ]; then
        break
    fi
    hosts[i]=$(echo "$host" | sed 's/\"$//' | sed 's/^\"//')
    usernames[i]=$(echo "$username" | sed 's/\"$//' | sed 's/^\"//')
    passwords[i]=$(echo "$password" | sed 's/\"$//' | sed 's/^\"//')
    workdirs[i]=$(echo "$workdir" | sed 's/\"$//' | sed 's/^\"//')
    threads[i]=$thread
    i=$((i + 1))
done

if [ ${#hosts[@]} -eq 0 ]; then
    echo "No hosts found in config file"
    exit 1
fi

# 检查分配文件
allocation_dir="$(dirname "$task_file")/allocations"
if [ ! -d "$allocation_dir" ]; then
    echo "Allocation directory not found: $allocation_dir"
    echo "Please run optimize_allocation.py first"
    exit 1
fi

# 生成运行时配置
date_tag=$(date +%Y%m%d-%H%M%S)
test_log_dir=${branch}_${date_tag}
if [ -z "$log_dir" ]; then
    log_dir="log/${test_log_dir}"
else
    log_dir="$log_dir/${test_log_dir}"
fi

mkdir -p "$log_dir"
rm -rf "${log_dir:?}"/*
mkdir "$log_dir/allure-results"

# 运行测试的函数
function run_tests_on_machine() {
    local machine_index=$1
    local machine_cases_file="$allocation_dir/machine_${machine_index}_cases.txt"
    
    if [ ! -f "$machine_cases_file" ]; then
        echo "Cases file not found for machine $machine_index: $machine_cases_file"
        return 1
    fi
    
    local host="${hosts[$machine_index]}"
    local username="${usernames[$machine_index]}"
    local password="${passwords[$machine_index]}"
    local workdir="${workdirs[$machine_index]}"
    local thread_count="${threads[$machine_index]}"
    
    echo "Running tests on machine $machine_index ($host) with $thread_count threads"
    
    # 为每台机器创建任务文件
    local task_file="$log_dir/machine_${machine_index}_task.txt"
    local lock_file="$log_dir/machine_${machine_index}_lock.txt"
    local index_file="$log_dir/machine_${machine_index}_index.txt"
    local success_case_file="$log_dir/machine_${machine_index}_success.txt"
    local failed_case_file="$log_dir/machine_${machine_index}_failed.txt"
    
    # 复制用例到任务文件
    cat "$machine_cases_file" > "$task_file"
    echo "" >> "$task_file"
    
    # 添加结束标记
    local i=0
    while [ $i -lt "$thread_count" ]; do
        echo "%%FINISHED%%" >> "$task_file"
        i=$((i + 1))
    done
    
    echo "0" > "$index_file"
    > "$success_case_file"
    > "$failed_case_file"
    
    # 启动线程
    local j=0
    while [ $j -lt "$thread_count" ]; do
        ("$(dirname "$0")/run_thread.sh" "$machine_index" "$j" "$task_file" "$lock_file" "$index_file" "$log_dir" "$workdir" "$username" "$password" "$host" "$ent" "$timeout_param" "$success_case_file" "$failed_case_file" "$web_server" "$test_log_dir") &
        j=$((j + 1))
    done
}

# 创建线程运行脚本
cat > "$(dirname "$0")/run_thread.sh" << 'EOF'
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
EOF

chmod +x "$(dirname "$0")/run_thread.sh"

# 启动所有机器的测试
for ((i=0; i<${#hosts[@]}; i++)); do
    run_tests_on_machine $i &
done

# 等待所有测试完成
echo "Waiting for all tests to complete..."
wait

# 汇总测试结果
total_cases=0
success_cases=0
failed_cases=0

for ((i=0; i<${#hosts[@]}; i++)); do
    success_file="$log_dir/machine_${i}_success.txt"
    failed_file="$log_dir/machine_${i}_failed.txt"
    
    if [ -f "$success_file" ]; then
        success_count=$(wc -l < "$success_file")
        success_cases=$((success_cases + success_count))
        total_cases=$((total_cases + success_count))
    fi
    
    if [ -f "$failed_file" ]; then
        failed_count=$(grep -v "^$" "$failed_file" | wc -l)
        failed_cases=$((failed_cases + failed_count))
        total_cases=$((total_cases + failed_count))
    fi
done

# 生成测试报告
echo "====================================================================="
echo "Test Results Summary"
echo "====================================================================="
echo "Total Cases: $total_cases"
echo "Successful:  $success_cases"
echo "Failed:      $failed_cases"
echo "====================================================================="

# 显示失败的测试用例
if [ $failed_cases -gt 0 ]; then
    echo "Failed Test Cases:"
echo "====================================================================="
    for ((i=0; i<${#hosts[@]}; i++)); do
        failed_file="$log_dir/machine_${i}_failed.txt"
        if [ -f "$failed_file" ]; then
            cat "$failed_file"
        fi
    done
    echo "====================================================================="
    exit 1
fi

echo "All tests completed successfully!"
echo "Log directory: $log_dir"

if [ -n "$web_server" ]; then
    echo "Test report: ${web_server}/${test_log_dir}"
fi

exit 0
