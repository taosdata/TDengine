#!/bin/bash
# set -x
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
while getopts "m:t:b:l:o:w:eh" opt; do
    case $opt in
    m)
        config_file=$OPTARG
        ;;
    t)
        t_file=$OPTARG
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
        exit 0
        ;;
    esac
done
#config_file=$1
if [ -z "$config_file" ]; then
    usage
    exit 1
fi
if [ ! -f "$config_file" ]; then
    echo "$config_file not found"
    usage
    exit 1
fi
#t_file=$2
if [ -z "$t_file" ]; then
    usage
    exit 1
fi
if [ ! -f "$t_file" ]; then
    echo "$t_file not found"
    usage
    exit 1
fi


date_tag=$(date +%Y%m%d-%H%M%S)
test_log_dir=${branch}_${date_tag}
if [ -z "$log_dir" ]; then
    log_dir="log/${test_log_dir}"
else
    log_dir="$log_dir/${test_log_dir}"
fi

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

function prepare_cases() {
    cat "$t_file" >>"$task_file"
    local i=0
    while [ $i -lt "$1" ]; do
        echo "%%FINISHED%%" >>"$task_file"
        i=$((i + 1))
    done
}

# 判断是否本地host
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

function get_remote_ssh_command() {
    local index=$1
    if [ -z "${passwords[index]}" ]; then
        echo "ssh -o StrictHostKeyChecking=no ${usernames[index]}@${hosts[index]}"
    else
        echo "sshpass -p ${passwords[index]} ssh -o StrictHostKeyChecking=no ${usernames[index]}@${hosts[index]}"
    fi
}

function get_remote_scp_command() {
    local index=$1
    if [ -z "${passwords[index]}" ]; then
        echo "scp -o StrictHostKeyChecking=no -r ${usernames[index]}@${hosts[index]}"
    else
        echo "sshpass -p ${passwords[index]} scp -o StrictHostKeyChecking=no -r ${usernames[index]}@${hosts[index]}"
    fi
}

function collect_coverage_data() {
    local index=$1
    local thread_no=$2
    local case_file=$3
    local log_dir=$4
    local status=$5  # success 或 failed
    
    echo "开始生成覆盖率信息: ${case_file} (${status})"
    
    # 创建覆盖率数据目录，使用递增序号避免冲突
    local base_coverage_dir="${log_dir}/${case_file}.coverage"
    local coverage_dir="$base_coverage_dir"
    local counter=0
    
    # 如果目录已存在，使用递增的序号
    while [ -d "$coverage_dir" ]; do
        coverage_dir="${log_dir}/${case_file}.${counter}.coverage"
        ((counter++))
    done

    mkdir -p "$coverage_dir"
    
    # 获取远程命令前缀
    local scpcmd=""
    local ssh_script=""
    if ! is_local_host "${hosts[index]}"; then
        scpcmd=$(get_remote_scp_command "$index")
        ssh_script=$(get_remote_ssh_command "$index")
    fi
    
    # 定义需要处理的debug目录列表
    local debug_dirs=(
        "${workdirs[index]}/debugSan"
        "${workdirs[index]}/debugNoSan"
    )
    
    local generated_info_files=()
    local total_info_count=0
    
    # 遍历所有debug目录，为每个目录生成覆盖率信息
    for debug_path in "${debug_dirs[@]}"; do
        local debug_name=$(basename "$debug_path")
        local info_filename="coverage_${case_file##*/}_${debug_name}.info"
        local remote_info_file="${workdirs[index]}/tmp/thread_volume/$thread_no/${info_filename}"
        
        echo "处理目录: $debug_path -> $info_filename"
        
        if ! is_local_host "${hosts[index]}"; then
            # 远程主机处理
            # 首先检查远程目录是否存在并有GCDA文件
            local check_cmd="${ssh_script} \"[ -d ${debug_path} ] && find ${debug_path} -name '*.gcda' -type f | wc -l || echo '0'\""
            local gcda_count=$(bash -c "$check_cmd" 2>/dev/null | tail -1)
            
            if [ "$gcda_count" -eq 0 ]; then
                echo "远程目录 $debug_path 中没有GCDA文件，跳过"
                continue
            fi
            
            echo "远程目录 $debug_path 中发现 $gcda_count 个GCDA文件"
            
            # 在远程主机上运行lcov生成覆盖率信息
            local lcov_cmd="${ssh_script} \"
                cd ${debug_path} && \\
                lcov --quiet -d . -capture \\
                    --rc lcov_branch_coverage=1 \\
                    --rc genhtml_branch_coverage=1 \\
                    --no-external \\
                    -b ${workdirs[index]}/TDinternal/community/ \\
                    -o ${remote_info_file} 2>/dev/null && \\
                echo 'lcov_success' || echo 'lcov_failed'
            \""
            
            echo "远程lcov命令: $lcov_cmd"
            local lcov_result=$(bash -c "$lcov_cmd" 2>/dev/null | tail -1)
            
            if [ "$lcov_result" = "lcov_success" ]; then
                # 检查生成的info文件大小
                local check_info_cmd="${ssh_script} \"[ -f ${remote_info_file} ] && stat -c%s ${remote_info_file} || echo '0'\""
                local info_size=$(bash -c "$check_info_cmd" 2>/dev/null | tail -1)
                
                if [ "$info_size" -gt 0 ]; then
                    echo "成功生成覆盖率信息文件: $info_filename (大小: $info_size 字节)"
                    
                    # 传输info文件到本地
                    local scp_cmd="$scpcmd:${remote_info_file} ${coverage_dir}/${info_filename}"
                    if bash -c "$scp_cmd" 2>/dev/null; then
                        echo "传输成功: ${info_filename}"
                        generated_info_files+=("${coverage_dir}/${info_filename}")
                        ((total_info_count++))
                    else
                        echo "传输失败: ${info_filename}"
                    fi
                    
                    # 清理远程临时文件
                    local cleanup_cmd="${ssh_script} \"rm -f ${remote_info_file}\""
                    bash -c "$cleanup_cmd" 2>/dev/null
                else
                    echo "生成的覆盖率信息文件为空: $info_filename"
                fi
            else
                echo "lcov执行失败: $debug_path"
            fi
            
        else
            # 本地主机处理
            if [ -d "$debug_path" ]; then
                local gcda_count=$(find "$debug_path" -name "*.gcda" -type f 2>/dev/null | wc -l)
                
                if [ "$gcda_count" -eq 0 ]; then
                    echo "本地目录 $debug_path 中没有GCDA文件，跳过"
                    continue
                fi
                
                echo "本地目录 $debug_path 中发现 $gcda_count 个GCDA文件"
                
                # 在本地运行lcov
                local local_info_file="${coverage_dir}/${info_filename}"
                local lcov_cmd="cd ${debug_path} && lcov --quiet -d . -capture \\
                    --rc lcov_branch_coverage=1 \\
                    --rc genhtml_branch_coverage=1 \\
                    --no-external \\
                    -b ${workdirs[index]}/TDinternal/community/ \\
                    -o ${local_info_file}"
                
                echo "本地lcov命令: $lcov_cmd"
                if bash -c "$lcov_cmd" 2>/dev/null; then
                    if [ -s "$local_info_file" ]; then
                        local info_size=$(stat -c%s "$local_info_file")
                        echo "成功生成覆盖率信息文件: $info_filename (大小: $info_size 字节)"
                        generated_info_files+=("$local_info_file")
                        ((total_info_count++))
                    else
                        echo "生成的覆盖率信息文件为空: $info_filename"
                        rm -f "$local_info_file"
                    fi
                else
                    echo "lcov执行失败: $debug_path"
                fi
            else
                echo "本地目录不存在，跳过: $debug_path"
            fi
        fi
    done
    
    # 如果生成了多个info文件，尝试合并它们
    if [ "$total_info_count" -gt 1 ]; then
        echo "合并 $total_info_count 个覆盖率信息文件..."
        local merged_info_file="${coverage_dir}/coverage_${case_file##*/}_merged.info"
        
        # 构建lcov合并命令
        local merge_cmd="lcov --quiet --rc lcov_branch_coverage=1"
        for info_file in "${generated_info_files[@]}"; do
            merge_cmd="$merge_cmd --add-tracefile '$info_file'"
        done
        merge_cmd="$merge_cmd -o '$merged_info_file'"
        
        echo "合并命令: $merge_cmd"
        if eval "$merge_cmd" 2>/dev/null; then
            if [ -s "$merged_info_file" ]; then
                local merged_size=$(stat -c%s "$merged_info_file")
                echo "成功合并覆盖率信息: coverage_${case_file##*/}_merged.info (大小: $merged_size 字节)"
                
                # 删除单独的info文件，保留合并后的文件
                for info_file in "${generated_info_files[@]}"; do
                    rm -f "$info_file"
                done
                generated_info_files=("$merged_info_file")
            else
                echo "合并后的文件为空，保留单独的文件"
                rm -f "$merged_info_file"
            fi
        else
            echo "合并失败，保留单独的info文件"
        fi
    fi
    
    # 记录统计信息
    echo "覆盖率信息生成完成 (${status}): ${case_file}"
    echo "生成的info文件数量: ${#generated_info_files[@]}"
    
    # 记录到日志
    echo "Coverage info files generated: ${#generated_info_files[@]}" >> "${log_dir}/${case_file}.txt"
    
    # 显示生成的文件信息
    if [ "${#generated_info_files[@]}" -gt 0 ]; then
        echo "生成的覆盖率信息文件:"
        for info_file in "${generated_info_files[@]}"; do
            if [ -f "$info_file" ]; then
                local file_size=$(stat -c%s "$info_file")
                local file_lines=$(wc -l < "$info_file")
                echo "  $(basename "$info_file"): $file_size 字节, $file_lines 行"
            fi
        done
        echo "覆盖率信息已保存到: $coverage_dir"
    else
        echo "警告: 未生成任何有效的覆盖率信息文件"
    fi
    
    # 创建一个索引文件，记录所有生成的info文件
    if [ "${#generated_info_files[@]}" -gt 0 ]; then
        local index_file="${coverage_dir}/info_files.list"
        printf "%s\n" "${generated_info_files[@]}" > "$index_file"
        echo "创建索引文件: $index_file"
    fi
}

function clean_tmp() {
    local index=$1
    local cmd=""
    if is_local_host "${hosts[index]}"; then
        cmd="rm -rf ${workdirs[index]}/tmp"
    else
        local ssh_script=""
        ssh_script=$(get_remote_ssh_command "$index")
        cmd="${ssh_script} rm -rf ${workdirs[index]}/tmp"
    fi
    $cmd
}

function run_thread() {
    local index=$1
    local thread_no=$2
    local runcase_script=""
    if ! is_local_host "${hosts[index]}"; then
        runcase_script=$(get_remote_ssh_command "$index")
    fi
    local count=0
    local script="${workdirs[index]}/TDengine/test/ci/run_container.sh"
    if [ $ent -ne 0 ]; then
        local script="${workdirs[index]}/TDinternal/community/test/ci/run_container.sh -e"
    fi

    # 为每个线程创建独立的 GCOV 输出目录
    local gcov_output_dir="${workdirs[index]}/tmp/thread_volume/$thread_no/gcov_data"
    local cmd=""
    if ! is_local_host "${hosts[index]}"; then
        cmd="${runcase_script} \"mkdir -p ${gcov_output_dir}\""
    else
        cmd="mkdir -p ${gcov_output_dir}"
    fi
    bash -c "$cmd"

    local cmd="${runcase_script} ${script}"

    # script="echo"
    while true; do
        local line
        line=$(flock -x "$lock_file" -c "head -n1 $task_file;sed -i \"1d\" $task_file")
        if [ "x$line" = "x%%FINISHED%%" ]; then
            break
        fi
        if [ -z "$line" ]; then
            continue
        fi
        if echo "$line" | grep -q "^#"; then
            continue
        fi
        local case_redo_time
        case_redo_time=$(echo "$line" | cut -d, -f2)
        if [ -z "$case_redo_time" ]; then
            case_redo_time=${DEFAULT_RETRY_TIME:-2}
        fi
        local case_build_san
        case_build_san=$(echo "$line" | cut -d, -f3)
        if [ "${case_build_san}" == "y" ]; then
            case_build_san="y"
            DEBUGPATH="debugSan"
        elif [[ "${case_build_san}" == "n" ]] || [[ "${case_build_san}" == "" ]]; then
            case_build_san="n"
            DEBUGPATH="debugNoSan"
        else
            usage
            exit 1
        fi
        local exec_dir
        exec_dir=$(echo "$line" | cut -d, -f4)
        local case_cmd
        case_cmd=$(echo "$line" | cut -d, -f5)
        local case_file=""
        # get the docs-examples test case from cases.task file
        if echo "$case_cmd" | grep -q "\.sh"; then
            case_file=$(echo "$case_cmd" | grep -o ".*\.sh" | awk '{print $NF}')
        fi

        # get python cases from cases.task file with asan
        if echo "$case_cmd" | grep -q "^./ci/pytest.sh"; then
            case_file=$(echo "$case_cmd" | grep -o ".*\.py" | awk '{print $NF}')
        fi

        # get python cases from cases.task file without asan or sim cases
        if echo "$case_cmd" | grep -q "^pytest"; then
            # get python cases from cases.task file without asan
            if [[ $case_cmd == *".py"* ]]; then
                case_file=$(echo "$case_cmd" | grep -o ".*\.py" | awk '{print $NF}')
            fi
            # get sim cases from cases.task file
            if [[ $case_cmd == *"--tsim"* ]]; then
                case_file=$(echo "$case_cmd" | grep -oP '(?<=--tsim=)[^ ]+')
            fi
        fi

        # if echo "$case_cmd" | grep -q "^python3"; then
        #     case_file=$(echo "$case_cmd" | grep -o ".*\.py" | awk '{print $NF}')
        # fi

        # get sim cases from cases.task file
        # if echo "$case_cmd" | grep -q "^pytest"; then
        #     # case_file=$(echo "$case_cmd" | grep -o ".*\.sim" | awk '{print $NF}')
        #     case_file=$(echo "$case_cmd" | grep -oP '(?<=--tsim=)[^ ]+')
        # fi
        if [ -z "$case_file" ]; then
            case_file=$(echo "$case_cmd" | awk '{print $NF}')
        fi
        if [ -z "$case_file" ]; then
            continue
        fi
        if [ "$exec_dir" == "." ]; then
            case_sql_file="${case_file}.sql"
            case_file="${case_file}.${index}.${thread_no}.${count}"
        else
            case_sql_file="${exec_dir}/${case_file}.sql"
            case_file="${exec_dir}/${case_file}.${index}.${thread_no}.${count}"
        fi
        count=$((count + 1))
        local case_path
        case_path=$(dirname "$case_file")
        if [ -n "$case_path" ]; then
            mkdir -p "$log_dir"/"$case_path"
        fi
        
        # 修改执行命令，添加 GCOV 环境变量
        local gcov_env_vars="GCOV_PREFIX=${gcov_output_dir} GCOV_PREFIX_STRIP=0"
        cmd="${runcase_script}  env ${gcov_env_vars} ${script} -w ${workdirs[index]} -c \"${case_cmd}\" -t ${thread_no} -d ${exec_dir}  -s ${case_build_san} ${timeout_param}"
        # echo "$thread_no $count $cmd"
        local ret=0
        local redo_count=1
        local case_log_file=$log_dir/${case_file}.txt
        start_time=$(date +%s)
        local case_index
        case_index=$(flock -x "$lock_file" -c "sh -c \"echo \$(( \$( cat $index_file ) + 1 )) | tee $index_file\"")
        case_index=$(printf "%5d" "$case_index")
        local case_info
        case_info=$(echo "$line" | cut -d, -f 3,4,5)
        while [ ${redo_count} -le "$case_redo_time" ]; do
            if [ -f "$case_log_file" ]; then
                cp "$case_log_file" "$log_dir"/"$case_file".${redo_count}.redotxt
            fi
            echo "${hosts[index]}-${thread_no} order:${count}, redo:${redo_count} task:${line}" >"$case_log_file"
            local current_time
            current_time=$(date "+%Y-%m-%d %H:%M:%S")
            echo -e "$case_index \e[33m START >>>>> \e[0m ${case_info} \e[33m[$current_time]\e[0m"
            echo "$current_time" >>"$case_log_file"
            local real_start_time
            real_start_time=$(date +%s)
            # echo "cmd:${cmd}"
            if ! is_local_host "${hosts[index]}"; then
                $cmd >>"$case_log_file" 2>&1
            else
                bash -c "$cmd" >>"$case_log_file" 2>&1
            fi
            ret=$?
            local real_end_time
            real_end_time=$(date +%s)
            local time_elapsed
            time_elapsed=$((real_end_time - real_start_time))
            echo "execute time: ${time_elapsed}s" >>"$case_log_file"
            current_time=$(date "+%Y-%m-%d %H:%M:%S")
            echo "${hosts[index]} $current_time exit code:${ret}" >>"$case_log_file"
            if [ $ret -eq 0 ]; then
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
            if [ $redo_count -lt "$case_redo_time" ]; then
                redo=1
            fi
            if [ $redo -eq 0 ]; then
                break
            fi
            redo_count=$((redo_count + 1))
        done
        end_time=$(date +%s)
        echo >>"$case_log_file"
        total_time=$((end_time - start_time))
        echo "${hosts[index]} total time: ${total_time}s" >>"$case_log_file"
        # echo "$thread_no ${line} DONE"

        local scpcmd=""
        local allure_report_results="${workdirs[index]}/tmp/thread_volume/$thread_no/allure-results"
        if ! is_local_host "${hosts[index]}"; then
            scpcmd=$(get_remote_scp_command "$index")
            cmd="$scpcmd:${allure_report_results}/* $log_dir/allure-results/"
            bash -c "$cmd"
        else
            cmd="cp -rf ${allure_report_results}/* $log_dir/allure-results/"
            bash -c "$cmd"
        fi
        echo "Save allure report results to $log_dir/allure-results/ from ${allure_report_results} with cmd: $cmd"
        if [ $ret -eq 0 ]; then
            echo -e "$case_index \e[34m DONE  <<<<< \e[0m ${case_info} \e[34m[${total_time}s]\e[0m \e[32m success\e[0m"
            flock -x "$lock_file" -c "echo \"${case_info}|success|${total_time}\" >>${success_case_file}"

            collect_coverage_data "$index" "$thread_no" "$case_file" "$log_dir" "success"
        else
            if [ -n "${web_server}" ]; then
                flock -x "$lock_file" -c "echo -e \"${hosts[index]} ret:${ret} ${line}\n  ${web_server}/$test_log_dir/${case_file}.txt\" >>${failed_case_file}"
            else
                flock -x "$lock_file" -c "echo -e \"${hosts[index]} ret:${ret} ${line}\n  log file: ${case_log_file}\" >>${failed_case_file}"
            fi
            mkdir -p "${log_dir}"/"${case_file}".coredump
            local remote_coredump_dir="${workdirs[index]}/tmp/thread_volume/$thread_no/coredump"
            if ! is_local_host "${hosts[index]}"; then
                cmd="$scpcmd:${remote_coredump_dir}/* $log_dir/${case_file}.coredump/"
            else
                cmd="cp -rf ${remote_coredump_dir}/* $log_dir/${case_file}.coredump/"
            fi
            bash -c "$cmd" >/dev/null

            collect_coverage_data "$index" "$thread_no" "$case_file" "$log_dir" "failed"

            local corefile
            corefile=$(ls "$log_dir/${case_file}.coredump/")
            echo -e "$case_index \e[34m DONE  <<<<< \e[0m ${case_info} \e[34m[${total_time}s]\e[0m \e[31m failed\e[0m"
            echo "=========================log============================"
            cat "$case_log_file"
            echo "====================================================="
            echo -e "\e[34m log file: $case_log_file \e[0m"
            if [ -n "${web_server}" ]; then
                echo "${web_server}/$test_log_dir/${case_file}.txt"
            fi
            if [ -n "$corefile" ]; then
                echo -e "\e[34m corefiles: $corefile \e[0m"
            fi
            local build_dir=$log_dir/build_${hosts[index]}
            local remote_build_dir="${workdirs[index]}/${DEBUGPATH}/build"
            local remote_unit_test_log_dir="${workdirs[index]}/${DEBUGPATH}/Testing/Temporary/"
            mkdir "$build_dir" >/dev/null
            if [ $? -eq 0 ]; then
                if ! is_local_host "${hosts[index]}"; then
                    cmd="$scpcmd:${remote_build_dir}/* ${build_dir}/"
                    echo "$cmd"
                    bash -c "$cmd" >/dev/null
                    cmd="$scpcmd:${remote_unit_test_log_dir}/* ${build_dir}/"
                    echo "$cmd"
                    bash -c "$cmd" >/dev/null
                else
                    cmd="cp -rf ${remote_build_dir}/* ${build_dir}/"
                    echo "$cmd"
                    bash -c "$cmd" >/dev/null
                    cmd="cp -rf ${remote_unit_test_log_dir}/* ${build_dir}/"
                    echo "$cmd"
                    bash -c "$cmd" >/dev/null
                fi
            fi
            local remote_sim_dir="${workdirs[index]}/tmp/thread_volume/$thread_no"
            if ! is_local_host "${hosts[index]}"; then
                cmd="$runcase_script sh -c \"cd $remote_sim_dir; tar -czf sim.tar.gz sim\""
            else
                cmd="cd $remote_sim_dir; tar -czf sim.tar.gz sim"
            fi
            bash -c "$cmd"
            local remote_sim_tar="${workdirs[index]}/tmp/thread_volume/$thread_no/sim.tar.gz"
            local remote_case_sql_file="${workdirs[index]}/tmp/thread_volume/$thread_no/${case_sql_file}"
            if ! is_local_host "${hosts[index]}"; then
                cmd="$scpcmd:${remote_sim_tar} $log_dir/${case_file}.sim.tar.gz"
                bash -c "$cmd"
                cmd="$scpcmd:${remote_case_sql_file} $log_dir/${case_file}.sql"
                bash -c "$cmd"
            else
                cmd="cp -f ${remote_sim_tar} $log_dir/${case_file}.sim.tar.gz"
                bash -c "$cmd"
                cmd="cp -f ${remote_case_sql_file} $log_dir/${case_file}.sql"
                bash -c "$cmd"
            fi
            # # backup source code (disabled)
            # source_tar_dir=$log_dir/TDengine_${hosts[index]}
            # source_tar_file=TDengine.tar.gz
            # if [ $ent -ne 0 ]; then
            #     source_tar_dir=$log_dir/TDinternal_${hosts[index]}
            #     source_tar_file=TDinternal.tar.gz
            # fi
            # mkdir "$source_tar_dir" 2>/dev/null
            # if [ $? -eq 0 ]; then
            #     cmd="$scpcmd:${workdirs[index]}/$source_tar_file $source_tar_dir"
            # fi
        fi
    done
}

# echo "hosts: ${hosts[@]}"
# echo "usernames: ${usernames[@]}"
# echo "passwords: ${passwords[@]}"
# echo "workdirs: ${workdirs[@]}"
# echo "threads: ${threads[@]}"
# TODO: check host accessibility

i=0
while [ $i -lt ${#hosts[*]} ]; do
    clean_tmp $i &
    i=$((i + 1))
done
wait

mkdir -p "$log_dir"
rm -rf "${log_dir:?}"/*
mkdir "$log_dir/allure-results"
task_file=$log_dir/$$.task
lock_file=$log_dir/$$.lock
index_file=$log_dir/case_index.txt
stat_file=$log_dir/stat.txt
failed_case_file=$log_dir/failed.txt
success_case_file=$log_dir/success.txt

echo "0" >"$index_file"

i=0
j=0
while [ $i -lt ${#hosts[*]} ]; do
    j=$((j + threads[i]))
    i=$((i + 1))
done
prepare_cases $j

i=0
while [ $i -lt ${#hosts[*]} ]; do
    j=0
    while [ $j -lt "${threads[i]}" ]; do
        run_thread $i $j &
        j=$((j + 1))
    done
    i=$((i + 1))
done

wait

rm -f "$lock_file"
rm -f "$task_file"

# docker ps -a|grep -v CONTAINER|awk '{print $1}'|xargs docker rm -f
echo "====================================================================="
echo "log dir: $log_dir"
total_cases=$(cat "$index_file")
failed_cases=0
if [ -f "$failed_case_file" ]; then
    if [ -n "$web_server" ]; then
        failed_cases=$(grep -c -v "$web_server" "$failed_case_file")
    else
        failed_cases=$(grep -c -v "log file:" "$failed_case_file")
    fi
fi
success_cases=$((total_cases - failed_cases))
echo "Total Cases: $total_cases" >"$stat_file"
echo "Successful:  $success_cases" >>"$stat_file"
echo "Failed:      $failed_cases" >>"$stat_file"
cat "$stat_file"

RET=0
i=1
if [ -f "${failed_case_file}" ]; then
    echo "====================================================="
    while read -r line; do
        if [ -n "${web_server}" ]; then

            if echo "$line" | grep -q "${web_server}"; then
                echo "    $line"
                continue
            fi
        else

            if echo "$line" | grep -q "log file:"; then
                echo "    $line"
                continue
            fi
        fi
        line=$(echo "$line" | cut -d, -f 3,4,5)
        echo -e "$i. $line \e[31m failed\e[0m" >&2
        i=$((i + 1))
    done <"${failed_case_file}"
    RET=1
fi

# generated test report
mount_reports_dir="/mnt/platform/reports"
results_dir="$mount_reports_dir/$branch/results"
report_dir="$mount_reports_dir/$branch/report"

# check report results directory
if [ ! -d "$results_dir" ]; then
    mkdir -p "$results_dir"
else
    # clear results
    rm -rf "$results_dir"/*
fi

# check report directory
if [ ! -d "$report_dir" ]; then
    mkdir -p "$report_dir"
else
    if [ -n "$(ls -A "$report_dir/history/")" ]; then
        mkdir -p "$results_dir/history/"
        # copy history to results
        echo "Moving history results to results directory..."
        cp -r "$report_dir/history/"* "$results_dir/history/"
    fi
fi

# copy results to server
cp -r "$log_dir/allure-results/"* "$results_dir"
cp_status=$?
echo "Copying allure results to $results_dir, status: $cp_status"

# generate the test report for pr
/opt/allure-2.22.1/bin/allure generate "$results_dir" -o "$report_dir" --clean
generate_status=$?
echo "Generating allure report, status: $generate_status"

# check report is generated successfully
if [ -f "$report_dir/index.html" ]; then
    echo "Allure report generated successfully at $report_dir."
    echo "Test report: https://platform.tdengine.net:8090/reports/$branch/report"
else
    echo "Error: Failed to generate Allure report."
fi

echo "${log_dir}" >&2
date

exit $RET
