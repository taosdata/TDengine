#!/bin/bash

function usage() {
    echo "$0"
    echo -e "\t -m vm config file"
    echo -e "\t -t task file"
    echo -e "\t -b branch"
    echo -e "\t -l log dir"
    echo -e "\t -d debug level"
    echo -e "\t -w log folder web server"
    echo -e "\t -s force setup"
    echo -e "\t -v TDengine version"
    echo -e "\t -n docker network prefix"
    echo -e "\t -N docker network"
    echo -e "\t -M docker network map file"
    echo -e "\t -o default timeout value"
    echo -e "\t -E environment file"
    echo -e "\t -c mnode count"
    echo -e "\t -e enable sub log dir"
    echo -e "\t -f enable send2feishu robot"
    echo -e "\t -h help"
}

send2feishu_enabled="true"

while getopts "m:t:b:l:o:v:d:c:w:n:N:M:E:f:esh" opt; do
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
            log_folder=$OPTARG
            ;;
        s)
            force_setup=1
            ;;
        w)
            web_server=$OPTARG
            ;;
        d)
            debug_level=$OPTARG
            ;;
        c)
            mnode_count=$OPTARG
            ;;
        n)
            docker_network_prefix=$OPTARG
            ;;
        N)
            docker_network=$OPTARG
            ;;
        M)
            docker_network_map_file=$OPTARG
            ;;
        E)
            setup_use_file=$OPTARG
            ;;
        o)
            TIMEOUT_PREFIX="timeout $OPTARG"
            ;;
        v)
            tdengine_version=$OPTARG
            ;;
        e)
            sublogdir_enabled=1
            ;;
        f)
            send2feishu_enabled=$OPTARG
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

if [ -z $t_file ]; then
    usage
    exit 1
fi
if [ ! -z $t_file ] && [ ! -f $t_file ]; then
    echo "$t_file not found"
    usage
    exit 1
fi
if [ -z $docker_network_prefix ]; then
    docker_network_prefix=taosnet
fi
if [ ! -z "$docker_network_map_file" ]; then
    if [ ! -f $docker_network_map_file ]; then
        echo "$docker_network_map_file not found"
        usage
        exit 1
    fi
fi

date_tag=`date +%Y%m%d-%H%M%S`
if [ ! -z "$sublogdir_enabled" ]; then
    test_log_dir="${branch}_${date_tag}"
fi
if [ -z $log_folder ]; then
    if [ ! -z "$test_log_dir" ]; then
        log_dir="log/${test_log_dir}"
    else
        log_dir="log"
    fi
else
    if [ ! -z "$test_log_dir" ]; then
        log_dir="$log_folder/${test_log_dir}"
    else
        log_dir="$log_folder"
    fi
fi

if [ ! -z "$log_dir" ]; then
    mkdir -p $log_dir
    # rm -rf $log_dir/*
    export TAOSTEST_LOG_DIR="$log_dir"
fi

hosts=()
usernames=()
passwords=()
workdirs=()
threads=()

i=0
while [ 1 ]; do
    host=`jq .[$i].host $config_file`
    if [ "$host" = "null" ]; then
        break
    fi
    username=`jq .[$i].username $config_file`
    if [ "$username" = "null" ]; then
        break
    fi
    password=`jq .[$i].password $config_file`
    if [ "$password" = "null" ]; then
        password=""
    fi
    workdir=`jq .[$i].workdir $config_file`
    if [ "$workdir" = "null" ]; then
        break
    fi
    thread=`jq .[$i].thread $config_file`
    if [ "$thread" = "null" ]; then
        break
    fi
    hosts[i]=`echo $host|sed 's/\"$//'|sed 's/^\"//'`
    usernames[i]=`echo $username|sed 's/\"$//'|sed 's/^\"//'`
    passwords[i]=`echo $password|sed 's/\"$//'|sed 's/^\"//'`
    workdirs[i]=`echo $workdir|sed 's/\"$//'|sed 's/^\"//'`
    threads[i]=$thread
    i=$(( i + 1 ))
done


function prepare_cases() {
    cat $t_file >>$task_file
    local i=0
    while [ $i -lt $1 ]; do
        echo "%%FINISHED%%" >>$task_file
        i=$(( i + 1 ))
    done
}

function run_thread() {
    local index=$1
    local thread_no=$2
    local runcase_script="sshpass -p ${passwords[index]} ssh -o StrictHostKeyChecking=no ${usernames[index]}@${hosts[index]}"
    if [ -z ${passwords[index]} ]; then
        runcase_script="ssh -o StrictHostKeyChecking=no ${usernames[index]}@${hosts[index]}"
    fi
    local count=0
    local script="TEST_ROOT=${workdirs[index]}/TestNG TAOSTEST_LOG_DIR=${log_dir}"
    if [ ! -z "$DATABASE_REPLICAS" ]; then
        script="$script DATABASE_REPLICAS=${DATABASE_REPLICAS}"
    fi
    if [ ! -z "$DATABASE_QUERY_POLICY" ]; then
        script="$script DATABASE_QUERY_POLICY=${DATABASE_QUERY_POLICY}"
    fi
    script="$script $TIMEOUT_PREFIX"

    # script="echo"
    while [ 1 ]; do
        local line=`flock -x $lock_file -c "head -n1 $task_file;sed -i \"1d\" $task_file"`
        if [ "x$line" = "x%%FINISHED%%" ]; then
            # echo "$index . $thread_no EXIT"
            break
        fi
        if [ -z "$line" ]; then
            continue
        fi
        echo "$line"|grep -q "^#"
        if [ $? -eq 0 ]; then
            continue
        fi
        local case_redo_time=""
        if [ -z "$case_redo_time" ]; then
            case_redo_time=${DEFAULT_RETRY_TIME:-1}
        fi
        local case_cmd=`echo "$line"`
        local case_file=""
        echo "$case_cmd" | grep -q "\-\-case"
        if [ $? -eq 0 ]; then
            local case_param=`echo "$case_cmd" | grep -o "\-\-case.*"`
            echo "$case_param" | grep -q "\-\-case="
            if [ $? -eq 0 ]; then
                case_file=`echo "$case_param" | cut -d= -f2 | cut -d' ' -f1`
            else
                case_file=`echo "$case_param" | awk '{print $2}'`
            fi
        fi
        if [ -z "$case_file" ]; then
            echo "no case file specified"
            continue
        fi
        local env_file=""
        local setup_use=""
        echo "$case_cmd" | grep -q "\-\-use"
        if [ $? -eq 0 ]; then
            setup_use="use"
        fi
        echo "$case_cmd" | grep -q "\-\-setup"
        if [ $? -eq 0 ]; then
            setup_use="setup"
        fi
        if [ ! -z "${setup_use}" ]; then
            local env_param=`echo "$case_cmd" | grep -o "\-\-${setup_use}.*"`
            echo "$env_param" | grep -q "\-\-${setup_use}="
            if [ $? -eq 0 ]; then
                env_file=`echo "$env_param" | cut -d= -f2 | cut -d' ' -f1`
            else
                env_file=`echo "$env_param" | awk '{print $2}'`
            fi
        fi
        if [ -z "$env_file" ]; then
            if [ -z "${setup_use_file}" ]; then
                echo "no env file specified"
                continue
            else
                case_cmd="$case_cmd --setup=${setup_use_file}"
                env_file="${setup_use_file}"
            fi
        else
            if [ ! -z "${setup_use_file}" ]; then
                case_cmd=`echo "$case_cmd"|sed "s:${env_file}:${setup_use_file}:g"`
                env_file="${setup_use_file}"
            fi
        fi
        # echo "$index_file"
        local case_index=`flock -x $lock_file -c "sh -c \"echo \\\$(( \\\$( cat $index_file ) + 1 )) | tee $index_file\""`
        case_index=`printf "%5d" $case_index`
        case_file="${case_file}.${index}.${thread_no}.${count}"
        count=$(( count + 1 ))
        local case_path=`dirname "$case_file"`
        if [ ! -z "$case_path" ]; then
            mkdir -p $log_dir/$case_path
        fi
        # generate cmd
        cmd="${runcase_script} ${script} ${case_cmd}"
        # echo "$thread_no $count $cmd"
        if [ ! -z $force_setup ]; then
            cmd="${cmd/--use/--setup}"
            cmd="${cmd/--keep/}"
        fi
        if [ ! -z "$tdengine_version" ]; then
            cmd="$cmd --server-pkg=${workdirs[index]}/TDinternal/community/release/TDengine-enterprise-server-${tdengine_version}-Linux-x64.tar.gz"
            cmd="$cmd --client-pkg=${workdirs[index]}/TDinternal/community/release/TDengine-enterprise-client-${tdengine_version}-Linux-x64.tar.gz"
        fi
        if [ ! -z "$debug_level" ]; then
            cmd="$cmd --log-level $debug_level"
        fi
        # set taostest_pkg
        local taostest_pkg=`ls -r ${workdirs[index]}/taos-test-framework/dist/*.whl 2>/dev/null|head -n1`
        if [ ! -z "$taostest_pkg" ]; then
            cmd="$cmd --taostest-pkg $taostest_pkg"
        fi
        if [ ! -z "$mnode_count" ]; then
            cmd="$cmd --mnode-count $mnode_count"
        fi
        # set network
        if [ -z "${docker_network}" ]; then
            docker_network=${docker_network_prefix}_${thread_no}
        fi
        if [ ! -z "${docker_network_map_file}" ]; then
            docker_network=`cat $docker_network_map_file | grep -w "^$env_file" | awk '{print $2}' | head -n1`
        fi
        cmd="$cmd --source-dir ${workdirs[index]}/TDinternal --docker-network ${docker_network} --sql_recording"
        local ret=0
        local redo_count=1
        start_time=`date +%s`
        local real_start_time=`date +%s`
        local real_end_time=`date +%s`
        local log_file=${case_file}.txt
        local log_full_path=$log_dir/$log_file
        while [ ${redo_count} -lt 6 ]; do
            if [ -f $log_full_path ]; then
                cp $log_full_path $log_dir/$case_file.${redo_count}.redotxt
            fi
            echo "${hosts[index]}-${thread_no} order:${count}, redo:${redo_count} task:${line}" >$log_full_path
            echo "${cmd}" >>$log_full_path
            current_time=`date "+%Y-%m-%d %H:%M:%S"`
            echo -e "$case_index \e[33m START >>>>> \e[0m ${case_cmd} \e[33m[$current_time]\e[0m"
            echo "$current_time" >>$log_full_path
            real_start_time=`date +%s`
            $cmd >>$log_full_path 2>&1
            ret=$?
            real_end_time=`date +%s`
            current_time=`date "+%Y-%m-%d %H:%M:%S"`
            echo "${hosts[index]} $current_time ret:${ret}" >>$log_full_path
            if [ $ret -eq 0 ]; then
                break
            fi
            redo=0
            grep -q "kex_exchange_identification: Connection closed by remote host" $log_full_path
            if [ $? -eq 0 ]; then
                redo=1
            fi
            grep -q "ssh_exchange_identification: Connection closed by remote host" $log_full_path
            if [ $? -eq 0 ]; then
                redo=1
            fi
            grep -q "kex_exchange_identification: read: Connection reset by peer" $log_full_path
            if [ $? -eq 0 ]; then
                redo=1
            fi
            if [ $redo_count -lt $case_redo_time ]; then
                redo=1
            fi
            if [ $redo -eq 0 ]; then
                break
            fi
            redo_count=$(( redo_count + 1 ))
        done
        end_time=`date +%s`
        echo >>$log_full_path
        local time_elapsed=$(( real_end_time - real_start_time ))
        echo "${hosts[index]} total time: $(( end_time - start_time ))s" >>$log_full_path
        echo "${hosts[index]} execute time: ${time_elapsed}s" >>$log_full_path
        # echo "$thread_no ${line} DONE"
        if [ $ret -ne 0 ]; then
            echo -e "$case_index \e[34m DONE  <<<<< \e[0m ${case_cmd} \e[34m[${time_elapsed}s]\e[0m \e[31m failed\e[0m"
            # echo "=========================log============================"
            # cat $log_full_path
            # echo "====================================================="
            echo -e "\e[34m log file: $log_full_path \e[0m"
            if [ ! -z "$web_server" ]; then
                if [ ! -z "$test_log_dir" ]; then
                    flock -x $lock_file -c "echo -e \"${hosts[index]} ret:${ret} ${line}\n  ${web_server}/$test_log_dir/$log_file\" >>$log_dir/failed.txt"
                    echo "$web_server/$test_log_dir/$log_file"
                else
                    flock -x $lock_file -c "echo -e \"${hosts[index]} ret:${ret} ${line}\n  ${web_server}/$log_file\" >>$log_dir/failed.txt"
                    echo "$web_server/$log_file"
                fi
            else
                flock -x $lock_file -c "echo -e \"${hosts[index]} ret:${ret} ${line}\n  log file: $log_full_path\" >>$log_dir/failed.txt"
            fi
        else
            echo -e "$case_index \e[34m DONE  <<<<< \e[0m ${case_cmd} \e[34m[${time_elapsed}s]\e[0m \e[32m success\e[0m"
        fi
    done
}

mkdir -p $log_dir
task_file=$log_dir/task.txt
lock_file=$log_dir/task.lock
index_file=$log_dir/case_index.txt
stat_file=$log_dir/stat.txt
failed_case_file=$log_dir/failed.txt
echo "0" >$index_file
start_time_all=`date +%Y_%m%d_%H%M%S`

i=0
j=0
while [ $i -lt ${#hosts[*]} ]; do
    j=$(( j + threads[i] ))
    i=$(( i + 1 ))
done
prepare_cases $j

i=0
while [ $i -lt ${#hosts[*]} ]; do
    j=0 
    while [ $j -lt ${threads[i]} ]; do
        run_thread $i $j &
        j=$(( j + 1 ))
    done
    i=$(( i + 1 ))
done

wait

rm -f $lock_file
rm -f $task_file

echo "====================================================================="
echo "log dir: $log_dir"
total_cases=`cat $index_file`
failed_cases=0
if [ -f $failed_case_file ]; then
    if [ ! -z "$web_server" ]; then
        failed_cases=`grep -v "$web_server" $failed_case_file|wc -l`
    else
        failed_cases=`grep -v "log file:" $failed_case_file|wc -l`
    fi
fi
success_cases=$(( total_cases - failed_cases ))
echo "Total Cases: $total_cases" >$stat_file
echo "Successful:  $success_cases" >>$stat_file
echo "Failed:      $failed_cases" >>$stat_file
cat $stat_file

if [ ${send2feishu_enabled} == "True" ] ;then
    # new file testcase statusFile
    curr_dir=$(readlink -f "$(dirname "$0")")
    echo "script dir:" $curr_dir

    status_file=${curr_dir}"/case_status.txt"

    if  [  -f $status_file ]; then
        echo "$t_file not found"
        rm -rf ${status_file}
        exit 1
    fi

    # define parameters of text for sending to feishu robot
    end_time_all=`date +%Y_%m%d_%H%M%S`

    if [ -f ${failed_case_file} ]; then
        result='failed'
    else
        result='success'
    fi
    if [[ ${t_file} =~ "query" ]];then
        owner="guoxy"
    elif [[ ${t_file} =~ "insert" ]];then
        owner="jiajb"
    elif [[ ${t_file} =~ "taox" ]];then
        owner="jiacy"
    else
        owner="lihui"
    fi

    result_detail="failed ${failed_cases},successful ${success_cases}"
    test_scope="${t_file} , querypolicy-[${DATABASE_QUERY_POLICY}] , buildNumber-[${BUILD_NUMBER}]"
    community_commit_id=${community_commit_id}
    enterprise_commit_id=${enterprise_commit_id}

    echo  -e "result:${result}\nresult_detail:${result_detail}\nstart_time:${start_time_all}\nend_time:${end_time_all}\ntest_scope:${test_scope}\nlog_dir:${log_dir}\ncommunity_commit_id:${community_commit_id}\nenterprise_commit_id:${enterprise_commit_id}\nowner:${owner}"  >> ${status_file}

    python3 feishuTalk.py 
    rm -rf ${status_file}
fi
if [ -f $failed_case_file ]; then
    echo -e "\e[31m TEST FAILED\e[0m"
    cat $failed_case_file
    if [ ! -z "$server_pkg" ]; then
        if [ -f "$server_pkg" ]; then
            cp -r $server_pkg $log_dir/
        fi
    fi
    if [ ! -z "$client_pkg" ]; then
        if [ -f $client_pkg ]; then
            cp -r $client_pkg $log_dir/
        fi
    fi
    ret=1
else
    echo -e "\e[32m TEST SUCCESS\e[0m"
    ret=0
fi
exit $ret

