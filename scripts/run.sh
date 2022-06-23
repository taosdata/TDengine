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
    echo -e "\t -f run last failed cases"
    echo -e "\t -v TDengine version"
    echo -e "\t -n docker network prefix"
    echo -e "\t -o default timeout value"
    echo -e "\t -h help"
}

while getopts "m:t:b:l:o:v:d:w:n:sfh" opt; do
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
        f)
            last_failed=1
            ;;
        w)
            web_server=$OPTARG
            ;;
        d)
            debug_level=$OPTARG
            ;;
        n)
            docker_network_prefix=$OPTARG
            ;;
        o)
            TIMEOUT_PREFIX="timeout $OPTARG"
            ;;
        v)
            tdengine_version=$OPTARG
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

if [ -z $t_file ] && [ -z $last_failed ]; then
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

date_tag=`date +%Y%m%d-%H%M%S`
test_log_dir="${branch}_${date_tag}"
if [ -z $log_folder ]; then
    log_dir="log/${test_log_dir}"
else
    log_dir="$log_folder/${test_log_dir}"
fi

mkdir -p $log_dir
rm -rf $log_dir/*

if [ ! -z "$last_failed" ]; then
    last_log_dir=`ls $log_folder|sort|tail -n2|head -n1`
    echo "last log dir: [${last_log_dir}]"
    if [ ! -z "$last_log_dir" ]; then
        last_log_dir="${log_folder}/${last_log_dir}"
        if [ -f "$last_log_dir/failed.txt" ]; then
            case_list_file=${log_dir}/last_failed_cases.txt
            t_file=$case_list_file
            cat $last_log_dir/failed.txt | grep -w taostest | sed "s/^.* ret:[0-9]* //" >$case_list_file
            echo "***** cases to run *****"
            cat $case_list_file
        else
            echo "***** no case to run *****"
            exit 0
        fi
    else
        echo "***** no case to run *****"
        exit 0
    fi
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
    local script="TEST_ROOT=${workdirs[index]}/TestNG $TIMEOUT_PREFIX"

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
            continue
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
        # set network
        cmd="$cmd --source-dir ${workdirs[index]}/TDinternal --docker-network ${docker_network_prefix}_${thread_no}"
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
                flock -x $lock_file -c "echo -e \"${hosts[index]} ret:${ret} ${line}\n  ${web_server}/$test_log_dir/$log_file\" >>$log_dir/failed.txt"
                echo "$web_server/$test_log_dir/$log_file"
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

