#!/bin/bash

function usage() {
    echo "$0"
    echo -e "\t -m vm config file"
    echo -e "\t -t task file"
    echo -e "\t -b branch"
    echo -e "\t -l log dir"
    echo -e "\t -o default timeout value"
    echo -e "\t -h help"
}

while getopts "m:t:b:l:o:h" opt; do
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
        o)
            timeout_param="-o $OPTARG"
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
if [ -z $config_file ]; then
    usage
    exit 1
fi
if [ ! -f $config_file ]; then
    echo "$config_file not found"
    usage
    exit 1
fi
#t_file=$2
if [ -z $t_file ]; then
    usage
    exit 1
fi
if [ ! -f $t_file ]; then
    echo "$t_file not found"
    usage
    exit 1
fi
date_tag=`date +%Y%m%d-%H%M%S`
if [ -z $log_dir ]; then
    log_dir="log/${branch}_${date_tag}"
else
    log_dir="$log_dir/${branch}_${date_tag}"
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

function clean_tmp() {
    # clean tmp dir
    local index=$1
    local ssh_script="sshpass -p ${passwords[index]} ssh -o StrictHostKeyChecking=no ${usernames[index]}@${hosts[index]}"
    if [ -z ${passwords[index]} ]; then
        ssh_script="ssh -o StrictHostKeyChecking=no ${usernames[index]}@${hosts[index]}"
    fi
    local cmd="${ssh_script} rm -rf ${workdirs[index]}/tmp"
    ${cmd}
}
# build source
function build_src() {
    echo "build source"
    local index=$1
    local ssh_script="sshpass -p ${passwords[index]} ssh -o StrictHostKeyChecking=no ${usernames[index]}@${hosts[index]}"
    if [ -z ${passwords[index]} ]; then
        ssh_script="ssh -o StrictHostKeyChecking=no ${usernames[index]}@${hosts[index]}"
    fi
    local script=". ~/.bashrc;cd ${workdirs[index]}/TDinternal;mkdir -p debug;cd debug;cmake .. -DBUILD_HTTP=false -DBUILD_TOOLS=true;make -j8;make install"
    local cmd="${ssh_script} sh -c \"$script\""
    echo "$cmd"
    ${cmd}
    if [ $? -ne 0 ]; then
        flock -x $lock_file -c "echo \"${hosts[index]} TDengine build failed\" >>$log_dir/failed.log"
        return
    fi
    script=". ~/.bashrc;cd ${workdirs[index]}/taos-tools;git submodule update --init --recursive;mkdir -p build;cd build;cmake ..;make -j4"
    cmd="${ssh_script} sh -c \"$script\""
    ${cmd}
    if [ $? -ne 0 ]; then
        flock -x $lock_file -c "echo \"${hosts[index]} taos-tools build failed\" >>$log_dir/failed.log"
        return
    fi
    script="cp -rf ${workdirs[index]}/taos-tools/build/build/bin/* ${workdirs[index]}/TDinternal/debug/build/bin/;cp -rf ${workdirs[index]}/taos-tools/build/build/lib/* ${workdirs[index]}/TDinternal/debug/build/lib/;cp -rf ${workdirs[index]}/taos-tools/build/build/lib64/* ${workdirs[index]}/TDinternal/debug/build/lib/;cp -rf ${workdirs[index]}/TDinternal/debug/build/bin/taosBenchmark ${workdirs[index]}/TDinternal/debug/build/bin/taosdemo"
    cmd="${ssh_script} sh -c \"$script\""
    ${cmd}
}
function rename_taosdemo() {
    local index=$1
    local ssh_script="sshpass -p ${passwords[index]} ssh -o StrictHostKeyChecking=no ${usernames[index]}@${hosts[index]}"
    if [ -z ${passwords[index]} ]; then
        ssh_script="ssh -o StrictHostKeyChecking=no ${usernames[index]}@${hosts[index]}"
    fi
    local script="cp -rf ${workdirs[index]}/TDinternal/debug/build/bin/taosBenchmark ${workdirs[index]}/TDinternal/debug/build/bin/taosdemo 2>/dev/null"
    cmd="${ssh_script} sh -c \"$script\""
    ${cmd}
}

function run_thread() {
    local index=$1
    local thread_no=$2
    local runcase_script="sshpass -p ${passwords[index]} ssh -o StrictHostKeyChecking=no ${usernames[index]}@${hosts[index]}"
    if [ -z ${passwords[index]} ]; then
        runcase_script="ssh -o StrictHostKeyChecking=no ${usernames[index]}@${hosts[index]}"
    fi
    local count=0
    local script="${workdirs[index]}/TDinternal/community/tests/parallel_test/run_container.sh"
    local cmd="${runcase_script} ${script}"

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
        local case_redo_time=`echo "$line"|cut -d, -f2`
        if [ -z "$case_redo_time" ]; then
            case_redo_time=${DEFAULT_RETRY_TIME:-2}
        fi
        local exec_dir=`echo "$line"|cut -d, -f3`
        local case_cmd=`echo "$line"|cut -d, -f4`
        local case_file=""
        echo "$case_cmd"|grep -q "^python3"
        if [ $? -eq 0 ]; then
            case_file=`echo "$case_cmd"|grep -o ".*\.py"|awk '{print $NF}'`
        fi
        echo "$case_cmd"|grep -q "\.sim"
        if [ $? -eq 0 ]; then
            case_file=`echo "$case_cmd"|grep -o ".*\.sim"|awk '{print $NF}'`
        fi
        if [ -z "$case_file" ]; then
            case_file=`echo "$case_cmd"|awk '{print $NF}'`
        fi
        if [ -z "$case_file" ]; then
            continue
        fi
        case_file="$exec_dir/${case_file}.${index}.${thread_no}.${count}"
        count=$(( count + 1 ))
        local case_path=`dirname "$case_file"`
        if [ ! -z "$case_path" ]; then
            mkdir -p $log_dir/$case_path
        fi
        cmd="${runcase_script} ${script} -w ${workdirs[index]} -c \"${case_cmd}\" -t ${thread_no} -d ${exec_dir} ${timeout_param}"
        # echo "$thread_no $count $cmd"
        local ret=0
        local redo_count=1
        start_time=`date +%s`
        while [ ${redo_count} -lt 6 ]; do
            if [ -f $log_dir/$case_file.log ]; then
                cp $log_dir/$case_file.log $log_dir/$case_file.${redo_count}.redolog
            fi
            echo "${hosts[index]}-${thread_no} order:${count}, redo:${redo_count} task:${line}" >$log_dir/$case_file.log
            echo -e "\e[33m >>>>> \e[0m ${case_cmd}"
            date >>$log_dir/$case_file.log
            # $cmd 2>&1 | tee -a $log_dir/$case_file.log
            # ret=${PIPESTATUS[0]}
            $cmd >>$log_dir/$case_file.log 2>&1
            ret=$?
            echo "${hosts[index]} `date` ret:${ret}" >>$log_dir/$case_file.log
            if [ $ret -eq 0 ]; then
                break
            fi
            redo=0
            grep -q "wait too long for taosd start" $log_dir/$case_file.log
            if [ $? -eq 0 ]; then
                redo=1
            fi
            grep -q "kex_exchange_identification: Connection closed by remote host" $log_dir/$case_file.log
            if [ $? -eq 0 ]; then
                redo=1
            fi
            grep -q "ssh_exchange_identification: Connection closed by remote host" $log_dir/$case_file.log
            if [ $? -eq 0 ]; then
                redo=1
            fi
            grep -q "kex_exchange_identification: read: Connection reset by peer" $log_dir/$case_file.log
            if [ $? -eq 0 ]; then
                redo=1
            fi
            grep -q "Database not ready" $log_dir/$case_file.log
            if [ $? -eq 0 ]; then
                redo=1
            fi
            grep -q "Unable to establish connection" $log_dir/$case_file.log
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
        echo >>$log_dir/$case_file.log
        echo "${hosts[index]} execute time: $(( end_time - start_time ))s" >>$log_dir/$case_file.log
        # echo "$thread_no ${line} DONE"
        if [ $ret -ne 0 ]; then
            flock -x $lock_file -c "echo \"${hosts[index]} ret:${ret} ${line}\" >>$log_dir/failed.log"
            mkdir -p $log_dir/${case_file}.coredump
            local remote_coredump_dir="${workdirs[index]}/tmp/thread_volume/$thread_no/coredump"
            local scpcmd="sshpass -p ${passwords[index]} scp -o StrictHostKeyChecking=no -r ${usernames[index]}@${hosts[index]}"
            if [ -z ${passwords[index]} ]; then
                scpcmd="scp -o StrictHostKeyChecking=no -r ${usernames[index]}@${hosts[index]}"
            fi
            cmd="$scpcmd:${remote_coredump_dir}/* $log_dir/${case_file}.coredump/"
            $cmd # 2>/dev/null
            local case_info=`echo "$line"|cut -d, -f 3,4`
            local corefile=`ls $log_dir/${case_file}.coredump/`
            corefile=`find $log_dir/${case_file}.coredump/ -name "core.*"`
            echo -e "$case_info \e[31m failed\e[0m"
            echo "=========================log============================"
            cat $log_dir/$case_file.log
            echo "====================================================="
            echo -e "\e[34m log file: $log_dir/$case_file.log \e[0m"
            if [ ! -z "$corefile" ]; then
                echo -e "\e[34m corefiles: $corefile \e[0m"
                local build_dir=$log_dir/build_${hosts[index]}
                local remote_build_dir="${workdirs[index]}/TDinternal/debug/build"
                mkdir $build_dir 2>/dev/null
                if [ $? -eq 0 ]; then
                    # scp build binary
                    cmd="$scpcmd:${remote_build_dir}/* ${build_dir}/"
                    echo "$cmd"
                    $cmd >/dev/null
                fi
            fi
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
    i=$(( i + 1 ))
done
wait

mkdir -p $log_dir
rm -rf $log_dir/*
task_file=$log_dir/$$.task
lock_file=$log_dir/$$.lock

i=0
while [ $i -lt ${#hosts[*]} ]; do
    # build_src $i &
    rename_taosdemo $i &
    i=$(( i + 1 ))
done
wait
# if [ -f "$log_dir/failed.log" ]; then
#     cat $log_dir/failed.log
#     exit 1
# fi

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

# docker ps -a|grep -v CONTAINER|awk '{print $1}'|xargs docker rm -f
RET=0
i=1
if [ -f "$log_dir/failed.log" ]; then
    echo "====================================================="
    while read line; do
        line=`echo "$line"|cut -d, -f 3,4`
        echo -e "$i. $line \e[31m failed\e[0m" >&2
        i=$(( i + 1 ))
    done <$log_dir/failed.log
    RET=1
fi

echo "${log_dir}" >&2

date

exit $RET
