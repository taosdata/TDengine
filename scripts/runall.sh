#!/bin/bash

function usage() {
    echo "$0"
    echo -e "\t -l log dir"
    echo -e "\t -s server pkg"
    echo -e "\t -c client pkg"
    echo -e "\t -f case file"
    echo -e "\t -e force setup environment"
    echo -e "\t -d debug log level"
    echo -e "\t -t max execution time of each case"
    echo -e "\t -h help"
}

while getopts "l:s:c:f:t:d:eh" opt; do
    case $opt in
        l)
            log_dir=$OPTARG
            ;;
        s)
            server_pkg=$OPTARG
            ;;
        c)
            client_pkg=$OPTARG
            ;;
        f)
            case_file=$OPTARG
            ;;
        d)
            debug_level=$OPTARG
            ;;
        t)
            TIMEOUT_PREFIX="timeout $OPTARG"
            ;;
        e)
            force_setup=1
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

# if log dir not specified, create one
if [ -z "$log_dir" ]; then
    log_dir=log
    mkdir -p log
fi

date_tag=`date +%Y%m%d-%H%M%S`
log_sub_dir=${log_dir}/${date_tag}
mkdir -p $log_sub_dir
log_file=$log_sub_dir/test.log
failed_case_file=$log_sub_dir/failed.log
env_file=$log_sub_dir/env.txt
touch $env_file
ret=0
cpwd=`dirname $0`
cd ${cpwd}
cpwd=`pwd`
cd ${cpwd}/..
export TEST_ROOT=`pwd`
cd ${cpwd}

if [ -z "$case_file" ]; then
    case_file=cases.txt
fi

function run() {
    local i=0
    while read line; do
        echo "$line" | grep -q "^#"
        if [ $? -eq 0 ]; then
            continue
        fi
        echo "$line" | grep -q "^$"
        if [ $? -eq 0 ]; then
            continue
        fi
        i=$(( i + 1 ))
        date
        echo -e "\e[33m $i >>>>> \e[0m $line"
        cmd="$line"
        if [ ! -z $force_setup ]; then
            cmd="${cmd/--use/--setup}"
        fi
        echo "$cmd" | grep -q "\-\-setup"
        if [ $? -eq 0 ]; then
            local setup_param=`echo "$cmd" | grep "\-\-setup.*"`
            local setup_file=""
            echo "$setup_param" | grep -q "\-\-setup="
            if [ $? -eq 0 ]; then
                setup_file=`echo "$setup_param" | cut -d= -f2 | cut -d' ' -f1`
            else
                setup_file=`echo "$setup_param" | awk '{print $2}'`
            fi
            if [ ! -z "$setup_file" ]; then
                grep -q "$setup_file" $env_file
                if [ $? -ne 0 ]; then
                    echo "$setup_file" >>$env_file
                fi
            fi
            if [ ! -z "$server_pkg" ]; then
                cmd="$cmd --server-pkg=$server_pkg"
            fi
            if [ ! -z "$client_pkg" ]; then
                cmd="$cmd --client-pkg=$client_pkg"
            fi
        fi
        if [ ! -z "$TIMEOUT_PREFIX" ]; then
            cmd="$TIMEOUT_PREFIX $cmd"
        fi
        if [ ! -z "$debug_level" ]; then
            cmd="$cmd --log-level $debug_level"
        fi
        echo "execute command: $cmd"
        $cmd
        ret=$?
        if [ $ret -ne 0 ]; then
            echo -e "$line \e[31m FAILED\e[0m  RET:$ret"
            echo -e "$line \e[31m FAILED\e[0m  RET:$ret" >>$failed_case_file
        else
            echo -e "$line \e[32m SUCCESS\e[0m"
        fi
    done <${case_file}
}

run 2>&1 | tee -a $log_file

echo "====================================================================="
echo "log file: $log_file"
if [ -f $failed_case_file ]; then
    echo -e "\e[31m TEST FAILED\e[0m"
    cat $failed_case_file
    ret=1
else
    echo -e "\e[32m TEST SUCCESS\e[0m"
    ret=0
fi
exit $ret

