#!/bin/bash

function usage() {
    echo "$0"
    echo -e "\t -l log dir"
    echo -e "\t -s server pkg"
    echo -e "\t -c client pkg"
    echo -e "\t -f case file"
    echo -e "\t -h help"
}

while getopts "l:s:c:f:h" opt; do
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
        i=$(( i + 1 ))
        echo "$line" | grep -q "^#"
        if [ $? -ne 0 ]; then
            date
            echo -e "\e[33m $i >>>>> \e[0m $line"
            echo "$line" | grep -q "\-\-setup"
            if [ $? -eq 0 ]; then
                if [ ! -z "$server_pkg" ]; then
                    line="$line --server-pkg=$server_pkg"
                fi
                if [ ! -z "$client_pkg" ]; then
                    line="$line --client-pkg=$client_pkg"
                fi
            fi
            $line
            if [ $? -ne 0 ]; then
                ret=1
                echo -e "$line \e[31m FAILED\e[0m"
                echo -e "$line \e[31m FAILED\e[0m" >>$failed_case_file
            else
                echo -e "$line \e[32m SUCCESS\e[0m"
            fi
        fi
    done <${case_file}
}

run 2>&1 | tee -a $log_file

echo "====================================================================="
echo "log file: $log_file"
if [ $ret -ne 0 ]; then
    echo -e "\e[31m TEST FAILED\e[0m"
    cat $failed_case_file
else
    echo -e "\e[32m TEST SUCCESS\e[0m"
fi
exit $ret

