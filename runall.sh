#!/bin/bash

function usage() {
    echo "$0"
    echo -e "\t -l log dir"
    echo -e "\t -h help"
}

while getopts "l:h" opt; do
    case $opt in
        l)
            log_dir=$OPTARG
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
export TEST_ROOT=`pwd`

function run() {
    local i=0
    while read line; do
        i=$(( i + 1 ))
        echo "$line" | grep -q "^#"
        if [ $? -ne 0 ]; then
            date
            echo -e "\e[33m $i >>>>> \e[0m $line"
            $line
            if [ $? -ne 0 ]; then
                ret=1
                echo -e "$line \e[31m FAILED\e[0m"
                echo -e "$line \e[31m FAILED\e[0m" >>$failed_case_file
            else
                echo -e "$line \e[32m SUCCESS\e[0m"
            fi
        fi
    done <cases.txt
}

run 2>&1 | tee -a $log_file

echo "====================================================================="
if [ $ret -ne 0 ]; then
    echo -e "\e[31m TEST FAILED\e[0m"
    cat $failed_case_file
else
    echo -e "\e[32m TEST SUCCESS\e[0m"
fi
exit $ret

