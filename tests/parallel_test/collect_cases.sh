#!/bin/bash

case_file=/tmp/cases.task

function usage() {
    echo "$0"
    echo -e "\t -o output case file"
    echo -e "\t -e enterprise edition"
    echo -e "\t -h help"
}

ent=0
while getopts "o:eh" opt; do
    case $opt in
        o)
            case_file=$OPTARG
            ;;
        e)
            ent=1
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

script_dir=`dirname $0`
cd $script_dir

if [ $ent -eq 0 ]; then
    echo ",,unit-test,bash test.sh" >$case_file
else
    echo ",,unit-test,bash test.sh -e" >$case_file
fi
cat ../script/jenkins/basic.txt |grep -v "^#"|grep -v "^$"|sed "s/^/,,script,/" >>$case_file
grep "^python" ../system-test/fulltest.sh |sed "s/^/,,system-test,/" >>$case_file

exit 0

