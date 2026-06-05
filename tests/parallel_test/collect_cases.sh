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
grep "^python" ../develop-test/fulltest.sh |sed "s/^/,,develop-test,/" >>$case_file
find ../docs-examples-test/ -name "*.sh" -printf '%f\n' | xargs -I {} echo ",,docs-examples-test,bash {}" >> $case_file
# tar source code for run.sh to use
# if [ $ent -eq 0 ]; then
#     cd ../../../
#     rm -rf TDengine.tar.gz
#     tar --exclude=TDengine/debug --exclude=TDengine/sim --exclude=TDengine/release -czf TDengine.tar.gz TDengine taos-connector-python
# else
#     cd ../../../../
#     rm -rf TDinternal.tar.gz
#     tar --exclude=TDinternal/debug --exclude=TDinternal/sim --exclude=TDinternal/community/debug --exclude=TDinternal/community/release --exclude=TDinternal/community/sim -czf TDinternal.tar.gz TDinternal taos-connector-python
# fi

exit 0

