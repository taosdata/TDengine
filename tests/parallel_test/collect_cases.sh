#!/bin/bash

case_file=/tmp/cases.task

if [ ! -z $1 ]; then
    case_file="$1"
fi

script_dir=`dirname $0`
cd $script_dir

echo ",,unit-test,bash test.sh" >$case_file
cat ../script/jenkins/basic.txt |grep -v "^#"|grep -v "^$"|sed "s/^/,,script,/" >>$case_file
grep "^python" ../system-test/fulltest.sh |sed "s/^/,,system-test,/" >>$case_file

exit 0

