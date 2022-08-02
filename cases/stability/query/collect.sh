#!/bin/bash
set -x

function usage() {
    echo "$0"
    echo -e "\t -l log dir"
    echo -e "\t -f file filter"
    echo -e "\t -p sql pattern filter"
    echo -e "\t -t target file"
    echo -e "\t -h help"
}

while getopts "l:f:t:p:h" opt; do
    case $opt in
        l)
            log_dir=$OPTARG
            ;;
        f)
            file_filter=$OPTARG
            ;;
        p)
            pattern_filter=$OPTARG
            ;;
        t)
            target_file=$OPTARG
            ;;
        h)
            usage
            exit 0
            ;;
        \?)
            echo "Invalid option: -$OPTARG"
            usage
            exit 1
            ;;
    esac
done

sql_file_name="test.sql"

if [ -z "$log_dir" ]; then
    echo "log dir not specified"
    usage
    exit 1
fi
if [ -z "$target_file" ]; then
    echo "target file not specified"
    usage
    exit 1
fi

if [ ! -d "$log_dir" ]; then
    echo "log dir not exist"
    usage
    exit 1
fi

cmd="find $log_dir -name \"$sql_file_name\""
if [ ! -z "$file_filter" ]; then
    cmd="$cmd | grep \"$file_filter\""
fi
if [ ! -z "$pattern_filter" ]; then
    cmd="$cmd | xargs cat | grep \"$pattern_filter\""
else
    cmd="$cmd | xargs cat "
fi

tmp_target_file=`basename $target_file`
tmp_target_file="/tmp/$tmp_target_file"
rm -rf $tmp_target_file
if [ -f "$target_file" ]; then
    cp $target_file $tmp_target_file
    wc -l $target_file
fi
echo "$cmd" | sh >>$tmp_target_file
cat $tmp_target_file|sort|uniq >$target_file

wc -l $target_file

exit 0

