#!/bin/bash

date_str=$(date -d "4 day ago" +%Y%m%d)
if [ ! -z "$1" ]; then
    date_str="$1"
fi
script_dir=$(dirname $0)
cd "${script_dir}"/log || exit
# date >>core.list
# find . -name "core.*" | grep "$date_str" >>core.list
# find . -name "core.*" | grep "$date_str" | xargs rm -rf
# find . -name "build_*" | grep "$date_str" | xargs rm -rf
for file in *; do
    if [[ $file == *"$date_str"* ]]; then
        rm -rf "$file"
    fi
done
