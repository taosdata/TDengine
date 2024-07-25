#!/bin/bash

script_dir=$(dirname $0)
cd $script_dir || exit
script_name=$(basename $0)
ps -ef|grep -v grep|grep -v $$|grep -q "$script_name"
if [ $? -eq 0 ]; then
    exit 0
fi
while [ 1 ]; do
    ps -ef|grep python|grep -q 8081
    if [ $? -ne 0 ]; then
        python3 -m http.server 8081
    fi
    sleep 60
done