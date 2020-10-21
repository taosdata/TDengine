#!/bin/bash

echo "==== start run cases001.go"

set +e
#set -x

script_dir="$(dirname $(readlink -f $0))"
#echo "pwd: $script_dir, para0: $0"

#execName=$0
#execName=`echo ${execName##*/}`
#goName=`echo ${execName%.*}`

###### step 3: start build
cd $script_dir
rm -f go.*
go mod init demotest
go build 
sleep 1s
./demotest -h $1 -p $2
