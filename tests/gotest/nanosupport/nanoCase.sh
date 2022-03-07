#!/bin/bash

echo "==== start run nanosupport.go "

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
go mod init nano
go mod tidy
go build
sleep 10s
./nano -h $1 -p $2
