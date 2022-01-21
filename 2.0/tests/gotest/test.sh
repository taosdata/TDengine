#!/bin/bash

##################################################
# 
# Do go test 
#
##################################################

set +e
#set -x

FILE_NAME=
RELEASE=0
while getopts "f:" arg
do
  case $arg in
    f)
      FILE_NAME=$OPTARG
      echo "input file: $FILE_NAME"
      ;;
    ?)
      echo "unknow argument"
      ;;
  esac
done

# start one taosd
bash ../script/sh/stop_dnodes.sh
bash ../script/sh/deploy.sh -n dnode1 -i 1
bash ../script/sh/cfg.sh -n dnode1 -c walLevel -v 0
bash ../script/sh/exec.sh -n dnode1 -s start

# start build test go file
caseDir=`echo ${FILE_NAME%/*}`
echo "caseDir: $caseDir"
cd $caseDir
rm go.*
go mod init $caseDir
go build 
sleep 1s
./$caseDir

