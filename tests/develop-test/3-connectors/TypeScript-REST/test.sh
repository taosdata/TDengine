#!/bin/bash

function stopProcess {
  echo "Stop $1"
  sudo systemctl stop $1 || echo 'no sudo or systemctl or stop fail'
  PID=`ps -ef|grep -w $1 | grep -v grep | awk '{print $2}'`
  while [ -n "$PID" ]
  do
    pkill -TERM -x $1
    sleep 1
    PID=`ps -ef|grep -w $1 | grep -v grep | awk '{print $2}'`
  done
}

stopProcess taosadapter
stopProcess taosd
rm -rf /var/lib/taos/*
rm -rf /var/log/taos/*

# curDir=$(dirname $(readlink -f "$0"))
# taosdConfig=$curDir/../../../../packaging/cfg
# adapterConfig=$curDir/../../../../src/plugins/taosadapter/example/config/taosadapter.toml

nohup taosd -c ${taosdConfig} > /dev/null 2>&1 &
nohup taosadapter -c ${adapterConfig} > /dev/null 2>&1 &
sleep 10

# echo `pwd`
cd ../../
WKC=`pwd`
echo ${WKC}
cd ${WKC}/src/connector/TypeScript-REST

npm install
npm run example 
# npm run test 



