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

nohup taosd > /dev/null 2>&1 &
nohup taosadapter > /dev/null 2>&1 &
sleep 10

# echo `pwd`
cd ../../
WKC=`pwd`
echo ${WKC}
cd ${WKC}/src/connector/TypeScript-REST

# test source code 
npm install
npm run example 
# npm run test 

# test published npm package td2.0-rest-connecto
cd ${WKC}/tests/examples/TypeScript-REST
npm install 
npm run test


