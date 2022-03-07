#!/bin/bash
function stopTaosd {
  echo "Stop taosd"
  sudo systemctl stop taosd || echo 'no sudo or systemctl or stop fail'
  PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
  while [ -n "$PID" ]
  do
    pkill -TERM -x taosd
    sleep 1
    PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
  done
}
stopTaosd
rm -rf /var/lib/taos/*
rm -rf /var/log/taos/*
nohup taosd -c /etc/taos/ > /dev/null 2>&1 &
sleep 10
cd ../../
WKC=`pwd`
cd ${WKC}/src/connector/nodejs
npm install
npm run test
cd ${WKC}/tests/examples/nodejs
npm install td2.0-connector > /dev/null 2>&1
node nodejsChecker.js host=localhost
node test1970.js
cd ${WKC}/tests/connectorTest/nodejsTest/nanosupport
npm install td2.0-connector > /dev/null 2>&1
node nanosecondTest.js
