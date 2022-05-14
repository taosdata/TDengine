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

curDir=$(dirname $(readlink -f "$0"))
taosdConfig=$curDir/../../../../packaging/cfg
adapterConfig=$curDir/../../../../src/plugins/taosadapter/example/config/taosadapter.toml

nohup taosd -c ${taosdConfig} > /dev/null 2>&1 &
nohup taosadapter -c ${adapterConfig} > /dev/null 2>&1 &
sleep 10

cd ../../../../
WKC=`pwd`
cd ${WKC}/src/connector/jdbc

mvn clean test > jdbc-out.log 2>&1
tail -n 20 jdbc-out.log

cases=`grep 'Tests run' jdbc-out.log | awk 'END{print $3}'`
totalJDBCCases=`echo ${cases/%,}`
failed=`grep 'Tests run' jdbc-out.log | awk 'END{print $5}'`
JDBCFailed=`echo ${failed/%,}`
error=`grep 'Tests run' jdbc-out.log | awk 'END{print $7}'`
JDBCError=`echo ${error/%,}`

totalJDBCFailed=`expr $JDBCFailed + $JDBCError`
totalJDBCSuccess=`expr $totalJDBCCases - $totalJDBCFailed`

if [ "$totalJDBCSuccess" -gt "0" ]; then
  echo -e "\n${GREEN} ### Total $totalJDBCSuccess JDBC case(s) succeed! ### ${NC}"
fi

if [ "$totalJDBCFailed" -ne "0" ]; then
  echo -e "\n${RED} ### Total $totalJDBCFailed JDBC case(s) failed! ### ${NC}"
  exit 8
fi

