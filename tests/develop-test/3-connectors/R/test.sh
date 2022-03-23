#!/bin/sh

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
echo ${WKC}

JDBC_PATH='/src/connector/jdbc/'
CASE_PATH='/examples/R/'
cd ${JDBC_PATH}

mvn clean package -Dmaven.test.skip=true

JDBC=`ls target|grep dist.jar`
JDBC_PATH=${JDBC_PATH}target

#echo ${jdbc}
#echo ${jdbc_path}
cd ${WKC}
# remove 
Rscript ${CASE_PATH}rjdbc.sample.R ${JDBC_PATH} ${JDBC} 
