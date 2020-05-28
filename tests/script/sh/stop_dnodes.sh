#!/bin/sh

PID=`ps -ef|grep /usr/bin/taosd | grep -v grep | awk '{print $2}'`
if [ -n "$PID" ]; then 
	echo systemctl stop taosd 
	systemctl stop taosd
fi
  
PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
while [ -n "$PID" ]; do
	echo kill -9 $PID 
	pkill -9 taosd
  fuser -k -n tcp 6030
  PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
done

PID=`ps -ef|grep -w tarbitrator | grep -v grep | awk '{print $2}'`
while [ -n "$PID" ]; do
	echo kill -9 $PID 
	pkill -9 tarbitrator
  fuser -k -n tcp 6040
  PID=`ps -ef|grep -w tarbitrator | grep -v grep | awk '{print $2}'`
done

