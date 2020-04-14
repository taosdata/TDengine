#!/bin/sh

PID=`ps -ef|grep /usr/bin/taosd | grep -v grep | awk '{print $2}'`
if [ -n "$PID" ]; then 
	echo sudo systemctl stop taosd 
	sudo systemctl stop taosd
fi
  
PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
while [ -n "$PID" ]; do
	echo sudo kill -9 $PID 
	sudo pkill -9 taosd
  sudo fuser -k -n tcp 6030
  PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
done
