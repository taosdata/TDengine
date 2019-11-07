#!/bin/sh

PID=`ps -ef|grep /usr/bin/taosd | grep -v grep | awk '{print $2}'`
if [ -n "$PID" ]; then 
	echo sudo systemctl stop taosd 
	sudo systemctl stop taosd
fi

for i in {1..10}  
do  
	PID=`ps -ef|grep taosd | grep -v grep | awk '{print $2}'`
	if [ -n "$PID" ]; then 
		echo sudo kill -9 $PID 
		sudo kill -9 $PID
	fi 
done
