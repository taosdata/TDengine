#!/bin/sh

UNAME_BIN=`which uname`
OS_TYPE=`$UNAME_BIN`

PID=`ps -ef|grep /usr/bin/taosd | grep -v grep | awk '{print $2}'`
if [ -n "$PID" ]; then
  echo systemctl stop taosd
  systemctl stop taosd
fi

PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
while [ -n "$PID" ]; do
  echo kill -9 $PID
  pkill -9 taosd
  echo "Killing processes locking on port 6030"
  if [ "$OS_TYPE" != "Darwin" ]; then
    fuser -k -n tcp 6030
  else
    lsof -nti:6030 | xargs kill -9
  fi
  PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
done

PID=`ps -ef|grep -w tarbitrator | grep -v grep | awk '{print $2}'`
while [ -n "$PID" ]; do
  echo kill -9 $PID
  pkill -9 tarbitrator
  if [ "$OS_TYPE" != "Darwin" ]; then
    fuser -k -n tcp 6040
  else
    lsof -nti:6040 | xargs kill -9
  fi
  PID=`ps -ef|grep -w tarbitrator | grep -v grep | awk '{print $2}'`
done

