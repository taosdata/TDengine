#!/bin/sh

set +e
#set -x

export LD_PRELOAD=
UNAME_BIN=`which uname`
OS_TYPE=`$UNAME_BIN`

PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
while [ -n "$PID" ]; do
  #echo "Killing taosd processes " $PID
  kill $PID

  PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
done
