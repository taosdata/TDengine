#!/bin/sh

set +e
#set -x

unset LD_PRELOAD
# UNAME_BIN=`which uname`
# OS_TYPE=$($UNAME_BIN)

PID=$(pgrep |grep -w taosd  | awk '{print $2}')
echo "Killing taosd processes " "$PID"
while [ -n "$PID" ]; do
  #echo "Killing taosd processes " $PID
  kill "$PID"
  PID=$(pgrep -ef|grep -w taosd | awk '{print $2}')
  echo "taosd processes" "$PID"
done
