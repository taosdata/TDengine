#!/bin/sh

set +e
#set -x

unset LD_PRELOAD
# UNAME_BIN=`which uname`
# OS_TYPE=$($UNAME_BIN)

PID=$(pgrep taosd)
echo "Killing taosd processes " "$PID"
while [ -n "$PID" ]; do
  #echo "Killing taosd processes" $PID
  for i in $PID; do
    kill -9 "$i"
  done  
  PID=$(pgrep taosd)
  echo "taosd processes" "$PID"
done
