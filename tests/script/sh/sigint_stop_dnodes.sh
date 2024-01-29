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
  kill -9 "$PID"
  PID=$(pgrep taosd)
  echo "taosd processes" "$PID"
done
