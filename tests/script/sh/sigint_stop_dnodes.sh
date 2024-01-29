#!/bin/sh

set +e
#set -x

unset LD_PRELOAD
# UNAME_BIN=`which uname`
# OS_TYPE=$($UNAME_BIN)

PID=$(pgrep taosd | grep -v defunct )
echo "Killing taosd processes " "$PID"
while [ -n "$PID" ]; do
  #echo "Killing taosd processes" $PID
  # shellcheck disable=SC2086
  kill -9 $PID
  PID=$(pgrep taosd | grep -v defunct )
  echo "taosd processes" "$PID"
done
