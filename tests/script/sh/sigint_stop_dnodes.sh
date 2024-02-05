#!/bin/sh
# shellcheck disable=SC2009
# shellcheck disable=SC2086

set +e
#set -x

unset LD_PRELOAD
# UNAME_BIN=`which uname`
# OS_TYPE=$($UNAME_BIN)


# shellcheck disable=SC2009
PID=$(ps -ef|grep -w taosd | grep -v grep | grep -v defunct |  awk '{print $2}')

echo "Killing taosd processes " "$PID"
while [ -n "$PID" ]; do
  #echo "Killing taosd processes" $PID
  # shellcheck disable=SC2086
  kill -9 $PID
  PID=$(ps -ef|grep -w taosd | grep -v grep | grep -v defunct |  awk '{print $2}')
  echo "taosd processes" "$PID"
done
