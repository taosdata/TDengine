#!/bin/sh
# shellcheck disable=SC2009
# shellcheck disable=SC2086
set +e
#set -x

unset LD_PRELOAD
UNAME_BIN=$(which uname)
OS_TYPE=$($UNAME_BIN)

kill_process() {
    PID=$(ps -ef|grep -w "$1" | grep -v grep | grep -v defunct |  awk '{print $2}')
    while [ -n "$PID" ]; do
      # shellcheck disable=SC2086
      echo "killing $1 processes, pid: $PID"
      kill -9 $PID
      if [ "$1" = "taosd" ]; then
        if [ "$OS_TYPE" != "Darwin" ]; then
          fuser -k -n tcp 6030
        else
          lsof -nti:6030 | xargs kill -9
        fi
      fi
      PID=$(ps -ef|grep "$1" | grep -v grep | grep -v defunct |  awk '{print $2}')
    done
}

PID1=$(ps -ef|grep /usr/bin/taosd | grep -v grep | grep -v defunct |  awk '{print $2}')
if [ -n "$PID1" ]; then
  echo systemctl stop taosd
  systemctl stop taosd
fi

kill_process taosd
kill_process taos
kill_process taosadapter
kill_process tmq_sim
