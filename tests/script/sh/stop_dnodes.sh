#!/bin/sh

set +e
#set -x

unset LD_PRELOAD
UNAME_BIN=$(which uname)
OS_TYPE=$($UNAME_BIN)

PID=$(pgrep taosd  | grep -v defunct )
if [ -n "$PID" ]; then
  echo systemctl stop taosd
  systemctl stop taosd
fi

PID=$(pgrep taosd | grep -v defunct )
while [ -n "$PID" ]; do
  echo kill -9 "$PID"
  #pkill -9 taosd
  # shellcheck disable=SC2086
  kill -9 $PID
  echo "Killing taosd processes"
  if [ "$OS_TYPE" != "Darwin" ]; then
    fuser -k -n tcp 6030
  else
    lsof -nti:6030 | xargs kill -9
  fi
  PID=$(pgrep taosd | grep -v defunct )
done

PID=$(pgrep taos | grep -v defunct )
while [ -n "$PID" ]; do
  echo kill -9 "$PID"
  #pkill -9 taos
  # shellcheck disable=SC2086
  kill -9 $PID
  echo "Killing taos processes"
  if [ "$OS_TYPE" != "Darwin" ]; then
    fuser -k -n tcp 6030
  else
    lsof -nti:6030 | xargs kill -9
  fi
  PID=$(pgrep taos | grep -v defunct )
done

PID=$(pgrep tmq_sim | grep -v defunct )
while [ -n "$PID" ]; do
  echo kill -9 "$PID"
  # shellcheck disable=SC2086
  kill -9 $PID
  echo "Killing tmq_sim processes"
  if [ "$OS_TYPE" != "Darwin" ]; then
    fuser -k -n tcp 6030
  else
    lsof -nti:6030 | xargs kill -9
  fi
  PID=$(pgrep tmq_sim | grep -v defunct )
done
