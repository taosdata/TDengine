#!/bin/sh

set +e
#set -x

unset LD_PRELOAD
UNAME_BIN=$(which uname)
OS_TYPE=$($UNAME_BIN)

PID=$(pgrep taosd )
if [ -n "$PID" ]; then
  echo systemctl stop taosd
  systemctl stop taosd
fi

PID=$(pgrep taosd)
while [ -n "$PID" ]; do
  echo kill -9 "$PID"
  #pkill -9 taosd
  for i in $PID; do
    kill -9 "$i"
  done
  echo "Killing taosd processes"
  if [ "$OS_TYPE" != "Darwin" ]; then
    fuser -k -n tcp 6030
  else
    lsof -nti:6030 | xargs kill -9
  fi
  PID=$(pgrep taosd)
done

PID=$(pgrep taos)
while [ -n "$PID" ]; do
  echo kill -9 "$PID"
  #pkill -9 taos
  for i in $PID; do
    kill -9 "$i"
  done
  echo "Killing taos processes"
  if [ "$OS_TYPE" != "Darwin" ]; then
    fuser -k -n tcp 6030
  else
    lsof -nti:6030 | xargs kill -9
  fi
  PID=$(pgrep taos)
done

PID=$(pgrep tmq_sim)
while [ -n "$PID" ]; do
  echo kill -9 "$PID"
  #pkill -9 tmq_sim
  for i in $PID; do
    kill -9 "$i"
  done
  echo "Killing tmq_sim processes"
  if [ "$OS_TYPE" != "Darwin" ]; then
    fuser -k -n tcp 6030
  else
    lsof -nti:6030 | xargs kill -9
  fi
  PID=$(pgrep tmq_sim)
done
