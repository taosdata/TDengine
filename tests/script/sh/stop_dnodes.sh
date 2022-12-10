#!/bin/sh

set +e
#set -x

unset LD_PRELOAD
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
  #pkill -9 taosd
  kill -9 $PID
  echo "Killing taosd processes"
  if [ "$OS_TYPE" != "Darwin" ]; then
    fuser -k -n tcp 6030
  else
    lsof -nti:6030 | xargs kill -9
  fi
  PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
done

PID=`ps -ef|grep -w taos | grep -v grep | awk '{print $2}'`
while [ -n "$PID" ]; do
  echo kill -9 $PID
  #pkill -9 taos
  kill -9 $PID
  echo "Killing taos processes"
  if [ "$OS_TYPE" != "Darwin" ]; then
    fuser -k -n tcp 6030
  else
    lsof -nti:6030 | xargs kill -9
  fi
  PID=`ps -ef|grep -w taos | grep -v grep | awk '{print $2}'`
done

PID=`ps -ef|grep -w tmq_sim | grep -v grep | awk '{print $2}'`
while [ -n "$PID" ]; do
  echo kill -9 $PID
  #pkill -9 tmq_sim
  kill -9 $PID
  echo "Killing tmq_sim processes"
  if [ "$OS_TYPE" != "Darwin" ]; then
    fuser -k -n tcp 6030
  else
    lsof -nti:6030 | xargs kill -9
  fi
  PID=`ps -ef|grep -w tmq_sim | grep -v grep | awk '{print $2}'`
done
