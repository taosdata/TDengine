#!/bin/bash

export PATH="/usr/local/taos/taosanode/venv/bin:$PATH"
export LANG=en_US.UTF-8
export LC_CTYPE=en_US.UTF-8
export LC_ALL=en_US.UTF-8

CONFIG_FILE="/usr/local/taos/taosanode/cfg/taosanode.ini"
TS_SERVER_FILE="/root/taos_ts_server.py"
TIMER_POE_FILE="/root/timer-moe/timer-moe_server.py"

if [ ! -f "$CONFIG_FILE" ]; then
  echo "Error: Configuration file $CONFIG_FILE not found!"
  exit 1
fi


if [ -f $TS_SERVER_FILE ];then
  echo "Starting tdtsfm server..."
  python3 $TS_SERVER_FILE --action server &
  TAOS_TS_PID=$!

  if ! ps -p $TAOS_TS_PID > /dev/null; then
    echo "Error: tdtsfm server failed to start!"
    exit 1
  fi
fi
if [ -f $TIMER_POE_FILE ];then
  echo "Starting timer-moe server..."
  cd $(dirname "$TIMER_POE_FILE")
  python3 $TIMER_POE_FILE --action server &
  TIMER_MOE_PID=$!

  if ! ps -p $TIMER_MOE_PID > /dev/null; then
    echo "Error: timer-moe server failed to start!"
    exit 1
  fi
fi

echo "Starting uWSGI with config: $CONFIG_FILE"
exec /usr/local/taos/taosanode/venv/bin/uwsgi --ini "$CONFIG_FILE"

if [ $? -ne 0 ]; then
  echo "uWSGI failed to start. Exiting..."
  exit 1
fi
