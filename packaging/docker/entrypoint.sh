#!/bin/bash

export PATH="/usr/local/taos/taosanode/venv/bin:$PATH"
export LANG=en_US.UTF-8
export LC_CTYPE=en_US.UTF-8
export LC_ALL=en_US.UTF-8

CONFIG_FILE="/usr/local/taos/taosanode/cfg/taosanode.ini"
TS_SERVER_FILE="/root/taos_ts_server.py"
TIME_MOE_FILE="/root/time-moe/time-moe_server.py"

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
if [ -f $TIME_MOE_FILE ];then
  echo "Starting time-moe server..."
  cd $(dirname "$TIME_MOE_FILE")
  python3 $TIME_MOE_FILE --action server &
  TIME_MOE_PID=$!

  if ! ps -p $TIME_MOE_PID > /dev/null; then
    echo "Error: time-moe server failed to start!"
    exit 1
  fi
fi

echo "Starting uWSGI with config: $CONFIG_FILE"
exec /usr/local/taos/taosanode/venv/bin/uwsgi --ini "$CONFIG_FILE"

if [ $? -ne 0 ]; then
  echo "uWSGI failed to start. Exiting..."
  exit 1
fi
