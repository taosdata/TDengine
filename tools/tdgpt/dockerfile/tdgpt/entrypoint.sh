#!/bin/bash

export PATH="/usr/local/taos/taosanode/venv/bin:$PATH"
export LANG=en_US.UTF-8
export LC_CTYPE=en_US.UTF-8
export LC_ALL=en_US.UTF-8

CONFIG_FILE="/usr/local/taos/taosanode/cfg/taosanode.ini"
if [ ! -f "$CONFIG_FILE" ]; then
  echo "Error: Configuration file $CONFIG_FILE not found!"
  exit 1
fi

echo "Starting uWSGI with config: $CONFIG_FILE"
exec /usr/local/taos/taosanode/venv/bin/uwsgi --ini "$CONFIG_FILE"

if [ $? -ne 0 ]; then
  echo "uWSGI failed to start. Exiting..."
  exit 1
fi
