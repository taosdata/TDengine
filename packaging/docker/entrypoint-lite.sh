#!/bin/bash

set -e

export PATH="/var/lib/taos/taosanode/venv/bin:$PATH"
export LANG=en_US.UTF-8

CONFIG_FILE="/usr/local/taos/taosanode/cfg/taosanode.config.py"

if [ ! -f "${CONFIG_FILE}" ]; then
  echo "Error: Configuration file ${CONFIG_FILE} not found!"
  exit 1
fi

cd /usr/local/taos/taosanode/lib/taosanalytics/
exec gunicorn -c "${CONFIG_FILE}"
