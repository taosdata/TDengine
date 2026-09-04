#!/bin/bash

set -e

export PATH="/var/lib/taos/taosanode/venv/bin:$PATH"
export LANG=en_US.UTF-8

CONFIG_FILE="/usr/local/taos/taosanode/cfg/taosanode.config.py"
SERVICE_MANAGER="/usr/local/taos/taosanode/bin/taosanode_service.py"

if [ ! -f "${CONFIG_FILE}" ]; then
  echo "Error: Configuration file ${CONFIG_FILE} not found!"
  exit 1
fi

# Dynamic TSFM resources are optional. The service manager validates mounted
# manifests and starts only complete model/runtime pairs.
if [ -f "${SERVICE_MANAGER}" ]; then
  /var/lib/taos/taosanode/venv/bin/python "${SERVICE_MANAGER}" \
    model-start -c "${CONFIG_FILE}" all || \
    echo "Warning: one or more dynamic TSFM resources could not be started"
fi

cd /usr/local/taos/taosanode/lib/taosanalytics/
exec gunicorn -c "${CONFIG_FILE}"
