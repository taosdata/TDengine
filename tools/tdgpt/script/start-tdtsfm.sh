#!/bin/bash

if [ -d "/var/lib/taos/taosanode/model/tdtsfm" ]; then
  echo "Directory /var/lib/taos/taosanode/model/tdtsfm exists."
else
  echo "Directory /var/lib/taos/taosanode/model/tdtsfm does not exist."
  exit 1
fi

cd /var/lib/taos/taosanode/model/tdtsfm
nohup python3 taos_ts_server.py  --action server &

