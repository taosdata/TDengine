#!/bin/bash

if [ -d "/var/lib/taos/taosanode/model/timer-moe" ]; then
  echo "Directory /var/lib/taos/taosanode/model/timer-moe exists."
else
  echo "Directory /var/lib/taos/taosanode/model/timer-moe does not exist."
  exit 1
fi

cd /var/lib/taos/taosanode/model/timer-moe
nohup python3 timer-moe_server.py -action server &