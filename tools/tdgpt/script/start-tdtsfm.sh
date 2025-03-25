#!/bin/bash

if [ -d "/var/lib/taos/taosanode/model/tdtsfm" ]; then
    echo "Directory /var/lib/taos/taosanode/model/tdtsfm exists."
    echo "Starting taos_ts_server.py"
    cd /var/lib/taos/taosanode/model/tdtsfm
    nohup python3 taos_ts_server.py  --action server &
    echo "check the pid of the taos_ts_server.py to confirm it is running"
else
    echo "Directory /var/lib/taos/taosanode/model/tdtsfm does not exist."
    exit 1
fi


