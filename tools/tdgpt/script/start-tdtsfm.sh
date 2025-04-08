#!/bin/bash

if [ -d "/var/lib/taos/taosanode/model/tdtsfm" ]; then
    echo "Directory /var/lib/taos/taosanode/model/tdtsfm exists."
    echo "Starting tdtsfm service"
    cd /var/lib/taos/taosanode/model/tdtsfm
    source /usr/local/taos/taosanode/venv/bin/activate
    nohup python3 taos_ts_server.py  --action server &
    echo "check the pid of the taos_ts_server.py to confirm it is running"
    pid=$(pgrep -f "taos_ts_server.py")
    echo "PID of the taos_ts_server.py is $pid"
else
    echo "Directory /var/lib/taos/taosanode/model/tdtsfm does not exist."
    exit 1
fi


