#!/bin/bash

if [ -d "/var/lib/taos/taosanode/model/tdtsfm" ]; then
    echo "Directory /var/lib/taos/taosanode/model/tdtsfm exists."
    echo "Starting tdtsfm service"
    cd /usr/local/taos/taosanode/lib/taosanalytics/tsfmservice
    source /usr/local/taos/taosanode/venv/bin/activate
    nohup python3 tdtsfm-server.py --action server &
    echo "check the pid of the tdtsfm-server.py to confirm it is running"
    pid=$(pgrep -f "tdtsfm-server.py")
    echo "PID of the tdtsfm-server.py is $pid"
else
    echo "Directory /var/lib/taos/taosanode/model/tdtsfm does not exist."
    exit 1
fi
