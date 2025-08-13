#!/bin/bash

if [ -d "/var/lib/taos/taosanode/model/time-moe" ]; then
    echo "Directory /var/lib/taos/taosanode/model/time-moe exists. "
    echo "Starting time-moe service"
    cd /var/lib/taos/taosanode/model/time-moe
    source /usr/local/taos/taosanode/venv/bin/activate
    nohup python3 time-moe_server.py --action server &
    echo "check the pid of the time-moe_server.py to confirm it is running"
    pid=$(pgrep -f "time-moe_server.py")
    echo "PID of the time-moe_server.py is $pid"
else
    echo "Directory /var/lib/taos/taosanode/model/time-moe does not exist."
    exit 1
fi
