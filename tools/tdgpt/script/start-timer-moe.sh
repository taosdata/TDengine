#!/bin/bash

if [ -d "/var/lib/taos/taosanode/model/timer-moe" ]; then
    echo "Directory /var/lib/taos/taosanode/model/timer-moe exists. "
    echo "Starting timer-moe service"
    cd /var/lib/taos/taosanode/model/timer-moe
    source /usr/local/taos/taosanode/venv/bin/activate
    nohup python3 timer-moe_server.py --action server &  
    echo "check the pid of the timer-moe_server.py to confirm it is running"
    pid=$(pgrep -f "timer-moe_server.py")
    echo "PID of the timer-moe_server.py is $pid"
else
    echo "Directory /var/lib/taos/taosanode/model/timer-moe does not exist."
    exit 1
fi
