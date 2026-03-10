#!/bin/bash

if [ -d "/var/lib/taos/taosanode/model/timemoe" ]; then
    echo "Directory /var/lib/taos/taosanode/model/timemoe exists. "
    echo "Starting timemoe service"
    cd /usr/local/taos/taosanode/lib/taosanalytics/tsfmservice
    source /usr/local/taos/taosanode/venv/bin/activate
    nohup python3 timemoe-server.py /var/lib/taos/taosanode/model/timemoe Maple728/TimeMoE-200M False &
    echo "check the pid of the timemoe-server.py to confirm it is running"
    pid=$(pgrep -f "timemoe-server.py")
    echo "PID of the timemoe-server.py is $pid"
else
    echo "Directory /var/lib/taos/taosanode/model/timemoe does not exist."
    exit 1
fi
