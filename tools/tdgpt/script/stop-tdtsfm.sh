#!/bin/bash

# Find the process ID (PID) of the running taos_ts_server.py script
pid=$(pgrep -f "taos_ts_server.py")

# If the process is found, kill it
if [ -n "$pid" ]; then
  echo "Stopping taos_ts_server.py with PID $pid"
  kill -9 $pid
  echo "taos_ts_server.py stopped"
else
  echo "taos_ts_server.py is not running"
fi