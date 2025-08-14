#!/bin/bash

# Find the process ID (PID) of the running time-moe_server.py script
pid=$(pgrep -f "time-moe_server.py")

# If the process is found, kill it
if [ -n "$pid" ]; then
  echo "Stopping time-moe_server.py with PID $pid"
  kill -9 $pid
  echo "time-moe_server.py stopped"
else
  echo "time-moe_server.py is not running"
fi