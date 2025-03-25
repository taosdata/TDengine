#!/bin/bash

# Find the process ID (PID) of the running timer-moe_server.py script
pid=$(pgrep -f "timer-moe_server.py")

# If the process is found, kill it
if [ -n "$pid" ]; then
  echo "Stopping timer-moe_server.py with PID $pid"
  kill -9 $pid
  echo "timer-moe_server.py stopped"
else
  echo "timer-moe_server.py is not running"
fi