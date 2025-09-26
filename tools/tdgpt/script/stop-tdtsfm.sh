#!/bin/bash

# Find the process ID (PID) of the running tdtsfm-server.py script
script_file="tdtsfm-server.py"
pid=$(pgrep -f ${script_file})

# If the process is found, kill it
if [ -n "$pid" ]; then
  echo "Stopping ${script_file} with PID $pid"
  kill -9 $pid
  echo "${script_file} stopped"
else
  echo "${script_file} is not running"
fi