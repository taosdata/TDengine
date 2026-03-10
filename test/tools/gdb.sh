#!/bin/bash

TAOSD_PID=$(ps -ef | grep -wi taosd | grep -v grep | awk '{print $2}' | head -n 1)

if [ -z "$TAOSD_PID" ]; then
    echo "Error: taosd process not found."
    exit 1
fi

gdb attach "$TAOSD_PID"