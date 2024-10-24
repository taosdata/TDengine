#!/bin/bash

csudo=""
if command -v sudo >/dev/null; then
  csudo="sudo "
fi

prefix="taos"
versionType="enterprise"

if [ "${versionType}" == "enterprise" ]; then
    services=(${prefix}"d" ${prefix}"adapter" ${prefix}"x" ${prefix}"-explorer" ${prefix}"keeper")
else
    services=(${prefix}"d" ${prefix}"adapter" ${prefix}"-explorer" ${prefix}"keeper")
fi

osType=$(uname)

MAX_RETRY=3


function start_service() {
    if [ "$osType" == "Linux" ]; then
        ${csudo}systemctl start $1
        while [ $MAX_RETRY -gt 0 ]; do
            sleep 0.5
            if systemctl is-active $1 >/dev/null; then
                echo "$1 has been started successfully"
                break            
            fi
            MAX_RETRY=$((MAX_RETRY - 1))
        done
        if [ $MAX_RETRY -eq 0 ]; then
            echo "failed to start $1"
        fi
    elif [ "$osType" == "Darwin" ]; then
        ${csudo}launchctl start com.tdengine.$1
        while [ $MAX_RETRY -gt 0 ]; do
            sleep 0.5
            if launchctl print system/com.tdengine.$1 | grep 'state = running' > /dev/null; then
                echo "$1 has been started successfully"
                break
            fi
            MAX_RETRY=$((MAX_RETRY - 1))
        done
        if [ $MAX_RETRY -eq 0 ]; then
            echo "failed to start $1"
        fi
    fi
}

for service in "${services[@]}"; do
    start_service $service
done
