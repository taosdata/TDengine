#!/bin/bash

csudo=""
if command -v sudo >/dev/null; then
  csudo="sudo "
fi

prefix="taos"
versionType="enterprise"
mode="full"

if [ "${versionType}" == "enterprise" ] && [ "${mode}" == "full" ]; then
    services=(${prefix}"d" ${prefix}"adapter" ${prefix}"x" ${prefix}"-explorer" ${prefix}"keeper")
else
    services=(${prefix}"d" ${prefix}"adapter" ${prefix}"-explorer" ${prefix}"keeper")
fi

osType=$(uname)

for service in "${services[@]}"; do
    if [ "$osType" == "Linux" ]; then    
        ${csudo}systemctl stop $service	
        if systemctl is-active $service >/dev/null; then
            echo "failed to stop $service"
        else        
            echo "$service has been stoped"
        fi
    elif [ "$osType" == "Darwin" ]; then
        ${csudo}launchctl stop com.tdengine.${service}
        sleep 1
        if launchctl print system/com.tdengine.${service} | grep 'state = running' > /dev/null; then
            echo "failed to stop $service"
        else
            echo "$service has been stoped"
        fi
    fi
done
