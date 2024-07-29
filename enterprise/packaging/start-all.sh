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

for service in "${services[@]}"; do
    if [ "$osType" == "Linux" ]; then
        ${csudo}systemctl start $service	
        if systemctl is-active $service >/dev/null; then
            echo "$service has been started successfully"
        else
            echo "failed to start $service"
        fi
    elif [ "$osType" == "Darwin" ]; then
        ${csudo}launchctl start com.tdengine.${service}
        sleep 1	    
        if launchctl print system/com.tdengine.${service} | grep 'state = running' > /dev/null; then
            echo "$service has been started successfully"
        else
            echo "failed to start $service"
        fi
    fi
done
