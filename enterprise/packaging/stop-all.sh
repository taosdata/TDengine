#!/bin/bash

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
        if [ "$EUID" -eq 0 ]; then
            sysctl_cmd_arr=(systemctl)
        else
            sysctl_cmd_arr=(systemctl --user)
        fi
        "${sysctl_cmd_arr[@]}" stop "$service"	
        if "${sysctl_cmd_arr[@]}" is-active "$service" >/dev/null; then
            echo "failed to stop $service"
        else        
            echo "$service has been stopped"
        fi
    elif [ "$osType" == "Darwin" ]; then
        if [ "$EUID" -eq 0 ]; then
            domain="system"
        else
            domain="gui/$(id -u)"
        fi
        launchctl stop "com.tdengine.${service}"
        sleep 1
        if launchctl print "${domain}/com.tdengine.${service}" 2>/dev/null | grep 'state = running' > /dev/null; then
            echo "failed to stop $service"
        else
            echo "$service has been stopped"
        fi
    fi
done
