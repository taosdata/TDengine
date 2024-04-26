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

for service in "${services[@]}"; do    
    ${csudo}systemctl stop $service
    if systemctl is-active $service >/dev/null; then
        echo "failed to stop $service"
    else        
        echo "$service has been stoped"
    fi
done